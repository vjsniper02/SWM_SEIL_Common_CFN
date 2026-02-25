import csv
import logging
import re
import tempfile
from datetime import datetime, timedelta, date
from typing import NamedTuple, Dict, List

import requests
from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration
from googleads import ad_manager
from googleads.common import ZeepServiceProxy
from googleads.errors import AdManagerReportError
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce

from app.clients.common import salesforce_update, salesforce_run_query

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)

ID_STRING_CHECK_PATTERN = re.compile(r"[^\d,]")


class ReportKey(NamedTuple):
    line_item: str
    date: str


class ReportValue(NamedTuple):
    impressions: int
    clicks: int


class ReportClient:
    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.api_version = config["gam_api_version"]
        self.report_downloader = ad_manager_client.GetDataDownloader(
            version=self.api_version
        )

    @xray_recorder.capture("report_client.sync_actuals")
    def sync_actuals(self, sf: Salesforce, sf_bulk: SalesforceBulk):
        days = (date.today(), date.today() - timedelta(days=1))
        ids = dict(
            zip(
                days,
                (self.get_sf_usage_items(sf_bulk, i) for i in self.get_sf_query(sf)),
            )
        )
        report = self.get_report()
        updates = []
        for today in days:
            for sf_id, gam_id in ids[today].items():
                report_key = ReportKey(gam_id, today.isoformat())
                if report_key in report:
                    logger.info(
                        f"Found actuals for line item with SF ID: {sf_id}, GAM ID: {gam_id} for {today}"
                        f", impressions/clicks: {report[report_key].impressions}/{report[report_key].clicks}"
                    )
                    updates.append(
                        {
                            "Id": sf_id,
                            "Ad_Impression__c": report[report_key].impressions,
                            "Ad_Clicks__c": report[report_key].clicks,
                        }
                    )
        self.update_sf(sf_bulk, updates)

    @xray_recorder.capture("report_client.get_report")
    def get_report(self) -> Dict[ReportKey, ReportValue]:
        logger.info(f"Creating report")
        # Create report job.
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        report_job = {
            "reportQuery": {
                "dimensions": ["LINE_ITEM_ID", "DATE"],
                "columns": ["AD_SERVER_IMPRESSIONS", "AD_SERVER_CLICKS"],
                "dateRangeType": "CUSTOM_DATE",
                "startDate": {
                    "year": yesterday.year,
                    "month": yesterday.month,
                    "day": yesterday.day,
                },
                "endDate": {
                    "year": today.year,
                    "month": today.month,
                    "day": today.day,
                },
            }
        }
        try:
            # Run the report and wait for it to finish.
            report_job_id = self.report_downloader.WaitForReport(report_job)
        except AdManagerReportError as e:
            logger.error(f"Failed to generate report: {e}")
            raise e
        export_format = "CSV_DUMP"
        report_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        self.report_downloader.DownloadReportToFile(
            report_job_id, export_format, report_file, use_gzip_compression=False
        )

        report_file.close()
        report_lineitems: Dict[ReportKey, ReportValue] = {}
        with open(report_file.name, newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=",", quotechar="|")
            for row in reader:
                report_lineitems[
                    ReportKey(line_item=row[0], date=row[1])
                ] = ReportValue(impressions=row[2], clicks=row[3])
        return report_lineitems

    @xray_recorder.capture("report_client.get_sf_query")
    def get_sf_query(self, sf: Salesforce) -> dict:
        session = requests.Session()
        auth_id = "Bearer " + sf.session_id
        req_headers = {"Authorization": auth_id}
        resp = session.post(
            f"https://{sf.sf_instance}/services/apexrest/UsageSummary/fetchUsages/*",
            headers=req_headers,
            json={"usageDate": datetime.today().strftime("%Y-%m-%d")},
        )
        resp.raise_for_status()
        if isinstance(resp.json(), str):
            return [resp.json()]
        else:
            return resp.json()

    @xray_recorder.capture("report_client.get_sf_usage")
    def get_sf_usage_items(self, sf_bulk: SalesforceBulk, query: str) -> Dict[str, str]:
        logger.info(f"Getting SF data")
        items = {}
        job = sf_bulk.create_queryall_job("Usage__c", contentType="JSON")
        batch = sf_bulk.query(
            job,
            query,
        )
        results = salesforce_run_query(sf_bulk, job, batch)

        for result in results:
            items[result["Id"]] = result["GAM_LineItemID__c"]
        return items

    @xray_recorder.capture("report_client.update_sf")
    def update_sf(
        self,
        sf_bulk: SalesforceBulk,
        updates: List[Dict],
    ) -> dict:
        logger.info(f"Actuals data size to update: {len(updates)}")
        if updates:
            results = salesforce_update(sf_bulk, "Usage__c", updates)
            errors = [e for e in results if e.success == "false"]
            for result in errors:
                logger.error(
                    f"Error occurred updating usage for {result.id}: {result.error}"
                )
            logger.info(f"{len(errors)} errors happened on usage updates")
