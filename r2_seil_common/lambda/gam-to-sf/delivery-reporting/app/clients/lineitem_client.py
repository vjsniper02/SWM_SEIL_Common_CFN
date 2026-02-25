import logging
import re
from typing import List

from attr import define
from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration
from googleads import ad_manager
from googleads.common import ZeepServiceProxy
from salesforce_bulk import SalesforceBulk

from app.clients.common import salesforce_update, salesforce_run_query
from gam_core.model.guard_exception import GuardException

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)

ID_STRING_CHECK_PATTERN = re.compile(r"[^\d,]")
DYNAMODB_TABLE_NAME = "gam_sf_sync_line_items"
GAM_LINEITEM_ID_COL = "AdServerOrderLineIdentifier"
GAM_STATUS_COL = "SWM_Status__c"


@define
class SalesforceAdOrderItemRecord:
    sf_id: str
    gam_id: str
    status: str

    def to_sf(self) -> dict:
        return {
            "Id": self.sf_id,
            GAM_LINEITEM_ID_COL: self.gam_id,
            GAM_STATUS_COL: self.status,
        }


class LineItemClient:
    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.api_version = config["gam_api_version"]
        self.client = ad_manager_client.GetService("LineItemService", self.api_version)

    @xray_recorder.capture("lineitem_client.sync_lineitems")
    def sync_lineitems(self, sf_bulk) -> dict:
        sf_lineitems = self.get_sf_lineitems(sf_bulk)
        if not sf_lineitems:
            logger.warning(
                "No lineitems found in Salesforce, not continuing with lineitem sync"
            )
            return {"results": []}
        gam_lineitems = self.get_gam_lineitems(sf_lineitems)
        if not gam_lineitems:
            logger.warning(
                "No matching lineitems found in GAM, not continuing with lineitem sync"
            )
            return {"results": []}

        updated_lineitems = []
        no_update_lineitems = []
        for lineitem in sf_lineitems:
            gam_id = lineitem[GAM_LINEITEM_ID_COL]
            if gam_id not in gam_lineitems:
                logger.warn(
                    f"Lineitem in GAM not found for AdOrderItem {lineitem['Id']} with GAM lineitem ID {gam_id}"
                )
                continue
            if (
                not lineitem[GAM_STATUS_COL]
                or gam_lineitems[gam_id].upper() != lineitem[GAM_STATUS_COL].upper()
            ):
                updated_lineitems.append(
                    SalesforceAdOrderItemRecord(
                        sf_id=lineitem["Id"],
                        gam_id=gam_id,
                        status=gam_lineitems[gam_id],
                    )
                )
                continue
            no_update_lineitems.append(lineitem["Id"])

        logger.info(
            f"The following SF line items do not need updating {len(no_update_lineitems)}: {','.join(no_update_lineitems)}"
        )
        logger.info(
            f"Found {len(updated_lineitems)} lineitems to update: {','.join([l.sf_id for l in updated_lineitems])}"
        )
        if updated_lineitems:
            self.update_salesforce(sf_bulk, [l.to_sf() for l in updated_lineitems])
        return {"results": updated_lineitems}

    @xray_recorder.capture("lineitem_client.get_gam_line_items")
    def get_gam_lineitems(self, sf_line_items) -> List[dict]:
        logger.info(f"Getting GAM lineitems")
        ids = [
            line_item["AdServerOrderLineIdentifier"]
            for line_item in sf_line_items
            if line_item["AdServerOrderLineIdentifier"] is not None
        ]
        # Remove duplicates
        ids = list(set(ids))
        idstring = ",".join(str(id_) for id_ in ids)
        logger.info(f"Query GAM with lineitem IDs: {idstring}")
        if re.search(ID_STRING_CHECK_PATTERN, idstring):
            raise GuardException(
                "GAM LINE ITEMS IDs contain invalid characters, only numbers are allowed"
            )

        statement = (
            ad_manager.StatementBuilder(version=self.api_version)
            .Where("id in (:ids)")
            .OrderBy("id", ascending=True)
            .WithBindVariable("ids", ids)
        )
        gam_statuses = {}
        while True:
            response = self.client.getLineItemsByStatement(statement.ToStatement())
            if "results" not in response or len(response["results"]) == 0:
                break
            for lineitem in response["results"]:
                gam_statuses[str(lineitem["id"])] = lineitem["status"]
            statement.offset += statement.limit
        logger.info(f"GAM lineitems found: {len(gam_statuses)}")
        return gam_statuses

    @xray_recorder.capture("lineitem_client.get_sf_lineitems")
    def get_sf_lineitems(self, sf_bulk) -> dict:
        logger.info(f"Getting SF lineitems")
        job = sf_bulk.create_queryall_job("AdOrderItem", contentType="JSON")
        batch = sf_bulk.query(
            job,
            f"SELECT Id, {GAM_LINEITEM_ID_COL}, {GAM_STATUS_COL} "
            "FROM AdOrderItem "
            "WHERE SWM_Status__c != 'Completed' "
            "AND Production_System__c = 'GAM' "
            "AND IsDeleted=FALSE",
        )
        lineitems = salesforce_run_query(sf_bulk, job, batch)

        missing_gam_ids = [l["Id"] for l in lineitems if not l[GAM_LINEITEM_ID_COL]]
        valid_lineitems = [l for l in lineitems if l[GAM_LINEITEM_ID_COL]]

        if missing_gam_ids:
            logger.warning(
                f"The following AdOrderItems in Salesforce did not have GAM IDs ({len(missing_gam_ids)}):"
                f" {','.join(missing_gam_ids)}"
            )
        logger.info(f"Valid AdOrderItems from Salesforce: {len(lineitems)}")
        return valid_lineitems

    @xray_recorder.capture("lineitem_client.update_salesforce")
    def update_salesforce(self, sf_bulk: SalesforceBulk, lineitems) -> dict:
        logger.info(f"Updating salesforce lineitems")
        results = salesforce_update(sf_bulk, "AdOrderItem", lineitems)
        errors = [e for e in results if e.success == "false"]
        if errors:
            for result in errors:
                logger.error(
                    f"Error occurred updating salesforce lineitem for {result.id}: {result.error}"
                )
            logger.error(f"{len(errors)} errors happened on lineitem updates")
        logger.info("No errors on update")
