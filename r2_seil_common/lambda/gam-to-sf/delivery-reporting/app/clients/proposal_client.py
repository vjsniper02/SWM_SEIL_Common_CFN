import json
import logging
import re
from typing import List

from attr import define
from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration
from googleads import ad_manager
from googleads.common import ZeepServiceProxy
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce

from app.clients.common import salesforce_update, salesforce_run_query
from gam_core.model.guard_exception import GuardException

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)

ID_STRING_CHECK_PATTERN = re.compile(r"[^\d,]")
GAM_QUOTE_ID_COL = "SWM_AdServerProposalIdentifier__c"
FULFILMENT_COL = "SWM_GAM_Proposal_Status__c"


@define
class SalesforceQuoteRecord:
    sf_id: str
    gam_id: str
    status: str

    def to_sf(self) -> dict:
        return {
            "Id": self.sf_id,
            GAM_QUOTE_ID_COL: self.gam_id,
            FULFILMENT_COL: self.status,
        }


class ProposalClient:
    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.api_version = config["gam_api_version"]
        self.client = ad_manager_client.GetService("ProposalService", self.api_version)

    @xray_recorder.capture("proposal_client.sync_proposals")
    def sync_proposals(self, sf_bulk):
        sf_quotes = self.get_sf_quotes(sf_bulk)
        if not sf_quotes:
            logger.warning(
                "No quotes found in Salesforce, not continuing with proposal sync"
            )
            return
        gam_quotes = self.get_gam_quotes(sf_quotes)
        if not gam_quotes:
            logger.warning(
                "No matching quotes found in GAM, not continuing with quote sync"
            )
            return
        updated_quotes = []
        no_update_quotes = []
        for quote in sf_quotes:
            gam_id = quote[GAM_QUOTE_ID_COL]
            if gam_id not in gam_quotes:
                logger.warning(
                    f"Quotes in GAM not found for quote {quote['Id']} with GAM quote ID {gam_id}"
                )
                continue
            if (
                gam_quotes[gam_id]["status"].upper() == "APPROVED"
                and gam_quotes[gam_id]["negotiationStatus"].upper() == "FINALIZED"
            ):
                updated_quotes.append(
                    SalesforceQuoteRecord(
                        sf_id=quote["Id"], gam_id=gam_id, status="Finalised"
                    )
                )
                continue
            no_update_quotes.append(gam_id)

        logger.info(
            f"The following SF quotes do not need updating {len(no_update_quotes)}: {','.join(no_update_quotes)}"
        )
        logger.info(
            f"Found {len(updated_quotes)} quotes to update: {','.join([o.sf_id for o in updated_quotes])}"
        )
        if updated_quotes:
            self.update_salesforce(sf_bulk, [o.to_sf() for o in updated_quotes])
        return

    @xray_recorder.capture("proposal_client.get_sf_quotes")
    def get_sf_quotes(self, sf_bulk: SalesforceBulk) -> List[dict]:
        logger.info(f"Getting SF quotes")
        job = sf_bulk.create_queryall_job("Quote", contentType="JSON")
        batch = sf_bulk.query(
            job,
            f"SELECT Id, {GAM_QUOTE_ID_COL}, {FULFILMENT_COL} "
            "from Quote "
            "WHERE SWM_AdServerProposalIdentifier__c != '' "
            "AND Opportunity.SWM_Campaign_Type__c = 'Programmatic Guaranteed' "
            "AND SWM_GAM_Proposal_Status__c = 'Submitted' ",
        )
        quotes = salesforce_run_query(sf_bulk, job, batch)
        logger.info(f"Valid quotes from Salesforce: {len(quotes)}")
        return quotes

    @xray_recorder.capture("proposal_client.get_proposal")
    def get_gam_quotes(self, sf_quotes) -> dict:
        logger.info(f"Getting GAM quotes")
        ids = [
            quote[GAM_QUOTE_ID_COL]
            for quote in sf_quotes
            if quote[GAM_QUOTE_ID_COL] is not None
        ]
        # Remove duplicates
        ids = list(set(ids))
        idstring = ",".join(str(id_) for id_ in ids)
        logger.info(f"Query GAM with quote IDs: " + idstring)
        if re.search(ID_STRING_CHECK_PATTERN, idstring):
            raise GuardException(
                "GAM QUOTE IDs contain invalid characters, only numbers are allowed"
            )

        statement = (
            ad_manager.StatementBuilder(version=self.api_version)
            .Where("id in (:ids)")
            .OrderBy("id", ascending=True)
            .WithBindVariable("ids", ids)
        )
        gam_statuses = {}
        while True:
            response = self.client.getProposalsByStatement(statement.ToStatement())
            if "results" not in response or len(response["results"]) == 0:
                break
            for quote in response["results"]:
                gam_statuses[str(quote["id"])] = {
                    "status": quote["status"],
                    "negotiationStatus": quote["marketplaceInfo"]["negotiationStatus"],
                }
            statement.offset += statement.limit
        logger.info(gam_statuses)
        logger.info(f"GAM Quotes found: {len(gam_statuses)}")
        return gam_statuses

    @xray_recorder.capture("proposal_client.update_salesforce")
    def update_salesforce(self, sf_bulk: SalesforceBulk, quotes):
        logger.info(f"Updating salesforce quotes")
        results = salesforce_update(sf_bulk, "Quote", quotes)
        logger.info(results)
        errors = [e for e in results if e.error == "true"]
        if errors:
            for result in errors:
                logger.error(
                    f"Error occurred updating salesforce quote for {result.id}: {result.error}"
                )
            logger.error(f"{len(errors)} errors happened on quote updates")
        logger.info("No errors on quote update")
