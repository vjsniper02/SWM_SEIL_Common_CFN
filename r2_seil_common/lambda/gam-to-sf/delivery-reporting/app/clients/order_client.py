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
GAM_ORDER_ID_COL = "SWM_GAMOrderId__c"
FULFILMENT_COL = "vlocity_cmt__FulfilmentStatus__c"


@define
class SalesforceOrderRecord:
    sf_id: str
    gam_id: str
    status: str

    def to_sf(self) -> dict:
        return {
            "Id": self.sf_id,
            GAM_ORDER_ID_COL: self.gam_id,
            FULFILMENT_COL: self.status,
        }


class OrderClient:
    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.api_version = config["gam_api_version"]
        self.client = ad_manager_client.GetService("OrderService", self.api_version)

    @xray_recorder.capture("order_client.sync_orders")
    def sync_orders(self, sf_bulk):
        sf_orders = self.get_sf_orders(sf_bulk)
        if not sf_orders:
            logger.warning(
                "No orders found in Salesforce, not continuing with order sync"
            )
            return
        gam_orders = self.get_gam_orders(sf_orders)
        if not gam_orders:
            logger.warning(
                "No matching orders found in GAM, not continuing with order sync"
            )
            return

        updated_orders = []
        no_update_orders = []
        for order in sf_orders:
            gam_id = order[GAM_ORDER_ID_COL]
            if gam_id not in gam_orders:
                logger.warning(
                    f"Order in GAM not found for order {order['Id']} with GAM order ID {gam_id}"
                )
                continue
            if (
                not order[FULFILMENT_COL]
                or gam_orders[gam_id].upper() != order[FULFILMENT_COL].upper()
            ):
                updated_orders.append(
                    SalesforceOrderRecord(
                        sf_id=order["Id"], gam_id=gam_id, status=gam_orders[gam_id]
                    )
                )
                continue
            no_update_orders.append(order["Id"])

        logger.info(
            f"The following SF orders do not need updating {len(no_update_orders)}: {','.join(no_update_orders)}"
        )
        logger.info(
            f"Found {len(updated_orders)} orders to update: {','.join([o.sf_id for o in updated_orders])}"
        )
        if updated_orders:
            self.update_salesforce(sf_bulk, [o.to_sf() for o in updated_orders])
        return

    @xray_recorder.capture("order_client.get_orders")
    def get_gam_orders(self, sf_orders) -> dict:
        logger.info(f"Getting GAM orders")
        ids = [
            order[GAM_ORDER_ID_COL]
            for order in sf_orders
            if order[GAM_ORDER_ID_COL] is not None
        ]
        # Remove duplicates
        ids = list(set(ids))
        idstring = ",".join(str(id_) for id_ in ids)
        logger.info(f"Query GAM with order IDs: " + idstring)
        if re.search(ID_STRING_CHECK_PATTERN, idstring):
            raise GuardException(
                "GAM ORDER IDs contain invalid characters, only numbers are allowed"
            )

        statement = (
            ad_manager.StatementBuilder(version=self.api_version)
            .Where("id in (:ids)")
            .OrderBy("id", ascending=True)
            .WithBindVariable("ids", ids)
        )
        gam_statuses = {}
        while True:
            response = self.client.getOrdersByStatement(statement.ToStatement())
            if "results" not in response or len(response["results"]) == 0:
                break
            for order in response["results"]:
                gam_statuses[str(order["id"])] = order["status"]
            statement.offset += statement.limit
        logger.info(f"GAM Orders found: {len(gam_statuses)}")
        return gam_statuses

    @xray_recorder.capture("order_client.get_sf_orders")
    def get_sf_orders(self, sf_bulk: SalesforceBulk) -> List[dict]:
        logger.info(f"Getting SF orders")
        job = sf_bulk.create_queryall_job("Order", contentType="JSON")
        batch = sf_bulk.query(
            job,
            f"SELECT Id, {GAM_ORDER_ID_COL}, {FULFILMENT_COL} "
            "FROM Order "
            "WHERE SWM_GAMUpdate_After_Complete__c > 0 "
            "AND IsDeleted=FALSE",
        )
        orders = salesforce_run_query(sf_bulk, job, batch)

        missing_gam_ids = [o["Id"] for o in orders if not o[GAM_ORDER_ID_COL]]
        valid_orders = [o for o in orders if o[GAM_ORDER_ID_COL]]
        if missing_gam_ids:
            logger.warning(
                f"The following orders in Salesforce did not have GAM Order IDs ({len(missing_gam_ids)}):"
                f" {','.join(missing_gam_ids)}"
            )
        logger.info(orders)
        logger.info(valid_orders)
        logger.info(f"Valid orders from Salesforce: {len(valid_orders)}")
        return valid_orders

    @xray_recorder.capture("order_client.update_salesforce")
    def update_salesforce(self, sf_bulk: SalesforceBulk, orders):
        logger.info(f"Updating salesforce orders")
        results = salesforce_update(sf_bulk, "Order", orders)
        errors = [e for e in results if e.error == "true"]
        if errors:
            for result in errors:
                logger.error(
                    f"Error occurred updating salesforce order for {result.id}: {result.error}"
                )
            logger.error(f"{len(errors)} errors happened on order updates")
        logger.info("No errors on update")
