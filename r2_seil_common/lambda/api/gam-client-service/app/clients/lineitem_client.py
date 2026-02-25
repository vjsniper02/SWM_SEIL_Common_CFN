import logging
import random
import re

import googleads.errors
import time
from typing import Union

import zeep.exceptions
from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration
from gam_core.model.guard_exception import GuardException
from gam_core.model.success_response import SuccessResponse
from gam_core.model.bad_request import BadRequest
from googleads import ad_manager
from googleads.common import ZeepServiceProxy

from app.models.create_lineitems_request import CreateLineItemsRequest
from app.models.create_lineitems_response import CreateLineItemsResponse
from app.models.update_lineitems_request import UpdateLineItemsRequest
from app.models.update_lineitems_response import UpdateLineItemsResponse
from app.models.perform_lineitem_action_response import PerformLineItemActionResponse
from app.models.get_lineitems_request import GetLineItemsRequest
from app.models.get_lineitems_response import GetLineItemsResponse

ID_STRING_CHECK_PATTERN = re.compile(r"[^0-9,]")

# Retries for update_lineitems
UPDATE_RETRIES = 3
# Max time between retries
UPDATE_MAX_INTERVAL = 5
random.seed()


class LineItemClient:
    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.apiVersion = config["gam_api_version"]
        self.client = ad_manager_client.GetService("LineItemService", self.apiVersion)
        self.supportedActions = {
            "archive": "ArchiveLineItems",
            "pause": "PauseLineItems",
            "resume": "ResumeLineItems",
        }
        self.logger = logging.getLogger(__name__)

    @xray_recorder.capture("client.process_lineitems")
    def process_lineitems(self, request: dict) -> Union[SuccessResponse, BadRequest]:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        transaction_context = request.get("transactionContext", {})
        http_method: str = transaction_context.get("method", "")
        path = transaction_context.get("path")

        body = request.get("body", {})
        if len(body.keys()) < 1 and http_method != "GET":
            raise GuardException("Expected body is missing")

        if len(transaction_context.keys()) > 0:
            if http_method == "POST":
                return self.create_lineitems(headers, body)
            elif http_method == "PUT":
                # Check path, see if it is "perform lineitem action"
                if "action" in path:
                    action = body.get("action")
                    if action is None:
                        raise GuardException("The action to perform is missing")
                    ids = body.get("ids")
                    if ids is None:
                        raise GuardException("The line item IDs are missing")
                    return self.perform_lineitem_action(headers, action, ids)
                # PUT /v1/gam/lineitem
                else:
                    return self.update_lineitems(headers, body)
            elif http_method == "GET":
                query_string_parameters = request.get("queryStringParameters", {})
                lineitem_id = query_string_parameters.get("lineitemid")
                order_id = query_string_parameters.get("orderid")
                if not ((lineitem_id is None) ^ (order_id is None)):
                    return BadRequest(
                        "400001",
                        "Either lineitemid or orderid must be provided",
                        headers,
                    )
                if lineitem_id is not None:
                    return self.get_lineitems_by_lineitemid(headers, lineitem_id)
                else:
                    return self.get_lineitems_by_orderid(headers, order_id)

        raise GuardException("API call is not defined in the spec, not supported")

    @xray_recorder.capture("client.perform_lineitem_action")
    def perform_lineitem_action(self, headers, action, ids):
        if action not in self.supportedActions.keys():
            raise GuardException(f"Unsupported line item action: {action}")

        idstring = ",".join(str(id) for id in ids)
        # Check the content of the ID string, it should not have anything other than numbers and commas
        if re.search(ID_STRING_CHECK_PATTERN, idstring):
            raise GuardException(
                "IDs contain invalid characters, only numbers are allowed"
            )
        # Create query. Code is adopted from
        # https://github.com/googleads/googleads-python-lib/blob/master/examples/ad_manager/v202205/line_item_service/pause_line_items.py
        statement = (
            ad_manager.StatementBuilder(version=self.apiVersion)
            .Where("id in (:ids)")
            .OrderBy("id", ascending=True)
            .WithBindVariable("ids", ids)
        )
        result_set_size = 0
        should_continue = True
        # Iterate over paged results from the statement.
        while should_continue:
            page = self.client.getLineItemsByStatement(statement.ToStatement())
            if "results" in page and len(page["results"]):
                result_set_size = len(page["results"])
            # Iterate over individual results in the page.
            for line_item in page["results"]:
                print(
                    f"Performing action {action} on line item with ID {line_item['id']}"
                )
            # Update statement for next page.
            statement.offset += statement.limit
            should_continue = statement.offset < result_set_size
        if result_set_size > 0:
            statement.offset = None
            statement.limit = None
            # Perform action on all Line Items that match the statement.
            update_result = self.client.performLineItemAction(
                {"xsi_type": self.supportedActions[action]}, statement.ToStatement()
            )
            if update_result and update_result["numChanges"] > 0:
                result_set_size = update_result["numChanges"]
                print("Updated %d line item(s)" % result_set_size)
            else:
                print("No line item actions were performed")
        perform_line_item_action_response = PerformLineItemActionResponse(
            result_set_size
        ).get_response()
        return SuccessResponse(headers, **perform_line_item_action_response)

    @xray_recorder.capture("client.update_lineitems")
    def update_lineitems(self, headers, body):
        lineitem_deltas = UpdateLineItemsRequest(body)
        ids = list(lineitem_deltas.get_request().keys())
        idstring = ",".join(str(id) for id in ids)
        # Check the content of the ID string, it should not have anything other than numbers and commas
        if re.search(ID_STRING_CHECK_PATTERN, idstring):
            raise GuardException(
                "IDs contain invalid characters, only numbers are allowed"
            )
        # Create query.
        statement = (
            ad_manager.StatementBuilder(version=self.apiVersion)
            .Where("id in (:ids)")
            .WithBindVariable("ids", ids)
        )
        # Get line items by statement.
        response = self.client.getLineItemsByStatement(statement.ToStatement())
        updated_line_items = []
        if "results" in response and len(response["results"]):
            for line_item in response["results"]:
                if not line_item["isArchived"]:
                    lineitem_delta = lineitem_deltas.get_request().get(line_item["id"])
                    for key in lineitem_delta.keys():
                        line_item[key] = lineitem_delta.get(key)
                    updated_line_items.append(line_item)

        for retry in range(UPDATE_RETRIES):
            try:
                self.logger.info(
                    f"Updating lineitem, attempt ({retry + 1}/{UPDATE_RETRIES})"
                )
                line_items = self.client.updateLineItems(updated_line_items)
                break
            except googleads.errors.GoogleAdsServerFault as e:
                self.logger.warn(f"Update failed: {e}")
                if retry + 1 >= UPDATE_RETRIES:
                    raise e
                time.sleep(  # nosec - not security related
                    random.randrange(UPDATE_MAX_INTERVAL)
                )

        update_line_items_response = UpdateLineItemsResponse(
            len(line_items)
        ).get_response()
        return SuccessResponse(headers, **update_line_items_response)

    @xray_recorder.capture("client.create_lineitems")
    def create_lineitems(self, headers, body):
        line_items = CreateLineItemsRequest(body)
        created_line_items = self.client.createLineItems(line_items.get_request())
        # Display results.
        for line_item in created_line_items:
            print(
                'Line item with id "%s", belonging to order id "%s", and named '
                '"%s" was created.' % (line_item.id, line_item.orderId, line_item.name)
            )
        create_line_items_response = CreateLineItemsResponse(
            created_line_items
        ).get_response()
        return SuccessResponse(headers, **create_line_items_response)

    @xray_recorder.capture("client.get_lineitems_by_lineitemid")
    def get_lineitems_by_lineitemid(self, headers, lineitem_id) -> SuccessResponse:
        line_items = GetLineItemsRequest(self.apiVersion, lineitem_id)

        # Get line items by statement.
        get_line_items = self.client.getLineItemsByStatement(
            line_items.get_lineItems_statement().ToStatement(),
        )

        if "results" not in get_line_items or len(get_line_items["results"]) < 1:
            raise GuardException(f"Line Item ID: {lineitem_id} does not exist")

        line_items_response = GetLineItemsResponse(
            get_line_items["results"]
        ).get_response()
        return SuccessResponse(headers, **line_items_response)

    @xray_recorder.capture("client.get_lineitems_by_orderid")
    def get_lineitems_by_orderid(self, headers, order_id) -> SuccessResponse:
        statement = (
            ad_manager.StatementBuilder(version=self.apiVersion)
            .Where("orderId = :orderid")
            .WithBindVariable("orderid", order_id)
        )
        # Get line items by statement.
        get_line_items = self.client.getLineItemsByStatement(statement.ToStatement())

        if "results" not in get_line_items or len(get_line_items["results"]) < 1:
            raise GuardException(f"Order ID: {order_id} does not exist")

        line_items_response = GetLineItemsResponse(
            get_line_items["results"]
        ).get_response()
        return SuccessResponse(headers, **line_items_response)
