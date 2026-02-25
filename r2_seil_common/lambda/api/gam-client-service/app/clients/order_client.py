from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration
from gam_core.model.guard_exception import GuardException
from gam_core.model.success_response import SuccessResponse
from gam_core.model.bad_request import BadRequest
from googleads import ad_manager
from googleads.common import ZeepServiceProxy

from app.models.create_order_request import CreateOrderRequest
from app.models.create_order_response import CreateOrderResponse
from app.models.perform_order_action_request import PerformOrderActionRequest
from app.models.perform_order_action_response import PerformOrderActionResponse
from app.models.get_orders_request import GetOrdersRequest
from app.models.get_orders_response import GetOrdersResponse


class OrderClient:
    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.apiVersion = config["gam_api_version"]
        self.client = ad_manager_client.GetService("OrderService", self.apiVersion)

    @xray_recorder.capture("client.create_order")
    def create_orders(self, request: dict) -> SuccessResponse:
        print(request)
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        body = request.get("body", {})
        if len(body.keys()) < 1:
            raise GuardException("Expected body is missing")

        orders = CreateOrderRequest(body)
        created_orders = self.client.createOrders(orders.get_request())
        create_order_response = CreateOrderResponse(created_orders).get_response()

        return SuccessResponse(headers, **create_order_response)

    @xray_recorder.capture("client.perform_order_action")
    def perform_order_action(self, request: dict) -> SuccessResponse:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        body = request.get("body", {})
        if len(body.keys()) < 1:
            raise GuardException("Expected body is missing")

        id = body["id"]
        action = body["action"]

        if id is None:
            raise GuardException("Expected order ID missing")
        if action is None:
            raise GuardException("Expected action is missing")
        if action not in self.supportedActions.keys():
            raise GuardException(f"Unsupported order action: {action}")

        statement = (
            ad_manager.StatementBuilder(version=self.apiVersion)
            .Where(("id = :id"))
            .WithBindVariable("id", id)
        )
        orders = self.client.getOrdersByStatement(statement.ToStatement())
        if "results" not in orders or len(orders["results"]) < 1:
            raise GuardException(f"Order ID: {id} does not exist")

        order_action = PerformOrderActionRequest(
            self.apiVersion, id, self.supportedActions[action]
        )
        perform_order_action = self.client.performOrderAction(
            order_action.get_order_action(),
            order_action.get_order_statement().ToStatement(),
        )
        perform_order_action_response = PerformOrderActionResponse(
            perform_order_action
        ).get_response()

        return SuccessResponse(headers, **perform_order_action_response)

    @xray_recorder.capture("client.get_orders")
    def get_orders(self, request: dict) -> SuccessResponse:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        path = request.get("transactionContext", {}).get("path")
        id = path.split("/")[-1]
        if id is None:
            return BadRequest(
                "400001", "Missing Order ID in path", headers
            ).get_response()

        params = {"id": id}
        get_order = GetOrdersRequest(self.apiVersion, params)

        orders = self.client.getOrdersByStatement(
            get_order.get_order_statement().ToStatement()
        )

        if "results" not in orders or len(orders["results"]) < 1:
            raise GuardException(f"Order ID: {id} does not exist")

        get_orders_response = GetOrdersResponse(orders["results"]).get_response()

        return SuccessResponse(headers, **get_orders_response)

    supportedActions = {
        "APPROVED": "ApproveOrders",
        "PAUSED": "PauseOrders",
        "RESUMED": "ResumeOrders",
    }
