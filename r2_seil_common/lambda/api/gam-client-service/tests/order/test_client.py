from unittest.mock import create_autospec, Mock

import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse
from googleads import AdManagerClient
from googleads.common import ZeepServiceProxy

from app.clients.order_client import OrderClient
from app.clients.order_client_mock import ORDER_SAMPLE_REQUEST, ORDER_SAMPLE_RESPONSE


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    ad_manager_client = create_autospec(AdManagerClient)

    xray_recorder.begin_segment("test_client.arrange")

    yield config, ad_manager_client


def test_result_returned(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    order_response = [ObjectFromDict(**ORDER_SAMPLE_RESPONSE)]
    service_client_mock.createOrders.return_value = order_response
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    result = OrderClient(config, ad_manager_client).create_orders(ORDER_SAMPLE_REQUEST)

    # Assert
    assert (
        ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    )  # must be set to avoid error in lambda environment.
    ad_manager_client.GetService.assert_any_call(
        "OrderService", config["gam_api_version"]
    )

    service_client_mock.createOrders.assert_called_once()

    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["orders"][0]["status"] == order_response[0].status
    )


def test_error_when_missing_headers(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        result = OrderClient(config, ad_manager_client).create_orders({})

    assert "Expected headers are missing" in e.value.args


def test_error_when_missing_body(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        result = OrderClient(config, ad_manager_client).create_orders(
            {"headers": {"x": "y"}}
        )

    assert "Expected body is missing" in e.value.args


@pytest.mark.parametrize(
    "orders, expected_error_message",
    [
        ([{}, {}], "Only single order supported at this time"),
        ([], "No orders provided"),
    ],
)
def test_error_when_unsupported_number_of_orders(
    orders, expected_error_message, arrange
):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        OrderClient(config, ad_manager_client).create_orders(
            {"headers": {"test": "header"}, "body": {"orders": orders}}
        )

    assert expected_error_message in e.value.args


def test_error_when_no_orders(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        OrderClient(config, ad_manager_client).create_orders(
            {"headers": {"test": "header"}, "body": {"x": "y"}}
        )

    assert str(e).__contains__("orders")


def test_error_when_invalid_request(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        OrderClient(config, ad_manager_client).create_orders(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
