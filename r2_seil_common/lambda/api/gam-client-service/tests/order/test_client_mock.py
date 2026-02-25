import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse

from app.clients.order_client_mock import (
    OrderClientMock,
    ORDER_SAMPLE_REQUEST,
    ORDER_SAMPLE_RESPONSE,
)


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    xray_recorder.begin_segment("test_client.arrange")

    yield config


def test_result_returned(arrange):
    (config) = arrange
    gam_response = ObjectFromDict(**ORDER_SAMPLE_RESPONSE)

    # Act
    result = OrderClientMock(config).create_orders(ORDER_SAMPLE_REQUEST)

    # Assert
    assert type(result) is SuccessResponse
    assert result.get_response()["data"]["orders"][0]["status"] == gam_response.status


def test_error_when_missing_headers(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        result = OrderClientMock(config).create_orders({"orders": []})

    assert "Expected headers are missing" in e.value.args


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
    (config) = arrange
    with pytest.raises(GuardException) as e:
        OrderClientMock(config).create_orders(
            {"headers": {"test": "header"}, "body": {"orders": orders}}
        )

    assert expected_error_message in e.value.args


def test_error_when_no_orders(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        OrderClientMock(config).create_orders(
            {"headers": {"test": "header"}, "body": {"test": "body"}}
        )

    assert "No orders provided" in e.value.args


def test_error_when_invalid_request(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        OrderClientMock(config).create_orders(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
