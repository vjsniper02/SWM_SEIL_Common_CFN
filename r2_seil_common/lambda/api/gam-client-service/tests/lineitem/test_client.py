from unittest.mock import create_autospec, Mock

import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse
from googleads import AdManagerClient
from googleads.common import ZeepServiceProxy

from app.clients.lineitem_client import LineItemClient
from app.clients.lineitem_client_mock import (
    LINEITEM_SAMPLE_REQUEST,
    LINEITEM_SAMPLE_RESPONSE,
    LINEITEM_SAMPLE_REQUEST_ACTION_INVALID_REQUEST,
)


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    ad_manager_client = create_autospec(AdManagerClient)

    xray_recorder.begin_segment("test_client.arrange")

    yield config, ad_manager_client


def test_result_returned(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    lineitem_response = [ObjectFromDict(**LINEITEM_SAMPLE_RESPONSE)]
    service_client_mock.createLineItems.return_value = lineitem_response
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    result = LineItemClient(config, ad_manager_client).process_lineitems(
        LINEITEM_SAMPLE_REQUEST
    )

    # Assert
    assert (
        ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    )  # must be set to avoid error in lambda environment.
    ad_manager_client.GetService.assert_any_call(
        "LineItemService", config["gam_api_version"]
    )

    service_client_mock.createLineItems.assert_called_once()

    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["lineItems"][0]["status"]
        == lineitem_response[0].status
    )


def test_error_when_missing_headers(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        result = LineItemClient(config, ad_manager_client).process_lineitems({})

    assert "Expected headers are missing" in e.value.args


def test_error_when_missing_body(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        result = LineItemClient(config, ad_manager_client).process_lineitems(
            {"headers": {"x": "y"}}
        )

    assert "Expected body is missing" in e.value.args


@pytest.mark.parametrize(
    "lineitems, expected_error_message",
    [
        ([], "No line items provided"),
    ],
)
def test_error_when_unsupported_number_of_lineitems(
    lineitems, expected_error_message, arrange
):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        LineItemClient(config, ad_manager_client).process_lineitems(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"lineItems": lineitems},
            }
        )

    assert expected_error_message in e.value.args


def test_error_when_no_lineitems(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        LineItemClient(config, ad_manager_client).process_lineitems(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "PUT", "path": "/v1/gam/lineitem/"},
                "body": {"x": "y"},
            }
        )

    assert str(e).__contains__("No line items provided")


def test_error_when_invalid_request(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        LineItemClient(config, ad_manager_client).process_lineitems(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args


def test_invalid_request_lineitem_action(arrange):
    (config, ad_manager_client) = arrange

    # Act
    with pytest.raises(GuardException) as e:
        LineItemClient(config, ad_manager_client).process_lineitems(
            LINEITEM_SAMPLE_REQUEST_ACTION_INVALID_REQUEST
        )

    assert "IDs contain invalid characters, only numbers are allowed" in e.value.args
