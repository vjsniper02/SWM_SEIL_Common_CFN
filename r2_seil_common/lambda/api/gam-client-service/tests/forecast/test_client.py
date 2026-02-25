from unittest.mock import create_autospec, Mock

import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse
from googleads import AdManagerClient
from googleads.common import ZeepServiceProxy

from app.clients.forecast_client import ForecastClient
from app.clients.forecast_client_mock import (
    FORECAST_SAMPLE_REQUEST,
    FORECAST_SAMPLE_RESPONSE,
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
    gam_response = ObjectFromDict(**FORECAST_SAMPLE_RESPONSE)
    service_client_mock.getAvailabilityForecast.return_value = gam_response
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    result = ForecastClient(config, ad_manager_client).get_forecast(
        FORECAST_SAMPLE_REQUEST
    )

    # Assert
    assert (
        ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    )  # must be set to avoid error in lambda environment.
    ad_manager_client.GetService.assert_any_call(
        "ForecastService", config["gam_api_version"]
    )

    service_client_mock.getAvailabilityForecast.assert_called_once()

    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["lineItems"][0]["lineItemId"]
        == gam_response.lineItemId
    )


def test_error_when_missing_headers(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        result = ForecastClient(config, ad_manager_client).get_forecast(
            {"lineItems": []}
        )

    assert "Expected headers are missing" in e.value.args


@pytest.mark.parametrize(
    "line_items, expected_error_message",
    [
        ([{}, {}], "Only single line item supported at this time"),
        ([], "No line items provided"),
    ],
)
def test_error_when_unsupported_line_items(line_items, expected_error_message, arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ForecastClient(config, ad_manager_client).get_forecast(
            {"headers": {"test": "header"}, "body": {"lineItems": line_items}}
        )

    assert expected_error_message in e.value.args


def test_error_when_no_line_items(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ForecastClient(config, ad_manager_client).get_forecast(
            {"headers": {"test": "header"}}
        )

    assert "Expected body is missing" in e.value.args


def test_error_when_invalid_request(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ForecastClient(config, ad_manager_client).get_forecast(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
