import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse

from app.clients.forecast_client_mock import (
    ForecastClientMock,
    FORECAST_SAMPLE_REQUEST,
    FORECAST_SAMPLE_RESPONSE,
)


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    xray_recorder.begin_segment("test_client.arrange")

    yield config


def test_result_returned(arrange):
    (config) = arrange
    gam_response = ObjectFromDict(**FORECAST_SAMPLE_RESPONSE)

    # Act
    result = ForecastClientMock(config).get_forecast(FORECAST_SAMPLE_REQUEST)

    # Assert
    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["lineItems"][0]["lineItemId"]
        == gam_response.lineItemId
    )


def test_error_when_missing_headers(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        result = ForecastClientMock(config).get_forecast({"body": {"lineItems": []}})

    assert "Expected headers are missing" in e.value.args


@pytest.mark.parametrize(
    "line_items, expected_error_message",
    [
        ([{}, {}], "Only single line item supported at this time"),
        ([], "No line items provided"),
    ],
)
def test_error_when_unsupported_line_items(line_items, expected_error_message, arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ForecastClientMock(config).get_forecast(
            {"headers": {"test": "header"}, "body": {"lineItems": line_items}}
        )

    assert expected_error_message in e.value.args


def test_error_when_no_line_items(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ForecastClientMock(config).get_forecast({"headers": {"test": "header"}})

    assert "Expected body is missing" in e.value.args


def test_error_when_invalid_request(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ForecastClientMock(config).get_forecast(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
