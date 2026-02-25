import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse

from app.clients.lineitem_client_mock import (
    LineItemClientMock,
    LINEITEM_SAMPLE_REQUEST,
    LINEITEM_SAMPLE_RESPONSE,
    LINEITEM_SAMPLE_RESPONSE_ACTION,
    LINEITEM_SAMPLE_REQUEST_ACTION,
)


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    xray_recorder.begin_segment("test_client.arrange")

    yield config


def test_result_returned(arrange):
    (config) = arrange
    gam_response = ObjectFromDict(**LINEITEM_SAMPLE_RESPONSE)

    # Act
    result = LineItemClientMock(config).process_lineitems(LINEITEM_SAMPLE_REQUEST)

    # Assert
    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["lineItems"][0]["status"] == gam_response.status
    )


def test_update_lineitem_result_returned(arrange):
    (config) = arrange
    gam_response = LINEITEM_SAMPLE_RESPONSE_ACTION

    # Act
    result = LineItemClientMock(config).process_lineitems(
        LINEITEM_SAMPLE_REQUEST_ACTION
    )

    # Assert
    assert type(result) is SuccessResponse
    assert result.get_response()["data"]["numChanges"] == gam_response["numChanges"]


def test_error_when_missing_headers(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        result = LineItemClientMock(config).process_lineitems({"lineItems": []})

    assert "Expected headers are missing" in e.value.args


@pytest.mark.parametrize(
    "lineitems, expected_error_message",
    [
        ([], "No line items provided"),
    ],
)
def test_error_when_unsupported_number_of_lineitems(
    lineitems, expected_error_message, arrange
):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        LineItemClientMock(config).process_lineitems(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"lineItems": lineitems},
            }
        )

    assert expected_error_message in e.value.args


def test_error_when_no_lineitems(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        LineItemClientMock(config).process_lineitems(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"test": "body"},
            }
        )

    assert "No line items provided" in e.value.args


def test_error_when_invalid_request(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        LineItemClientMock(config).process_lineitems(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
