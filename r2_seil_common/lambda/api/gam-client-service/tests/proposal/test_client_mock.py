import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse

from app.clients.proposal_client_mock import (
    ProposalClientMock,
    PROPOSAL_SAMPLE_REQUEST,
    PROPOSAL_SAMPLE,
    PROPOSAL_RESPONSE_ACTION,
)


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    xray_recorder.begin_segment("test_client.arrange")

    yield config


def test_result_returned(arrange):
    (config) = arrange
    gam_response = PROPOSAL_SAMPLE

    # Act
    result = ProposalClientMock(config).create_proposal(PROPOSAL_SAMPLE_REQUEST)

    # Assert
    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["proposals"][0]["isProgrammatic"]
        == gam_response["isProgrammatic"]
    )


def test_update_proposal_result_returned(arrange):
    (config) = arrange
    gam_response = PROPOSAL_SAMPLE

    # Act
    result = ProposalClientMock(config).update_proposal(PROPOSAL_SAMPLE_REQUEST)

    # Assert
    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["proposals"][0]["isProgrammatic"]
        == gam_response["isProgrammatic"]
    )


def test_error_create_when_missing_headers(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        result = ProposalClientMock(config).create_proposal({"proposals": []})

    assert "Expected headers are missing" in e.value.args


def test_error_update_when_missing_headers(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        result = ProposalClientMock(config).update_proposal({"proposals": []})

    assert "Expected headers are missing" in e.value.args


@pytest.mark.parametrize(
    "lineitems, expected_error_message",
    [
        ([], "No proposal provided"),
    ],
)
def test_error_when_unsupported_number_of_lineitems(
    lineitems, expected_error_message, arrange
):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalClientMock(config).create_proposal(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"proposals": lineitems},
            }
        )

    assert expected_error_message in e.value.args


def test_error_when_no_lineitems(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalClientMock(config).create_proposal(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"test": "body"},
            }
        )

    assert "No proposal provided" in e.value.args


def test_error_when_invalid_request(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalClientMock(config).create_proposal(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
