import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse

from app.clients.proposal_lineitem_client_mock import (
    ProposalLineItemClientMock,
    PROPOSAL_LINEITEM_SAMPLE_REQUEST,
    PROPOSAL_LINEITEM_SAMPLE,
)


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    xray_recorder.begin_segment("test_client.arrange")

    yield config


def test_result_returned(arrange):
    (config) = arrange
    gam_response = PROPOSAL_LINEITEM_SAMPLE

    # Act
    result = ProposalLineItemClientMock(config).create_proposal_lineitem(
        PROPOSAL_LINEITEM_SAMPLE_REQUEST
    )

    # Assert
    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["proposalLineitems"][0]["name"]
        == gam_response["name"]
    )


def test_update_proposal_result_returned(arrange):
    (config) = arrange
    gam_response = PROPOSAL_LINEITEM_SAMPLE

    # Act
    update_sample_request = PROPOSAL_LINEITEM_SAMPLE_REQUEST
    update_sample_request["body"]["proposalLineitems"][0]["id"] = 196230046
    result = ProposalLineItemClientMock(config).update_proposal_lineitem(
        update_sample_request
    )

    # Assert
    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["proposalLineitems"][0]["name"]
        == gam_response["name"]
    )


def test_error_create_when_missing_headers(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClientMock(config).create_proposal_lineitem(
            {"proposalLineitems": []}
        )

    assert "Expected headers are missing" in e.value.args


def test_error_update_when_missing_headers(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClientMock(config).update_proposal_lineitem(
            {"proposalLineitems": []}
        )

    assert "Expected headers are missing" in e.value.args


@pytest.mark.parametrize(
    "proposal_lineitems, expected_error_message",
    [
        ([], "No proposal line items provided"),
    ],
)
def test_error_when_unsupported_number_of_lineitems(
    proposal_lineitems, expected_error_message, arrange
):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClientMock(config).create_proposal_lineitem(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"proposalLineitems": proposal_lineitems},
            }
        )

    assert expected_error_message in e.value.args


def test_error_when_no_lineitems(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClientMock(config).create_proposal_lineitem(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"test": "body"},
            }
        )

    assert "No proposal line items provided" in e.value.args


def test_error_when_invalid_request(arrange):
    (config) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClientMock(config).create_proposal_lineitem(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
