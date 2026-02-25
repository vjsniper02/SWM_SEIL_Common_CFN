from unittest.mock import create_autospec, Mock

import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse
from googleads import AdManagerClient
from googleads.common import ZeepServiceProxy

from app.clients.proposal_client import ProposalClient
from app.clients.proposal_client_mock import (
    PROPOSAL_SAMPLE_REQUEST,
    PROPOSAL_SAMPLE,
    # LINEITEM_SAMPLE_REQUEST_ACTION_INVALID_REQUEST,
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
    proposal_response = [PROPOSAL_SAMPLE]
    service_client_mock.createProposals.return_value = proposal_response
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    result = ProposalClient(config, ad_manager_client).create_proposal(
        PROPOSAL_SAMPLE_REQUEST
    )

    # Assert
    assert (
        ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    )  # must be set to avoid error in lambda environment.
    ad_manager_client.GetService.assert_any_call(
        "ProposalService", config["gam_api_version"]
    )

    service_client_mock.createProposals.assert_called_once()

    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["proposals"][0]["isProgrammatic"]
        == proposal_response[0]["isProgrammatic"]
    )


def test_error_when_missing_headers(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        result = ProposalClient(config, ad_manager_client).create_proposal({})

    assert "Expected headers are missing" in e.value.args


def test_error_when_missing_body(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        result = ProposalClient(config, ad_manager_client).create_proposal(
            {"headers": {"x": "y"}}
        )

    assert "Expected body is missing" in e.value.args


@pytest.mark.parametrize(
    "proposals, expected_error_message",
    [
        ([], "No proposal provided"),
    ],
)
def test_error_when_unsupported_number_of_lineitems(
    proposals, expected_error_message, arrange
):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalClient(config, ad_manager_client).create_proposal(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"proposals": proposals},
            }
        )

    assert expected_error_message in e.value.args


def test_error_when_no_lineitems(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalClient(config, ad_manager_client).create_proposal(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "PUT", "path": "/v1/gam/proposal/"},
                "body": {"x": "y"},
            }
        )

    assert str(e).__contains__("No proposal provided")


def test_error_when_invalid_request(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalClient(config, ad_manager_client).create_proposal(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
