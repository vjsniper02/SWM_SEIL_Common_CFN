from unittest.mock import create_autospec, Mock

import pytest
from aws_xray_sdk.core import xray_recorder
from googleads import AdManagerClient
from googleads.common import ZeepServiceProxy

from app.clients.proposal_lineitem_client import ProposalLineItemClient
from app.clients.proposal_lineitem_client_mock import (
    PROPOSAL_LINEITEM_SAMPLE_REQUEST,
    PROPOSAL_LINEITEM_SAMPLE,
    PERFORM_PROPOSAL_LINEITEM_ACTION_SAMPLE_REQUEST,
)
from gam_core.model.guard_exception import GuardException
from gam_core.model.success_response import SuccessResponse


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    ad_manager_client = create_autospec(AdManagerClient)

    xray_recorder.begin_segment("test_client.arrange")

    yield config, ad_manager_client


def test_result_returned(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    proposal_lineitem_response = [PROPOSAL_LINEITEM_SAMPLE]
    service_client_mock.createProposalLineItems.return_value = (
        proposal_lineitem_response
    )
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    result = ProposalLineItemClient(config, ad_manager_client).create_proposal_lineitem(
        PROPOSAL_LINEITEM_SAMPLE_REQUEST
    )

    # Assert
    assert (
        ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    )  # must be set to avoid error in lambda environment.
    ad_manager_client.GetService.assert_any_call(
        "ProposalLineItemService", config["gam_api_version"]
    )

    service_client_mock.createProposalLineItems.assert_called_once()
    assert type(result) is SuccessResponse
    assert (
        result.get_response()["data"]["proposalLineitems"][0]["name"]
        == proposal_lineitem_response[0]["name"]
    )


def test_perform_action(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    ad_manager_client.GetService.return_value = service_client_mock
    service_client_mock.getProposalLineItemsByStatement.side_effect = [
        {"results": [PROPOSAL_LINEITEM_SAMPLE]},
        {},
    ]
    service_client_mock.performProposalLineItemAction.return_value = {"numChanges": 1}

    result = ProposalLineItemClient(
        config, ad_manager_client
    ).perform_proposal_lineitem_action(
        "archive",
        PERFORM_PROPOSAL_LINEITEM_ACTION_SAMPLE_REQUEST,
    )

    assert service_client_mock.getProposalLineItemsByStatement.call_count == 2
    service_client_mock.performProposalLineItemAction.assert_called_once()
    assert result.get_response()["data"]["numChanges"] == 1


def test_error_when_missing_headers(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClient(config, ad_manager_client).create_proposal_lineitem({})

    assert "Expected headers are missing" in e.value.args


def test_error_when_missing_body(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClient(config, ad_manager_client).create_proposal_lineitem(
            {"headers": {"x": "y"}}
        )

    assert "Expected body is missing" in e.value.args


@pytest.mark.parametrize(
    "proposal_lineitems, expected_error_message",
    [
        ([], "No proposal line items provided"),
    ],
)
def test_error_when_unsupported_number_of_lineitems(
    proposal_lineitems, expected_error_message, arrange
):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClient(config, ad_manager_client).create_proposal_lineitem(
            {
                "headers": {"test": "header"},
                "transactionContext": {"method": "POST"},
                "body": {"proposalLineitems": proposal_lineitems},
            }
        )

    assert expected_error_message in e.value.args


def test_error_when_no_proposal_lineitems(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClient(config, ad_manager_client).create_proposal_lineitem(
            {
                "headers": {"test": "header"},
                "transactionContext": {
                    "method": "PUT",
                    "path": "/v1/gam/proposallineitem/",
                },
                "body": {"x": "y"},
            }
        )

    assert str(e).__contains__("No proposal line items provided")


def test_error_when_invalid_request(arrange):
    (config, ad_manager_client) = arrange
    with pytest.raises(GuardException) as e:
        ProposalLineItemClient(config, ad_manager_client).create_proposal_lineitem(1234)

    assert "Request must be valid JSON and not type <class 'int'>" in e.value.args
