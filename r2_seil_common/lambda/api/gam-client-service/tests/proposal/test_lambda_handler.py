from unittest.mock import create_autospec

import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.container import bootstrap
from gam_core.model.guard_exception import GuardException
from googleads.errors import GoogleAdsServerFault

from app.clients.proposal_client import ProposalClient
from app.clients.proposal_client_mock import ProposalClientMock
from app.lambda_handler import handler


@pytest.fixture(scope="function")
def arrange():
    container = bootstrap(
        None,
        None,
        None,
        None,
        None,
        None,
        ProposalClient,
        ProposalClientMock,
        None,
        None,
        None,
        None,
        modules=[handler.__module__],
    )

    client = create_autospec(ProposalClient)
    container.proposal_api_client.override(client)

    xray_recorder.begin_segment("test_lambda_handler.arrange")

    yield client

    container.unwire()


def test_handler_calls_correct_service(arrange):
    client = arrange

    # Act
    event = {
        "test": "call",
        "transactionContext": {
            "time": "2022-06-17T13:39:38.605875+1000",
            "correlationId": "postman-testing-1",
            "applicationLabel": "postman",
            "path": "/v1/gam/proposal",
            "method": "POST",
        },
    }
    result = handler(event=event, context={})

    # Assert
    client.create_proposal.assert_called_once_with(event)


@pytest.mark.parametrize(
    "exception,expected_http_code",
    [(GuardException, 400), (GoogleAdsServerFault, 400), (Exception, 500)],
)
def test_handler_throws_correct_error(exception, expected_http_code, arrange):
    client = arrange
    test_error_message = "This is a fake exception"
    client.create_proposal.side_effect = exception("test")

    # Act
    event = {
        "test": "call",
        "transactionContext": {
            "time": "2022-06-17T13:39:38.605875+1000",
            "correlationId": "postman-testing-1",
            "applicationLabel": "postman",
            "path": "/v1/gam/proposal",
            "method": "POST",
        },
    }
    result = handler(event=event, context={})

    # Assert
    client.create_proposal.assert_called_once_with(event)
    assert result["code"] == expected_http_code
