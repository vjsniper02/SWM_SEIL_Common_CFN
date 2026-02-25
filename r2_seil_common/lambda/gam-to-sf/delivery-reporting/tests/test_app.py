from unittest.mock import create_autospec, Mock

import pytest
from aws_xray_sdk.core import xray_recorder
from gam_core.model.guard_exception import GuardException
from gam_core.model.object_from_dict import ObjectFromDict
from gam_core.model.success_response import SuccessResponse
from googleads import AdManagerClient
from googleads.common import ZeepServiceProxy

from app.clients.order_client import OrderClient
from app.clients.lineitem_client import LineItemClient
from app.clients.report_client import ReportClient
from app.clients.proposal_client import ProposalClient


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_api_version": ""}
    ad_manager_client = create_autospec(AdManagerClient)

    xray_recorder.begin_segment("test_client.arrange")

    yield config, ad_manager_client


def test_result_returned(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    OrderClient(config, ad_manager_client)

    # Assert
    assert ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    ad_manager_client.GetService.assert_any_call(
        "OrderService", config["gam_api_version"]
    )


def test_proposal_service(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    ProposalClient(config, ad_manager_client)

    # Assert
    assert ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    ad_manager_client.GetService.assert_any_call(
        "ProposalService", config["gam_api_version"]
    )


def test_lineitem_service(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    ad_manager_client.GetService.return_value = service_client_mock

    # Act
    LineItemClient(config, ad_manager_client)

    # Assert
    assert ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    ad_manager_client.GetService.assert_any_call(
        "LineItemService", config["gam_api_version"]
    )


def test_report_service(arrange):
    (config, ad_manager_client) = arrange
    service_client_mock = Mock()
    ad_manager_client.GetDataDownloader.return_value = service_client_mock

    # Act
    ReportClient(config, ad_manager_client)

    # Assert
    assert ad_manager_client.cache == ZeepServiceProxy.NO_CACHE
    ad_manager_client.GetDataDownloader.assert_any_call(config["gam_api_version"])
