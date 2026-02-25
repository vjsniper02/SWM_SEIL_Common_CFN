from unittest.mock import create_autospec, Mock

import pytest
from aws_xray_sdk.core import xray_recorder
from googleads import AdManagerClient

from gam_core.client_factory import ClientFactory


class ApiTestClient:
    def __init__(self, *args, **kwargs):
        pass


class ApiTestMockClient:
    def __init__(self, *args, **kwargs):
        pass


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_mock_aws_param_name": "test", "gam_api_version": ""}
    aws_ssm_client = Mock()
    gam_client = create_autospec(AdManagerClient)
    xray_recorder.begin_segment("test_client_factory.arrange")

    yield config, aws_ssm_client, gam_client


@pytest.mark.parametrize(
    "use_mock,expected_client_type",
    [("true", ApiTestMockClient), ("false", ApiTestClient)],
)
def test_correct_client_return(arrange, use_mock: bool, expected_client_type):
    (config, aws_ssm_client, gam_client) = arrange
    aws_ssm_client.get_parameter.return_value = {"Parameter": {"Value": use_mock}}

    # Act
    client = ClientFactory.create_client(
        config,
        aws_ssm_client,
        gam_client,
        api_client=ApiTestClient,
        api_mock_client=ApiTestMockClient,
    )

    # Assert
    aws_ssm_client.get_parameter.assert_called_once_with(
        Name=config["gam_mock_aws_param_name"]
    )
    assert type(client) is expected_client_type
