from unittest.mock import Mock, patch

import pytest
from aws_xray_sdk.core import xray_recorder

from gam_core.service_account import GoogleServiceAccountClientFactory


@pytest.fixture(scope="function")
def arrange():
    config = {"gam_account_aws_secret_name": "test"}
    aws_secret_client = Mock()
    aws_secret_client.get_secret_value.return_value = {"SecretString": ""}

    xray_recorder.begin_segment("test_service_account.arrange")

    yield config, aws_secret_client


def test_gam_service_account_key_retrieved(arrange):
    (config, aws_secret_client) = arrange

    # Act
    with patch("os.path.exists", return_value=False):
        GoogleServiceAccountClientFactory.create_client(
            config, aws_secret_client, account_client_factory=Mock()
        )

    # Assert
    aws_secret_client.get_secret_value.assert_called_once_with(
        SecretId=config["gam_account_aws_secret_name"]
    )


def test_gam_service_account_key_not_retrieved_if_cached(arrange):
    (config, aws_secret_client) = arrange

    # Act
    with patch("os.path.exists", return_value=True):
        GoogleServiceAccountClientFactory.create_client(
            config, aws_secret_client, account_client_factory=Mock()
        )

    # Assert
    aws_secret_client.get_secret_value.assert_not_called()
