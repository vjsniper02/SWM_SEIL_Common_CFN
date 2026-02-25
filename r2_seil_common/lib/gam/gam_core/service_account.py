import logging
import os

from aws_xray_sdk.core import xray_recorder
from botocore.client import BaseClient
from googleads import oauth2
from googleads.oauth2 import GoogleOAuth2Client

from gam_core import SERVICE_ACCOUNT_KEYFILE

logger = logging.getLogger("service_account")
logger.setLevel(logging.INFO)


def persist_gam_service_account_key_file(config: dict, aws_secret_client: BaseClient):
    # The service account key file will be retrieved from secret storage when missing
    # and persisted within the lambda execution environment's file storage.
    if os.path.exists(SERVICE_ACCOUNT_KEYFILE):
        return

    try:
        secret_value_response = aws_secret_client.get_secret_value(
            SecretId=config["gam_account_aws_secret_name"]
        )

        sa_file = open(SERVICE_ACCOUNT_KEYFILE, "w")
        sa_file.write(secret_value_response["SecretString"])
        sa_file.close()
    except Exception as e:
        logger.exception(str(e))
        raise


class GoogleServiceAccountClientFactory:
    @classmethod
    @xray_recorder.capture("service_account.create_client")
    def create_client(
        cls, config: dict, aws_secret_client: BaseClient, account_client_factory
    ) -> GoogleOAuth2Client:
        persist_gam_service_account_key_file(config, aws_secret_client)

        return account_client_factory(
            key_file=SERVICE_ACCOUNT_KEYFILE, scope=oauth2.GetAPIScope("ad_manager")
        )
