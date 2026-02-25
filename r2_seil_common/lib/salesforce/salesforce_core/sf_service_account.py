import base64

import json
import logging
from aws_xray_sdk.core import xray_recorder
from botocore.client import BaseClient
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce

logger = logging.getLogger("sf_service_account")
logger.setLevel(logging.INFO)


API_VERSION = "54.0"


def get_credentials(config: dict, aws_secret_client: BaseClient) -> dict:
    # The service account key file will be retrieved from secret storage when missing
    # and persisted within the lambda execution environment's file storage.
    try:
        logger.info(
            f"Reading Salesforce credentials from secret {config['sf_account_aws_secret_name']}"
        )
        secret_value_response = aws_secret_client.get_secret_value(
            SecretId=config["sf_account_aws_secret_name"]
        )
        secret = {}
        if "SecretString" in secret_value_response:
            secret_str = secret_value_response["SecretString"]
            secret = json.loads(secret_str)
            if "privatekey" in secret:
                secret["privatekey"] = base64.b64decode(secret["privatekey"])
        if config["environment"].lower() == "prod":
            return dict(**secret)
        return dict(**secret, domain="test")
    except Exception as e:
        logger.exception(str(e))
        raise


class SalesForceClientFactory:
    @classmethod
    @xray_recorder.capture("sf_service_account.create_simple_client")
    def create(cls, config: dict, aws_secret_client: BaseClient) -> Salesforce:
        return Salesforce(
            **get_credentials(config, aws_secret_client), version=API_VERSION
        )


class SalesForceBulkClientFactory:
    @classmethod
    @xray_recorder.capture("sf_service_account.create_bulk_client")
    def create_bulk(cls, config: dict, aws_secret_client: BaseClient):
        sf = SalesForceClientFactory.create(
            config=config, aws_secret_client=aws_secret_client
        )
        return SalesforceBulk(
            sessionId=sf.session_id,
            host=sf.sf_instance,
            domain=sf.domain,
            API_version=API_VERSION,
        )
