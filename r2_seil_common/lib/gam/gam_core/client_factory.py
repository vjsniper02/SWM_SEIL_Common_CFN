import logging

from aws_xray_sdk.core import xray_recorder
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from googleads import AdManagerClient

logger = logging.getLogger("client_factory")


class ClientFactory:
    @classmethod
    @xray_recorder.capture("client_factory.create_client")
    def create_client(
        cls,
        config: dict,
        aws_ssm_client: BaseClient,
        gam_client: AdManagerClient,
        api_client: callable(object),
        api_mock_client: callable(object),
    ):

        should_use_mock_gam_client = True

        try:
            should_use_mock_gam_client = (
                aws_ssm_client.get_parameter(Name=config["gam_mock_aws_param_name"])[
                    "Parameter"
                ]["Value"]
                != "false"
            )
            logger.info(f"Using {'MOCK' if should_use_mock_gam_client else 'REAL'} GAM client")
        except ClientError as e:
            logger.exception(str(e))
            if config["environment"] != "local":
                raise

        if should_use_mock_gam_client:
            return api_mock_client(config)
        else:
            return api_client(config, gam_client)
