import json
import logging
from aws_xray_sdk.core import xray_recorder
from botocore.client import BaseClient

from tech1_ci.rpc import DoImportRequest, Auth

logger = logging.getLogger("client_factory")


class ClientFactory:
    @classmethod
    @xray_recorder.capture("t1_client_factory.create_client")
    def create_do_import(
        cls, config: dict, aws_secret_client: BaseClient
    ) -> DoImportRequest:
        t1_creds = json.loads(
            aws_secret_client.get_secret_value(SecretId=config["t1_creds_secret"])[
                "SecretString"
            ]
        )
        return DoImportRequest(
            wsdl=t1_creds["WSDL"],
            auth=Auth(
                UserId=t1_creds["UserId"],
                Password=t1_creds["Password"],
                Config=t1_creds["Config"],
            ),
            warehouse_name=config["t1_warehouse_name"],
            warehouse_table_name=config["t1_warehouse_table_name"],
        )
