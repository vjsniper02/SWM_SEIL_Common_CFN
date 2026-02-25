import logging
import requests
from urllib.parse import urljoin

from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration

from core.model.success_response import SuccessResponse
from core.model.server_error import ServerError
from core.exceptions import BusinessException
from auth_client.client_credentials import ClientAuthToken

logger = logging.getLogger("techone_client")
logger.setLevel(logging.INFO)


class TechOneClient:
    def __init__(self, config: Configuration, tech1_auth_client: ClientAuthToken):
        self.api_scheme = config.get("tech1_api_scheme")
        self.api_host = config.get("tech1_api_host")
        self.api_fx_path = config.get("tech1_api_fx_path")
        self.api_debtor_creditlimit_path = config.get(
            "tech1_api_debtor_creditlimit_path"
        )
        self.api_timeout = config.get("tech1_api_timeout")
        self.auth_client = tech1_auth_client

    @xray_recorder.capture("client.get_foreign_exchange_rate")
    def get_foreign_exchange_rate(self, headers) -> SuccessResponse:
        try:
            resp = requests.get(
                urljoin(f"{self.api_scheme}://{self.api_host}/", self.api_fx_path),
                headers={
                    "Accept": "application/json",
                    "Authorization": self.auth_client(),
                },
                timeout=self.api_timeout,
            )
            logger.info(f"techone raw response: {resp}")
            resp_data = resp.json()
            if "DataSet" in resp_data and len(resp_data["DataSet"]) == 1:
                fx_data = resp_data["DataSet"][0]
                fx_data = dict((k.lower(), v) for k, v in fx_data.items())
                logger.info(f"techone response - fx_data: {fx_data}")
            else:
                logger.error("Missing Foreign Exchange Rate from TechOne")
                logger.error(f"techone error - response: {resp}")
                raise BusinessException("Missing Foreign Exchange Rate from TechOne")

            return SuccessResponse(headers, **fx_data)
        except Exception as e:
            logger.exception(f"Server error - {str(e)}")
            return ServerError("500001", e, headers).get_response()

    @xray_recorder.capture("client.get_debtor_remaining_credit_limit")
    def get_debtor_remaining_credit_limit(
        self, debtor_number, headers
    ) -> SuccessResponse:
        try:
            payload = {
                "IgnoreOverride": False,
                "System": "DEFAULT",
                "Name": "DEBTOR_REMAINING_CREDIT",
                "Data": {"Variables": {"DebtorNumber": debtor_number}},
            }
            resp = requests.post(
                urljoin(
                    f"{self.api_scheme}://{self.api_host}/",
                    self.api_debtor_creditlimit_path,
                ),
                headers={
                    "Accept": "application/json",
                    "Authorization": self.auth_client(),
                },
                json=payload,
                timeout=self.api_timeout,
            )
            resp_data = resp.json()
            if (
                "Messages" in resp_data
                and len(resp_data["Messages"]) == 1
                and "Message" in resp_data["Messages"][0]
            ):
                debtor_credit_data = resp_data["Messages"][0]
                message_data = resp_data["Messages"][0]["Message"]
                remaining_credit_limit = message_data.split("=")[1]
                debtor_credit_data = dict(
                    (k.lower(), v) for k, v in debtor_credit_data.items()
                )
                debtor_credit_data.pop("message", None)
                debtor_credit_data["remaining_credit_limit"] = remaining_credit_limit
                logger.info(
                    f"techone response - debtor_credit_data: {debtor_credit_data}"
                )
            else:
                logger.error("Missing Debtor Credit Limit from TechOne")
                raise BusinessException("Missing Debtor Credit Limit from TechOne")

            return SuccessResponse(headers, **debtor_credit_data)
        except Exception as e:
            logger.exception(f"Server error - {str(e)}")
            return ServerError("500001", e, headers).get_response()
