import re
import logging

from aws_xray_sdk.core import xray_recorder, patch_all
from dependency_injector.wiring import Provide, inject

from core.model.server_error import ServerError
from core.model.bad_request import BadRequest
from app.container import Container
from app.techone_client import TechOneClient

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)

patch_all()  # Patch AWS libs with xray

FX_REQ_PATTERN = re.compile(r"/v1/techone/foreign-exchange-rate/aud-usd")
CR_LM_REQ_PATTERN = re.compile(
    r"/v1/techone/debtor/([a-zA-Z0-9]{3,10})/remaining-credit-limit"
)


@xray_recorder.capture("lambda_handler.handler")
@inject
def handler(
    event: dict,
    context: dict,
    tech1_client: TechOneClient = Provide[Container.tech1_client],
):
    logger.info(f"event: {event}")
    headers = event.get("headers", {})
    path = event.get("transactionContext", {}).get("path")
    if path is None:
        return BadRequest("400001", "Invalid Path from Event", headers).get_response()

    try:
        if re.match(FX_REQ_PATTERN, path):
            return tech1_client.get_foreign_exchange_rate(headers).get_response()
        elif re.match(CR_LM_REQ_PATTERN, path):
            result_match = CR_LM_REQ_PATTERN.search(path)
            if result_match is None or result_match.group(1) is None:
                return BadRequest(
                    "400001", "Missing Debtor Number", headers
                ).get_response()

            debtor_number = result_match.group(1)
            logger.info(f"debtor_number: {debtor_number}")
            return tech1_client.get_debtor_remaining_credit_limit(
                debtor_number, headers
            ).get_response()
        else:
            return BadRequest(
                "400001", "Invalid Path from Event", headers
            ).get_response()
    except Exception as e:
        logger.exception(f"Server error - {str(e)}")
        return ServerError("500001", e, headers).get_response()
    finally:
        logging.shutdown()


if __name__ == "__main__":
    # This section is ignored when AWS activates the lambda.
    xray_recorder.begin_segment(__name__)  # Ensure xray has something to record against
