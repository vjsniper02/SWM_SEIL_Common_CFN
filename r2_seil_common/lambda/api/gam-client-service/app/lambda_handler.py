import logging
import os
import re
from typing import Union

from aws_xray_sdk.core import xray_recorder, patch_all
from dependency_injector.wiring import Provide, inject
from gam_core.container import Container, bootstrap
from gam_core.model.bad_request import BadRequest
from gam_core.model.guard_exception import GuardException
from gam_core.model.server_error import ServerError
from gam_core.model.success_response import SuccessResponse
from googleads.errors import GoogleAdsServerFault

from app.clients.lineitem_client import LineItemClient
from app.clients.lineitem_client_mock import LineItemClientMock, LINEITEM_SAMPLE_REQUEST
from app.clients.order_client import OrderClient
from app.clients.order_client_mock import OrderClientMock, ORDER_SAMPLE_REQUEST
from app.clients.forecast_client import ForecastClient
from app.clients.forecast_client_mock import ForecastClientMock, FORECAST_SAMPLE_REQUEST
from app.clients.proposal_client import ProposalClient
from app.clients.proposal_client_mock import ProposalClientMock, PROPOSAL_SAMPLE_REQUEST
from app.clients.proposal_lineitem_client import ProposalLineItemClient
from app.clients.proposal_lineitem_client_mock import (
    ProposalLineItemClientMock,
    PROPOSAL_LINEITEM_SAMPLE_REQUEST,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", logging.INFO))
logging.getLogger("googleads.soap").setLevel(logging.DEBUG)
logger = logging.getLogger("lambda_handler")

patch_all()  # Patch AWS libs with xray

LINEITEM_REQ_PATTERN = re.compile(r"/v1(/gam)?/lineitem(/[0-9]*)?")
FORECAST_REQ_PATTERN = re.compile(r"/v1(/gam)?/forecast")
ORDER_REQ_PATTERN = re.compile(r"/v1(/gam)?/order(/[0-9]*)?")
PROPOSAL_REQ_PATTERN = re.compile(r"/v1(/gam)?/proposal(/[0-9]*)?")
PROPOSAL_LINEITEM_REQ_PATTERN = re.compile(r"/v1(/gam)?/proposallineitem(/[0-9]*)?")


@xray_recorder.capture("lambda_handler.handler")
@inject
def handler(event: dict, context: dict):
    logger.info(f"event: {event}")
    logger.info(f"context: {context}")
    headers = event.get("headers", {})
    path = event.get("transactionContext", {}).get("path")
    logger.info(f"path: {path}")
    if path is None:
        return BadRequest("400001", "Invalid Path from Event", headers).get_response()

    try:
        if re.match(LINEITEM_REQ_PATTERN, path):
            logger.info("gam lineitem request")
            return handle_lineitem_requests(event).get_response()
        elif re.match(ORDER_REQ_PATTERN, path):
            logger.info("gam order request")
            return handle_order_requests(event).get_response()
        elif re.match(FORECAST_REQ_PATTERN, path):
            logger.info("gam forecast request")
            return handle_forecast_requests(event).get_response()
        elif re.match(PROPOSAL_LINEITEM_REQ_PATTERN, path):
            logger.info("gam proposal line item request")
            return handle_proposal_lineitem_requests(event).get_response()
        elif re.match(PROPOSAL_REQ_PATTERN, path):
            logger.info("gam proposal request")
            return handle_proposal_requests(event).get_response()
        else:
            return BadRequest(
                "400001", "Invalid Path from Event", headers
            ).get_response()
    except (GuardException, GoogleAdsServerFault) as e:
        logger.exception(f"Bad request - {str(e)}")
        return BadRequest("400001", e, headers).get_response()
    except Exception as e:
        logger.exception(f"Server error - {str(e)}")
        return ServerError("500001", e, headers).get_response()
    finally:
        logging.shutdown()


@xray_recorder.capture("lambda_handler.handle_lineitem_requests")
def handle_lineitem_requests(
    event: dict,
    lineitem_client: LineItemClient = Provide[Container.lineitem_api_client],
) -> SuccessResponse:
    headers = event.get("headers", {})
    method = event.get("transactionContext", {}).get("method", "")

    return lineitem_client.process_lineitems(event)


@xray_recorder.capture("lambda_handler.handle_order_requests")
def handle_order_requests(
    event: dict, order_client: OrderClient = Provide[Container.order_api_client]
) -> Union[SuccessResponse, BadRequest]:
    headers = event.get("headers", {})
    query_string_parameters = event.get("queryStringParameters", {})
    method = event.get("transactionContext", {}).get("method", "")
    path = event.get("transactionContext", {}).get("path")

    if method == "POST":
        body = event.get("body")
    elif method == "PUT":
        action = query_string_parameters.get("action")
        if action is None:
            return BadRequest("400001", "Missing queryStringParameters action", headers)

        id_ = path.split("/")[-1]
        if id_ is None:
            return BadRequest(
                "400001", "Missing Order ID in path", headers
            ).get_response()

        body = {"action": action, "id": id_}
        event["body"] = body
    elif method == "GET":
        return order_client.get_orders(event)
    else:
        return BadRequest("400001", "Invalid Method from Event", headers)

    if body is not None and body.get("action") is not None:
        return order_client.perform_order_action(event)
    return order_client.create_orders(event)


@xray_recorder.capture("lambda_handler.handle_forecast_requests")
def handle_forecast_requests(
    event: dict,
    forecast_client: ForecastClient = Provide[Container.forecast_api_client],
) -> SuccessResponse:
    return forecast_client.get_forecast(event)


@xray_recorder.capture("lambda_handler.handle_proposal_requests")
def handle_proposal_requests(
    event: dict,
    proposal_client: ProposalClient = Provide[Container.proposal_api_client],
) -> Union[SuccessResponse, BadRequest]:
    headers = event.get("headers", {})
    query_string_parameters = event.get("queryStringParameters", {})
    method = event.get("transactionContext", {}).get("method", "")

    if method == "POST":
        return proposal_client.create_proposal(event)
    elif method == "PUT":
        action = query_string_parameters.get("action")
        if action is not None:
            return proposal_client.perform_proposal_action(event)
        return proposal_client.update_proposal(event)
    elif method == "GET":
        return proposal_client.get_proposals(event)
    else:
        return BadRequest("400001", "Invalid Method from Event", headers)


@xray_recorder.capture("lambda_handler.handle_proposal_lineitem_requests")
def handle_proposal_lineitem_requests(
    event: dict,
    proposal_lineitem_client: ProposalLineItemClient = Provide[
        Container.proposal_lineitem_api_client
    ],
) -> Union[SuccessResponse, BadRequest]:
    headers = event.get("headers", {})
    method = event.get("transactionContext", {}).get("method", "")
    query_string_parameters = event.get("queryStringParameters", {})

    if method == "POST":
        return proposal_lineitem_client.create_proposal_lineitem(event)
    elif method == "PUT":
        action = query_string_parameters.get("action")
        if action is not None:
            return proposal_lineitem_client.perform_proposal_lineitem_action(
                action, event
            )
        return proposal_lineitem_client.update_proposal_lineitem(event)
    elif method == "GET":
        return proposal_lineitem_client.get_proposal_lineitem(event)
    else:
        return BadRequest("400001", "Invalid Method from Event", headers)


if __name__ == "__main__":
    # This section is ignored when AWS activates the lambda.
    xray_recorder.begin_segment(__name__)  # Ensure xray has something to record against
    bootstrap(
        LineItemClient,
        LineItemClientMock,
        OrderClient,
        OrderClientMock,
        ForecastClient,
        ForecastClientMock,
        ProposalClient,
        ProposalClientMock,
        ProposalLineItemClient,
        ProposalLineItemClientMock,
        None,
        None,
        modules=[__name__],
    )
    print(handler(event=LINEITEM_SAMPLE_REQUEST, context={}))
    print(handler(event=ORDER_SAMPLE_REQUEST, context={}))
    print(handler(event=FORECAST_SAMPLE_REQUEST, context={}))
    print(handler(event=PROPOSAL_SAMPLE_REQUEST, context={}))
    print(handler(event=PROPOSAL_LINEITEM_SAMPLE_REQUEST, context={}))
