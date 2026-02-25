import logging

from aws_xray_sdk.core import xray_recorder, patch_all
from dependency_injector.wiring import Provide, inject
from salesforce_core.container import SfContainer, bootstrap as sf_bootstrap

from app.clients.proposal_client import ProposalClient
from app.clients.proposal_client_mock import ProposalClientMock
from app.clients.lineitem_client import LineItemClient
from app.clients.lineitem_client_mock import LineItemClientMock
from app.clients.order_client import OrderClient
from app.clients.order_client_mock import OrderClientMock
from app.clients.report_client import ReportClient
from app.clients.report_client_mock import ReportClientMock
from app.models.sync_errors import SyncErrors
from gam_core.container import Container, bootstrap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lambda_handler")

patch_all()  # Patch AWS libs with xray


@xray_recorder.capture("lambda_handler.handler")
@inject
def handler(
    event: dict,
    context: dict,
    proposal_client: ProposalClient = Provide[Container.proposal_api_client],
    order_client: OrderClient = Provide[Container.order_api_client],
    lineitem_api_client: LineItemClient = Provide[Container.lineitem_api_client],
    report_api_client: ReportClient = Provide[Container.report_api_client],
    sf_bulk=Provide[SfContainer.sf_bulk],
    sf=Provide[SfContainer.sf],
):
    logger.info(f"Starting the request")

    errors = []

    logger.info("handle proposal sync")
    try:
        proposal_client.sync_proposals(sf_bulk)
    except Exception as e:
        errors.append(e)
        logger.error(f"An error occurred on proposal sync: {str(e)}")

    logger.info("handle orders sync")
    try:
        order_client.sync_orders(sf_bulk)
    except Exception as e:
        errors.append(e)
        logger.error(f"An error occurred on orders sync: {str(e)}")

    logger.info("handle lineitems sync")
    try:
        lineitem_api_client.sync_lineitems(sf_bulk)
    except Exception as e:
        errors.append(e)
        logger.error(f"An error occurred on line items sync: {str(e)}")

    logger.info("handle GAM reporting and usage")

    try:
        report_api_client.sync_actuals(sf, sf_bulk)
    except Exception as e:
        errors.append(e)
        logger.error(f"An error occurred on delivery actuals sync: {str(e)}")
        raise

    if errors:
        raise SyncErrors(errors)
    return {"success": True}


if __name__ == "__main__":
    # This section is ignored when AWS activates the lambda.
    xray_recorder.begin_segment(__name__)  # Ensure xray has something to record against
    bootstrap(
        LineItemClient,
        LineItemClientMock,
        OrderClient,
        OrderClientMock,
        None,
        None,
        ProposalClient,
        ProposalClientMock,
        None,
        None,
        ReportClient,
        ReportClientMock,
        modules=[__name__],
    )
    sf_bootstrap(modules=[__name__])
    handler(None, None)
