from gam_core.container import bootstrap

from app.clients.lineitem_client import LineItemClient
from app.clients.lineitem_client_mock import LineItemClientMock
from app.clients.order_client import OrderClient
from app.clients.order_client_mock import OrderClientMock
from app.clients.forecast_client import ForecastClient
from app.clients.forecast_client_mock import ForecastClientMock
from app.clients.proposal_client import ProposalClient
from app.clients.proposal_client_mock import ProposalClientMock
from app.clients.proposal_lineitem_client import ProposalLineItemClient
from app.clients.proposal_lineitem_client_mock import ProposalLineItemClientMock
from app.lambda_handler import handler

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
    modules=[handler.__module__],
)
