from gam_core.container import bootstrap as gam_core_bootstrap
from salesforce_core.container import bootstrap as salesforce_core_bootstrap

from app.clients.proposal_client import ProposalClient
from app.clients.proposal_client_mock import ProposalClientMock
from app.clients.order_client import OrderClient
from app.clients.order_client_mock import OrderClientMock
from app.clients.lineitem_client import LineItemClient
from app.clients.lineitem_client_mock import LineItemClientMock
from app.clients.report_client import ReportClient
from app.clients.report_client_mock import ReportClientMock
from app.lambda_handler import handler

gam_core_bootstrap(
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
    modules=[handler.__module__],
)

salesforce_core_bootstrap(
    modules=[handler.__module__],
)
