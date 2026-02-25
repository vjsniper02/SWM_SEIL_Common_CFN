# Facilitates a Programmatic Guaranteed CreateProposalLineItems request to Google Ad Manager client.
import logging
from gam_core.model.guard_exception import GuardException
from gam_core.model.proposal_lineitem import ProposalLineItem

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)


class CreateProposalLineItemRequest:
    def __init__(self, request_body):
        logger.info(f"request_body: {request_body}")
        request_proposal_lineitems = request_body.get("proposalLineitems", [])
        if len(request_proposal_lineitems) < 1:
            raise GuardException("No proposal line items provided")
        logger.info(f"request_proposal_lineitems: {request_proposal_lineitems}")

        proposal_lineitems = []
        for request_proposal_lineitem in request_proposal_lineitems:
            proposal_lineitems.append(
                ProposalLineItem(**request_proposal_lineitem).get_request()
            )

        self.proposal_lineitems = proposal_lineitems

    def get_request(self):
        return self.proposal_lineitems
