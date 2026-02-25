# Facilitates a Programmatic Guaranteed CreateProposal request to Google Ad Manager client.
import logging
from gam_core.model.guard_exception import GuardException
from gam_core.model.proposal import Proposal


class CreateProposalRequest:
    def __init__(self, request_body):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"request_body: {request_body}")
        proposals = request_body.get("proposals", [])
        if len(proposals) < 1:
            raise GuardException("No proposal provided")
        if len(proposals) > 1:
            raise GuardException("Only single proposal supported at this time")
        self.logger.info(f"proposals: {proposals}")
        proposal_name = proposals[0].get("name")
        if proposal_name is None:
            raise GuardException("No proposal name provided")

        self.proposal = Proposal(**proposals[0])

    def get_request(self):
        return [self.proposal.get_request()]
