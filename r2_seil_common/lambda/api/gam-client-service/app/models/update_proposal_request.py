# Facilitates a Programmatic Guaranteed UpdatePoposal request to Google Ad Manager client.
import logging
from gam_core.model.guard_exception import GuardException
from googleads import ad_manager
from gam_core.model.proposal import Proposal

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)


class UpdateProposalRequest:
    def __init__(self, api_version, proposal_id, request_body):
        logger.info(f"request_body: {request_body}")
        logger.info(f"proposal_id: {proposal_id}")
        proposals = request_body.get("proposals", [])
        if len(proposals) < 1:
            raise GuardException("No proposal provided")
        if len(proposals) > 1:
            raise GuardException("Only single proposal supported at this time")

        logger.info(f"proposals: {proposals}")
        self.proposal = Proposal(**proposals[0])
        self.statement = (
            ad_manager.StatementBuilder(version=api_version)
            .Where(("id = :proposalId"))
            .WithBindVariable("proposalId", proposal_id)
        )

    def get_proposal(self):
        return self.proposal

    def update_proposal(self, old_proposal):
        try:
            updated_proposal = {"id": old_proposal["id"]}
            if not old_proposal["isArchived"]:
                proposal = self.proposal.get_request()
                for key in proposal.keys():
                    if proposal[key] != "" and proposal[key] != None:
                        updated_proposal[key] = proposal[key]
        except:
            raise GuardException("Invalid proposal to update")
        return updated_proposal

    def get_proposal_statement(self):
        return self.statement
