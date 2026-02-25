# Facilitates a PerformProposalAction request to Google Ad Manager client.
from gam_core.model.guard_exception import GuardException
from googleads import ad_manager


class PerformProposalActionRequest:
    def __init__(self, api_version, proposal_id, action):
        self.proposal_action = {"xsi_type": action}
        self.statement = (
            ad_manager.StatementBuilder(version=api_version)
            .Where(("id = :proposalId"))
            .WithBindVariable("proposalId", proposal_id)
        )

    def get_proposal_action(self):
        return self.proposal_action

    def get_proposal_statement(self):
        return self.statement
