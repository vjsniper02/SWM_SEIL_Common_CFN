# Facilitates a getProposalsByStatement request to Google Ad Manager client.
from gam_core.model.guard_exception import GuardException
from googleads import ad_manager


class GetProposalLineItemRequest:
    def __init__(self, api_version, request_params):
        proposal_id = request_params.get("id")
        if proposal_id is None:
            raise GuardException(f"The proposal id field required")

        self.statement = (
            ad_manager.StatementBuilder(version=api_version)
            .Where(("proposalId = :proposalId"))
            .WithBindVariable("proposalId", proposal_id)
        )

    def get_proposal_lineitem_statement(self):
        return self.statement
