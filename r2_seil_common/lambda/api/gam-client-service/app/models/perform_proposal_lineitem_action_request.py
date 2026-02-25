from googleads import ad_manager

from gam_core.model.guard_exception import GuardException


class PerformProposalLineitemActionRequest:
    def __init__(self, api_version: str, proposal_lineitem_ids: [int], action: str):
        for id_ in proposal_lineitem_ids:
            if not isinstance(id_, int):
                raise GuardException(
                    "Only integers are supported for proposalLineItemIds"
                )
        ids = ",".join(str(id_) for id_ in proposal_lineitem_ids)
        self.proposal_lineitem_action = {"xsi_type": action}
        self.statement = (
            ad_manager.StatementBuilder(version=api_version)
            .Where("id in (:ids)")
            .WithBindVariable("ids", proposal_lineitem_ids)
        )

    @property
    def get_proposal_lineitem_action(self):
        return self.proposal_lineitem_action

    @property
    def get_proposal_lineitem_statement(self):
        return self.statement
