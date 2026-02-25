# Represents a response from the Programmatic GuaranteedUpdateProposalLineItems API in Google Ad Manager.
import zeep


class UpdateProposalLineItemResponse:
    def __init__(self, proposal_lineitems):
        if len(proposal_lineitems) > 0 and isinstance(proposal_lineitems[0], dict):
            self.proposal_lineitems = proposal_lineitems
        else:
            self.proposal_lineitems = [
                zeep.helpers.serialize_object(p) for p in proposal_lineitems
            ]

    def get_response(self):
        return {"proposalLineitems": self.proposal_lineitems}
