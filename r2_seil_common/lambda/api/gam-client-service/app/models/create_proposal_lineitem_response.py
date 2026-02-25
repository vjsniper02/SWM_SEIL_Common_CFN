# Represents a response from the Programmatic Guaranteed CreateProposalLineItems API in Google Ad Manager.
import zeep


class CreateProposalLineItemResponse(dict):
    def __init__(self, proposal_lineitems):
        if len(proposal_lineitems) > 0 and isinstance(proposal_lineitems[0], dict):
            self.proposal_lineitems = proposal_lineitems
        else:
            self.proposal_lineitems = [
                zeep.helpers.serialize_object(p) for p in proposal_lineitems
            ]

    def get_response(self):
        return {"proposalLineitems": self.proposal_lineitems}
