# Represents a response from the getProposalsByStatement API in Google Ad Manager.
import zeep


class GetProposalLineItemResponse:
    def __init__(self, proposal_lineitems):
        if len(proposal_lineitems) > 0 and isinstance(proposal_lineitems[0], dict):
            self.proposal_lineitems = proposal_lineitems
        else:
            self.proposals = [
                zeep.helpers.serialize_object(p) for p in proposal_lineitems
            ]

    def get_response(self):
        return {"proposal_lineitems": self.proposals}
