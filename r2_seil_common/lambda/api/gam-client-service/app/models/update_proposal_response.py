# Represents a response from the Programmatic GuaranteedUpdateProposal API in Google Ad Manager.
import zeep


class UpdateProposalResponse:
    def __init__(self, proposals):
        if len(proposals) > 0 and isinstance(proposals[0], dict):
            self.proposals = proposals
        else:
            self.proposals = [zeep.helpers.serialize_object(p) for p in proposals]

    def get_response(self):
        return {"proposals": self.proposals}
