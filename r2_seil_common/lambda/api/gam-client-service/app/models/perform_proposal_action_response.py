# Represents a response from the PerformProposalAction API in Google Ad Manager.
class PerformProposalActionResponse:
    def __init__(self, result):
        num_changes = 0
        if result and int(result["numChanges"]) > 0:
            num_changes += int(result["numChanges"])
        self.proposals_pushed_to_marketplace = num_changes

    def get_response(self):
        return {"numChanges": self.proposals_pushed_to_marketplace}
