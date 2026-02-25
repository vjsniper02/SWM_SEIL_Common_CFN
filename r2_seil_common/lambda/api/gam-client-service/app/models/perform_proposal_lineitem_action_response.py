class PerformProposalLineItemResponse:
    def __init__(self, number_of_updates):
        self.number_of_updates = number_of_updates

    def get_response(self):
        return {"numChanges": self.number_of_updates}
