# Represents a response from the Create Order API in Google Ad Manager.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
class UpdateLineItemsResponse:
    def __init__(self, number_of_updates):
        self.number_of_updates = number_of_updates

    def get_response(self):
        return {"numChanges": self.number_of_updates}
