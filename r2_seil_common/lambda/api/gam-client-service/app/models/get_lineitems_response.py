# Represents a response from the GetLineItemsResponse API in Google Ad Manager.
import zeep


class GetLineItemsResponse:
    def __init__(self, line_items):
        if len(line_items) > 0 and isinstance(line_items[0], dict):
            self.line_items = line_items
        else:
            self.line_items = [zeep.helpers.serialize_object(p) for p in line_items]

    def get_response(self):
        return {"lineItems": self.line_items}
