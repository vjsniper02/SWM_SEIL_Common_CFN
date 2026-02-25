# Represents a response from the Create Order API in Google Ad Manager.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
class CreateLineItemsResponse:
    def __init__(self, line_items):
        self.line_items = line_items

    def get_response(self):
        return {
            "lineItems": [
                {
                    "id": line_item.id,
                    "orderId": line_item.orderId,
                    "name": line_item.name,
                    "status": line_item.status,
                }
                for line_item in self.line_items
            ]
        }
