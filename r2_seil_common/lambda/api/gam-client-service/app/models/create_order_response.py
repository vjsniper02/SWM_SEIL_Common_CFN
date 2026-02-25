# Represents a response from the Create Order API in Google Ad Manager.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
class CreateOrderResponse:
    def __init__(self, orders):
        self.orders = orders

    def get_response(self):
        return {
            "orders": [
                {"id": order.id, "name": order.name, "status": order.status}
                for order in self.orders
            ]
        }
