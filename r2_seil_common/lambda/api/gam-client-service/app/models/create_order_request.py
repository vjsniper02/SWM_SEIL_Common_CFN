# Facilitates a CreateOrder request to Google Ad Manager client.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
from gam_core.model.guard_exception import GuardException
from gam_core.model.order import Order


class CreateOrderRequest:
    def __init__(self, request_body):
        orders = request_body.get("orders", [])
        if len(orders) < 1:
            raise GuardException("No orders provided")
        if len(orders) > 1:
            raise GuardException("Only single order supported at this time")

        self.order = Order(**orders[0])

    def get_request(self):
        return [self.order.get_request()]
