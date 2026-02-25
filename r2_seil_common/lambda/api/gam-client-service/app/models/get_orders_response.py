# Represents a response from the getOrdersByStatement API in Google Ad Manager.
import zeep


class GetOrdersResponse:
    def __init__(self, orders):
        if len(orders) > 0 and isinstance(orders[0], dict):
            self.orders = orders
        else:
            self.orders = [zeep.helpers.serialize_object(p) for p in orders]

    def get_response(self):
        return {"orders": self.orders}
