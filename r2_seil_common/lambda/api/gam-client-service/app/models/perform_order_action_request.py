# Facilitates a performOrderAction request to Google Ad Manager client.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.

from googleads import ad_manager


class PerformOrderActionRequest:
    def __init__(self, apiVersion, orderId, action):
        self.order_action = {"xsi_type": action}
        self.statement = (
            ad_manager.StatementBuilder(version=apiVersion)
            .Where("id = :orderId")
            .WithBindVariable("orderId", orderId)
        )

    def get_order_action(self):
        return self.order_action

    def get_order_statement(self):
        return self.statement
