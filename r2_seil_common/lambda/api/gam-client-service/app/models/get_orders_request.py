# Facilitates a getOrdersByStatement request to Google Ad Manager client.
from gam_core.model.guard_exception import GuardException
from googleads import ad_manager


class GetOrdersRequest:
    def __init__(self, apiVersion, request_params):
        orderId = request_params.get("id")
        if orderId is None:
            raise GuardException(f"The orderId field required")

        self.statement = (
            ad_manager.StatementBuilder(version=apiVersion)
            .Where(("id = :orderId"))
            .WithBindVariable("orderId", orderId)
        )

    def get_order_statement(self):
        return self.statement
