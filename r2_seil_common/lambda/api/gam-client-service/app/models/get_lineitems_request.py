# Facilitates a GetLineItemsRequest request to Google Ad Manager client.
from gam_core.model.guard_exception import GuardException
from googleads import ad_manager


class GetLineItemsRequest:
    def __init__(self, apiVersion, line_item_id):
        if line_item_id is None:
            raise GuardException("Expected line items ID missing")

        self.statement = (
            ad_manager.StatementBuilder(version=apiVersion)
            .Where(("id = :lineItemId"))
            .WithBindVariable("lineItemId", line_item_id)
        )

    def get_lineItems_statement(self):
        return self.statement
