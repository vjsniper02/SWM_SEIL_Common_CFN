# Facilitates a UpdateLineItem request to Google Ad Manager client.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
from gam_core.model.guard_exception import GuardException

from app.models.lineitem_update import LineItemUpdate


class UpdateLineItemsRequest:
    def __init__(self, request):
        request_line_items = request.get("lineItems", [])
        if len(request_line_items) < 1:
            raise GuardException("No line items provided")
        line_items = {}
        for request_line_item in request_line_items:
            lineitem_delta = LineItemUpdate(**request_line_item).get_request()
            line_items[lineitem_delta["id"]] = lineitem_delta
        self.lineitem_deltas = line_items

    def get_request(self):
        return self.lineitem_deltas
