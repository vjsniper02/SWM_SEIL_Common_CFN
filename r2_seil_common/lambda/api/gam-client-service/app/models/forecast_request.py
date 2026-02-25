# Facilitates a GetAvailabilityForecastRequest to Google Ad Manager client.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
from gam_core.model.guard_exception import GuardException
from gam_core.model.line_item import LineItem


class ForecastRequest:
    def __init__(self, request):
        line_items = request.get("lineItems", [])
        if len(line_items) < 1:
            raise GuardException("No line items provided")
        if len(line_items) > 1:
            raise GuardException("Only single line item supported at this time")

        self.line_item = LineItem(**line_items[0].get("lineItem"))

    def get_request(self):
        return {"lineItem": self.line_item.get_request()}
