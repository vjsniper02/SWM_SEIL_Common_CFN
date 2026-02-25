# Represents a single line item in Google Ad Manager.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.


class ForecastResponse:
    def __init__(self, forecast):
        self.forecast = forecast

    def get_response(self):
        return {
            "lineItems": [
                {
                    "lineItemId": self.forecast.lineItemId,
                    "orderId": self.forecast.orderId,
                    "unitType": self.forecast.unitType,
                    "availableUnits": self.forecast.availableUnits,
                    "deliveredUnits": self.forecast.deliveredUnits,
                    "matchedUnits": self.forecast.matchedUnits,
                    "possibleUnits": self.forecast.possibleUnits,
                    "reservedUnits": self.forecast.reservedUnits,
                }
            ]
        }
