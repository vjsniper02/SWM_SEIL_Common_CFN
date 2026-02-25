from dependency_injector.providers import Configuration
from gam_core.model.object_from_dict import ObjectFromDict
from googleads.common import ZeepServiceProxy

from app.clients.forecast_client import ForecastClient

FORECAST_SAMPLE_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "body": {
        "lineItems": [
            {
                "lineItem": {
                    "orderId": "395527713",
                    "name": "Print Banner",
                    "startDateTime": {
                        "date": {"year": "2022", "month": "6", "day": "12"},
                        "hour": "0",
                        "minute": "0",
                        "second": "0",
                        "timeZoneId": "America/Los_Angeles",
                    },
                    "endDateTime": {
                        "date": {"year": "2022", "month": "6", "day": "25"},
                        "hour": "23",
                        "minute": "59",
                        "second": "59",
                        "timeZoneId": "America/Los_Angeles",
                    },
                    "startDateTimeType": "USE_START_DATE_TIME",
                    "unlimitedEndDateTime": "false",
                    "lineItemType": "STANDARD",
                    "costPerUnit": {"currencyCode": "AUD", "microAmount": "200"},
                    "costType": "CPM",
                    "creativePlaceholders": [
                        {"size": {"width": 100, "height": 100}, "isAmpOnly": "true"}
                    ],
                    "primaryGoal": {
                        "goalType": "EVENLY",
                        "unitType": "IMPRESSIONS",
                        "units": 20,
                    },
                    "targeting": {
                        "geoTargeting": {"targetedLocations": [{"id": "2276"}]},
                        "inventoryTargeting": {
                            "targetedAdUnits": [
                                {"adUnitId": "64670553", "includeDescendants": "true"}
                            ]
                        },
                    },
                    "rules": {"route": {"toGam": "true"}},
                }
            }
        ]
    },
    "transactionContext": {
        "time": "2022-06-17T13:39:38.605875+1000",
        "correlationId": "postman-testing-1",
        "applicationLabel": "postman",
        "path": "/v1/gam/forecast",
        "method": "POST",
    },
}

FORECAST_SAMPLE_RESPONSE = {
    "lineItemId": "MockLineItemId",
    "orderId": "MockOrderId",
    "unitType": "IMPRESSIONS",
    "availableUnits": 0,
    "deliveredUnits": 33078,
    "matchedUnits": 0,
    "possibleUnits": 0,
    "reservedUnits": 100,
    "breakdowns": [],
    "targetingCriteriaBreakdowns": [],
    "contendingLineItems": [],
    "alternativeUnitTypeForecasts": [
        {
            "unitType": "IMPRESSIONS",
            "matchedUnits": 0,
            "availableUnits": 0,
            "possibleUnits": 0,
        },
        {
            "unitType": "VIEWABLE_IMPRESSIONS",
            "matchedUnits": 0,
            "availableUnits": 0,
            "possibleUnits": 0,
        },
    ],
    "demographicBreakdowns": [],
}


class ForecastClientMock(ForecastClient):
    def __init__(self, config: Configuration, *args):
        super().__init__(config, ad_manager_client=self)
        self.cache = ZeepServiceProxy.NO_CACHE

    def GetService(self, service_name, version):
        return self

    def getAvailabilityForecast(self, forecast_request, forecast_options):
        return ObjectFromDict(**FORECAST_SAMPLE_RESPONSE)
