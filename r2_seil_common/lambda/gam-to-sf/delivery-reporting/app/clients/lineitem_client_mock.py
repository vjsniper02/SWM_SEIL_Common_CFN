from app.clients.lineitem_client import LineItemClient

LINEITEM_SAMPLE_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "body": {
        "lineItems": [
            {
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
        ]
    },
    "transactionContext": {
        "time": "2022-06-17T13:39:38.605875+1000",
        "correlationId": "postman-testing-1",
        "applicationLabel": "postman",
        "path": "/v1/gam/lineitem",
        "method": "POST",
    },
}

LINEITEM_SAMPLE_RESPONSE_STATEMENT = {
    "results": [
        {
            "id": 5062505,
            "orderId": 395527713,
            "name": "lineitem-name-1",
            "status": "DRAFT",
            "isArchived": False,
        }
    ]
}


class LineItemClientMock(LineItemClient):
    def __init__(self, config):
        super().__init__(config, ad_manager_client=self)
        # self.cache = ZeepServiceProxy.NO_CACHE

    def GetService(self, service_name, version):
        return self

    def getLineItemsByStatement(self, request):
        return LINEITEM_SAMPLE_RESPONSE_STATEMENT
