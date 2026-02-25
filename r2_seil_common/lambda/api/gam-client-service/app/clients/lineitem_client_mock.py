from gam_core.model.object_from_dict import ObjectFromDict
from app.clients.lineitem_client import LineItemClient

LINEITEMS_SAMPLE = [
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

LINEITEM_SAMPLE_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "body": {"lineItems": LINEITEMS_SAMPLE},
    "transactionContext": {
        "time": "2022-06-17T13:39:38.605875+1000",
        "correlationId": "postman-testing-1",
        "applicationLabel": "postman",
        "path": "/v1/gam/lineitem",
        "method": "POST",
    },
}

LINEITEM_SAMPLE_RESPONSE = {
    "id": 5062505,
    "orderId": 395527713,
    "name": "lineitem-name-1",
    "status": "DRAFT",
}

LINEITEM_SAMPLE_REQUEST_ACTION = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "transactionContext": {"method": "PUT", "path": "/v1/gam/lineitem/action"},
    "body": {
        "ids": [1234, 2345],
        "action": "archive",
        "rules": {"route": {"toGam": "true"}},
    },
}

LINEITEM_SAMPLE_REQUEST_ACTION_INVALID_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "transactionContext": {"method": "PUT", "path": "/v1/gam/lineitem/action"},
    "body": {
        "ids": [1234, 2345, "true; select * from users"],
        "action": "archive",
        "rules": {"route": {"toGam": "true"}},
    },
}

LINEITEM_SAMPLE_RESPONSE_ACTION = {"numChanges": 1}

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

    def createLineItems(self, request):
        return [ObjectFromDict(**LINEITEM_SAMPLE_RESPONSE)]

    def updateLineItems(self, lineItems):
        return LINEITEM_SAMPLE_RESPONSE

    def getLineItemsByStatement(self, request):
        return LINEITEM_SAMPLE_RESPONSE_STATEMENT

    def performLineItemAction(self, request, statement):
        return LINEITEM_SAMPLE_RESPONSE_ACTION
