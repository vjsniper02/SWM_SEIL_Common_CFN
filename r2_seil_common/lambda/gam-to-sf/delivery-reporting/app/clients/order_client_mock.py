from app.clients.order_client import OrderClient

ORDER_SAMPLE_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "body": {
        "orders": [
            {
                "name": "TOrder111",
                "startDateTime": {
                    "date": {"year": 2022, "month": 2, "day": 15},
                    "hour": 0,
                    "minute": 0,
                    "second": 0,
                    "timeZoneId": "America/Los_Angeles",
                },
                "notes": "string",
                "externalOrderId": 1234,
                "poNumber": "tbd",
                "currencyCode": "AUD",
                "advertiserId": "TODO: use test-only company",
                "traffickerId": "TODO: use test-only user",
                "totalBudget": {"currencyCode": "AUD", "microAmount": 1000000},
            }
        ],
    },
    "transactionContext": {
        "time": "2022-06-17T13:39:38.605875+1000",
        "correlationId": "postman-testing-1",
        "applicationLabel": "postman",
        "path": "/v1/gam/order",
        "method": "POST",
    },
}

ORDER_SAMPLE_RESPONSE = {
    "results": [{"id": "5062505", "name": "order-name-1", "status": "PAUSED"}]
}


class OrderClientMock(OrderClient):
    def __init__(self, config):
        super().__init__(config, ad_manager_client=self)

    def GetService(self, service_name, version):
        return self

    def getOrdersByStatement(self, request):
        return ORDER_SAMPLE_RESPONSE
