from app.clients.proposal_client import ProposalClient

PROPOSAL_SAMPLE_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "body": {
        "proposals": [
            {
                "id": "1234",
                "name": "MockProposal1",
                "isProgrammatic": True,
                "internalNotes": "Proposal additional notes",
                "currencyCode": "AUD",
                "primaryTraffickerId": "TODO: use test-only user",
                "salesPlannerIds": "TODO: use test-only company",
                "isSold": False,
            }
        ],
    },
    "transactionContext": {
        "time": "2022-06-17T13:39:38.605875+1000",
        "correlationId": "postman-testing-1",
        "applicationLabel": "postman",
        "path": "/v1/gam/proposal",
        "method": "POST",
    },
}

PROPOSAL_SAMPLE = {
    "id": "1234",
    "name": "MockProposal1",
    "isProgrammatic": True,
    "dfpOrderId": None,
    "internalNotes": "Proposal additional notes",
    "status": "DRAFT",
    "isArchived": False,
    "startDateTime": {
        "date": {"year": 2022, "month": 2, "day": 15},
        "hour": 0,
        "minute": 0,
        "second": 0,
        "timeZoneId": "America/Los_Angeles",
    },
    "endDateTime": {
        "date": {"year": 2022, "month": 2, "day": 15},
        "hour": 0,
        "minute": 0,
        "second": 0,
        "timeZoneId": "America/Los_Angeles",
    },
    "currencyCode": "AUD",
    "primaryTraffickerId": "TODO: use test-only user",
    "salesPlannerIds": "TODO: use test-only company",
    "isSold": False,
}


class ProposalClientMock(ProposalClient):
    def __init__(self, config):
        super().__init__(config, ad_manager_client=self)
        # self.cache = ZeepServiceProxy.NO_CACHE

    def GetService(self, service_name, version):
        return self

    def getLineItemsByStatement(self, request):
        return PROPOSAL_SAMPLE
