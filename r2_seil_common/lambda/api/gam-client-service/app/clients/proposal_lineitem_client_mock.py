from app.clients.proposal_lineitem_client import ProposalLineItemClient

PROPOSAL_LINEITEM_SAMPLE_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "body": {
        "proposalLineitems": [
            {
                "proposalId": 22860381,
                "name": "Test | 2",
                "deliveryRateType": "EVENLY",
                "startDateTime": {
                    "date": {"year": 2021, "month": 11, "day": 18},
                    "hour": 16,
                    "minute": 50,
                    "second": 0,
                    "timeZoneId": "Australia/Sydney",
                },
                "endDateTime": {
                    "date": {"year": 2022, "month": 2, "day": 1},
                    "hour": 2,
                    "minute": 59,
                    "second": 0,
                    "timeZoneId": "Australia/Sydney",
                },
                "netRate": {"currencyCode": "USD", "microAmount": "2000000"},
                "targeting": {
                    "inventoryTargeting": {"targetedAdUnits": {"adUnitId": 61675953}},
                    "technologyTargeting": {
                        "deviceCapabilityTargeting": {
                            "targetedDeviceCapabilities": [],
                            "excludedDeviceCapabilities": [
                                {"id": 5005, "name": "Mobile Apps"}
                            ],
                        }
                    },
                },
                "creativePlaceholders": [
                    {"size": {"width": "300", "height": "250"}},
                    {"size": {"width": "120", "height": "600"}},
                ],
                "rateType": "CPM",
                "lineItemType": "STANDARD",
            }
        ],
    },
    "transactionContext": {
        "time": "2022-06-17T13:39:38.605875+1000",
        "correlationId": "postman-testing-1",
        "applicationLabel": "postman",
        "path": "/v1/gam/proposallineitem",
        "method": "POST",
    },
}

PROPOSAL_LINEITEM_SAMPLE = {
    "id": 196230046,
    "proposalId": 22860381,
    "name": "Test | 2",
    "deliveryRateType": "EVENLY",
    "startDateTime": {
        "date": {"year": 2021, "month": 11, "day": 18},
        "hour": 16,
        "minute": 50,
        "second": 0,
        "timeZoneId": "Australia/Sydney",
    },
    "endDateTime": {
        "date": {"year": 2022, "month": 2, "day": 1},
        "hour": 2,
        "minute": 59,
        "second": 0,
        "timeZoneId": "Australia/Sydney",
    },
    "netRate": {"currencyCode": "USD", "microAmount": "2000000"},
    "targeting": {
        "inventoryTargeting": {"targetedAdUnits": {"adUnitId": 61675953}},
        "technologyTargeting": {
            "deviceCapabilityTargeting": {
                "targetedDeviceCapabilities": [],
                "excludedDeviceCapabilities": [{"id": 5005, "name": "Mobile Apps"}],
            }
        },
    },
    "creativePlaceholders": [
        {"size": {"width": "300", "height": "250"}},
        {"size": {"width": "120", "height": "600"}},
    ],
    "rateType": "CPM",
    "lineItemType": "STANDARD",
    "isArchived": False,
}

PERFORM_PROPOSAL_LINEITEM_ACTION_SAMPLE_REQUEST = {
    "headers": {"Content-Type": "application/json", "X-Application-Label": "testing"},
    "body": {
        "proposalLineItemIds": [1],
    },
}

PERFORM_PROPOSAL_LINEITEM_ACTION_SAMPLE = {"numChanges": 1}


class ProposalLineItemClientMock(ProposalLineItemClient):
    def __init__(self, config):
        super().__init__(config, ad_manager_client=self)

    def GetService(self, service_name, version):
        return self

    def getProposalLineItemsByStatement(self, get_proposal_lineitem_request):
        return {"results": [PROPOSAL_LINEITEM_SAMPLE]}

    def createProposalLineItems(self, create_proposal_lineitems_request):
        return [PROPOSAL_LINEITEM_SAMPLE]

    def updateProposalLineItems(self, update_proposal_lineitems_request):
        return [PROPOSAL_LINEITEM_SAMPLE]

    def performProposalLineItemAction(self, action, statement):
        return PERFORM_PROPOSAL_LINEITEM_ACTION_SAMPLE
