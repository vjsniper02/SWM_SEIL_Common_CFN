from datetime import date, timedelta

from app import ReportClient
from app.clients.report_client import ReportKey, ReportValue

MOCK_DAYS = (date.today().isoformat(), (date.today() - timedelta(days=1)).isoformat())


class AdManagerMock:
    def GetDataDownloader(self, **kwargs):
        return None


class MockGetSfUsage:
    def __init__(self):
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count += 1
        return {
            f"sf{self.count}1": "123",
            f"sf{self.count}2": "456",
            "no_gam_id": None,
            "invalid_gam_id": "1111",
        }


def mock_get_report(*args, **kwargs):
    return {
        ReportKey("123", MOCK_DAYS[0]): ReportValue(1, 2),
        ReportKey("456", MOCK_DAYS[0]): ReportValue(3, 4),
        ReportKey("not_in_sf", MOCK_DAYS[0]): ReportValue(0, 0),
        ReportKey("123", MOCK_DAYS[1]): ReportValue(5, 6),
        ReportKey("456", MOCK_DAYS[1]): ReportValue(7, 8),
    }


def mock_update_sf(self, sf_bulk, updates):
    assert updates == [
        {"Ad_Clicks__c": 2, "Ad_Impression__c": 1, "Id": "sf11"},
        {"Ad_Clicks__c": 4, "Ad_Impression__c": 3, "Id": "sf12"},
        {"Ad_Clicks__c": 6, "Ad_Impression__c": 5, "Id": "sf21"},
        {"Ad_Clicks__c": 8, "Ad_Impression__c": 7, "Id": "sf22"},
    ]


def test_sync_actuals(mocker):
    report_client = ReportClient({"gam_api_version": None}, AdManagerMock())
    mocker.patch("app.ReportClient.get_sf_query", lambda self, sf_bulk: [None, None])
    mocker.patch("app.ReportClient.get_sf_usage_items", MockGetSfUsage())
    mocker.patch("app.ReportClient.get_report", mock_get_report)
    mocker.patch("app.ReportClient.update_sf", mock_update_sf)

    report_client.sync_actuals(None, None)
