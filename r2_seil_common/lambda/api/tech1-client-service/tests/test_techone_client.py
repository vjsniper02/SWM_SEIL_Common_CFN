import pytest
from unittest import mock
from aws_xray_sdk.core import xray_recorder

from app.techone_client import TechOneClient
from core.model.success_response import SuccessResponse


SAMPLE_RESPONSE = {
    "DataSet": [
        {
            "MAJOR_CCY_CODE": "AUD",
            "TRANS_CCY_CODE": "USD",
            "END_DATEI": None,
            "EXCH_RATE_AMT": 0.717526,
            "START_DATEI": "2021-12-13",
        }
    ],
    "TotalRecordCount": 1,
    "Key": "094c0a24-9c8a-44a0-8f79-3b4e2c8b7b62",
    "Messages": [],
}

REMAINING_CREDITLIMIT_SAMPLE_RESPONSE = {
    "Data": {
        "RunId": "00000000-0000-0000-0000-000000000000",
        "RunType": "",
        "RunType_Desc": "",
        "DpJobId": 0,
        "DpJobId_Desc": "",
        "Variables": {},
        "Steps": [
            {
                "SuiteId": "CES",
                "StepReference": "GetDebtor",
                "Entity": "F1.CHARTACCOUNT.AR",
                "ServiceId": "cda42a66-f2ea-403a-9de4-b7b07adeb3bb",
                "Name": "GetDebtor",
            },
            {
                "SuiteId": "CES",
                "StepReference": "GetTransactions",
                "Entity": "F1.LDG.ACCT.TRANS.AR",
                "ServiceId": "00000000-0000-0000-0000-000000000000",
                "Name": "GetTransactions",
            },
        ],
        "InputData": [],
        "InputVariables": [],
        "CustomFunctions": [],
    },
    "System": "DEFAULT",
    "System_Desc": "",
    "Name": "DEBTOR_REMAINING_CREDIT",
    "Name_Desc": "",
    "Key": "5684d64bae074316b2f4be7372a81d7c",
    "Messages": [
        {
            "Code": "BPPR0140",
            "Message": "RemainingCreditLimit=1713.56",
            "NotificationType": "Information",
        }
    ],
}


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse(SAMPLE_RESPONSE, 200)


def mocked_requests_post(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse(REMAINING_CREDITLIMIT_SAMPLE_RESPONSE, 200)


def mocked_token_get(*args, **kwargs):
    return "token"


@pytest.fixture(scope="module")
def arrange():
    config = {
        "tech1_api_scheme": "https",
        "tech1_api_host": "tech1_api_host",
        "tech1_api_fx_path": "tech1_api_fx_path",
        "tech1_api_debtor_creditlimit_path": "tech1_api_debtor_creditlimit_path",
        "tech1_api_timeout": 20,
    }
    # auth_client = create_autospec(ClientAuthToken, return_value='token')
    xray_recorder.begin_segment("test_client.arrange")

    yield config


@mock.patch("app.techone_client.requests.get", side_effect=mocked_requests_get)
@mock.patch(
    "auth_client.client_credentials.ClientAuthToken", side_effect=mocked_token_get
)
def test_result_returned(ClientAuthTokenMock, mock_get, arrange):
    (config) = arrange
    headers = {"Accept": "application/json"}
    techone_client = TechOneClient(config, ClientAuthTokenMock)
    result = techone_client.get_foreign_exchange_rate(headers)
    print(result)

    assert type(result) is SuccessResponse
    assert result.get_response()["code"] == 200
    assert (
        result.get_response()["data"]["exch_rate_amt"]
        == SAMPLE_RESPONSE["DataSet"][0]["EXCH_RATE_AMT"]
    )


@mock.patch("app.techone_client.requests.post", side_effect=mocked_requests_post)
@mock.patch(
    "auth_client.client_credentials.ClientAuthToken", side_effect=mocked_token_get
)
def test_creditlimit(ClientAuthTokenMock, mock_post, arrange):
    (config) = arrange
    headers = {"Accept": "application/json"}
    debtor_number = "360DEG30"
    techone_client = TechOneClient(config, ClientAuthTokenMock)
    result = techone_client.get_debtor_remaining_credit_limit(debtor_number, headers)
    print(result)

    assert type(result) is SuccessResponse
    assert result.get_response()["code"] == 200
    assert (
        result.get_response()["data"]["remaining_credit_limit"]
        == REMAINING_CREDITLIMIT_SAMPLE_RESPONSE["Messages"][0]["Message"].split("=")[1]
    )
