from dependency_injector.providers import Configuration

from core.model.success_response import SuccessResponse
from app.techone_client import TechOneClient

SAMPLE_FX_RESPONSE = {
    "major_ccy_code": "AUD",
    "trans_ccy_code": "USD",
    "end_datei": None,
    "exch_rate_amt": 0.717526,
    "start_datei": "2021-12-13",
}


class TechOneClientMock(TechOneClient):
    def __init__(self, config: Configuration):
        super().__init__(config, tech1_auth_client=self)

    def get_foreign_exchange_rate(self, headers) -> SuccessResponse:
        return SuccessResponse(headers, **SAMPLE_FX_RESPONSE)
