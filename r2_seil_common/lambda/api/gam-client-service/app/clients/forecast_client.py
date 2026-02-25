from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration
from gam_core.model.guard_exception import GuardException
from gam_core.model.success_response import SuccessResponse
from googleads import AdManagerClient
from googleads.common import ZeepServiceProxy

from app.models.forecast_request import ForecastRequest
from app.models.forecast_response import ForecastResponse


class ForecastClient:
    def __init__(self, config: Configuration, ad_manager_client: AdManagerClient):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.client = ad_manager_client.GetService(
            "ForecastService", config["gam_api_version"]
        )

    @xray_recorder.capture("client.get_forecast")
    def get_forecast(self, request: dict) -> SuccessResponse:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})

        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        body = request.get("body", {})
        if len(body.keys()) < 1:
            raise GuardException("Expected body is missing")

        forecast_request = ForecastRequest(body)
        forecast_options = body.get(
            "forecastOptions", {}
        )  # not currently supported by allow passing in
        forecast = self.client.getAvailabilityForecast(
            forecast_request.get_request(), forecast_options
        )
        forecast_response = ForecastResponse(forecast).get_response()

        return SuccessResponse(headers=headers, **forecast_response)
