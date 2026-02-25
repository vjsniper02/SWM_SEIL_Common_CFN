import json
import logging

from aws_xray_sdk.core import xray_recorder, patch_all
from dependency_injector.wiring import Provide, inject
from core.container import Container, bootstrap
from core.client import ForecastClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)
patch_all()  # Patch AWS libs with xray


@xray_recorder.capture("business logic")
@inject
def handler(
    event,
    context,
    forecast_service: ForecastClient = Provide[Container.forecast_client],
):

    forecast_result = forecast_service.get_forecast(context)


@xray_recorder.capture("lamdba_handler.handler")
def handler(event, context):
    return json.loads(
        json.dumps(
            {
                "statusCode": 200,
                "body": {
                    "fn": "dev-demo-hello",
                    "payload": {"result": event},
                },
            },
            default=str,
        )
    )


if __name__ == "__main__":
    # This section is ignored when AWS activates the lambda.
    xray_recorder.begin_segment(__name__)  # Ensure xray has something to record against
    print(handler(event={}, context=None))
