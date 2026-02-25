import re
import json
import logging
import time

from aws_xray_sdk.core import xray_recorder, patch_all
from botocore.client import Config
from boto3 import client as boto3_client
from dependency_injector.wiring import inject

from core import response_mapper
from core import transaction_context as txcontext
from core.utils import extract_validate_value, extract_environment_variable

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)

patch_all()  # Patch AWS libs with xray

aws_region = extract_environment_variable("LAMBDA_REGION", default="ap-southeast-2")
arn_tech1_client_service = extract_environment_variable("ARN_TECH1_CLIENT_SERVICE")
config = Config(retries=dict(max_attempts=3))
lambda_client = boto3_client("lambda", region_name=aws_region)


@xray_recorder.capture("lambda_handler.handler")
@inject
def handler(event, context):
    start_time = time.time()
    logger.info(f"start_time: {str(start_time)}")
    logger.info(f"event: {event}")
    downstream_request = {}
    headers = {}
    transaction_context = txcontext.TransactionContext(event, context)
    headers["X-Correlation-Id"] = transaction_context.get_correlationId()
    headers["X-Application-Label"] = transaction_context.get_applicationLabel()
    downstream_request["headers"] = headers
    path = event.get("path")
    logger.info(f"path: {path}")

    try:
        downstream_request["transactionContext"] = transaction_context.get_context()
        logger.info(f"downstream_request: {downstream_request}")

        invoke_response = lambda_client.invoke(
            FunctionName=arn_tech1_client_service,
            Payload=json.dumps(downstream_request),
        )
        logger.info(f"invoke_response: {invoke_response}")
        lambda_response_payload = json.loads(invoke_response["Payload"].read())
        logger.info(f"lambda_response_payload: {lambda_response_payload}")
        if extract_validate_value(lambda_response_payload, "code", True) is None:
            error_msg = "Downstream processing failure"
            if (
                extract_validate_value(lambda_response_payload, "errorMessage", True)
                is not None
            ):
                error_msg = lambda_response_payload["errorMessage"]
            logger.info(f"error_msg: {str(error_msg)}")
            return response_mapper.create_server_error_response(
                transaction_context, error_msg
            )
        return response_mapper.create_lambda_response(lambda_response_payload)
        # return json.loads(invoke_response["Payload"].read())
    except Exception as e:
        logger.exception(f"Failure to call downstream service - {str(e)}")
        return response_mapper.create_server_error_response(transaction_context, e)
    finally:
        logger.info(f"elapsed time in seconds: {str(time.time() - start_time)}")
        logging.shutdown()


if __name__ == "__main__":
    # This section is ignored when AWS activates the lambda.
    xray_recorder.begin_segment(__name__)  # Ensure xray has something to record against
