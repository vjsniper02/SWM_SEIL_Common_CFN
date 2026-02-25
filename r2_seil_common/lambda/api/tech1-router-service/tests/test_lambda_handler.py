import os
import json
import uuid
import pytest
from unittest import mock
from aws_xray_sdk.core import xray_recorder
from pathlib3x import Path
from aws_lambda_context import LambdaContext
from app.lambda_handler import handler

# Constants
TEST_INPUT_DIR = Path(__file__).resolve().parent / "resources"
LAMBDA_NAME = "lineitem-router-service"
LAMBDA_VERSION = "v1"
ARN_TECH1_CLIENT_SERVICE = (
    "arn:aws:lambda:ap-southeast-2:[REDACTED_AWS_ACCOUNT_ID]:function:dev-api-tech1-client-service"
)


def read_file_as_json(file):
    with open(TEST_INPUT_DIR / file) as f:
        return json.load(f)


def set_lambda_context(function_name, aws_request_id, function_version):
    LambdaContext.function_name = function_name
    LambdaContext.aws_request_id = aws_request_id
    LambdaContext.function_version = function_version
    return LambdaContext


@pytest.fixture(scope="module")
def arrange():
    mock_lambda_client = mock.Mock()
    xray_recorder.begin_segment("test_lambda_handler.arrange")
    yield mock_lambda_client


@mock.patch.dict(
    os.environ,
    {
        "APPLICATION_NAME": "dev-api-tech1-router-service",
        "ENVIRONMENT": "local",
        "LAMBDA_REGION": "ap-southeast-2",
        "ARN_TECH1_CLIENT_SERVICE": ARN_TECH1_CLIENT_SERVICE,
    },
)
def test_500_default_response(arrange):
    lambda_context = set_lambda_context(LAMBDA_NAME, str(uuid.uuid4()), LAMBDA_VERSION)
    data = read_file_as_json("event_payload.json")

    result = handler(data, lambda_context)
    print(result)
    assert "500" in str(result["statusCode"])
