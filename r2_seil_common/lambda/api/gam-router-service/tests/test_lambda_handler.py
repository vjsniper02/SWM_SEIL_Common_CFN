import json
import uuid
import os
from unittest import mock
from unittest.mock import Mock
from aws_lambda_context import LambdaContext
from requests.models import Response
import pytest
from aws_xray_sdk.core import xray_recorder
from pathlib3x import Path
from app.lambda_handler import handler

# Constants
TEST_INPUT_DIR = Path(__file__).resolve().parent / "resources"
# TEST_INPUT_DIR = 'resources/'
LAMBDA_NAME = "gam-router-service"
LAMBDA_VERSION = "v1"


def read_file_as_json(file):
    with open(TEST_INPUT_DIR / file) as f:
        return json.load(f)


def read_file(file):
    with open(TEST_INPUT_DIR / file) as f:
        return f.read()


def generate_mock_response(status_code, payload):
    response = Mock(Response)
    response.status_code = status_code
    response.text = None
    if payload is not None:
        response.text = read_file(payload)
    return response


def set_lambda_context(function_name, aws_request_id, function_version):
    LambdaContext.function_name = function_name
    LambdaContext.aws_request_id = aws_request_id
    LambdaContext.function_version = function_version
    return LambdaContext


@pytest.fixture(scope="module")
def arrange():
    mock_lambda_client = Mock()
    xray_recorder.begin_segment("test_lambda_handler.arrange")
    yield mock_lambda_client


@mock.patch.dict(
    os.environ,
    {
        "APPLICATION_NAME": "dev-api-gam-router-service",
        "ENVIRONMENT": "local",
        "LAMBDA_REGION": "ap-southeast-2",
        "ARN_GAM_SERVICE": "arn:aws:lambda:ap-southeast-2:[REDACTED_AWS_ACCOUNT_ID]:function:dev-api"
        "-gam-client-service",
    },
)
def test_400_response(arrange):
    mock_lambda_client = arrange
    lambda_context = set_lambda_context(LAMBDA_NAME, str(uuid.uuid4()), LAMBDA_VERSION)
    data = read_file_as_json("event_payload_invalid.json")
    print(data)
    mocked_response = mock.Mock()
    mocked_response.status_code = 200

    mock_lambda_client.invoke.return_value = mocked_response
    result = handler(data, lambda_context)
    print(result)
    assert "400" in str(result["statusCode"])
