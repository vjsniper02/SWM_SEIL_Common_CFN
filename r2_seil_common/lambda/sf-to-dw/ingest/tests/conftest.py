import os
import pytest
import boto3
import moto


# Fake creds
def get_cred():
    return ""


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = get_cred()
    os.environ["AWS_SECRET_ACCESS_KEY"] = get_cred()
    os.environ["AWS_SECURITY_TOKEN"] = get_cred()
    os.environ["AWS_SESSION_TOKEN"] = get_cred()
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with moto.mock_xray_client(), moto.mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with moto.mock_xray_client(), moto.mock_s3():
        yield boto3.resource("s3", region_name="us-east-1")


@pytest.fixture(scope="function")
def dynamodb(aws_credentials):
    with moto.mock_xray_client(), moto.mock_dynamodb2():
        yield boto3.resource("dynamodb", region_name="us-east-1")


@pytest.fixture(scope="function")
def lambda_client(aws_credentials):
    with moto.mock_xray_client(), moto.mock_lambda():
        yield boto3.client("lambda", region_name="us-east-1")
