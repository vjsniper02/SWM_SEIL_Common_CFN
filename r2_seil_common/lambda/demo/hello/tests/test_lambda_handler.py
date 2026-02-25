from aws_xray_sdk.core import xray_recorder

from app.lambda_handler import handler

xray_recorder.begin_segment("test_app")


def test_handler_calls_correct_service():
    # Act
    result = handler(event=None, context=None)

    # Assert
    assert result["statusCode"] == 200
