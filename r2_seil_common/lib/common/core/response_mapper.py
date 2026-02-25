import json


def create_errors(code, message):
    errors = list()
    error = {"code": code, "message": message}
    errors.append(error)
    return errors


def create_validation_error_response(transaction_context, message):
    return create_response_body(
        transaction_context,
        code=400,
        message="Bad Request",
        status="400000",
        errors=create_errors("EC40001", message),
    )


def create_server_error_response(transaction_context, exception):
    return create_response_body(
        transaction_context,
        code=500,
        message="Internal Server Error",
        status="500000",
        errors=create_errors("EC50001", str(exception)),
    )


def create_response_body(transaction_context, **kwargs):
    response_body = {}
    code = kwargs.get("code", None)
    if code is None:
        code = 500
    response_body["code"] = code
    response_body["applicationLabel"] = transaction_context.get_applicationLabel()
    response_body["time"] = transaction_context.get_time()
    response_body["correlationId"] = transaction_context.get_correlationId()
    message = kwargs.get("message", None)
    if message is not None:
        response_body["message"] = message
    status = kwargs.get("status", None)
    if status is not None:
        response_body["status"] = status
    data = kwargs.get("data", None)
    if data is not None:
        response_body["data"] = data
    errors = kwargs.get("errors", None)
    if errors is not None:
        response_body["errors"] = errors
    response_body["path"] = transaction_context.get_path()
    response_body["method"] = transaction_context.get_method()
    return create_lambda_response(response_body)


def create_lambda_response(response_body):
    return {
        "statusCode": response_body["code"],
        "body": json.dumps(response_body),
        "headers": {"Content-Type": "application/json"},
    }
