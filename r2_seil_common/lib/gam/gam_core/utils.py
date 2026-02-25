from typing import List, Callable

from gam_core.model.guard_exception import GuardException


def validate_request(request: dict, required_body_fields: List[str] = None):
    """
    Validation decorator that ensures body and headers are present and non-empty in request and any
    required fields are provided in the body
    """
    if required_body_fields is None:
        required_body_fields = []

    if (
        not isinstance(request, dict)
        or not len(request) > 0
    ):
        raise GuardException(
            f"Request must be valid JSON and not type {type(request)}"
        )
    if (
        "body" not in request
        or not isinstance(request["body"], dict)
        or len(request["body"]) == 0
    ):
        raise GuardException("Expected body is missing")
    if (
        "headers" not in request
        or not isinstance(request["headers"], dict)
        or len(request["headers"]) == 0
    ):
        raise GuardException("Expected headers are missing")
    missing_fields = []
    for field in required_body_fields:
        if field not in request["body"] or not request["body"][field]:
            missing_fields.append(field)
    if missing_fields:
        raise GuardException(f"Fields missing from body: {missing_fields}")
