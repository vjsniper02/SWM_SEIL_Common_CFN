import pytz
import uuid
from datetime import datetime
from core.utils import extract_validate_value


class TransactionContext:
    def __init__(self, event, context):
        self._time = str(
            datetime.now()
            .astimezone(tz=pytz.timezone("Australia/Sydney"))
            .strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )
        correlation_id = str(uuid.uuid4())
        application_label = str(context.function_name)
        if event["headers"] is not None:
            if (
                extract_validate_value(event["headers"], "X-Application-Label", True)
                is not None
            ):
                application_label = event["headers"]["X-Application-Label"]
            elif (
                extract_validate_value(event["headers"], "x-application-label", True)
                is not None
            ):
                application_label = event["headers"]["x-application-label"]
            if (
                extract_validate_value(event["headers"], "X-Correlation-Id", True)
                is not None
            ):
                correlation_id = event["headers"]["X-Correlation-Id"]
            elif (
                extract_validate_value(event["headers"], "x-correlation-id", True)
                is not None
            ):
                correlation_id = event["headers"]["x-correlation-id"]
        self.set_applicationLabel(application_label)
        self.set_correlationId(correlation_id)
        request_path = ""
        request_method = ""
        if extract_validate_value(event, "path", True) is not None:
            request_path = event["path"]
        if extract_validate_value(event, "httpMethod", True) is not None:
            request_method = event["httpMethod"]
        if (
            extract_validate_value(event, "requestContext", True) is not None
            and extract_validate_value(event["requestContext"], "http", True)
            is not None
        ):
            if (
                not request_path
                and extract_validate_value(
                    event["requestContext"]["http"], "path", True
                )
                is not None
            ):
                request_path = event["requestContext"]["http"]["path"]
            if (
                not request_method
                and extract_validate_value(
                    event["requestContext"]["http"], "method", True
                )
                is not None
            ):
                request_method = event["requestContext"]["http"]["method"]
        self.set_path(request_path)
        self.set_method(request_method)

    def set_correlationId(self, correlationId):
        self._correlationId = correlationId

    def set_applicationLabel(self, applicationLabel):
        self._applicationLabel = applicationLabel

    def set_path(self, path):
        self._path = path

    def set_method(self, method):
        self._method = method

    def get_time(self):
        return self._time

    def get_correlationId(self):
        return self._correlationId

    def get_applicationLabel(self):
        return self._applicationLabel

    def get_path(self):
        return self._path

    def get_method(self):
        return self._method

    def get_context(self):
        return {
            "time": self._time,
            "correlationId": self._correlationId,
            "applicationLabel": self._applicationLabel,
            "path": self._path,
            "method": self._method,
        }
