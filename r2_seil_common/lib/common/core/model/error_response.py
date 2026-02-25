import datetime
import json
import uuid


class ErrorResponse(Exception):
    def __init__(self, err_code, err_msg, headers: dict):
        self.application_label = headers.get("X-Application-Label", "Unknown")
        self.time = datetime.datetime.now().isoformat()
        self.correlation_id = headers.get("X-Correlation-Id", str(uuid.uuid4()))
        self.errors = [{"code": err_code, "message": str(err_msg)}]

    def __repr__(self):
        return self.get_response()

    def __str__(self):
        return json.dumps(self.__repr__(), indent=2)

    def get_response(self):
        return self.__dict__
