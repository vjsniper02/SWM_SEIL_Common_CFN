import datetime
import json
import uuid


class SuccessResponse:
    def __init__(self, headers: dict, **kwargs):
        self.application_label = headers.get("X-Application-Label", "Unknown")
        self.time = datetime.datetime.now().isoformat()
        self.correlation_id = headers.get("X-Correlation-Id", str(uuid.uuid4()))
        self.code = 200
        self.message = "success"
        self.status = 200000
        self.data = {}
        self.data.update(**kwargs)

    def __repr__(self):
        return self.get_response()

    def __str__(self):
        return json.dumps(self.__repr__(), indent=2)

    def get_response(self):
        return self.__dict__
