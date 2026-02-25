from core.model.error_response import ErrorResponse


class ServerError(ErrorResponse):
    def __init__(self, err_code, err_msg, headers: dict):
        super().__init__(err_code, err_msg, headers)
        self.code = 500
        self.message = "servererror"
        self.status = 500000
