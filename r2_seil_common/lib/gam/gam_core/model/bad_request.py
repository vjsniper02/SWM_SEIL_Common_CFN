from gam_core.model.error_response import ErrorResponse


class BadRequest(ErrorResponse):
    def __init__(self, err_code, err_msg, headers: dict):
        super().__init__(err_code, err_msg, headers)
        self.code = 400
        self.message = "badrequest"
        self.status = 400000
