from api import module_api


class APIException(Exception):
    def __init__(self, status_code: int, error_msg: str):
        self.status_code = status_code
        self.content = {
            "code": self.status_code,
            "error_msg": error_msg,
            "result": "",
        }

class CustomException(Exception):
    # define custom exception classes
    def __init__(self, message, status_code):
        self.message = message
        self.status_code = status_code


class InvalidInputException(CustomException):
    def __init__(self, message):
        super().__init__(message, 400)
