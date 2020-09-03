from api import module_api


class CustomException(Exception):
    # define custom exception classes
    def __init__(self, message, status_code):
        self.message = message
        self.status_code = status_code


class InvalidInputException(CustomException):
    def __init__(self, message):
        super().__init__(message, 400)
