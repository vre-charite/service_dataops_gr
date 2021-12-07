from flask_restx import Api
from resources.errors import APIException

module_api = Api(version='1.0', title='DataOps API',
                 description='Data Operation API for VRE', doc='/v1/api-doc')

@module_api.errorhandler(APIException)
def http_exception_handler(exc: APIException):
    return exc.content, exc.status_code

