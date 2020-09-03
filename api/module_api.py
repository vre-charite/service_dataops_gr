from flask_restx import Api

module_api = Api(version='1.0', title='DataOps API',
                 description='Data Operation API for VRE', doc='/v1/api-doc')