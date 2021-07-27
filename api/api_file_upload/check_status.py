from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from models.api_file_upload_models import file_upload_form_factory
from services.logger_services.logger_factory_service import SrvLoggerFactory
import services.file_upload as srv_upload
from .namespace import api_file_upload_ns
from resources.utils import generate_chunk_name
from resources.get_session_id import get_session_id
from app import executor
import os, json
import errno
from config import ConfigClass
from flask_jwt import jwt_required, current_identity
from resources.decorator import check_role

args_model_get = {
    'timestamp': {'type': 'string'},
}


class CheckUploadStateRestful(Resource):
    _logger = SrvLoggerFactory('api_file_upload').get_logger()

    @api_file_upload_ns.doc(params=args_model_get)
    @jwt_required()
    # def get(self, container_id):
    def get(self, project_geid):
        '''
        This method allow to check file upload status. timestamp optional,
        default return today's session
        '''

        # init resp
        _res = APIResponse()
        self._logger.info('CheckUploadStatusRestful Request IP: ' + str(request.remote_addr))
        self._logger.info(
            'Check Upload Task Status on container {} with info {}'.format(str(project_geid), request.args.to_dict()))
        self._logger.debug(request.headers.__dict__)
        session_id_gotten = get_session_id()
        self._logger.debug('session_id_gotten: {}'.format(session_id_gotten))
        timestamp = request.args.get('timestamp', None)
        session_id = session_id_gotten if session_id_gotten else \
            srv_upload.session_id_generator_by_timestamp(timestamp) if timestamp else \
            srv_upload.session_id_generator()

        result = srv_upload.get_by_session_id(session_id)
        _res.set_result([json.loads(record) for record in result ])
        _res.set_code(EAPIResponseCode.success)

        return _res.to_dict, _res.code

    def delete(self, project_geid):
        '''
        This method allow to delete file upload status.
        '''

        # init resp
        _res = APIResponse()
        self._logger.info('CleanUploadStateRestful Request IP: ' + str(request.remote_addr))
        self._logger.debug(request.headers.__dict__)
        session_id_gotten = get_session_id()
        self._logger.debug('session_id_gotten: {}'.format(session_id_gotten))

        result = srv_upload.get_by_session_id(session_id_gotten)

        for record in result:
            record = json.loads(record)
            srv_upload.delete_by_session_id(record["key"])

        _res.set_result('Success')
        _res.set_code(EAPIResponseCode.success)
        return _res.to_dict, _res.code

