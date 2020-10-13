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

class CheckUploadStatusRestfulDeprecated(Resource):
    _logger = SrvLoggerFactory('api_file_upload').get_logger()

    @api_file_upload_ns.doc(params={'task_id': {'type': 'string'}})
    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        This method allow to check file upload status. deprecated
        '''
        # init resp
        _res = APIResponse()
        self._logger.info('CheckUploadStatusRestful Request IP: ' + str(request.remote_addr))
        self._logger.info(
            'Check Upload Task Status on container {} with info {}'.format(str(container_id), request.args.to_dict()))

        task_id = request.args.get('task_id', None)
        if(task_id is None):
            return {'result': 'task id is required.'}, 403

        task_done = executor.futures.done(task_id)
        if task_done is None:
            return {'result': 'task does not exist'}, 404

        if not task_done:
            result = {'status': 'running'}
            return {'result': result}, 200

        try:
            future = executor.futures.pop(task_id)
            success, msg = future.result()
        except Exception as e:
            return {'result': str(e)}, 403

        # based on the return format the response
        if success:
            result = {'status': 'success'}
        else:
            result = {'status': 'error', 'message': msg}

        return {'result': result}, 200

class CheckUploadStateRestful(Resource):
    _logger = SrvLoggerFactory('api_file_upload').get_logger()

    @api_file_upload_ns.doc(params=args_model_get)
    @jwt_required()
    def get(self, container_id):
        '''
        This method allow to check file upload status. timestamp optional,
        default return today's session
        '''
        
        # init resp
        _res = APIResponse()
        self._logger.info('CheckUploadStatusRestful Request IP: ' + str(request.remote_addr))
        self._logger.info(
            'Check Upload Task Status on container {} with info {}'.format(str(container_id), request.args.to_dict()))
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