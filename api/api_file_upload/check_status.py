from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from models.api_file_upload_models import file_upload_form_factory
from services.logger_services.logger_factory_service import SrvLoggerFactory
from .namespace import api_file_upload_ns
from resources.utils import generate_chunk_name
from app import executor
import os
import errno
from config import ConfigClass
from flask_jwt import jwt_required, current_identity
from resources.decorator import check_role


class CheckUploadStatusRestful(Resource):
    _logger = SrvLoggerFactory('api_file_upload').get_logger()

    @api_file_upload_ns.doc(params={'task_id': {'type': 'string'}})
    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        This method allow to check file upload status.
        '''
        # init resp
        _res = APIResponse()

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
