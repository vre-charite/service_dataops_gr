from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.data_operations.data_operation_status import set_status, get_status
import os, json, time
import errno
from config import ConfigClass
import shutil
import requests

_logger = SrvLoggerFactory('api_file_transfer').get_logger()

class FileActionsStatus(Resource):
    def get(self):
        try:
            _res = APIResponse()
            session_id = request.args.get('session_id', 'default_session')
            job_id = request.args.get('job_id', None)
            project_code = request.args.get('project_code', None)
            action = request.args.get('action', None)
            # operator = request.args.get('operator', None)
            if session_id:
                pass
            else:
                _res.set_code(EAPIResponseCode.bad_request)
                _res.set_error_msg('session_id is required')
                return _res.to_dict, _res.code

            fetched = get_status(
                session_id,
                job_id,
                project_code,
                action
            )
            _res.set_code(EAPIResponseCode.success)
            _res.set_result(fetched)
            return _res.to_dict, _res.code
        except Exception as e:
            _res.set_error_msg("Internal Error: " + str(e))
            _res.set_code(EAPIResponseCode.internal_error)
            _logger.error("FileActionsStatus: " + str(e))
            return _res.to_dict, _res.code
    def post(self):
        '''
            update file action status
        '''
        try:
            # init resp
            _res = APIResponse()
            req_content = request.get_json()
            session_id = req_content.get('session_id', 'default_session')
            job_id = req_content.get('job_id', 'default_job')
            source = req_content.get('source', None)
            action = req_content.get('action', None)
            target_status = req_content.get('target_status', None)
            project_code = req_content.get('project_code', None)
            operator = req_content.get('operator', None)
            progress = req_content.get('progress', 0)
            payload = req_content.get('payload', {})

            if session_id and source and job_id \
                and target_status and operator and project_code:
                pass
            else:
                _res.set_code(EAPIResponseCode.bad_request)
                _res.set_error_msg('session_id/job_id/project_code/source/input_files/operator is required')
                return _res.to_dict, _res.code

            set_status(
                session_id,
                job_id,
                source,
                action,
                target_status,
                project_code,
                operator,
                progress,
                payload
            )
            
            _res.set_result("Updated FileActionsStatus: " + target_status)
            _res.set_code(EAPIResponseCode.success)

            return _res.to_dict, _res.code
        
        except Exception as e:
            _res.set_error_msg("Internal Error: " + str(e))
            _res.set_code(EAPIResponseCode.internal_error)
            _logger.error("FileActionsStatus: " + str(e))
            return _res.to_dict, _res.code