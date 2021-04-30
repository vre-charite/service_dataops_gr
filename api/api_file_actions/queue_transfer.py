from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os, json, time
import errno
from config import ConfigClass
import shutil
import requests
from services.data_operations.data_transfer import transfer_file_message, interpret_operation_location
from services.data_operations.data_operation_status import set_status, EDataActionType
from services.data_operations.check_path import check_is_greenroom_raw
from flask_jwt import current_identity

_logger = SrvLoggerFactory('api_file_transfer').get_logger()

class FileTransferQueueRestful(Resource):
    def post(self):
        '''
            handle file transfer request
        '''
        try:
            # init resp
            _res = APIResponse()
            req_content = request.get_json()
            session_id = req_content.get('session_id', 'default_session')
            # job_id = 'default_job'
            job_id =  EDataActionType.data_transfer.name + "-" + str(time.time() * 1000)
            input_files = req_content.get('input_files', None)
            project_code = req_content.get('project_code', None)
            operator = req_content.get('operator', None)
            operation_type = req_content.get('operation_type', None)
            repeat_file_list = []
            processed_list = []
            _logger.debug("input_files" + str(input_files))
            if not session_id or not input_files or not operator or operation_type == None:
                _logger.error('session_id/input_files/operator/operation_type is required: ' + str(req_content))
                _res.set_code(EAPIResponseCode.bad_request)
                _res.set_error_msg('session_id/input_files/operator/operation_type is required: ' + str(req_content))
                return _res.to_dict, _res.code

            for record in input_files:
                input_path = record['input_path']
                generate_id = record.get('generate_id', 'undefined')
                filename = os.path.basename(input_path)
                destination = interpret_operation_location(operation_type, filename, project_code)

                # ## check is valid path
                # if check_is_greenroom_raw(input_path, project_code):
                #     _res.set_code(EAPIResponseCode.bad_request)
                #     _res.set_error_msg('Invalid input path: ' + input_path + ', cannot be Greenroom Raw File')
                #     return _res.to_dict, _res.code

                if os.path.exists(destination):
                    _logger.debug("File already exists in destination" + str(input_path))
                    repeat_file_list.append(input_path)
                    continue

                uploader = record.get('uploader')
                _logger.debug("input_file: " + input_path)
                transfer_file_message(_logger,
                    session_id, job_id,
                    input_path, project_code, generate_id, uploader, operator,
                    operation_type)

                set_status(
                    session_id,
                    job_id,
                    input_path,
                    EDataActionType.data_transfer.name,
                    'running',
                    project_code,
                    operator,
                    0
                )
                _logger.debug("File starts processing" + str(input_path))
                processed_list.append(input_path)
                
            _res.set_result({
                "job_id": job_id,
                "repeat_file_list": repeat_file_list,
                "processed_list": processed_list
            })
            _res.set_code(EAPIResponseCode.accepted)

            return _res.to_dict, _res.code
        
        except Exception as e:
            print(e)
            _logger.error("file transfer: " + str(e))
