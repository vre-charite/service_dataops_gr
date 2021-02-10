from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os, json
import errno
from config import ConfigClass
import shutil
import requests
from services.oprations_log.operations_query import query
from services.oprations_log .oprations_create import add
from models.api_response import APIResponse, EAPIResponseCode
from resources.decorator import check_role

from flask_jwt import current_identity, jwt_required

_logger = SrvLoggerFactory('api_file_actions_query').get_logger()

class ActionsQueryRestful(Resource):
    @jwt_required()
    @check_role("uploader")
    def get(self):
        '''
            fetch operations logs
        '''
        _res = APIResponse()

        try:
            role = current_identity['project_role']
            
            page_size = request.args.get('page_size', None)
            page = request.args.get('page', 0)

            start_date = request.args.get('start_date', None)
            end_date = request.args.get('end_date', None)

            operation_type = request.args.get('operation_type', None)
            project_code = request.args.get('project_code', None)

            owner = request.args.get('owner', None)
            operator = request.args.get('operator', None)

            if not operation_type or not project_code:
                _res.set_code(EAPIResponseCode.bad_request)
                _res.set_error_msg('operation_type/project_code is required')
                return _res.to_dict, _res.code

            if role != 'admin' and not operator:
                _res.set_code(EAPIResponseCode.bad_request)
                _res.set_error_msg('operator is required when role is not admin')
                return _res.to_dict, _res.code

            result = query(page, page_size, operation_type, project_code, start_date, end_date, owner, operator, role)
            _res.set_code(EAPIResponseCode.success)
            _res.set_result(result['entities'])
            _res.set_total(result['approximateCount'])
            return _res.to_dict, _res.code

        except Exception as e:
            print(e)
            _logger.error("actions query: " + str(e))

    def post(self):
        '''
            add operations logs
        '''
        _res = APIResponse()

        try:
            req_content = request.get_json()

            operation_type = req_content.get('operation_type', None)
            owner = req_content.get('owner', None)
            operator = req_content.get('operator', None)
            input_file_path = req_content.get('input_file_path', None)
            output_file_path = req_content.get('output_file_path', None)
            file_size = req_content.get('file_size', None)
            project_code = req_content.get('project_code', None)
            generate_id = req_content.get('generate_id', None)
            process_pipeline = req_content.get('process_pipeline', None)

            _logger.info("create entity body: " + str(operation_type), str(owner), str(operator), str(input_file_path), str(output_file_path), str(file_size), str(project_code), str(generate_id), str(process_pipeline))

            add(operation_type, owner, operator, input_file_path, output_file_path, file_size, project_code, generate_id, process_pipeline)

            _res.set_code(EAPIResponseCode.success)
            _res.set_result({"result": "success"})

        except Exception as e:
            print(e)
            _logger.error("actions query: " + str(e))