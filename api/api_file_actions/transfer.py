from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os, json
import errno
from config import ConfigClass
import shutil
import requests
from services.data_operations.data_transfer import transfer_file
from services.data_operations.data_on_processed_catelogue import SrvProcessedCatelogue
from flask_jwt import jwt_required, current_identity
from resources.decorator import check_role

_logger = SrvLoggerFactory('api_file_transfer').get_logger()

class FileTransferRestful(Resource):
    @jwt_required()
    @check_role("uploader")
    def post(self):
        '''
            handle file transfer request
        '''
        try:
            # init resp
            _res = APIResponse()
            req_content = request.get_json()
            input_files = req_content.get('input_files', None)
            target_disk = req_content.get('target_disk', None)
            username = current_identity['username']

            if not input_files:
                _res.set_code(EAPIResponseCode.bad_request)
                _res.set_error_msg('input_files is required')
                return _res.to_dict, _res.code
            

            if not target_disk:
                # by default is VRE CORE
                target_disk = '/vre-data'

            input_list = []
            output_list = []

            for record in input_files:
                input_path = record['input_path']
                entity_type = record.get('entity_type', None)
                generate_id = record.get('generate_id', None)
                owner = record.get('owner', None)

                file_name = os.path.basename(input_path)
                input_dirname = os.path.dirname(input_path)
                dirnames = input_dirname.split('/')

                output_path = '{}/{}/{}/{}'.format(target_disk, dirnames[-2], dirnames[-1], file_name)

                transfer_file(_logger, input_path, output_path)

                processed_meta = dict()
                processed_meta['processed_full_path'] = output_path
                
                if os.path.isfile(output_path):
                    processed_meta['size'] = os.path.getsize(output_path)
                    processed_meta['status'] = 'succeed'

                source_meta = dict()
                source_meta['source_full_path'] = input_path
                source_meta['generate_id'] = generate_id

                project_code = dirnames[-2]

                service_processed_catelogue = SrvProcessedCatelogue(processed_meta, source_meta, 'data_transfer', owner,username, 'FILE_COPY', project_code, None)

                service_processed_catelogue.create_meta()

                input_list.append(source_meta)
                output_list.append(processed_meta)
            
            _res.set_result({ "input_path": input_list, "output_path": output_list })

            return _res.to_dict, _res.code
        
        except Exception as e:
            print(e)
            _logger.error("file transfer: " + str(e))
