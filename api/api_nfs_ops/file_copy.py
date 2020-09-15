from flask import request, current_app
from flask_restx import Api, Resource, fields
from config import ConfigClass
from models.api_response import APIResponse, EAPIResponseCode
from api import module_api, nfs_entity_ns
import os
from shutil import copyfile
import time
import requests
from services.logger_services.logger_factory_service import SrvLoggerFactory


post_model = module_api.model("copy_file_post", {
    'input_path': fields.String,
    'output_path': fields.String,
    'uploader': fields.String,
    'project': fields.String,
    'create_time': fields.Float,
    'generate_id': fields.String,
    'pipeline': fields.String
})


class fileCopyRestful(Resource):
    _logger = SrvLoggerFactory('api_file_copy').get_logger()

    @nfs_entity_ns.expect(post_model)
    def post(self):
        '''
        This method allow to copy file from different env and create atlas record.
        '''
        post_data = request.get_json()
        self._logger.info(
            'API file copy is requested with {} '.format(post_data))

        # init resp
        _res = APIResponse()

        input_path = post_data.get('input_path', None)
        output_path = post_data.get('output_path', None)

        if not input_path or not output_path:
            self._logger.error('input_path and output_path are required.')
            _res.set_code(EAPIResponseCode.bad_request)
            _res.set_error_msg('input_path and output_path are required.')
            return _res.to_dict, _res.code
        output_folder = output_path.rpartition('/')[0]
        filename = output_path.rpartition('/')[2]

        # check if source path is valid
        if not os.path.isfile(input_path):
            self._logger.error('input_path is not valid.')
            _res.set_code(EAPIResponseCode.bad_request)
            _res.set_error_msg('input_path is not valid.')
            return _res.to_dict, _res.code

        # check if destination exists
        if os.path.isfile(output_path):
            self._logger.error('output_path is not valid.')
            _res.set_code(EAPIResponseCode.bad_request)
            _res.set_error_msg('output_path is not valid.')
            return _res.to_dict, _res.code

        # copy file from source to destination
        try:
            if not os.path.isdir(output_folder):
                os.makedirs(output_folder)
            copyfile(input_path, output_path)
            copy_time = time.time()
        except Exception as e:
            self._logger.error('Failed to copy file: ' + str(e))
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('Failed to copy file: ' + str(e))
            return _res.to_dict, _res.code

        # create record in atlas
        uploader = post_data.get('uploader', None)
        size = os.path.getsize(output_path)
        project_code = post_data.get('project', None)
        generate_id = post_data.get('generate_id', None)
        pipeline = post_data.get('pipeline', None)
        create_time = post_data.get('create_time', None)

        atlas_data = {
            'referredEntities': {},
            'entity': {
                'typeName': 'nfs_file_cp',
                'attributes': {
                    'owner': uploader,
                    'modifiedTime': 0,
                    'replicatedTo': None,
                    'userDescription': None,
                    'isFile': False,
                    'numberOfReplicas': 0,
                    'replicatedFrom': None,
                    'qualifiedName': filename,
                    'displayName': None,
                    'description': None,
                    'extendedAttributes': None,
                    'nameServiceId': None,
                    'path': output_folder,
                    'posixPermissions': None,
                    'createTime': create_time,
                    'copyTime': copy_time,  # time when copied file
                    'fileSize': size,
                    'clusterName': None,
                    'name': output_path,
                    'isSymlink': False,
                    'group': None,
                    'updateBy': 'test_no_auth',
                    'bucketName': project_code,
                    'fileName': filename,
                    'generateID': generate_id,
                    'pipeline': pipeline
                },
                'isIncomplete': False,
                'status': 'ACTIVE',
                'createdBy': uploader,
                'version': 0,
                'relationshipAttributes': {
                    'schema': [],
                    'inputToProcesses': [],
                    'meanings': [],
                    'outputFromProcesses': []
                },
                'customAttributes': {},
                'labels': []
            }
        }
        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity',
                                json=atlas_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                raise Exception(res.text)
        except Exception as e:
            self._logger.error('Failed to create Atlas record: ' + str(e))
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('Failed to create Atlas record: ' + str(e))
            return _res.to_dict, _res.code

        _res.set_result('Succeed.')
        return _res.to_dict, _res.code
