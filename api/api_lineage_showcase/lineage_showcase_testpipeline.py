from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
from .namespace import api_lineage_showcase_ns
from app import executor
import os, json
import errno
import datetime
from config import ConfigClass
from flask_jwt import jwt_required, current_identity
from resources.decorator import check_role
import subprocess
import shutil
import requests

_logger = SrvLoggerFactory('api_lineage_showcase').get_logger()

class LineageShowcaseRestful(Resource):
    # @jwt_required()
    def post(self):
        '''
        create lineage showcase, cannot use on prod
        '''
        # init resp
        _res = APIResponse()
        # get request body
        req_content = request.get_json()
        input_path = req_content['input_path']
        output_path = req_content['output_path']
        output_dir = os.path.dirname(output_path)
        output_file_name = os.path.basename(output_path)
        sec_output_file_name = output_dir + "/" + output_file_name.split('.')[0] + '_twice_processed_' + output_file_name.split('.')[1]
        self.transfer_file(input_path, output_path, 'pipeline_one')
        self.transfer_file(output_path, sec_output_file_name, 'pipeline_two')
        return _res.to_dict, _res.code

    def transfer_file(self, input_path, output_path, pipeline_name):
        try:
            output_dir = os.path.dirname(output_path)
            output_file_name = os.path.basename(output_path)
            _logger.info('start transfer file {} to {}'.format(input_path, output_path))

            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                _logger.info('creating output directory: {}'.format(output_dir))

            if os.path.isdir(input_path):
                _logger.info('starting to copy directory: {}'.format(input_path))
            else:
                _logger.info('starting to copy file: {}'.format(input_path))
            subprocess.call(['rsync', '-avz', '--min-size=1', input_path, output_path])
            # shutil.copyfile(input_path, output_path)
            store_file_meta_data(output_path, output_file_name, input_path, pipeline_name)
            create_lineage(input_path, output_path, 'testpipeline', pipeline_name, 'test pipeline', datetime.datetime.utcnow().isoformat())
            _logger.info('Successfully copied file from {} to {}'.format(input_path, output_path))
        except Exception as e:
            _logger.error('Failed to copy file from {} to {}\n {}'.format(input_path, output_path, str(e)))

    def create_file_meta(self):
        pass

def store_file_meta_data(output_full_path, file_name, raw_file_path, pipeline):
    my_url = "http://10.3.7.234:5063"
    payload  = {
        "path": output_full_path,
        "bucket_name": 'testpipeline',
        "file_name": file_name,
        "raw_file_path": raw_file_path,
        "size": os.path.getsize(output_full_path),
        "process_pipeline": pipeline,
        "job_name": "test_pipeline",
        "status": "success",
        "generate_id": "undefined"
    }
    _logger.debug("Saving Meta: " + str(payload))
    res = requests.post(
            url=my_url + "/v1/files/processed",
            json=payload
    )
    _logger.info('Meta Saved: ' + file_name + "  result: " + res.text)
    if res.status_code != 200:
        raise Exception(res.text)
    return res.json()

def create_lineage(inputFullPath, outputFullPath, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
    }
    '''
    my_url = ConfigClass.METADATA_API
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
        'process_timestamp': create_time
    }
    _logger.debug("Creating Lineage: " + str(payload))
    res = requests.post(
            url=my_url + '/v1/lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + inputFullPath + ':to:' + outputFullPath)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))