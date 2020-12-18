from models.service_meta_class import MetaService
import requests, time, os
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory

_logger = SrvLoggerFactory('srv_processed_catelogue').get_logger()

class SrvProcessedCatelogue(metaclass=MetaService):
    def __init__(self, processed_meta: dict,
        source_meta: dict,
        process_pipeline,
        owner,
        operator,
        operation_type,
        project_code,
        job_name = None):
        ## proccessed_meta decode
        self.create_time = str(round(time.time()))
        self.processed_full_path = processed_meta.get('processed_full_path', None)
        self.processed_size = processed_meta.get('size', None)
        self.status = processed_meta.get('status', 'failed')
        self.processed_file_name = os.path.basename(self.processed_full_path)
        self.project_code = project_code
        self.process_pipeline = process_pipeline
        self.owner = owner
        self.job_name = job_name if job_name else self.process_pipeline + self.create_time
        self.operator = operator
        self.operation_type = operation_type
        if self.processed_full_path and self.processed_size:
            pass
        else:
            _logger.error("Invalid processed_meta: " + str(processed_meta))
        
        ## source_meta decode
        self.source_full_path = source_meta.get('source_full_path', None)
        self.generate_id = source_meta.get('generate_id', None)
        self.source_file_name = os.path.basename(self.source_full_path)
        if self.source_full_path:
            pass
        else:
            _logger.error("Invalid source_meta: " + str(source_meta))
        

    def create_meta(self):
        res_create_entity = self.create_entity()
        if res_create_entity[1] != 200:
            return res_create_entity
        res_create_lineage = self.create_lineage(
            self.source_full_path,
            self.processed_full_path,
            self.project_code,
            self.process_pipeline,
            'Created By Data SrvProcessedCatelogue',
            self.create_time
        )
        if res_create_lineage[1] != 200:
            return res_create_entity
        return 'ProcessedCatelogue Succeed On: ' + self.processed_full_path, 200

    def create_entity(self):
        # create entity in atlas
        try:
            post_data = {
                'referredEntities': {},
                'entity': {
                    'typeName': 'file_operation_logs',
                    'attributes': {
                        'owner': self.owner,
                        'operator': self.operator,
                        'operationType': self.operation_type,
                        'modifiedTime': 0,
                        'replicatedTo': None,
                        'userDescription': None,
                        'isFile': False,
                        'numberOfReplicas': 0,
                        'replicatedFrom': None,
                        'qualifiedName': self.processed_file_name,
                        'displayName': None,
                        'description': None,
                        'extendedAttributes': None,
                        'nameServiceId': None,
                        'path': self.processed_full_path,
                        'posixPermissions': None,
                        'createTime': time.time(),
                        'fileSize': self.processed_size,
                        'clusterName': None,
                        'name': self.processed_full_path,
                        'isSymlink': False,
                        'group': None,
                        'updateBy': 'admin',
                        'bucketName': self.project_code,
                        'fileName': self.processed_file_name,
                        'generateID': self.generate_id if self.generate_id else 'undefined',
                        'process_pipeline': self.process_pipeline,
                        'jobName': self.job_name,
                        'status': self.status
                    },
                    'isIncomplete': False,
                    'status': 'ACTIVE',
                    'createdBy': 'admin',
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
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity',
                                json=post_data, headers={'content-type': 'application/json'})
            data = res.json()
            print('++++++++++', data)
            return res.json(), res.status_code
        except Exception as e:
            return str(e), 500

    def create_lineage(self, inputFullPath, outputFullPath, projectCode,
        pipelineName, description, create_time):
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
            'process_timestamp': str(create_time)
        }
        _logger.debug("Creating Lineage: " + str(payload))
        res = requests.post(
                url=my_url + '/v1/lineage',
                json=payload
        )
        if res.status_code == 200:
            _logger.info('Lineage Created: ' + inputFullPath + ':to:' + outputFullPath)
            return res.json(), 200
        else:
            _logger.error(res.text)
            return res.text, 500