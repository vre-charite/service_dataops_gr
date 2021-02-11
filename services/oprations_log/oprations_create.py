import time
import requests
import os
from config import ConfigClass

def add(operation_type, owner, operator, input_file_path, output_file_path, file_size, project_code, generate_id, process_pipeline):
    try:
        file_name = os.path.basename(output_file_path)

        current_time = time.time()
        
        post_data = {
            'referredEntities': {},
            'entity': {
                'typeName': 'file_operation_logs',
                'attributes': {
                    'owner': owner,
                    'operator': operator,
                    'operationType': operation_type,
                    'modifiedTime': 0,
                    'replicatedTo': None,
                    'userDescription': None,
                    'isFile': False,
                    'numberOfReplicas': 0,
                    'replicatedFrom': None,
                    'qualifiedName': output_file_path + '_' + str(current_time),
                    'displayName': None,
                    'description': None,
                    'extendedAttributes': None,
                    'nameServiceId': None,
                    'path': output_file_path,
                    'originPath': input_file_path,
                    'posixPermissions': None,
                    'createTime': time.time(),
                    'fileSize': file_size,
                    'clusterName': None,
                    'name': output_file_path,
                    'isSymlink': False,
                    'group': None,
                    'updateBy': 'admin',
                    'bucketName': project_code,
                    'fileName': file_name,
                    'generateID': generate_id if generate_id else 'undefined',
                    'process_pipeline': process_pipeline if process_pipeline else 'undefined',
                    'jobName': None,
                    'status': None
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
        return res.json(), res.status_code
    except Exception as e:
        return str(e), 500
