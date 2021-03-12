from flask import request, send_file, Response, current_app
import os
import requests
from flask_restx import Api, Resource
from flask_jwt import jwt_required, current_identity
import uuid
import json
import zipfile
import io
import urllib
import jwt
import uuid

from datetime import datetime, timedelta, timezone
from dateutil import tz
import time

from config import ConfigClass

from app import executor
from api import nfs_entity_ns
import errno

from resources.swagger_modules import file_upload, file_upload_last_response, file_upload_status, file_download
from resources.utils import fs
from resources.decorator import check_role

from services.data_providers.redis import SrvRedisSingleton
from resources.get_session_id import get_session_id

# auto zone detection
from_zone = tz.tzutc()
to_zone = tz.tzlocal()


class file_predownload(Resource):
    @jwt_required()
    @check_role("uploader")

    def post(self, container_id):
        post_data = request.get_json()
        current_app.logger.info(
            'Call API for predownload to container {} with info {}'.format(str(container_id), post_data))
        current_app.logger.info('Request IP: ' + str(request.remote_addr))

        files = post_data.get("files", None)
        if not files or not isinstance(files, list) or len(files) < 1:
            return {'result': 'Please provide files in correct format.'}, 403

        try:
            # Check if container is valid
            url = ConfigClass.NEO4J_SERVICE + \
                'nodes/Dataset/node/' + str(container_id)
            res = requests.get(url=url)
        except Exception as e:
            return {'result': str(e)}, 403

        # From reponse get the mapped path from container
        datasets = res.json()
        if res.status_code != 200:
            return {'result': res.json()}, res.status_code
        if len(datasets) == 0:
            current_app.logger.error(
                'neo4j service: container does not exist.')
            return {'result': 'Container does not exist.'}, 404

        container_path = datasets[0].get('path', None)
        if not container_path:
            current_app.logger.error(
                'neo4j service: cannot find the path attribute.')
            return {'result': 'Cannot find the path attribute.'}, 403

        # just before the download we create the log
        # create entity in atlas
        username = current_identity['username']
        my_uuid = uuid.uuid4().hex

        # loop over each file to download
        for f in files:
            try:
                post_data = {
                    'referredEntities': {},
                    'entity': {
                        'typeName': 'nfs_file_download',
                        'attributes': {
                            'modifiedTime': 0,
                            'replicatedTo': None,
                            'userDescription': None,
                            'isFile': False,
                            'numberOfReplicas': 0,
                            'replicatedFrom': None,
                            'displayName': None,
                            'description': None,
                            'extendedAttributes': None,
                            'nameServiceId': None,
                            'posixPermissions': None,
                            'clusterName': None,
                            'isSymlink': False,
                            'group': None,
                            'uuid': my_uuid,
                            'createTime': time.time(),
                            'downloader': username,
                            'bucketName': container_path,
                            'fileName': None,
                            'qualifiedName': 'nfs_file_download:' + str(container_path) + ':' + str(time.time()) + ':' + my_uuid,
                            'name': 'nfs_file_download:' + str(container_path) + ':' + str(time.time()) + ':' + my_uuid,
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
                post_data['entity']['attributes']['fileName'] = f['file']
                res = requests.post(ConfigClass.METADATA_API+'/v1/entity',
                                    json=post_data, headers={'content-type': 'application/json'})
                if res.status_code != 200:
                    return res.json(), res.status_code
            except Exception as e:
                print(str(e))

        # store in redis
        redis_mgr = SrvRedisSingleton()

        session_id = get_session_id()
        if session_id:
            session_id = session_id + ':' + my_uuid

        if(len(files) == 1):
            filename = files[0]['file']
            # store in redis
        
            path = files[0]['path']
            full_path = os.path.join(
                ConfigClass.NFS_ROOT_PATH, container_path, path, filename)
            if not os.path.exists(full_path):
                current_app.logger.error(
                    'nfs file check: file %s not found.' % filename)
                if session_id:
                    redis_mgr.set_by_key(session_id, json.dumps({'filename': filename, 'key': session_id, 'create_time': str(time.time()), 'status': 'failed', 'container': container_path}))
                return {'result': 'File %s not found.' % filename}, 404

            if session_id:
                redis_mgr.set_by_key(session_id, json.dumps({'filename': filename, 'key': session_id, 'create_time': str(time.time()), 'status': 'success', 'container': container_path}))

            # Generate download token
            download_token = jwt.encode({
                'filename': filename,
                'path': path,
                'container_id': container_id,
                'container_path': container_path,
                'iat': int(time.time()),
                'exp': int(time.time()) + (5 * 60)  # Token expired in 5 mins
            }, ConfigClass.DOWNLOAD_KEY, algorithm='HS256')
           
            # Return download token
            return {'result': download_token.decode('utf-8'), 'filename': filename, 'container': container_path}, 200

        else:
            # Start zipping files
            zip_filename = container_path + '_'+str(int(time.time()))+'.zip'
            target_path = os.path.join(
                ConfigClass.NFS_ROOT_PATH, container_path, 'workdir')
            zip_multi_files.submit_stored(
                zip_filename, zip_filename, target_path, container_path, files, redis_mgr, session_id)
            # if session_id:
            #     redis_mgr.set_by_key(session_id, json.dumps({'filename': zip_filename, 'status': 'success'}))

            return {'result': {'task_id': zip_filename, 'message': 'Start processing zip file.', 'container': container_path}}, 201

    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        This method allow to check download file zipping status.
        '''
        current_app.logger.info(
            'Call API for check download status to container {} with info {}'.format(str(container_id), request.args.to_dict()))
        current_app.logger.info('Request IP: ' + str(request.remote_addr))

        task_id = request.args.get('task_id', None)
        if(task_id is None):
            return {'result': 'task id is required.'}, 403

        try:
            url = ConfigClass.NEO4J_SERVICE + \
                'nodes/Dataset/node/' + str(container_id)
            res = requests.get(url=url)
        except Exception as e:
            return {'result': str(e)}, 403

        # From reponse get the mapped path from container
        datasets = res.json()
        container_path = datasets[0].get('path', None)

        task_done = executor.futures.done(task_id)
        if task_done is None:
            return {'result': 'task does not exist'}, 404

        if not task_done:
            result = {'status': 'running'}
            return {'result': result}

        try:
            future = executor.futures.pop(task_id)
            print(future.result())
            success, msg = future.result()
        except Exception as e:
            return {'result': str(e)}, 403

        # based on the return format the response
        if success:
            target_path = os.path.join(
                ConfigClass.NFS_ROOT_PATH, container_path, 'workdir')
            # Generate download token
            download_token = jwt.encode({
                'filename': task_id,
                'path': target_path,
                'container_id': container_id,
                'container_path': container_path,
                'iat': int(time.time()),
                'exp': int(time.time()) + (5 * 60)  # Token expired in 5 mins
            }, ConfigClass.DOWNLOAD_KEY, algorithm='HS256')
            result = {'status': 'success',
                      'token': download_token.decode('utf-8')}
        else:
            result = {'status': 'error', 'message': msg}

        return {'result': result}, 200


class file_download_log(Resource):
    @jwt_required()
    @check_role("uploader")
    def get(self):
        # add new query string here for pagination
        default_page_size = 25
        page_size = int(request.args.get('page_size', default_page_size))
        # note here the offset is page_size * page
        page = int(request.args.get('page', 0))

        # use the stage variable to indicate entity type
        # stage = request.args.get('stage', 'raw')
        entity_type = 'nfs_file_download'
        # if stage == 'raw':
        #     entity_type = 'nfs_file'
        # else:
        #     entity_type = 'nfs_file_processed'

        # also add new query string for sorting the column default is time
        sorting = request.args.get('column', 'createTime')
        # also can search the text
        # text = request.args.get('text', None)
        # default order is desceding
        order = request.args.get('order', 'desc')
        order = 'ASCENDING' if order == 'asc' else 'DESCENDING'

        # call the atlas service to get the file information
        post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': False,
            'includeSubTypes': False,
            'includeClassificationAttributes': False,
            # 'entityFilters': {
            #     'attributeName': 'bucketName',
            #     'attributeValue': container_path,
            #     'operator': 'eq'
            # },
            'tagFilters': None,
            'attributes': ['bucketName', 'fileName', 'downloader'],
            'limit': page_size,
            'offset': page * page_size,
            # 'query': '"ABC-1234"',
            'sortBy': sorting,
            'sortOrder': order,
            'typeName': entity_type,
            'classification': None,
            'termName': None
        }
        # if we have the full text search input
        # if text:
        #     post_data.update({'query': '"%s"'%text})

        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=post_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                return {'result': res.json()}, 403
            res = res.json()['result']
        except Exception as e:
            return {'result': str(e)}, 403

        # because if there is no file it will not return entities field
        # so check it up
        if not res.get('entities', None):
            res.update({'entities': []})

        # also change the timestamp from int to string
        for e in res['entities']:
            timestamp_int = e['attributes'].get('createTime', None)
            # print(timestamp_int)
            if timestamp_int:
                central = datetime.fromtimestamp(timestamp_int, tz=timezone.utc)
                e['attributes']['createTime'] = central.strftime(
                    '%Y-%m-%d %H:%M:%S')

        return {"result": res}, 200



class file_download_checker(Resource):
    @jwt_required()
    #@check_role("uploader")

    def get(self):
        '''
        This method allow to check download history of current session.
        '''
        redis_mgr = SrvRedisSingleton()
        session_id = get_session_id()

        if not session_id:
            return {'result': 'Session ID not found.'}, 403

        records = redis_mgr.mget_by_prefix(session_id)

        result = [record.decode('utf-8') for record in records] if records else []

        return {'result': [json.loads(record) for record in result ]}, 200

    def delete(self):
        '''
        This method allow to clear download history of current session.
        '''
        redis_mgr = SrvRedisSingleton()
        session_id = get_session_id()

        if not session_id:
            return {'result': 'Session ID not found.'}, 403

        records = redis_mgr.mget_by_prefix(session_id)

        result = [record.decode('utf-8') for record in records] if records else []

        for record in result:
            record = json.loads(record)
            redis_mgr.delete_by_key(record["key"])

        return {'result': 'Download history deleted'}, 200


@executor.job
def zip_multi_files(zip_filename, target_path, container_path, files, redis_mgr, session_id):
    try:
        target_file = os.path.join(target_path, zip_filename)
        with zipfile.ZipFile(target_file, 'w', zipfile.ZIP_STORED) as zf:
            for f in files:
                full_path = os.path.join(
                    ConfigClass.NFS_ROOT_PATH, container_path, f['path'], f['file'])
                if not os.path.exists(full_path):
                    return {'result': 'File %s not found.' % f['file']}, 404
                with open(full_path, 'rb') as fp:
                    zf.writestr(f['file'], fp.read())
        if session_id:
            redis_mgr.set_by_key(session_id, json.dumps({'filename': zip_filename, 'status': 'success', 'key': session_id, 'create_time': str(time.time()), 'container': container_path}))
        
    except Exception as e:
        print(e)
        return False, str(e)

    return True, 'success'


class file(Resource):
    @ nfs_entity_ns.expect(file_download)
    # @jwt_required()
    def get(self):
        '''
        This method allow to download single file.
        '''
        current_app.logger.info('Request IP: ' + str(request.remote_addr))
        # Check if token provided
        token = request.args.get('token', None)
        if not token:
            return {'result': 'Download token is required.'}, 403

        # Verify and decode token
        try:
            res = jwt.decode(token, ConfigClass.DOWNLOAD_KEY,
                             algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            return {'result': 'Download token already expired.'}, 403
        except Exception as e:
            return {'result': str(e)}, 403

        filename = res['filename']
        path = res['path']
        container_path = res['container_path']
        if(not filename or not path or not container_path):
            return {'result': 'Download token is not valid.'}, 403

        # Use root to generate the path
        full_path = os.path.join(
            ConfigClass.NFS_ROOT_PATH, container_path, path, filename)
        if not os.path.exists(full_path):
            return {'result': 'File %s not found.' % filename}, 404
        return send_file(full_path,
                         mimetype='application/octet-stream',
                         as_attachment=True,
                         attachment_filename=filename,
                         cache_timeout=-1)


class processedFile(Resource):
    # after raw file pass throught the pipeline
    # new entity will be created and the relationship will link to raw
    # @jwt_required()
    # @check_role("member")
    def post(self):
        current_app.logger.info('Calling processedFile post')
        post_data = request.get_json()
        current_app.logger.info('received payload:%s', json.dumps(post_data))

        # must have
        # name = post_data.get('name', None)
        path = post_data.get('path', None)
        bucket_name = post_data.get('bucket_name', None)
        file_name = post_data.get('file_name', None)
        size = post_data.get('size', 0)

        generate_id = post_data.get('generate_id', None)
        raw_file_path = post_data.get('raw_file_path', None)
        pipeline = post_data.get('process_pipeline', None)
        job_name = post_data.get('job_name', None)
        status = post_data.get('status', None)

        owner = post_data.get('owner', None)

        if not owner:
            try:
                # first try to get the owner
                post_data = {
                    'excludeDeletedEntities': True,
                    'includeSubClassifications': False,
                    'includeSubTypes': False,
                    'includeClassificationAttributes': False,
                    'entityFilters': {
                        'attributeName': 'name',
                        'attributeValue': raw_file_path,
                        'operator': 'eq'
                    },
                    'tagFilters': None,
                    'attributes': ['generateID', 'fileName', 'fileSize', 'path'],
                    'limit': 1,
                    'offset': 0,
                    'typeName': 'nfs_file',
                    'classification': None,
                    'termName': None
                }
                res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                    json=post_data, headers={'content-type': 'application/json'})
                if res.status_code != 200:
                    return {'result': res.json()}, 403
                res = res.json()['result']
                # print(res)
                # try to get the entities
                entities = res.get('entities', [])
                if len(entities) == 0:
                    raise Exception(
                        "owner can not be found for: " + file_name)
                # else pick the owner
                owner = entities[0]['attributes']['owner']

            except Exception as e:
                return {'result': str(e)}, 403

        # create entity in atlas
        post_data = {
            'referredEntities': {},
            'entity': {
                'typeName': 'nfs_file_processed',
                'attributes': {
                    'owner': owner,
                    'modifiedTime': 0,
                    'replicatedTo': None,
                    'userDescription': None,
                    'isFile': False,
                    'numberOfReplicas': 0,
                    'replicatedFrom': None,
                    'qualifiedName': path,
                    'displayName': None,
                    'description': None,
                    'extendedAttributes': None,
                    'nameServiceId': None,
                    'path': path,
                    'posixPermissions': None,
                    'createTime': time.time(),
                    'fileSize': size,
                    'clusterName': None,
                    'name': path,
                    'isSymlink': False,
                    'group': None,
                    'updateBy': 'admin',
                    'bucketName': bucket_name,
                    'fileName': file_name,
                    'generateID': generate_id if generate_id else 'undefined',
                    'process_pipeline': pipeline,
                    'jobName': job_name,
                    'status': status
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

        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity',
                                json=post_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                return res.json(), res.status_code
        except Exception as e:
            return str(e), 403

        return res.json(), 200

    # # @jwt_required()
    # # @check_role("member")
    # def put(self):
    #     post_data = request.get_json()

    #     sample_post_payload = {
    #         'owner': 'admin',
    #         'modifiedTime': 0,
    #         'replicatedTo': None,
    #         'userDescription': None,
    #         'isFile': False,
    #         'numberOfReplicas': 0,
    #         'replicatedFrom': None,
    #         'displayName': None,
    #         'description': None,
    #         'extendedAttributes': None,
    #         'nameServiceId': None,
    #         'posixPermissions': None,
    #         'clusterName': None,
    #         'isSymlink': False,
    #         'group': None,
    #         'updateBy': 'admin'
    #     }
    #     sample_post_payload.update(post_data)

    #     # same api but different payload
    #     post_data = {
    #         'referredEntities': {},
    #         'entity': {
    #             'typeName': 'nfs_file_processed',
    #             'attributes': sample_post_payload,
    #             'isIncomplete': False,
    #             'status': 'ACTIVE',
    #             'createdBy': 'admin',
    #             'version': 0,
    #             'relationshipAttributes': {
    #                 'schema': [],
    #                 'inputToProcesses': [],
    #                 'meanings': [],
    #                 'outputFromProcesses': []
    #             },
    #             'customAttributes': {},
    #             'labels': []
    #         }
    #     }

    #     try:
    #         res = requests.post(ConfigClass.METADATA_API+'/v1/entity',
    #                             json=post_data, headers={'content-type': 'application/json'})
    #         print(res.text)
    #         if res.status_code != 200:
    #             return res.json(), res.status_code
    #     except Exception as e:
    #         print(e)
    #         return str(e), 403

    #     return res.json(), 200


class totalFileCount(Resource):
    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        Get file count from total raw and processed base on container id
        '''
        try:
            # Fetch upload path from neo4j service
            url = ConfigClass.NEO4J_SERVICE + \
                'nodes/Dataset/node/' + str(container_id)
            res = requests.get(url=url)
        except Exception as e:
            return {'result': str(e)}, 403

        project_role = current_identity['project_role']
        datasets = res.json()
        if res.status_code != 200:
            return {'result': res.json()}, res.status_code
        if len(datasets) == 0:
            return {'result': 'Container does not exist.'}, 404
        container_path = datasets[0]['path']
        if not container_path:
            return {'result': 'Cannot find the path attribute.'}, 403

        # call the atlas service to get the file information
        criterion = [
            {
                'attributeName': 'bucketName',
                'attributeValue': container_path,
                'operator': 'eq'
            }
        ]
        delete_criterion = [
            {
                'attributeName': 'bucketName',
                'attributeValue': container_path,
                'operator': 'eq'
            },
            {
                'attributeName': '__customAttributes',
                'attributeValue': "archived",
                'operator': 'CONTAINS'
            }
        ]
        post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': False,
            'includeSubTypes': False,
            'includeClassificationAttributes': False,
            'entityFilters': {
                "condition": "AND",
                "criterion": criterion
            },
            'tagFilters': None,
            'attributes': ['__customAttributes'],
            'limit': 100,
            'offset': 0,
            'typeName': 'nfs_file',
            'classification': None,
            'termName': None
        }

        delete_post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': False,
            'includeSubTypes': False,
            'includeClassificationAttributes': False,
            'entityFilters': {
                "condition": "AND",
                "criterion": delete_criterion
            },
            'tagFilters': None,
            'attributes': ['__customAttributes'],
            'limit': 100,
            'offset': 0,
            'typeName': 'nfs_file',
            'classification': None,
            'termName': None
        }

        if project_role != 'admin':
            criterion = [
                {
                    'attributeName': 'bucketName',
                    'attributeValue': container_path,
                    'operator': 'eq'
                },
                {
                    'attributeName': 'owner',
                    'attributeValue': current_identity['username'],
                    'operator': 'eq'
                }
            ]
            delete_criterion = [
                {
                    'attributeName': 'bucketName',
                    'attributeValue': container_path,
                    'operator': 'eq'
                },
                {
                    'attributeName': 'owner',
                    'attributeValue': current_identity['username'],
                    'operator': 'eq'
                },
                {
                    'attributeName': '__customAttributes',
                    'attributeValue': "archived",
                    'operator': 'CONTAINS'
                }
            ]
            post_data['entityFilters'] = {
                "condition": "AND",
                "criterion": criterion
            }
            delete_post_data['entityFilters'] = {
                "condition": "AND",
                "criterion": delete_criterion
            }

        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=post_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                return {'result': res.json()}, 403
            res = res.json()['result']
            raw_count = res['approximateCount']

            delete_res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=delete_post_data, headers={'content-type': 'application/json'})
            if delete_res.status_code != 200:
                return {'result': delete_res.json()}, 403
            delete_res = delete_res.json()['result']
            delete_raw_count = delete_res['approximateCount']
        except Exception as e:
            return {'result': str(e)}, 403

        # next use the nfs_processed to get the count of processed file
        post_data['typeName'] = 'nfs_file_processed'
        delete_post_data['typeName'] = 'nfs_file_processed'
        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=post_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                return {'result': res.json()}, 403
            res = res.json()['result']
            processed_count = res['approximateCount']

            delete_res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=delete_post_data, headers={'content-type': 'application/json'})
            if delete_res.status_code != 200:
                return {'result': delete_res.json()}, 403
            delete_res = delete_res.json()['result']
            delete_processed_count = delete_res['approximateCount']
        except Exception as e:
            return {'result': str(e)}, 403
        return {'result': {'raw_file_count': raw_count - delete_raw_count, 'process_file_count': processed_count - delete_processed_count }}, 200


class dailyFileCount(Resource):
    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        Get file count from total raw and processed base on container id
        '''
        try:
            # Fetch upload path from neo4j service
            url = ConfigClass.NEO4J_SERVICE + \
                'nodes/Dataset/node/' + str(container_id)
            res = requests.get(url=url)
        except Exception as e:
            return {'result': str(e)}, 403

        datasets = res.json()
        if res.status_code != 200:
            return {'result': res.json()}, res.status_code
        if len(datasets) == 0:
            return {'result': 'Container does not exist.'}, 404
        container_path = datasets[0]['path']
        if not container_path:
            return {'result': 'Cannot find the path attribute.'}, 403

        # new parameter admin view
        # since this api will have different return if there are different roles
        # the uploader will only see the file he uploaded
        # while admin will see all the files but somehow the admin want to see
        # the file only he uploaded
        admin_view = request.args.get('admin_view', True)
        # new parameter to retrieve the number of logs
        page_size = request.args.get('size', None)
        page = request.args.get('page', 0)

        # allow to filter on the user
        user = request.args.get('user', None)

        # only return download or upload
        action = request.args.get('action', None)

        # new update here we can filter on the date range
        # format should be yyyy-mm-dd or yyyy/mm/dd
        start_timestamp = request.args.get('start_date', None)
        end_timestamp = request.args.get('end_date', None)

        # also it is daily so we get the today's timestamp as default one
        if start_timestamp == None or end_timestamp == None:
            today_date = datetime.now().date()
            today_datetime = datetime.combine(today_date, datetime.min.time())
            start_date = int(today_datetime.timestamp())

            end_datetime = datetime.combine(
                today_date + timedelta(days=1), datetime.min.time())
            end_date = int(end_datetime.timestamp())
        else:
            # start_datetime = datetime.strptime(start_timestamp, '%Y-%m-%d')
            # start_date = int(start_datetime.timestamp())

            # end_datetime = datetime.strptime(end_timestamp, '%Y-%m-%d')
            # end_date = int(end_datetime.timestamp())
            start_date = int(start_timestamp)
            end_date = int(end_timestamp)

        # if the current user is uploader then he can only see the count by himself
        criterion_template = [
            {
                'attributeName': 'bucketName',
                'attributeValue': container_path,
                'operator': 'eq'
            },
            {
                'attributeName': 'createTime',
                'attributeValue': int(start_date),
                'operator': 'gte'
            },
            {
                'attributeName': 'createTime',
                'attributeValue': int(end_date),
                'operator': 'lte'
            }
        ]
        criterion = list(criterion_template)
        if current_identity['project_role'] != 'admin':
            criterion.append({
                'attributeName': 'owner',
                'attributeValue': current_identity['username'],
                'operator': 'eq'
            })
        elif current_identity['project_role'] == 'admin' and admin_view != True:
            criterion.append({
                'attributeName': 'owner',
                'attributeValue': current_identity['username'],
                'operator': 'eq'
            })
        # add user filter ONLY inside admin view
        elif current_identity['project_role'] == 'admin' and admin_view == True:
            if user:
                criterion.append({
                    'attributeName': 'owner',
                    'attributeValue': user,
                    'operator': 'eq'
                })

        # call the atlas service to get the file information
        post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': False,
            'includeSubTypes': False,
            'includeClassificationAttributes': False,
            'entityFilters': {
                "condition": "AND",
                "criterion": criterion
            },
            'tagFilters': None,
            'attributes': ['owner', 'downloader', 'fileName'],
            # 'limit': page_size,
            # 'offset': str(int(page) * int(page_size)),
            'sortBy': 'createTime',
            'sortOrder': 'DESCENDING',
            'typeName': 'nfs_file',
            'classification': None,
            'termName': None
        }

        if page_size:
            post_data['limit'] = page_size
            post_data['offset'] = str(int(page) * int(page_size))

        try:
            if action == 'upload' or action == 'all':
                res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                    json=post_data, headers={'content-type': 'application/json'})
                if res.status_code != 200:
                    return {'result': res.json()}, 403
                res = res.json()['result']
                upload_count = res['approximateCount']

                # because if there is no file it will not return entities field
                # so check it up
                if not res.get('entities', None):
                    res.update({'entities': []})

                # also change the timestamp from int to string
                # for e in res['entities']:
                #     timestamp_int = e['attributes'].get('createTime', None)
                #     # print(timestamp_int)
                #     if timestamp_int:
                #         central = datetime.fromtimestamp(timestamp_int, tz=timezone.utc)
                #         e['attributes']['createTime'] = central.strftime(
                #             '%Y-%m-%d %H:%M:%S')
                upload_log = res['entities']
            else:
                upload_count = 0
                upload_log = []

        except Exception as e:
            return {'result': str(e)}, 403

        # next use the nfs_file_download to get the count of download count
        post_data['typeName'] = 'nfs_file_download'
        # same here get the download count and log based on the role
        criterion = list(criterion_template)
        if current_identity['project_role'] != 'admin':
            criterion.append({
                'attributeName': 'downloader',
                'attributeValue': current_identity['username'],
                'operator': 'eq'
            })
            post_data['entityFilters']['criterion'] = criterion
        elif current_identity['project_role'] == 'admin' and admin_view != True:
            criterion.append({
                'attributeName': 'downloader',
                'attributeValue': current_identity['username'],
                'operator': 'eq'
            })
            post_data['entityFilters']['criterion'] = criterion
        # add user filter ONLY inside admin view
        elif current_identity['project_role'] == 'admin' and admin_view == True:
            if user:
                criterion.append({
                    'attributeName': 'downloader',
                    'attributeValue': user,
                    'operator': 'eq'
                })
            post_data['entityFilters']['criterion'] = criterion

        try:
            if action == 'download' or action == 'all':
                res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                    json=post_data, headers={'content-type': 'application/json'})
                if res.status_code != 200:
                    return {'result': res.json()}, 403
                res = res.json()['result']
                download_count = res['approximateCount']

                # because if there is no file it will not return entities field
                # so check it up
                if not res.get('entities', None):
                    res.update({'entities': []})

                # also change the timestamp from int to string
                # for e in res['entities']:
                #     timestamp_int = e['attributes'].get('createTime', None)
                #     if timestamp_int:
                #         central = datetime.fromtimestamp(timestamp_int, tz=timezone.utc)
                #         e['attributes']['createTime'] = central.strftime(
                #             '%Y-%m-%d %H:%M:%S')
                download_log = res['entities']
            else:
                download_count = 0
                download_log = []

        except Exception as e:
            return {'result': str(e)}, 403

        return {'result': {'recent_upload': upload_log, 'upload_count': upload_count,
                           'recent_download': download_log, 'download_count': download_count}}, 200

class FileExists(Resource):
    def post(self):
        post_data = request.get_json()
        full_path = post_data.get('full_path', None)
        return {"result": os.path.isfile(full_path)}, 200
