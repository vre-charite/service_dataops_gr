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

from datetime import datetime, timedelta
from dateutil import tz
import time

from config import ConfigClass

from app import executor
from api import nfs_entity_ns
import errno

from resources.swagger_modules import file_upload, file_upload_last_response, file_upload_status, file_download
from resources.utils import fs
from resources.decorator import check_role

# auto zone detection
from_zone = tz.tzutc()
to_zone = tz.tzlocal()


class files(Resource):

    @nfs_entity_ns.expect(file_upload)
    @nfs_entity_ns.response(200, file_upload_last_response)
    @jwt_required()
    @check_role("uploader")
    def post(self, container_id):
        '''
        Upload a file chunk to NFS and create corresponding record in atlas when all chunks received
        '''
        current_app.logger.info(
            'Call API for upload chunks to container {} with info {}'.format(str(container_id), request.form.to_dict()))
        try:
            # get file info
            resumable_identifier = request.form.get(
                'resumableIdentifier', default='error', type=str)
            resumable_filename = request.form.get(
                'resumableFilename', default='error', type=str)
            resumabl_chunk_number = request.form.get(
                'resumableChunkNumber', default=1, type=int)
            resumable_total_chunks = request.form.get(
                'resumableTotalChunks', type=int)
            resumable_total_size = request.form.get(
                'resumableTotalSize', type=int)

            # the input might be undefined
            tags = request.form.get('tags', 'undefined', type=str)
            if tags == 'undefined':
                tags = []
            generateID = request.form.get('generateID', None)
            uploader = request.form.get('uploader', '')
            metadatas = {
                'generateID': generateID
            }

            # For Generate project, add generate id as prefix
            if generateID and generateID != 'undefined':
                resumable_filename = generateID + '_' + resumable_filename

            chunk_data = request.files['file']

            # do the initial check for the first chunk
            if resumabl_chunk_number == 1:
                # Check if container exist and fetch upload path
                url = ConfigClass.NEO4J_SERVICE + 'nodes/Dataset/node/' + str(container_id)
                res = requests.get(url=url)
                if res.status_code != 200:
                    current_app.logger.error(
                        'neo4j service: {}'.format(res.json()))
                    return {'result': res.json()}, res.status_code

                datasets = res.json()
                if len(datasets) == 0:
                    current_app.logger.error(
                        'neo4j service: container does not exist.')
                    return {'result': 'Container does not exist.'}, 404
                d_path = datasets[0].get('path', None)
                if not d_path:
                    current_app.logger.error(
                        'neo4j service: cannot find the path attribute.')
                    return {'result': 'Cannot find the path attribute.'}, 403

                # (optional) check if the folder is not existed
                path = os.path.join(
                    ConfigClass.NFS_ROOT_PATH, d_path)
                # subPath = request.form.get(
                #     'subPath', default='', type=str)
                path = os.path.join(path, "raw")
                if not os.path.isdir(path):
                    current_app.logger.error(
                        'nfs service: folder raw does not existed!')
                    return {'Error': 'Folder raw does not existed!'}, 403

                # check if the file is already existed
                file_path = os.path.join(path, resumable_filename)
                if os.path.isfile(file_path):
                    current_app.logger.error(
                        'nfs service: File %s is already existed!' % resumable_filename)
                    return {'Error': 'File %s is already existed!' % resumable_filename}, 403

            # make our temp directory
            try:
                temp_dir = os.path.join(
                    ConfigClass.TEMP_BASE, resumable_identifier)
                if not os.path.isdir(temp_dir):
                    os.makedirs(temp_dir)
                    current_app.logger.info(
                        'success to create temp dir {}.'.format(temp_dir))
            except OSError as e:
                if e.errno == errno.EEXIST:
                    current_app.logger.warning(
                        'folder {} exists. '.format(temp_dir))
                    pass
                else:
                    return {'result': 'make temp dir'+str(e)}, 403

            except Exception as e:
                current_app.logger.error(
                    'failed to create temp dir {}: {}'.format(temp_dir, str(e)))

                return {'result': 'make temp dir'+str(e)}, 403

            # save the chunk data
            def get_chunk_name(uploaded_filename, chunk_number):
                return uploaded_filename + '_part_%03d' % chunk_number

            try:
                chunk_name = get_chunk_name(
                    resumable_filename, resumabl_chunk_number)
                chunk_file = os.path.join(temp_dir, chunk_name)
                chunk_data.save(chunk_file)
                chunk_paths = [
                    os.path.join(
                        temp_dir,
                        get_chunk_name(resumable_filename, x)
                    )
                    for x in range(1, resumable_total_chunks+1)
                ]
                upload_complete = all([os.path.exists(p) for p in chunk_paths])
            except Exception as e:
                current_app.logger.error(
                    'failed to save chunk in tmp.')
                return {'result': 'save chunk in tmp'+str(e)}, 403

            # combine all the chunks to create the final file
            if upload_complete:
                print('All chunks received, start background task...')

                # Fetch upload path from neo4j service
                url = ConfigClass.NEO4J_SERVICE + 'nodes/Dataset/node/' + str(container_id)
                res = requests.get(url=url)
                datasets = res.json()
                if res.status_code != 200:
                    return {'result': res.json()}, res.status_code
                if len(datasets) == 0:
                    return {'result': 'Container does not exist.'}, 404
                # since we check it before so dont check it again
                path = datasets[0].get('path', None)

                # format upload path with subfolder if required
                upload_path = os.path.join(
                    ConfigClass.NFS_ROOT_PATH,
                    path,  # root path
                    "raw")

                print('File will be uploaded to %s' % upload_path)

                target_file_name = os.path.join(temp_dir, resumable_filename)
                task_id = 'upload' + str(uuid.uuid4())
                current_app.logger.info(
                    'All chunks received, start background task {}'.format(task_id))

                # start thread for uploading
                fs().upload_to_nfs.submit_stored(task_id, temp_dir, chunk_paths,
                                                 target_file_name, upload_path, uploader,
                                                 tags, metadatas, path, resumable_total_size)

                return {'result': {'task_id': task_id, 'message': 'All chunks received, task_id is %s' % task_id}}, 200

            return {'result': 'success'}, 200

        except Exception as e:
            current_app.logger.error(
                'Error in uploading chunks: {}'.format(str(e)))
            return {'result': str(e)}, 403

    @nfs_entity_ns.doc(params={'task_id': {'type': 'string'}})
    @nfs_entity_ns.response(200, file_upload_status)
    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        This method allow to check file upload status.
        '''
        current_app.logger.info(
            'Call API for upload chunks to container {} with info {}'.format(str(container_id), request.args.to_dict()))

        task_id = request.args.get('task_id', None)
        if(task_id is None):
            return {'result': 'task id is required.'}, 403

        task_done = executor.futures.done(task_id)
        if task_done is None:
            return {'result': 'task does not exist'}, 404

        if not task_done:
            result = {'status': 'running'}
            return {'result': result}

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


class file_predownload(Resource):
    @jwt_required()
    @check_role("uploader")
    def post(self, container_id):

        post_data = request.get_json()
        current_app.logger.info(
            'Call API for predownload to container {} with info {}'.format(str(container_id), post_data))
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
                    'uuid': uuid.uuid4().hex,
                    'createTime': time.time(),
                    'downloader': username,
                    'bucketName': container_path,
                    'fileName': None
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

        # loop over each file to download
        for f in files:
            try:
                post_data['entity']['attributes']['fileName'] = f['file']
                res = requests.post(ConfigClass.METADATA_API+'/v1/entity',
                                    json=post_data, headers={'content-type': 'application/json'})
                if res.status_code != 200:
                    return res.json(), res.status_code
            except Exception as e:
                print(str(e))

        if(len(files) == 1):
            filename = files[0]['file']
            path = files[0]['path']
            full_path = os.path.join(
                ConfigClass.NFS_ROOT_PATH, container_path, path, filename)
            if not os.path.exists(full_path):
                current_app.logger.error(
                    'nfs file check: file %s not found.' % filename)
                return {'result': 'File %s not found.' % filename}, 404

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
            return {'result': download_token.decode('utf-8')}, 200

        else:
            # Start zipping files
            zip_filename = container_path + '_'+str(int(time.time()))+'.zip'
            target_path = os.path.join(
                ConfigClass.NFS_ROOT_PATH, container_path, 'workdir')
            zip_multi_files.submit_stored(
                zip_filename, zip_filename, target_path, container_path,  files)
            return {'result': {'task_id': zip_filename, 'message': 'Start processing zip file.'}}, 201

    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        This method allow to check download file zipping status.
        '''
        current_app.logger.info(
            'Call API for check download status to container {} with info {}'.format(str(container_id), request.args.to_dict()))
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
                central = datetime.fromtimestamp(timestamp_int)
                e['attributes']['createTime'] = central.strftime(
                    '%Y-%m-%d %H:%M:%S')

        return {"result": res}, 200


@executor.job
def zip_multi_files(zip_filename, target_path, container_path, files):
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
    except Exception as e:
        return False, str(e)

    return True, 'success'


class file(Resource):
    @ nfs_entity_ns.expect(file_download)
    # @jwt_required()
    def get(self):
        '''
        This method allow to download single file.
        '''
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


class fileInfo(Resource):

    query_sample_return = '''
    {
        "result": {
            "entities": [
                {
                    "meanings": [],
                    "labels": [],
                    "displayText": "/usr/data/test1111/init.txt",
                    "attributes": {
                        "name": "/usr/data/test1111/init.txt",
                        "owner": "admin",
                        "generateID": "BXT-1234!",
                        "createTime": 0,
                        "bucketName": "test1111"
                    },
                    "classifications": [],
                    "classificationNames": [],
                    "typeName": "nfs_file",
                    "isIncomplete": false,
                    "status": "ACTIVE",
                    "guid": "c03d51b2-11be-400b-82ab-6b080ea12c50",
                    "meaningNames": []
                }
            ],
            "searchParameters": {
                "excludeDeletedEntities": true,
                "limit": 25,
                "includeClassificationAttributes": true,
                "attributes": [
                    "generateID"
                ],
                "includeSubClassifications": true,
                "includeSubTypes": true,
                "typeName": "nfs_file",
                "entityFilters": {
                    "operator": "contains",
                    "attributeValue": "test1111",
                    "attributeName": "bucketName"
                },
                "offset": 0
            },
            "approximateCount": 2,
            "queryType": "BASIC"
        }
    }
    '''

    # get the file info under the container
    @ nfs_entity_ns.response(200, query_sample_return)
    @ nfs_entity_ns.param('page_size', 'number of entities return per request')
    @ nfs_entity_ns.param('page', 'offset of query which page to start')
    @ nfs_entity_ns.param('stage', 'possible value: raw(default)/processed indicates if it is raw or processed file')
    @ nfs_entity_ns.param('column', 'which column user want to order')
    @ nfs_entity_ns.param('text', 'full text search')
    @ nfs_entity_ns.param('order', 'possible value asc and desc to tell order of return')
    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):
        '''
        Get file detail infomation under container
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

        # add new query string here for pagination
        default_page_size = 25
        page_size = int(request.args.get('page_size', default_page_size))
        # note here the offset is page_size * page
        page = int(request.args.get('page', 0))

        # use the stage variable to indicate entity type
        entity_type = 'nfs_file'

        # also add new query string for sorting the column default is time
        sorting = request.args.get('column', 'createTime')
        # also can search the text
        text = request.args.get('text', None)
        # default order is desceding
        order = request.args.get('order', 'desc')
        order = 'ASCENDING' if order == 'asc' else 'DESCENDING'

        # new parameter admin view
        # since this api will have different return if there are different roles
        # the uploader will only see the file he uploaded
        # while admin will see all the files but somehow the admin want to see
        # the file only he uploaded
        admin_view = request.args.get('admin_view', True)

        # if the current user is uploader then he can only see the count by himself
        criterion_template = [
            {
                'attributeName': 'bucketName',
                'attributeValue': container_path,
                'operator': 'eq'
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
            'attributes': ['generateID', 'fileName', 'fileSize', 'path'],
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
        if text:
            post_data.update({'query': '"%s"' % text})

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
                central = datetime.fromtimestamp(timestamp_int)
                e['attributes']['createTime'] = central.strftime(
                    '%Y-%m-%d %H:%M:%S')

        return {"result": res}, 200


# deprecated
class fileExistCheck(Resource):

    # @jwt_required()
    # @check_role("member")
    def get(self, container_id):
        '''
        Get file detail infomation under container
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

        # use the query string to get the file name
        filename = request.args.get('filename', None)
        # then make query into atlas to see if file exist or not
        post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': True,
            'includeSubTypes': True,
            'includeClassificationAttributes': True,
            'entityFilters': {
                'condition': 'AND',
                'criterion': [
                    {
                        'attributeName': 'bucketName',
                        'operator': 'eq',
                        'attributeValue': container_path
                    },
                    {
                        "attributeName": "fileName",
                        "operator": "eq",
                        "attributeValue": filename
                    }
                ]
            },
            'tagFilters': None,
            'attributes': [],
            'limit': 10,
            'offset': 0,
            'typeName': 'nfs_file',
            'classification': None,
            'termName': None
        }

        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=post_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                return {'result': res.json()}, 403
            res = res.json()['result']
        except Exception as e:
            return {'result': str(e)}, 403

        # if there is entity array then there is file exist return true
        if res.get('entities', None):
            return {'result': True}, 200

        return {'result': False}, 200


class processedFile(Resource):
    @jwt_required()
    @check_role("member")
    # use query string to find the files
    def get(self):
        container_id = request.args.get('container_id', None)
        pipeline = request.args.get('pipeline', None)
        if not container_id or not pipeline:
            return {'result': 'query parameter container_id and pipeline are required'}, 403
        # add new query string here for pagination
        default_page_size = 25
        page_size = int(request.args.get('page_size', default_page_size))
        # note here the offset is page_size * page
        page = int(request.args.get('page', 0))
        # also add new query string for sorting the column default is time
        sorting = request.args.get('column', 'createTime')
        # also can search the text
        text = request.args.get('text', None)
        # default order is desceding
        order = request.args.get('order', 'desc')
        order = 'ASCENDING' if order == 'asc' else 'DESCENDING'

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

        # call the atlas service to get the file information
        post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': False,
            'includeSubTypes': False,
            'includeClassificationAttributes': False,
            'entityFilters': {
                "condition": "AND",
                "criterion": [
                    {
                        'attributeName': 'bucketName',
                        'attributeValue': container_path,
                        'operator': 'eq'
                    },
                    {
                        'attributeName': 'pipeline',
                        'attributeValue': pipeline,
                        'operator': 'eq'
                    }
                ]
            },
            'tagFilters': None,
            'attributes': ['generateID', 'upload_status', 'fileName', 'fileSize', 'path', 'pipeline'],
            'limit': page_size,
            'offset': page * page_size,
            # 'query': '"ABC-1234"',
            'sortBy': sorting,
            'sortOrder': order,
            'typeName': 'nfs_file_processed',
            'classification': None,
            'termName': None
        }
        # if we have the full text search input
        if text:
            post_data.update({'query': '"%s"' % text})

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
                central = datetime.fromtimestamp(timestamp_int)
                e['attributes']['createTime'] = central.strftime(
                    '%Y-%m-%d %H:%M:%S')

        return {'result': res}, 200

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
        pipeline = post_data.get('pipeline', None)
        job_name = post_data.get('job_name', None)
        status = post_data.get('status', None)

        owner = None
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

        try:
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
                    "the raw file does not exist please check your path")
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
                    'qualifiedName': file_name,
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
                    'generateID': file_name[0:8],
                    'pipeline': pipeline,
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

        # # use the name to create the relationship
        # post_data = {
        #     "createTime": int(time.time()),
        #     "createdBy": "admin",
        #     "end1": {
        #         "typeName": "nfs_file_processed",
        #         "uniqueAttributes": {
        #             "name": path
        #         }
        #     },
        #     "end2": {
        #         "typeName": "nfs_file",
        #         "uniqueAttributes": {
        #             "name": raw_file_path
        #         }
        #     },
        #     "propagateTags": "NONE",
        #     "label": "vre_pipeline",
        #     "status": "ACTIVE",
        #     "provenanceType": 1,
        #     "updateTime": int(time.time()),
        #     "updatedBy": "admin",
        #     "version": 1,
        #     "typeName": "vre_pipeline"
        # }
        # try:
        #     res = requests.post(ConfigClass.METADATA_API+'/v1/relation',
        #                         json=post_data, headers={'content-type': 'application/json'})
        #     print(res.text)
        #     if res.status_code != 200:
        #         return res.text, res.status_code
        # except Exception as e:
        #     print(e)
        #     return str(e), 403

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
    @check_role("admin")
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

        # call the atlas service to get the file information
        post_data = {
            'excludeDeletedEntities': True,
            'includeSubClassifications': False,
            'includeSubTypes': False,
            'includeClassificationAttributes': False,
            'entityFilters': {
                'attributeName': 'bucketName',
                'attributeValue': container_path,
                'operator': 'eq'
            },
            'tagFilters': None,
            'attributes': [],
            'limit': 1,
            'offset': 0,
            'typeName': 'nfs_file',
            'classification': None,
            'termName': None
        }

        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=post_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                return {'result': res.json()}, 403
            res = res.json()['result']
            raw_count = res['approximateCount']
        except Exception as e:
            return {'result': str(e)}, 403

        # next use the nfs_processed to get the count of processed file
        post_data['typeName'] = 'nfs_file_processed'
        try:
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity/basic',
                                json=post_data, headers={'content-type': 'application/json'})
            if res.status_code != 200:
                return {'result': res.json()}, 403
            res = res.json()['result']
            processed_count = res['approximateCount']
        except Exception as e:
            return {'result': str(e)}, 403

        return {'result': {'raw_file_count': raw_count, 'process_file_count': processed_count}}, 200


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
        page_size = request.args.get('size', 10)
        page = request.args.get('page', 0)

        # allow to filter on the user
        user = request.args.get('user', None)

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
            start_datetime = datetime.strptime(start_timestamp, '%Y-%m-%d')
            start_date = int(start_datetime.timestamp())

            end_datetime = datetime.strptime(end_timestamp, '%Y-%m-%d')
            end_date = int(end_datetime.timestamp())
            print(start_datetime)

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
                'operator': 'gt'
            },
            {
                'attributeName': 'createTime',
                'attributeValue': int(end_date),
                'operator': 'lt'
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
            'limit': page_size,
            'offset': page,
            'sortBy': 'createTime',
            'sortOrder': 'DESCENDING',
            'typeName': 'nfs_file',
            'classification': None,
            'termName': None
        }

        try:
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
            for e in res['entities']:
                timestamp_int = e['attributes'].get('createTime', None)
                # print(timestamp_int)
                if timestamp_int:
                    central = datetime.fromtimestamp(timestamp_int)
                    e['attributes']['createTime'] = central.strftime(
                        '%Y-%m-%d %H:%M:%S')
            upload_log = res['entities']
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
            for e in res['entities']:
                timestamp_int = e['attributes'].get('createTime', None)
                # print(timestamp_int)
                if timestamp_int:
                    central = datetime.fromtimestamp(timestamp_int)
                    e['attributes']['createTime'] = central.strftime(
                        '%Y-%m-%d %H:%M:%S')
            download_log = res['entities']
        except Exception as e:
            return {'result': str(e)}, 403

        return {'result': {'recent_upload': upload_log, 'upload_count': upload_count,
                           'recent_download': download_log, 'download_count': download_count}}, 200
