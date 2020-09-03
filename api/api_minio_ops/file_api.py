from flask import request
from config import ConfigClass
import os
from resources.utils import fs
from app import executor
import uuid
import json
import requests
from flask_restx import Api, Resource
from api import minio_entity_ns
from resources.swagger_modules import file_upload, file_upload_last_response, file_upload_status


class files(Resource):
    
    @minio_entity_ns.expect(file_upload)
    @minio_entity_ns.response(200, file_upload_last_response)
    def post(self, container_id):
        '''
        This method allow to file chuncks.
        '''
        try:
            # get file info
            resumableIdentifier = request.form.get(
                'resumableIdentifier', default='error', type=str)
            resumableFilename = request.form.get(
                'resumableFilename', default='error', type=str)
            resumableChunkNumber = request.form.get(
                'resumableChunkNumber', default=1, type=int)
            resumableTotalChunks = request.form.get(
                'resumableTotalChunks', type=int)

            # the input might be undefined
            tags = request.form.get('tags', 'undefined', type=str)
            if tags == 'undefined':
                tags = []
            generateID = request.form.get('generateID', "Not Provided")
            uploader = request.form.get('uploader', "Not Provided")
            metadatas = {
                "generateID": generateID
            }

            chunk_data = request.files['file']

            # do the initial check for the first chunk
            if resumableChunkNumber == 1:
                # Check if container exist and fetch upload path
                url = ConfigClass.NEO4J_SERVICE + "nodes/Dataset/node/" + container_id
                res = requests.get(url=url)
                datasets = json.loads(res.text)
                if(res.status_code != 200):
                    return {'result': json.loads(res.text)}, res.status_code
                if(len(datasets) == 0):
                    return {'result': "Container is not exist."}, 403

                # (optional) check if the folder is not existed
                path = os.path.join(
                    ConfigClass.NFS_ROOT_PATH, datasets[0]['path'])
                subPath = request.form.get(
                    'subPath', default='', type=str)
                if(len(subPath) != 0):
                    path = os.path.join(path, subPath)
                    if not os.path.isdir(path):
                        return {'Error': "Folder %s is not existed!" % subPath}, 403

                # check if the file is already existed
                file_path = os.path.join(path, resumableFilename)
                if os.path.isfile(file_path):
                    return {'Error': "File %s is already existed!" % resumableFilename}, 403

            # make our temp directory
            temp_dir = os.path.join(ConfigClass.TEMP_BASE, resumableIdentifier)
            if not os.path.isdir(temp_dir):
                os.makedirs(temp_dir)

            # save the chunk data
            def get_chunk_name(uploaded_filename, chunk_number):
                return uploaded_filename + "_part_%03d" % chunk_number
            chunk_name = get_chunk_name(
                resumableFilename, resumableChunkNumber)
            chunk_file = os.path.join(temp_dir, chunk_name)
            chunk_data.save(chunk_file)
            chunk_paths = [
                os.path.join(
                    temp_dir,
                    get_chunk_name(resumableFilename, x)
                )
                for x in range(1, resumableTotalChunks+1)
            ]
            upload_complete = all([os.path.exists(p) for p in chunk_paths])

            # combine all the chunks to create the final file
            if upload_complete:
                print("All chunks received, start background task...")

                # Fetch upload path from neo4j service
                url = ConfigClass.NEO4J_SERVICE + "nodes/Dataset/node/" + container_id
                res = requests.get(url=url)
                datasets = json.loads(res.text)
                if(res.status_code != 200):
                    return {'result': json.loads(res.text)}, res.status_code
                if(len(datasets) == 0):
                    return {'result': "Container is not exist."}, 403
                path = datasets[0]['path']

                # format upload path with subfolder if required
                upload_path = os.path.join(
                    ConfigClass.NFS_ROOT_PATH,
                    path,  # root path
                    request.form.get('subPath', default='', type=str))
                # upload_path = ConfigClass.NFS_ROOT_PATH

                print("File will be uploaded to %s" % upload_path)

                target_file_name = os.path.join(temp_dir, resumableFilename)
                task_id = 'upload' + str(uuid.uuid4())

                # start thread for uploading
                fs().upload_to_nfs.submit_stored(task_id, temp_dir, chunk_paths,
                                                 target_file_name, upload_path, uploader,
                                                 tags, metadatas)

                return {'result': "All chunks received, task_id is %s" % task_id}, 200

            return {'result': 'success'}, 200

        except Exception as e:
            return {'result': str(e)}, 403


    @minio_entity_ns.doc(params={'task_id': {'type': 'string'}})
    @minio_entity_ns.response(200, file_upload_status)
    def get(self, container_id):
        '''
        This method allow to check file upload status.
        '''
        try:
            task_id = request.args.get("task_id", None)
            if(task_id is None):
                return {'result': "task id is required."}, 403

            task_done = executor.futures.done(task_id)
            if task_done is None:
                return {'result': 'task does not exist'}

            if not task_done:
                result = {'status': 'running'}
                return {'result': result}

            future = executor.futures.pop(task_id)
            success, msg = future.result()
            if success:
                result = {'status': 'success'}
            else:
                result = {'status': 'error', 'message': msg}

        except Exception as e:
            return {'result': str(e)}, 403

        return {'result': result}, 200
