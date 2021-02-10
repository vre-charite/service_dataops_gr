from app import executor
from config import ConfigClass
import os
import time
import shutil

import pika
import json
import requests
from flask import current_app
import datetime
import services.file_upload as srv_upload
import models.fsm_file_upload as fsmup
from services.tags_services.tags_manager import SrvTagsMgr
from services.data_operations.data_meta_manager import SrvFileDataMgr


class fs(object):
    # -------------------------------------------------------------------- #
    #                        File related functions                        #
    # -------------------------------------------------------------------- #
    @executor.job
    def upload_to_nfs(temp_dir, chunk_paths, target_file_name,
                      upload_path, uploader, tags, metadatas, bucket_name, size,
                      status_mgr: srv_upload.SrvFileUpStateMgr, container_id):
        # Upload task to combine file chunks and upload to nfs
        already_moved = False
        try:
            with open(target_file_name, 'ab') as target_file:
                for p in chunk_paths:
                    stored_chunk_file_name = p
                    stored_chunk_file = open(stored_chunk_file_name, 'rb')
                    target_file.write(stored_chunk_file.read())
                    stored_chunk_file.close()
                    os.unlink(stored_chunk_file_name)

        except FileNotFoundError:
            already_moved = True
            current_app.logger.warning(
                'folder {} is already empty'.format(temp_dir))
            # return True, 'success'

        except Exception as e:
            current_app.logger.error('combining chunks but {}'.format(str(e)))
            return False, 'Combining chunks but %s' % str(e)

        current_app.logger.info('done with combinging chunks')

        try:
            # copy file to NFS folder
            file_name = target_file_name.split('/')[-1]
            shutil.move(target_file_name, upload_path)
            current_app.logger.info('already_moved: ' + str(already_moved))
            if not already_moved:
                project_code = bucket_name
                input_path = upload_path + '/' + file_name
                # timezone_dt = datetime.datetime.now(datetime.timezone.utc)
                payload = {
                    "event_type": "data_uploaded",
                    "payload": {
                        "input_path": input_path,
                        "project": project_code,
                        "generate_id": metadatas.get('generateID'),
                        "uploader": uploader
                    },
                    "create_timestamp": time.time()
                }
                send_to_queue(payload)

                # copy_payload = {
                #     "event_type": "data_processed",
                #     "payload": {
                #         "input_path": input_path,
                #         "project": project_code,
                #         "generate_id": metadatas.get('generateID'),
                #         "uploader": uploader
                #     },
                #     "create_timestamp": time.time()
                # }
                # send_to_queue(copy_payload)

        except Exception as e:
            current_app.logger.error('moving file but {}'.format(str(e)))
            return False, 'Moving file but %s' % str(e)

        current_app.logger.info(
            'done with moving files {}'.format(target_file_name))

        try:
            current_app.logger.debug(
                'create atlas record but tags {} in format {}'.format(tags, file_name))
            # create file meta v2
            file_meta_mgr = SrvFileDataMgr(current_app.logger)
            res_create_meta = file_meta_mgr.create(
                uploader,
                file_name,
                upload_path,
                size,
                'Raw file in greenroom',
                'greenroom',
                'raw',
                bucket_name,
                tags,
                metadatas.get('generateID', 'undefined'))
            if res_create_meta.get('error'):
                current_app.logger.error(str(res_create_meta))
                raise Exception('error when creating meta v2')
            else:
                current_app.logger.info('done with creating atlas record v2')
            # create entity in atlas
            post_data = {
                'referredEntities': {},
                'entity': {
                    'typeName': 'nfs_file',
                    'attributes': {
                        'owner': uploader,
                        'modifiedTime': 0,
                        'replicatedTo': None,
                        'userDescription': None,
                        'isFile': False,
                        'numberOfReplicas': 0,
                        'replicatedFrom': None,
                        'qualifiedName': upload_path+'/'+file_name,
                        'displayName': None,
                        'description': None,
                        'extendedAttributes': None,
                        'nameServiceId': None,
                        'path': upload_path,
                        'posixPermissions': None,
                        'createTime': time.time(),
                        'fileSize': size,
                        'clusterName': None,
                        'name': upload_path+'/'+file_name,
                        'isSymlink': False,
                        'group': None,
                        'updateBy': 'test_no_auth',
                        'bucketName': bucket_name,
                        'fileName': file_name,
                        'generateID': metadatas.pop('generateID')
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
                    'customAttributes': metadatas,
                    'labels': tags
                }
            }
            res = requests.post(ConfigClass.METADATA_API+'/v1/entity',
                                json=post_data, headers={'content-type': 'application/json'})
            current_app.logger.debug(ConfigClass.METADATA_API + '/v1/entity +' + str(post_data))
            if res.status_code != 200:
                raise Exception(res.text)

        except Exception as e:
            current_app.logger.error(
                'create atlas record but {}'.format(str(e)))
            return False, 'Create atlas record but %s' % str(e)
        current_app.logger.info('done with creating atlas record')

        try:
            # update tag frequence in redis
            for tag in tags:
                SrvTagsMgr().add_freq(container_id, tag)
        except Exception as e:
            current_app.logger.error(
                'add tag in redis but {}'.format(str(e)))
            return False, 'Add tags in redis but %s' % str(e)

        status_mgr.go(fsmup.EState.FINALIZED)
        try:
            # clean up tmp folder
            shutil.rmtree(temp_dir)
        except Exception as e:
            current_app.logger.error('clean up folder but {}'.format(str(e)))
            return False, 'Clean up folder but %s' % str(e)

        current_app.logger.info('done with clean up folders.')
        status_mgr.go(fsmup.EState.SUCCEED)
        return True, 'success'

    # -------------------------------------------------------------------- #
    #                      Folder related functions                        #
    # -------------------------------------------------------------------- #
    def create_folder(self, parent, name):
        # check if parent path exist
        if(parent):
            root = os.path.join(ConfigClass.NFS_ROOT_PATH, parent)
        else:
            root = ConfigClass.NFS_ROOT_PATH[:-1]

        if not os.path.isdir(root):
            raise ValueError('Path to folder does not exist.')

        # check if folder name duplicate
        new_path = os.path.join(root, name)
        if os.path.isdir(new_path):
            raise ValueError('Folder is already exist.')

        # create new folder
        os.makedirs(new_path)
        return new_path

    def list_folder(self, path, field):
        def fast_scandir(dirname):
            file_list = []
            for f in os.scandir(dirname):
                if field != 'file' and f.is_dir():  # list folders
                    sub_list = {}
                    sub_list[f.name] = fast_scandir(f.path)
                    file_list.append(sub_list)
                if field != 'folder' and not f.is_dir():  # list files
                    file_list.append(f.name)
            return file_list

        # check if parent path exist
        if not os.path.isdir(path):
            raise ValueError('Path to folder does not exist.')

        # recursively list folder
        res = fast_scandir(path)
        return res

    def remove_folder(self, path):
        shutil.rmtree(path)


def send_to_queue(payload):
    _logger = current_app.logger
    url = ConfigClass.service_queue_send_msg_url
    _logger.info("Sending Message To Queue: " + str(payload))
    res = requests.post(
        url=url,
        json=payload,
        headers={"Content-type": "application/json; charset=utf-8"}
    )
    _logger.info(res.text)
    return json.loads(res.text)

# save the chunk data


def generate_chunk_name(uploaded_filename, chunk_number):
    return uploaded_filename + '_part_%03d' % chunk_number
