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

def get_geid():
    url = ConfigClass.UTILITY_SERVICE + "utility/id"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['result']
    else:
        raise Exception('get_geid {}: {}'.format(response.status_code, url))



class fs(object):
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
