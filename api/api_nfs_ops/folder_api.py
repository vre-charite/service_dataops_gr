from flask import request, current_app
from flask_restful import Resource
from flask_jwt import jwt_required
from resources.utils import fs
from urllib.parse import unquote
import os
from config import ConfigClass
import requests
import json
from flask_restx import Api, Resource
from api import nfs_entity_ns

from resources.swagger_modules import folder, success_return, folder_return
from resources.decorator import check_role


class folders(Resource):

    @nfs_entity_ns.expect(folder)
    @nfs_entity_ns.response(200, success_return)
    # @jwt_required()
    # @check_role("member")
    def post(self):
        '''
        This method allow to create a folder as path in args
        '''
        # Get folder path from args
        post_data = request.get_json()
        container_id = post_data.get('container_id', None)

        # Determine root path
        service = post_data.get('service', None)
        root_path = ConfigClass.NFS_ROOT_PATH
        if service == 'VRE':
            root_path = ConfigClass.VRE_ROOT_PATH

        # Fetch upload path from neo4j service
        container_path = ''
        if(container_id is not None):
            try:
                url = ConfigClass.NEO4J_SERVICE + \
                    'nodes/Dataset/node/' + str(container_id)
                res = requests.get(url=url)
            except Exception as e:
                return {'Error': str(e)}, 403

            datasets = res.json()
            if(res.status_code != 200):
                return {'result': res.json()}, res.status_code
            if(len(datasets) == 0):
                return {'result': 'Container does not exist.'}, 404
            container_path = datasets[0].get('path', None)
            if not container_path:
                return {'result': 'Cannot find the path attribute.'}, 403

        sub_path = post_data.get('path', '')
        full_path = os.path.join(
            root_path, container_path, sub_path)

        # Create a folder
        try:
            parent, folder_name = os.path.split(full_path)
            fs().create_folder(parent, folder_name)  # remove first "/"
        except Exception as e:
            return {'Error': str(e)}, 403

        return {'result': 'success'}, 200

    @nfs_entity_ns.doc(params={'field': {'type': 'string'}})
    @nfs_entity_ns.expect(folder)
    @nfs_entity_ns.response(200, folder_return)
    @jwt_required()
    @check_role("member")
    def get(self):
        '''
        This method allow to walk through folders.
        '''

        # Get folder path from args, decode utf-8
        container_id = request.args.get('container_id', None)
        container_path = ""

        # Fetch upload path from neo4j service
        if(container_id is not None):
            try:
                url = ConfigClass.NEO4J_SERVICE + \
                    'nodes/Dataset/node/' + str(container_id)
                res = requests.get(url=url)
            except Exception as e:
                return {'Error': str(e)}, 403

            datasets = res.json()
            if(res.status_code != 200):
                return {'result': res.json()}, res.status_code
            if(len(datasets) == 0):
                return {'result': 'Container does not exist.'}, 404
            container_path = datasets[0].get('path', None)
            if not container_path:
                return {'result': 'Cannot find the path attribute.'}, 403

        sub_path = unquote(request.args.get('path', ''))
        gr_full_path = os.path.join(
            ConfigClass.NFS_ROOT_PATH, container_path, sub_path)
        vre_full_path = os.path.join(
            ConfigClass.VRE_ROOT_PATH, container_path, sub_path)

        # Specify the output field: file/folder/both
        field = request.args.get('field', None)

        # Recusive Fetch folder list
        try:
            gr_folders = fs().list_folder(gr_full_path, field)
            vre_folders = fs().list_folder(vre_full_path, field)

        except Exception as e:
            if str(e) == 'Path to folder does not exist.':
                return {'Error': str(e)}, 404
            return {'Error': str(e)}, 403

        return {'result': {'gr': gr_folders, 'vre': vre_folders}}, 200
