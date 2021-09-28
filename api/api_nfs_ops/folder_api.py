from flask import request, current_app
from flask_restful import Resource
from flask_jwt import jwt_required, current_identity
from resources.utils import fs
from urllib.parse import unquote
import os
from config import ConfigClass
import requests
import json
from flask_restx import Api, Resource
from api import nfs_entity_ns
from itertools import product

from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.minio_service.minio_client import Minio_Client
from services.minio_service.policy_templates import create_admin_policy, \
    create_collaborator_policy, create_contributor_policy

from minio.versioningconfig import OFF, SUSPENDED, VersioningConfig, ENABLED
from minio.notificationconfig import (NotificationConfig, PrefixFilterRule,
                                      QueueConfig)
from minio.sseconfig import Rule, SSEConfig

from resources.swagger_modules import folder, success_return, folder_return
from resources.decorator import check_role


class folders(Resource):

    _logger = SrvLoggerFactory('api_nfs_ops').get_logger()


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
        # container_id = post_data.get('container_id', None)
        project_geid = post_data.get('project_geid', None)
        # Determine root path
        service = post_data.get('service', None)
        root_path = ConfigClass.NFS_ROOT_PATH
        if service == 'VRE':
            root_path = ConfigClass.VRE_ROOT_PATH

        # Fetch upload path from neo4j service
        container_path = ''

        if(project_geid is not None):
            try:
                query_params = {"global_entity_id": project_geid}
                container_id = get_container_id(query_params)
                url = ConfigClass.NEO4J_SERVICE + \
                    'nodes/Container/node/' + str(container_id)
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


        # also create a bucket in minio but skip the error
        # only create at root level
        try:
            if project_geid is None:
                bucket_prefix = "gr-"
                if service == 'VRE':
                    bucket_prefix = "core-"
                project_code = sub_path.split('/')[0]
                bukcet_name = bucket_prefix + project_code

                # set minio bucket with versioning and encrytion
                mc = Minio_Client()
                if mc.client.bucket_exists(bukcet_name) != True:
                    mc.client.make_bucket(bukcet_name)
                    mc.client.set_bucket_versioning(bukcet_name, VersioningConfig(ENABLED))
                    mc.client.set_bucket_encryption(
                        bukcet_name, SSEConfig(Rule.new_sse_s3_rule()),
                    )

                    # also create the three policy for admin/contribute/collaborator
                    policy_name = create_admin_policy(project_code)
                    stream = os.popen('mc admin policy add minio %s %s'%(project_code+"-admin", policy_name))
                    policy_name = create_contributor_policy(project_code)
                    stream = os.popen('mc admin policy add minio %s %s'%(project_code+"-contributor", policy_name))
                    policy_name = create_collaborator_policy(project_code)
                    stream = os.popen('mc admin policy add minio %s %s'%(project_code+"-collaborator", policy_name))


        except Exception as e:
            self._logger.error(e)


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
        project_geid = request.args.get('project_geid', None)
        container_id = request.args.get('container_id', None)
        container_path = ""
        system_folder = ['workdir','logs','trash']
        # Fetch upload path from neo4j service
        if(project_geid is not None):
            try:
                query_params = {"global_entity_id": project_geid}
                container_id = get_container_id(query_params)
                url = ConfigClass.NEO4J_SERVICE + \
                    'nodes/Container/node/' + str(container_id)
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

        if current_identity['project_role'] == 'collaborator' or current_identity['project_role'] == 'contributor':
            gr_full_path = os.path.join(
                ConfigClass.NFS_ROOT_PATH, container_path, 'raw')
       
        vre_full_path = os.path.join(
            ConfigClass.VRE_ROOT_PATH, container_path, sub_path)
        # Specify the output field: file/folder/both
        field = request.args.get('field', None)

        # Recusive Fetch folder list
        try:
            gr_folders = fs().list_folder(gr_full_path, field)          
            for folder, unused_folder in product(gr_folders, system_folder):
                if unused_folder in folder:
                    gr_folders.remove(folder)
            vre_folders = fs().list_folder(vre_full_path, field)

        except Exception as e:
            if str(e) == 'Path to folder does not exist.':
                return {'Error': str(e)}, 404
            return {'Error': str(e)}, 403

        
        gr_folders.append({'trash': []})
        vre_folders.append({'trash': []})

        if current_identity['project_role'] == 'contributor':
            return {'result': {'gr': gr_folders }}, 200

        return {'result': {'gr': gr_folders, 'vre': vre_folders}}, 200


def get_container_id(query_params):
    url = ConfigClass.NEO4J_SERVICE + f"nodes/Container/query"
    payload = {
        **query_params
    }
    result = requests.post(url, json=payload)
    if result.status_code != 200 or result.json() == []:
        return None
    result = result.json()[0]
    container_id = result["id"]
    return container_id
