from flask_restx import Resource, fields
from flask import request
from flask_jwt import jwt_required, current_identity

from config import ConfigClass
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
from api import module_api
from .namespace import api_archive

import requests
import os
from zipfile import ZipFile

get_model = module_api.model("get_archive", {
    "file_path": fields.String,
})

get_returns = """
    {   
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 1,
        'result': {   '2020-10-07-095522_grim.png': {   
                        'filename': '2020-10-07-095522_grim.png',
                        'is_dir': False,
                        'size': 20979
                      },
                      'tes_folder': {'is_dir': True}},
        'total': 1
    }
"""


class ArchiveList(Resource):
    _logger = SrvLoggerFactory('api_archive').get_logger()

    @jwt_required()
    @api_archive.expect(get_model)
    @api_archive.response(200, get_returns)
    def get(self):
        file_path = request.args.get("file_path")
        project_geid = request.args.get("project_geid")

        self._logger.info('[test01] Get zip files from : ' + file_path)
        api_response = APIResponse()
        if not file_path or not project_geid:
            api_response.set_code(EAPIResponseCode.bad_request)
            api_response.set_result("file_path, project_geid is required")
            return api_response.to_dict
        rel_path = file_path.rstrip('/')
        if ConfigClass.NFS_ROOT_PATH in rel_path:
            folder_start_level = 3
        elif ConfigClass.VRE_ROOT_PATH in rel_path:
            folder_start_level = 2
        else:
            api_response.set_code(EAPIResponseCode.bad_request)
            api_response.set_result("Unknown path")
            return api_response.to_dict
        rel_path = rel_path.lstrip(ConfigClass.NFS_ROOT_PATH).lstrip(
            ConfigClass.VRE_ROOT_PATH)
        if len(rel_path.split('/')) > folder_start_level:
            start_label = 'Folder'
        else:
            start_label = 'Dataset'
        file_type = os.path.splitext(file_path)[1]
        if file_type not in ConfigClass.ARCHIVE_TYPES:
            api_response.set_code(EAPIResponseCode.bad_request)
            api_response.set_result("File is not a zip")
            return api_response.to_dict

        if file_type == ".zip":
            ArchiveFile = ZipFile

        role = current_identity["role"]
        self._logger.info('[test01] Current User Platform Role: ' + str(role))
        response = requests.post(
            ConfigClass.NEO4J_SERVICE + "nodes/File/query", json={"full_path": file_path})
        file = response.json()[0]
        self._logger.info('[test01] File: ' + str(file))
        # Get related dataset
        relation_query = {
            "start_label": start_label,
            "end_label": "File",
            "end_params": {
                "id": file["id"]
            }
        }

        response = requests.post(
            ConfigClass.NEO4J_SERVICE + "relations/query", json=relation_query)
        dataset = response.json()[0]["start_node"]

        self._logger.info('[test01] dataset: ' + str(response.json()))

        # Get Project
        project_res = http_query_node(
            'Dataset', {"global_entity_id": project_geid})
        self._logger.info('[test01] Project global_entity_id: ' + project_geid)
        if project_res.status_code == 200:
            self._logger.info('[test01] Project: ' + str(project_res.text))
        else:
            self._logger.error('[test01] Project: ' + str(project_res.text))
        project = project_res.json()[0]

        relation_query = {
            "start_id": current_identity["user_id"],
            "end_id": project["id"],
        }
        self._logger.info('[test01] NEO4J query: ' + str(relation_query))
        response = requests.get(
            ConfigClass.NEO4J_SERVICE + "relations", params=relation_query)

        # Platform admin can edit any files
        if not role == "admin":
            self._logger.info('[test01] NEO4J response: ' + response.text)

            if not response.json():
                # User doesn't belong to the project
                api_response.set_code(EAPIResponseCode.unauthorized)
                api_response.set_result("Permission Denied")
                return api_response.to_dict
            project_role = response.json()[0]['r']['type']

            self._logger.info('[test01] Project Role: ' + project_role)

            if project_role == "contributor":
                if "Processed" in file["labels"] or "VRECore" in file["labels"]:
                    api_response.set_code(EAPIResponseCode.unauthorized)
                    api_response.set_result("Permission Denied")
                    return api_response.to_dict
                if "Greenroom" in file["labels"] and file["uploader"] != current_identity["username"]:
                    api_response.set_code(EAPIResponseCode.unauthorized)
                    api_response.set_result("Permission Denied")
                    return api_response.to_dict
            elif project_role == "collaborator":
                if "Processed" in file["labels"] and "Greenroom" in file["labels"]:
                    api_response.set_code(EAPIResponseCode.unauthorized)
                    api_response.set_result("Permission Denied")
                    return api_response.to_dict
                if "Greenroom" in file["labels"] and file["uploader"] != current_identity["username"]:
                    api_response.set_code(EAPIResponseCode.unauthorized)
                    api_response.set_result("Permission Denied")
                    return api_response.to_dict

        results = {}
        with ArchiveFile(file_path, 'r') as archive:
            for file in archive.infolist():
                # get filename for file
                filename = file.filename.split("/")[-1]
                if not filename:
                    # get filename for folder
                    filename = file.filename.split("/")[-2]
                current_path = results
                for path in file.filename.split("/")[:-1]:
                    if not current_path.get(path):
                        current_path[path] = {"is_dir": True}
                    current_path = current_path[path]

                if not file.is_dir():
                    current_path[filename] = {
                        "filename": filename,
                        "size": file.file_size,
                        "is_dir": False,
                    }

        api_response.set_result(results)
        return api_response.to_dict


def http_query_node(main_label, query_params={}):
    payload = {
        **query_params
    }
    node_query_url = ConfigClass.NEO4J_SERVICE + \
        "nodes/{}/query".format(main_label)
    response = requests.post(node_query_url, json=payload)
    return response
