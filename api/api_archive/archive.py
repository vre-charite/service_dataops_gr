from flask_restx import Resource, fields
from flask import request
from flask_jwt import jwt_required, current_identity

from config import ConfigClass
from models.api_response import APIResponse, EAPIResponseCode
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

    @jwt_required()
    @api_archive.expect(get_model)
    @api_archive.response(200, get_returns)
    def get(self):
        file_path = request.args.get("file_path")
        api_response = APIResponse()
        if not file_path:
            api_response.set_code(EAPIResponseCode.bad_request)
            api_response.set_result("file_path is required")
            return api_response.to_dict
        if not os.path.isfile(file_path):
            api_response.set_code(EAPIResponseCode.not_found)
            api_response.set_result("File is not found")
            return api_response.to_dict
        file_type = os.path.splitext(file_path)[1]
        if file_type not in ConfigClass.ARCHIVE_TYPES:
            api_response.set_code(EAPIResponseCode.bad_request)
            api_response.set_result("File is not a zip")
            return api_response.to_dict

        if file_type == ".zip":
            ArchiveFile = ZipFile

        role = current_identity["role"]
        response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/File/query", json={"full_path": file_path})
        file = response.json()[0]
        # Get related dataset
        relation_query = {
            "start_label": "Dataset",
            "end_label": "File",
            "end_params": {
                "id": file["id"]
            }
        }
        response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/query", json=relation_query)
        dataset = response.json()[0]["start_node"]

        relation_query = {
            "start_id": current_identity["user_id"],
            "end_id": dataset["id"],
        }
        response = requests.get(ConfigClass.NEO4J_SERVICE + "relations", params=relation_query)
        # Platform admin can edit any files
        if not role == "admin":
            if not response.json():
                # User doesn't belong to the project
                api_response.set_code(EAPIResponseCode.unauthorized)
                api_response.set_result("Permission Denied")
                return api_response.to_dict
            project_role = response.json()[0]['r']['type']

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
