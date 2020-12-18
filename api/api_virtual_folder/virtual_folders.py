from flask_restx import Resource
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
from config import ConfigClass
from flask_jwt import jwt_required, current_identity
from resources.decorator import check_folder_permissions
import requests
from datetime import datetime, timezone

_logger = SrvLoggerFactory('api_virtual_folder').get_logger()


class VirtualFolder(Resource):

    @jwt_required()
    def get(self):
        """ Get folders belonging to user """
        response = APIResponse()
        container_id = request.args.get("container_id")
        username = current_identity.get("username")
        if not username or not container_id:
            response.set_result("Missing required data")
            response.set_code(EAPIResponseCode.bad_request)
            return response.to_dict

        # Get folder
        url = ConfigClass.NEO4J_SERVICE + 'relations/query'
        payload = {
            "start_label": "User",
            "end_label": "VirtualFolder",
            "start_params": {
                "name": username
            },
            "end_params": {
                "container_id": int(container_id)
            },
        }
        result = requests.post(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        result = result.json()
        folders = [i["end_node"] for i in result]
        response.set_result(folders)
        return response.to_dict, response.code

    @jwt_required()
    def post(self):
        """ Create new folder """
        response = APIResponse()
        data = request.get_json()
        folder_name = data.get("name")
        container_id = data.get("container_id")
        username = current_identity.get("username")
        user_id = current_identity.get("user_id")
        role = current_identity.get("role")
        if not container_id or not folder_name:
            response.set_result('Missing required fields')
            response.set_code(EAPIResponseCode.bad_request)
            return response.to_dict
        if not username or user_id == None:
            response.set_result("Couldn't get user info from jwt token")
            response.set_code(EAPIResponseCode.bad_request)
            return response.to_dict

        if role != "admin":
            # Check user belongs to dataset
            url = ConfigClass.NEO4J_SERVICE + 'relations/query'
            payload = {
                "start_label": "User",
                "end_label": "Dataset",
                "start_params": {
                    "id": int(user_id)
                },
                "end_params": {
                    "id": int(container_id),
                },
            }
            result = requests.post(url, json=payload)
            result = result.json()
            if len(result) < 1:
                response.set_result("User doesn't belong to project")
                response.set_code(EAPIResponseCode.forbidden)
                return response.to_dict

        # Folder count check
        url = ConfigClass.NEO4J_SERVICE + 'relations/query'
        payload = {
            "start_label": "User",
            "end_label": "VirtualFolder",
            "start_params": {
                "id": int(user_id)
            },
            "end_params": {
                "container_id": int(container_id),
            },
        }
        result = requests.post(url, json=payload)
        result = result.json()
        if len(result) >= 10:
            response.set_result("Folder limit reached")
            response.set_code(EAPIResponseCode.bad_request)
            return response.to_dict

        # duplicate check
        url = ConfigClass.NEO4J_SERVICE + 'relations/query'
        payload = {
            "start_label": "User",
            "end_label": "VirtualFolder",
            "start_params": {
                "id": int(user_id)
            },
            "end_params": {
                "container_id": int(container_id),
                "name": folder_name
            },
        }
        result = requests.post(url, json=payload)
        result = result.json()
        if len(result) > 0:
            response.set_result("Found duplicate folder")
            response.set_code(EAPIResponseCode.conflict)
            return response.to_dict

        # Create vfolder in neo4j
        url = ConfigClass.NEO4J_SERVICE + "nodes/VirtualFolder"
        payload = {
            "name": folder_name,
            "container_id": container_id,
        }
        result = requests.post(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        vfolder = result.json()[0]
        response.set_result(vfolder)

        # Add relation to user
        url = ConfigClass.NEO4J_SERVICE + "relations/owner"
        payload = {
            "start_id": user_id,
            "end_id": vfolder["id"],
        }
        result = requests.post(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        return response.to_dict



class VirtualFolderFile(Resource):

    @jwt_required()
    @check_folder_permissions
    def get(self, folder_id):
        """ Get files in folder """
        response = APIResponse()
        type = request.args.get("type", "nfs_file")
        page = request.args.get("page", 0)
        page_size = request.args.get("page_size", 25)
        sorting = request.args.get('column', 'createTime')
        order = request.args.get("order", "")

        # Get file by folder relation
        url = ConfigClass.NEO4J_SERVICE + f"relations/query"
        payload = {
            "start_label": "VirtualFolder",
            "end_label": "VirtualFile",
            "start_params": {
                "id": int(folder_id),
            },
        }
        result = requests.post(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, response.status_code 
        result = result.json()
        if not result:
            # No file found in folder
            response.set_result([])
            return response.to_dict

        guids = [i["end_node"]["guid"] for i in result]

        # Pagination
        total = len(result)
        response.set_total(total)
        response.set_page(page)
        response.set_num_of_pages(int(int(total) / int(page_size)))

        if not guids:
            response.set_result([])
            return response.to_dict

        res = requests.post(ConfigClass.METADATA_API + '/v1/entity/guid/bulk', json={"guids": guids})

        if res.status_code != 200:
            return {"error": res.json()}, 500
        entities = res.json()['result'].get('entities', [])
        offset = int(page) * int(page_size)
        limit = int(page_size)
        if sorting:
            entities = sorted(entities, key=lambda item: item["attributes"][sorting])
        if order == 'asc':
            entities.reverse()

        entities = entities[offset:offset+limit]

        # also change the timestamp from int to string
        for e in entities:
            timestamp_int = e['attributes'].get('createTime', None)
            # print(timestamp_int)
            if timestamp_int:
                central = datetime.fromtimestamp(timestamp_int,tz=timezone.utc)
                e['attributes']['createTime'] = central.strftime(
                    '%Y-%m-%d %H:%M:%S')

        # approximateCount is wrong in atlas so using neo4j length
        response.set_result(entities)
        return response.to_dict

    @jwt_required()
    @check_folder_permissions
    def put(self, folder_id):
        """ Edit folder name """
        response = APIResponse()
        data = request.get_json()
        folder_name = data.get("name")
        if not folder_name:
            response.set_result('Missing required fields')
            response.set_code(EAPIResponseCode.bad_request)
            return response.to_dict

        # update vfolder in neo4j
        url = ConfigClass.NEO4J_SERVICE + f"nodes/VirtualFolder/node/{folder_id}"
        payload = {
            "name": folder_name,
        }
        result = requests.put(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        vfolder = result.json()
        response.set_result(vfolder)
        return response.to_dict

    @jwt_required()
    @check_folder_permissions
    def delete(self, folder_id):
        response = APIResponse()

        if not folder_id:
            response.set_result('Missing required fields')
            response.set_code(EAPIResponseCode.bad_request)
            return response.to_dict

        # Get file by folder relation
        url = ConfigClass.NEO4J_SERVICE + f"relations/query"
        payload = {
            "start_label": "VirtualFolder",
            "end_label": "VirtualFile",
            "start_params": {
                "id": int(folder_id),
            },
        }
        result = requests.post(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, response.status_code 
        files = result.json()
        ids = [i["end_node"]["id"] for i in files]

        for id in ids:
            if id:
                # Remove node from neo4j
                url = ConfigClass.NEO4J_SERVICE + f"nodes/VirtualFile/node/{id}"
                result = requests.delete(url)
                if result.status_code != 200:
                    return {"error": result.json()}, 500
        url = ConfigClass.NEO4J_SERVICE + f"nodes/VirtualFolder/node/{folder_id}"
        result = requests.delete(url)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        response.set_result('success')
        return response.to_dict


class VirtualFileBulk(Resource):

    @jwt_required()
    @check_folder_permissions
    def post(self, folder_id):
        """ Add file to folder """
        response = APIResponse()
        guids = request.get_json().get("guids")
        if not guids:
            response.set_result("Missing required data")
            response.set_code(EAPIResponseCode.bad_request)
            return response.to_dict

        # Check folder belongs to user
        url = ConfigClass.NEO4J_SERVICE + f"relations/query"
        user_id = current_identity["user_id"]
        payload = {
            "start_label": "User",
            "end_label": "VirtualFolder",
            "start_params": {
                "id": int(user_id),
            },
            "end_params": {
                "id": int(folder_id),
            },
        }
        result = requests.post(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        result = result.json()
        if len(result) < 1:
            response.set_result("Folder not found")
            response.set_code(EAPIResponseCode.not_found)
            return response

        vfolder = result[0]["end_node"]

        # Get folders dataset
        url = ConfigClass.NEO4J_SERVICE + f"nodes/Dataset/node/{vfolder['container_id']}"
        result = requests.get(url)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        if len(result.json()) < 1:
            response.set_result("Project not found")
            response.set_code(EAPIResponseCode.not_found)
            return response.to_dict

        dataset = result.json()[0]

        duplicate = False
        for guid in guids:
            #Duplicate check
            url = ConfigClass.NEO4J_SERVICE + f"relations/query"
            payload = {
                "start_label": "VirtualFolder",
                "end_label": "VirtualFile",
                "start_params": {
                    "id": int(folder_id),
                },
                "end_params": {
                    "guid": guid,
                },
            }
            result = requests.post(url, json=payload)
            if result.status_code != 200:
                return {"error": result.json()}, 500

            if len(result.json()) > 0:
                duplicate = True
                continue

            # Get file from atlas
            result = requests.get(ConfigClass.METADATA_API + f'/v1/entity/guid/{guid}')
            result = result.json()

            if not result["result"]:
                response.set_result("File not found in atlas")
                response.set_code(EAPIResponseCode.not_found)
                return response.to_dict

            # Check to make sure it's a VRE core file 
            if not result["result"]["entity"]["attributes"]["path"].startswith(ConfigClass.VRE_ROOT_PATH):
                response.set_result("Can't add file from greenroom")
                response.set_code(EAPIResponseCode.forbidden)
                return response.to_dict

            # Make sure the file belongs to the project
            if result["result"]["entity"]["attributes"]["bucketName"] != dataset["code"]:
                response.set_result("File does not belong to project")
                response.set_code(EAPIResponseCode.forbidden)
                return response.to_dict

            # Add file node
            url = ConfigClass.NEO4J_SERVICE + f"nodes/VirtualFile"
            payload = {
                "guid": guid,
                "name": result["result"]["entity"]["attributes"]["qualifiedName"],
            }
            result = requests.post(url, json=payload)
            if result.status_code != 200:
                return {"error": result.json()}, 500
            file = result.json()[0]

            # Add folder relation to file
            url = ConfigClass.NEO4J_SERVICE + f"relations/contains"
            payload = {
                "start_id": vfolder["id"],
                "end_id": file["id"],
            }
            result = requests.post(url, json=payload)
            if result.status_code != 200:
                return {"error": result.json()}, 500

        if duplicate:
            response.set_result("duplicate")
        else:
            response.set_result("success")
        return response.to_dict

    @jwt_required()
    @check_folder_permissions
    def delete(self, folder_id):
        """ Remove file from folder """
        response = APIResponse()
        guids = request.get_json().get("guids")
        if not guids:
            response.set_result("Missing required data")
            response.set_response(EAPIResponseCode.bad_request)
            return response.to_dict

        # Check folder belongs to user
        url = ConfigClass.NEO4J_SERVICE + f"relations/query"
        user_id = current_identity["user_id"]
        payload = {
            "start_label": "User",
            "end_label": "VirtualFolder",
            "start_params": {
                "id": int(user_id),
            },
            "end_params": {
                "id": int(folder_id),
            },
        }
        result = requests.post(url, json=payload)
        if result.status_code != 200:
            return {"error": result.json()}, 500
        result = result.json()
        if len(result) < 1:
            response.set_result("Folder does not belong to user")
            response.set_code(EAPIResponseCode.forbidden)
            return response

        for guid in guids:
            # Get file
            url = ConfigClass.NEO4J_SERVICE + f"relations/query"
            payload = {
                "start_label": "VirtualFolder",
                "end_label": "VirtualFile",
                "start_params": {
                    "id": int(folder_id),
                },
                "end_params": {
                    "guid": guid,
                },
            }
            result = requests.post(url, json=payload)
            if result.status_code != 200:
                return {"error": result.json()}, 500
            result = result.json()
            if len(result) > 1:
                return {"error": "multiple files, aborting"}, 400
            file_id = result[0]["end_node"]["id"]

            # Remove node from neo4j
            url = ConfigClass.NEO4J_SERVICE + f"nodes/VirtualFile/node/{file_id}"
            result = requests.delete(url)
            if result.status_code != 200:
                return {"error": result.json()}, 500
        response.set_result('success')
        return response.to_dict
