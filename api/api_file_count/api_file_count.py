from flask import request
from flask_restx import Resource
from models.api_response import APIResponse, EAPIResponseCode
from config import ConfigClass
import requests


class FileCount(Resource):

    # def get(self, container_id):
    def get(self, project_geid):
        api_response = APIResponse()

        try:
            query_params = {"global_entity_id": project_geid}
            container_id = get_container_id(query_params)
            response = requests.get(ConfigClass.NEO4J_HOST + f"/v1/neo4j/nodes/Container/node/{container_id}")
            dataset = response.json()[0]
        except Exception as e:
            api_response.set_code(EAPIResponseCode.internal_error)
            api_response.set_error_msg("Neo4j error: " + str(e))
            return api_response.to_dict, api_response.code

        relation_payload = {
            "start_label": "Container",
            "end_labels": ["File:Raw:Greenroom"],
            "query": {
                "start_params": {"code": dataset["code"]},
                "end_params": {
                    'File:Raw:Greenroom': {
                        "archived": False,
                    }
                }
            },
        }
        if request.args.get("uploader"):
            relation_payload["query"]["end_params"]["File:Raw:Greenroom"]["uploader"] = request.args.get("uploader")
        try:
            response = requests.post(ConfigClass.NEO4J_HOST + "/v2/neo4j/relations/query", json=relation_payload)
        except Exception as e:
            api_response.set_code(EAPIResponseCode.internal_error)
            api_response.set_error_msg("Neo4j error: " + str(e))
            return api_response.to_dict, api_response.code
        raw_count = response.json()["total"]

        relation_payload = {
            "start_label": "Container",
            "end_labels": ["File:Processed:Greenroom"],
            "query": {
                "start_params": {"code": dataset["code"]},
                "end_params": {
                    'File:Processed:Greenroom': {"archived": False}
                }
            },
        }
        if request.args.get("uploader"):
            relation_payload["query"]["end_params"]["File:Processed:Greenroom"]["uploader"] = request.args.get("uploader")
        try:
            response = requests.post(ConfigClass.NEO4J_HOST + "/v2/neo4j/relations/query", json=relation_payload)
        except Exception as e:
            api_response.set_code(EAPIResponseCode.internal_error)
            api_response.set_error_msg("Neo4j error: " + str(e))
            return api_response.to_dict, api_response.code
        processed_count = response.json()["total"]
        result = {
            "raw_file_count": raw_count,
            "process_file_count": processed_count,
        }
        api_response.set_result(result)
        api_response.set_code(EAPIResponseCode.success)
        return api_response.to_dict, api_response.code


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
    return str(container_id)
