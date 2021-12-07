import requests

from config import ConfigClass
from models.api_response import EAPIResponseCode
from resources.errors import APIException


def relation_query(query_data):
    response = requests.post(ConfigClass.NEO4J_SERVICE_V2 + "relations/query", json=query_data)
    if response.status_code != 200:
        error_msg = f"Error calling Neo4j service: {response.json()}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
    return response.json()


def query_node(label, query_data):
    response = requests.post(ConfigClass.NEO4J_SERVICE + f"nodes/{label}/query", json=query_data)
    if response.status_code != 200:
        error_msg = f"Error calling Neo4j service: {response.json()}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
    if not response.json():
        error_msg = f"{label} not found: {query_data}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.not_found.value)
    return response.json()[0]


def bulk_update_entity(label, geids, update_data):
    data = []
    if not geids:
        return False
    for geid in geids:
        data.append({**{"global_entity_id": geid}, **update_data})
    put_data = {
        "data": data,
        "merge_key": (label, "global_entity_id"),
    }
    response = requests.put(ConfigClass.NEO4J_SERVICE_V2 + "nodes/batch/update", json=put_data)
    if response.status_code != 200:
        error_msg = f"Error calling Neo4j service: {response.json()}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
    return True


def bulk_get_by_geids(geids):
    query_data = {"geids": geids}
    response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/query/geids", json=query_data)
    if response.status_code != 200:
        error_msg = f"Error calling Neo4j service: {response.json()}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
    return response.json()["result"]


def bulk_add_relationships(label, geids, request_geid):
    relation_data = {
        "payload": [],
        "params_location": ["start", "end"],
        "start_label": "Request",
        "end_label": label,
    }
    if not geids:
        return False
    for geid in geids:
        relation_data["payload"].append(
            {
                "start_params": {
                    "global_entity_id": request_geid
                },
                "end_params": {"global_entity_id": geid},
            }
        )
    response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/includes/batch", json=relation_data)
    if response.status_code != 200:
        error_msg = f"Error calling Neo4j service: {response.json()}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
    return True


def get_entity_parent(entity_node):
    query_data = {
        "display_path": "/".join(entity_node["display_path"].split("/")[:-1]),
        "project_code": entity_node["project_code"]
    }
    response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Folder/query", json=query_data)
    if response.status_code != 200:
        error_msg = f"Error calling Neo4j service: {response.json()}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
    return response.json()[0]


def get_node_by_geid(geid):
    response = requests.get(ConfigClass.NEO4J_SERVICE + f"nodes/geid/{geid}")
    if response.status_code != 200:
        error_msg = f"Error calling Neo4j service: {response.json()}"
        raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
    if not response.json():
        return {}
    return response.json()[0]
