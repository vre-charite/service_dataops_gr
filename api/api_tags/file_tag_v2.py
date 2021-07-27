from flask import request
from flask_restx import Resource
from services.logger_services.logger_factory_service import SrvLoggerFactory
from models.api_response import APIResponse, EAPIResponseCode
from config import ConfigClass
from .utils import validate_taglist
import requests

class FileTagRestfulV2(Resource):
    _logger = SrvLoggerFactory('api_file_tag').get_logger()
    """deprecated new endpoint in dataops_utility service : /v2/{entity}/{entity_geid}/tags"""
    def post(self, container_id):
        _res = APIResponse()
        post_data = request.get_json()
        taglist = post_data.get('taglist', None)
        geid = post_data.get('geid', None)
        internal = post_data.get('internal', False)

        if not geid or not isinstance(taglist, list) or not isinstance(geid, str):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('taglist and geid are required.')
            return _res.to_dict, _res.code

        valid, validate_data = validate_taglist(taglist, internal)
        if not valid:
            _res.set_error_msg(validate_data["error"])
            _res.set_code(validate_data["code"])
            return _res.to_dict, _res.code

        # update label in neo4j 
        try:
            response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/File/query", json={"global_entity_id": geid})
            if not response.json():
                _res.set_code(EAPIResponseCode.not_found)
                _res.set_error_msg("File not found")
                return _res.to_dict, _res.code
            file_id = response.json()[0]["id"]
            response = requests.put(ConfigClass.NEO4J_SERVICE + f"nodes/File/node/{file_id}", json={"tags": taglist})
        except Exception as e:
            self._logger.error(
                'Failed to update labels in Neo4j.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('neo4j error: ' + str(e))
            return _res.to_dict, _res.code
        _res.set_result('Success')
        _res.set_code(EAPIResponseCode.success)
        return _res.to_dict, _res.code

    def delete(self, container_id):
        _res = APIResponse()
        post_data = request.get_json()
        taglist = post_data.get('taglist', [])
        geid = post_data.get('geid', None)
        if not geid or not isinstance(taglist, list) or not isinstance(geid, str):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('taglist and geid are required.')
            return _res.to_dict, _res.code

        # update label in atlas
        try:
            response = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/File/query", json={"global_entity_id": geid})
            if not response.json():
                _res.set_code(EAPIResponseCode.not_found)
                _res.set_error_msg("File not found")
                return _res.to_dict, _res.code
            file_id = response.json()[0]["id"]
            response = requests.put(ConfigClass.NEO4J_SERVICE + f"nodes/File/node/{file_id}", json={"tags": taglist})
        except Exception as e:
            self._logger.error(
                'Failed to update labels in Neo4j.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('neo4j error: ' + str(e))
            return _res.to_dict, _res.code

        _res.set_result('Success')
        _res.set_code(EAPIResponseCode.success)
        return _res.to_dict, _res.code


class FileTagValidate(Resource):

    def post(self, container_id):
        _res = APIResponse() 
        post_data = request.get_json()
        taglist = post_data.get('taglist', [])
        if not taglist or not isinstance(taglist, list):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('taglist required.')
            return _res.to_dict, _res.code

        valid, validate_data = validate_taglist(taglist)
        if not valid:
            _res.set_error_msg(validate_data["error"])
            _res.set_code(validate_data["code"])
            return _res.to_dict, _res.code
        _res.set_result("valid")
        return _res.to_dict, _res.code


