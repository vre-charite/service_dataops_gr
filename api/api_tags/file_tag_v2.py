from flask import request
from flask_restx import Resource
from services.logger_services.logger_factory_service import SrvLoggerFactory
from models.api_response import APIResponse, EAPIResponseCode
from config import ConfigClass
import requests
import re


class FileTagRestfulV2(Resource):
    _logger = SrvLoggerFactory('api_file_tag').get_logger()

    def post(self, container_id):
        _res = APIResponse()
        post_data = request.get_json()
        taglist = post_data.get('taglist', None)
        geid = post_data.get('geid', None)
        if not taglist or not geid or not isinstance(taglist, list) or not isinstance(geid, str):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('taglist and geid are required.s')
            return _res.to_dict, _res.code

        tag_requirement = re.compile("^[a-z0-9-]{1,32}$")
        for tag in taglist:
            if not re.search(tag_requirement, tag):
                _res.set_code(EAPIResponseCode.forbidden)
                _res.set_error_msg('invalid tag, must be 1-32 characters lower case, number or hyphen')
                return _res.to_dict, _res.code

        # duplicate check
        if len(taglist) != len(set(taglist)):
            _res.set_code(EAPIResponseCode.bad_request)
            _res.set_error_msg('duplicate tags not allowed')
            return _res.to_dict, _res.code

        if len(taglist) > 10:
            _res.set_code(EAPIResponseCode.bad_request)
            _res.set_error_msg('limit of 10 tags')
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
