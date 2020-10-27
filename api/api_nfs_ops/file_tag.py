from flask import request
from flask_restx import Api, Resource, fields
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.tags_services.tags_manager import SrvTagsMgr
from services.atlas_services.atlas_manager import SrvAtlasManager
from models.api_response import APIResponse, EAPIResponseCode
from api import nfs_entity_ns
import json


class FileTagRestful(Resource):
    _logger = SrvLoggerFactory('api_file_tag').get_logger()

    def get(self, container_id):
        '''
        This method allow to list all tag and its frequency in the project
        '''
        _res = APIResponse()
        tags_mgr = SrvTagsMgr()
        query = request.args.get('query', False)
        pattern = request.args.get('pattern', None)
        length = request.args.get('length', None)
        try:
            if query:
                res = tags_mgr.list_freq_by_pattern(
                    container_id, pattern, length)
            else:
                res = tags_mgr.list_freq_by_project(container_id, length)
        except Exception as e:
            self._logger.error(
                'Failed to list all tags in the project.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('redis error: ' + str(e))
            return _res.to_dict, _res.code

        _res.set_result(res)
        _res.set_code(EAPIResponseCode.success)
        return _res.to_dict, _res.code

    def post(self, container_id):
        '''
        This method allow to add up frequency of the tag in the project
        '''
        _res = APIResponse()
        post_data = request.get_json()
        taglist = post_data.get('taglist', None)
        guid = post_data.get('guid', None)
        if not taglist or not guid or not isinstance(taglist, list) or not isinstance(guid, str):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('taglist and guid are required.s')
            return _res.to_dict, _res.code

        tag = post_data.get('tag', None)
        if not tag or not isinstance(tag, str):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('tag is required')
            return _res.to_dict, _res.code

        # update label in atlas
        atlas_mgr = SrvAtlasManager()
        try:
            atlas_mgr.update_file_label(guid, taglist)
        except Exception as e:
            self._logger.error(
                'Failed to update labels in Atlas.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('redis error: ' + str(e))
            return _res.to_dict, _res.code

        # update tag freq in redis
        tags_mgr = SrvTagsMgr()
        try:
            tags_mgr.add_freq(container_id, tag)
        except Exception as e:
            self._logger.error(
                'Failed to add up frequency of the tag in the project.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('redis error: ' + str(e))
            return _res.to_dict, _res.code

        _res.set_result('Success')
        _res.set_code(EAPIResponseCode.success)
        return _res.to_dict, _res.code

    def delete(self, container_id):
        '''
        This method allow to reduce frequency of the tag in the project
        '''
        _res = APIResponse()
        post_data = request.get_json()
        taglist = post_data.get('taglist', [])
        guid = post_data.get('guid', None)
        if not guid or not isinstance(taglist, list) or not isinstance(guid, str):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('taglist and guid are required.')
            return _res.to_dict, _res.code

        tag = post_data.get('tag', None)
        if not tag or not isinstance(tag, str):
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('tag is required')
            return _res.to_dict, _res.code

        # update label in atlas
        atlas_mgr = SrvAtlasManager()
        try:
            atlas_mgr.update_file_label(guid, taglist)
        except Exception as e:
            self._logger.error(
                'Failed to update labels in Atlas.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('redis error: ' + str(e))
            return _res.to_dict, _res.code

        # update tag freq in redis
        tags_mgr = SrvTagsMgr()
        try:
            tags_mgr.reduce_freq(container_id, tag)
        except Exception as e:
            self._logger.error(
                'Failed to reduce frequency of the tag in the project.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('redis error: ' + str(e))
            return _res.to_dict, _res.code

        _res.set_result('Success')
        _res.set_code(EAPIResponseCode.success)
        return _res.to_dict, _res.code
