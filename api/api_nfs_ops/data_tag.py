from flask import request
from flask_restx import Api, Resource, fields
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.tags_services.tags_manager import SrvTagsMgr
from services.atlas_services.atlas_manager import SrvAtlasManager
from models.api_response import APIResponse, EAPIResponseCode
from api import nfs_entity_ns
import json

class DataTagRestful(Resource):
    _logger = SrvLoggerFactory('api_data_tag').get_logger()
    
    def post(self):
        '''
        This method allow to add data tags
        '''
        _res = APIResponse()
        post_data = request.get_json()
        taglist = post_data.get('taglist', None)
        guid = post_data.get('guid', None)
        if not taglist or not guid or not isinstance(taglist, list) or not isinstance(guid, str):
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
        _res.set_result('Success')
        _res.set_code(EAPIResponseCode.success)
        return _res.to_dict, _res.code