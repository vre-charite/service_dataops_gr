from flask import request, current_app
from flask_restx import Api, Resource, fields
from flask_jwt import jwt_required, current_identity
from api import module_api, nfs_entity_ns
from services.neo4j_services.container_manager import SrvContainerManager
from services.atlas_services.atlas_manager import SrvAtlasManager
from resources.decorator import check_role
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
import json

query_sample_return = '''
    {
        "result": {
            "entities": [
                {
                    "meanings": [],
                    "labels": [],
                    "displayText": "/usr/data/test1111/init.txt",
                    "attributes": {
                        "name": "/usr/data/test1111/init.txt",
                        "owner": "admin",
                        "generateID": "BXT-1234!",
                        "createTime": 0,
                        "bucketName": "test1111"
                    },
                    "classifications": [],
                    "classificationNames": [],
                    "typeName": "nfs_file",
                    "isIncomplete": false,
                    "status": "ACTIVE",
                    "guid": "c03d51b2-11be-400b-82ab-6b080ea12c50",
                    "meaningNames": []
                }
            ],
            "searchParameters": {
                "excludeDeletedEntities": true,
                "limit": 25,
                "includeClassificationAttributes": true,
                "attributes": [
                    "generateID"
                ],
                "includeSubClassifications": true,
                "includeSubTypes": true,
                "typeName": "nfs_file",
                "entityFilters": {
                    "operator": "contains",
                    "attributeValue": "test1111",
                    "attributeName": "bucketName"
                },
                "offset": 0
            },
            "approximateCount": 2,
            "queryType": "BASIC"
        }
    }
    '''


class FileMetaRestful(Resource):
    _logger = SrvLoggerFactory('api_file_meta').get_logger()

    @ nfs_entity_ns.response(200, query_sample_return)
    @ nfs_entity_ns.param('page_size', 'number of entities return per request')
    @ nfs_entity_ns.param('page', 'offset of query which page to start')
    @ nfs_entity_ns.param('stage', 'possible value: raw(default)/processed indicates if it is raw or processed file')
    @ nfs_entity_ns.param('column', 'which column user want to order')
    @ nfs_entity_ns.param('text', 'full text search')
    @ nfs_entity_ns.param('order', 'possible value asc and desc to tell order of return')
    @jwt_required()
    @check_role("uploader")
    def get(self, container_id):

        # init resp
        _res = APIResponse()

        # basic query parameters
        # add new query string here for pagination
        default_page_size = 25
        page_size = int(request.args.get('page_size', default_page_size))
        # note here the offset is page_size * page
        page = int(request.args.get('page', 0))
        # use the stage variable to indicate entity type
        entity_type = request.args.get('entity_type', 'nfs_file')
        # also add new query string for sorting the column default is time
        sorting = request.args.get('column', 'createTime')
        # default order is desceding
        order = request.args.get('order', 'desc')
        order = 'ASCENDING' if order == 'asc' else 'DESCENDING'
        pipeline = request.args.get('pipeline', None)

        # new parameter filter
        # the parameter should pass as stringfied json default is "{}"
        # in below will parase out and json loads into dict
        # if there is an error when parasing it will set to default value
        filter_condition = request.args.get('filter', '{}')
        try:
            filter_condition = json.loads(filter_condition)
        except Exception as e:
            self._logger.warning(
                'Failed to convert fileter_condition into json.')
            filter_condition = {}

        # project member/uploader could only access files uploaded by self (Green Room)
        if entity_type == "nfs_file" and current_identity['project_role'] != 'admin':
            filter_condition.update({'owner': current_identity['username']})

        # possesed file should query with pipeline name (Green Room)
        if entity_type == "nfs_file_processed":
            if current_identity['project_role'] == 'uploader':
                self._logger.error(
                    'Uploader is not allowed to query proccessed data.')
                _res.set_code(EAPIResponseCode.forbidden)
                _res.set_error_msg('Permission Deined')
                return _res.to_dict, _res.code

            if not pipeline:
                self._logger.error(
                    'Query parameter container_id and pipeline are required.')
                _res.set_code(EAPIResponseCode.forbidden)
                _res.set_error_msg(
                    'Query parameter container_id and pipeline are required')
                return _res.to_dict, _res.code

            filter_condition.update({'pipeline': pipeline})

        # only project admin/member could access files in vre core
        if entity_type == "nfs_file_cp" and current_identity['project_role'] == 'uploader':
            self._logger.error(
                'Uploader is not allowed to query vre core data.')
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('Permission Deined')
            return _res.to_dict, _res.code

        # fetch project path
        try:
            res = SrvContainerManager().fetch_container_by_id(container_id)
            if len(res) == 0:
                return {'result': 'Container does not exist.'}, 404
            container_path = res[0]['path']
            if not container_path:
                return {'result': 'Cannot find the path attribute.'}, 403
        except Exception as e:
            self._logger.error(
                'Failed to query neo4j: ' + str(e))
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('Failed to query neo4j: ' + str(e))
            return _res.to_dict, _res.code

        filter_condition.update({'bucketName': container_path})

        # query metadata
        try:
            res = SrvAtlasManager().query_file_meta(container_id, filter_condition,
                                                    page_size, page, sorting, order, entity_type)
        except Exception as e:
            self._logger.error(
                'Failed to query file metadata: ' + str(e))
            _res.set_code(EAPIResponseCode.forbidden)
            _res.set_error_msg('Failed to query file metadata: ' + str(e))
            return _res.to_dict, _res.code

        return {"result": res}, 200