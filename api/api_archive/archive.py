from flask_restx import Resource, fields
from flask import request

from config import ConfigClass
from models.api_response import APIResponse, EAPIResponseCode
from models.api_archive import ArchivePreviewModel
from services.logger_services.logger_factory_service import SrvLoggerFactory
from api import module_api
from app.database import db
from .namespace import api_archive

import requests
import os
import json
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

    @api_archive.expect(get_model)
    @api_archive.response(200, get_returns)
    def get(self):
        """ Get a Zip preview """
        file_geid = request.args.get("file_geid")
        project_geid = request.args.get("project_geid")

        self._logger.info('Get zip preview for: ' + str(file_geid))
        api_response = APIResponse()
        if not file_geid:
            self._logger.info('Missing file_geid')
            api_response.set_code(EAPIResponseCode.bad_request)
            api_response.set_result("file_geid is required")
            return api_response.to_dict, api_response.code
        archive_model = db.session.query(ArchivePreviewModel).filter(
            ArchivePreviewModel.file_geid==file_geid, 
        ).first()
        if not archive_model:
            self._logger.info(f'Preview not found for file_geid: {file_geid}')
            api_response.set_code(EAPIResponseCode.not_found)
            api_response.set_result("Archive preview not found")
            return api_response.to_dict, api_response.code
        api_response.set_result(json.loads(archive_model.archive_preview))
        return api_response.to_dict

    def post(self):
        """ Create a ZIP preview given a file_geid and preview as a dict """
        file_geid = request.get_json().get("file_geid")
        archive_preview = request.get_json().get("archive_preview")
        archive_preview = json.dumps(archive_preview)
        self._logger.info('POST zip preview for: ' + file_geid)
        api_response = APIResponse()
        try:
            query_result = db.session.query(ArchivePreviewModel).filter(
                ArchivePreviewModel.file_geid==file_geid, 
            ).first()
            if query_result:
                self._logger.info(f'Duplicate entry for file_geid: {file_geid}')
                api_response.set_code(EAPIResponseCode.conflict)
                api_response.set_result("Duplicate entry for preview")
                return api_response.to_dict, api_response.code

            archive_model = ArchivePreviewModel(
                file_geid=file_geid, 
                archive_preview=archive_preview
            )
            db.session.add(archive_model)
            db.session.commit()
        except Exception as e:
            self._logger.error("Psql error: " + str(e))
            api_response.set_error_msg("Psql error: " + str(e))
            api_response.set_code(EAPIResponseCode.internal_error)
            return api_response.to_dict, api_response.code
        api_response.set_result("success")
        return api_response.to_dict

    def delete(self):
        """ Delete preview given a file_geid """
        file_geid = request.get_json().get("file_geid")
        self._logger.info('DELETE zip preview for: ' + str(file_geid))
        api_response = APIResponse()
        try:
            archive_model = db.session.query(ArchivePreviewModel).filter(
                ArchivePreviewModel.file_geid==file_geid, 
            ).delete()
            db.session.commit()
        except Exception as e:
            self._logger.error("Psql error: " + str(e))
            api_response.set_error_msg("Psql error: " + str(e))
            api_response.set_code(EAPIResponseCode.internal_error)
            return api_response.to_dict, api_response.code
        api_response.set_result("success")
        return api_response.to_dict
