from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from models.api_file_upload_models import file_upload_form_factory
from services.logger_services.logger_factory_service import SrvLoggerFactory
import services.file_upload as srv_upload
import models.fsm_file_upload as fsmup
from resources.utils import generate_chunk_name
from resources.get_session_id import get_session_id
import os
import errno
from config import ConfigClass
from flask_jwt import jwt_required
from resources.decorator import check_role
from .namespace import api_file_upload_ns
from api import module_api
import gc

post_model = module_api.model("upload_chunks_post", {
    'file': fields.Raw,
    "resumableIdentifier": fields.String,
    "resumableFilename": fields.String,
    "resumableChunkNumber": fields.Integer,
    "resumableChunkSize": fields.Integer,
    "resumableTotalChunks": fields.Integer,
    "resumableTotalSize": fields.Integer,
    "uploader": fields.String,
})


class ChunkUploadRestful(Resource):
    _logger = SrvLoggerFactory('api_file_upload').get_logger()

    @api_file_upload_ns.expect(post_model)
    @jwt_required()
    @check_role("uploader")
    def post(self, container_id):
        '''
        This method allow to upload file chunks
        '''
        try:
            # init resp
            _res = APIResponse()
            # check ip address
            self._logger.info('ChunkUploadRestful Request IP: ' + str(request.remote_addr))
            # form
            _form = request.form
            file_upload_form = file_upload_form_factory(_form, container_id)
            # init session id
            session_id_gotten = get_session_id()
            session_id = session_id_gotten if session_id_gotten else srv_upload.session_id_generator()
            status_mgr = srv_upload.SrvFileUpStateMgr(
                session_id,
                container_id,
                file_upload_form.resumable_identifier)
            self._logger.info('Start Uploading Chunks To Container: ' + str(container_id) + "----- Chunk Number: "
                              + str(file_upload_form.resumable_chunk_number))
            self._logger.info(
                'ChunkUploadRestful file_upload_form: ' + str(file_upload_form.to_dict))
            chunk_data = request.files['file']
            temp_dir = os.path.join(
                ConfigClass.TEMP_BASE, file_upload_form.resumable_identifier)
            try:
                chunk_name = generate_chunk_name(
                    file_upload_form.resumable_filename, file_upload_form.resumable_chunk_number)
                chunk_file = os.path.join(temp_dir, chunk_name)
                self._logger.info(
                    'Start to save chunk {} to destination {}'.format(chunk_name, chunk_file))
                chunk_data.save(chunk_file)
            except Exception as e:
                self._logger.error(
                    'Failed to save chunk in tmp.')
                _res.set_code(EAPIResponseCode.forbidden)
                _res.set_error_msg('Failed to save chunk in tmp' + str(e))
                status_mgr.go(fsmup.EState.TERMINATED)
            _res.set_result('Succeed.')
            self._logger.info('Succeed: Uploaded Chunks To Container: ' + str(container_id) + "----- Chunk Number: "
                              + str(file_upload_form.resumable_chunk_number))
            return _res.to_dict, _res.code
        except Exception as e:
            self._logger.error('Failed to save chunk ' + str(e))
            return {}, EAPIResponseCode.internal_error.name
