from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from models.api_file_upload_models import file_upload_form_factory
from services.logger_services.logger_factory_service import SrvLoggerFactory
import services.file_upload as srv_upload
import models.fsm_file_upload as fsmup
from resources.utils import generate_chunk_name, fs
from resources.get_session_id import get_session_id
from config import ConfigClass
import requests
import os
import uuid
from flask_jwt import jwt_required
from resources.decorator import check_role
from .namespace import api_file_upload_ns
from api import module_api

post_model = module_api.model("upload_on_success_post", {
    "resumableIdentifier": fields.String,
    "resumableFilename": fields.String,
    "resumableTotalChunks": fields.Integer,
    "resumableTotalSize": fields.Integer,
    "uploader": fields.String,
    "generateID": fields.Raw(required=False, example="ZMA-6666", description="Optional String", title="generateID"),
})


class ChunkUploadSuccessRestful(Resource):
    _logger = SrvLoggerFactory('api_file_upload').get_logger()

    @api_file_upload_ns.expect(post_model)
    @jwt_required()
    @check_role("uploader")
    def post(self, container_id):
        '''
        This method allow to create a flask executor to combine chunks uploaded.
        '''
        self._logger.info(
            'All chunks received, start background task...: ' + str(container_id))
        # init resp
        _res = APIResponse()
        self._logger.info(
            'ChunkUploadSuccessRestful Request IP: ' + str(request.remote_addr))
        # form
        _form = request.form
        file_upload_form = file_upload_form_factory(_form, container_id)
        self._logger.debug(
            'ChunkUploadSuccessRestful file_upload_form: ' + str(file_upload_form.to_dict))
        # init session id
        session_id_gotten = get_session_id()
        self._logger.debug(
            'ChunkUploadSuccessRestful session_id_gotten: ' + str(session_id_gotten))
        session_id = session_id_gotten if session_id_gotten else srv_upload.session_id_generator()
        self._logger.debug(
            'ChunkUploadSuccessRestful session_id: ' + str(session_id))
        status_mgr = srv_upload.SrvFileUpStateMgr(
            session_id,
            container_id,
            file_upload_form.resumable_identifier)
        status_mgr.go(fsmup.EState.CHUNK_UPLOADED)
        # Fetch upload path from neo4j service
        url = ConfigClass.NEO4J_SERVICE + \
            'nodes/Dataset/node/' + str(container_id)
        res = requests.get(url=url)
        datasets = res.json()
        temp_dir = os.path.join(
            ConfigClass.TEMP_BASE, file_upload_form.resumable_identifier)
        if res.status_code != 200:
            return {'result': res.json()}, res.status_code
        if len(datasets) == 0:
            return {'result': 'Container does not exist.'}, 404
        # since we check it before so dont check it again
        path = datasets[0].get('path', None)

        # format upload path with subfolder if required
        upload_path = os.path.join(
            ConfigClass.NFS_ROOT_PATH,
            path,  # root path
            "raw")
        self._logger.info('File will be uploaded to %s' % upload_path)
        self._logger.info("file_upload_form.resumable_filename: " +
                          file_upload_form.resumable_filename)
        target_file_name = os.path.join(
            temp_dir, file_upload_form.resumable_filename)
        self._logger.info("target_file_name: " + target_file_name)
        task_id = 'upload' + str(uuid.uuid4())
        self._logger.info(
            'All chunks received, start background task {}'.format(task_id))
        # start thread for uploading
        chunk_paths = [
            os.path.join(
                temp_dir,
                generate_chunk_name(file_upload_form.resumable_filename, x)
            )
            for x in range(1, file_upload_form.resumable_total_chunks + 1)
        ]
        self._logger.info(chunk_paths)
        fs().upload_to_nfs.submit_stored(
            task_id, temp_dir, chunk_paths,
            target_file_name, upload_path, file_upload_form.uploader,
            file_upload_form.tags, file_upload_form.metadatas, path,
            file_upload_form.resumable_total_size,
            status_mgr, container_id
        )

        result = {
            'task_id': task_id,
            'session_id': session_id,
            'message': 'All chunks received, task_id is %s' % task_id}
        _res.set_code(EAPIResponseCode.success)
        _res.set_result(result)
        return _res.to_dict, _res.code
