from flask_restx import Api, Resource, fields
from flask import request
from models.api_response import APIResponse, EAPIResponseCode
from config import ConfigClass
import os
import errno
import requests
from services.logger_services.logger_factory_service import SrvLoggerFactory
from models.api_file_upload_models import file_upload_form_factory
from flask_jwt import jwt_required
from resources.decorator import check_role
from .namespace import api_file_upload_ns
from api import module_api

post_model = module_api.model("upload_pre_post", {
    "resumableIdentifier": fields.String,
    "resumableFilename": fields.String,
})

post_success_model = module_api.model(
    "upload_pre_post_success", {
        'code': fields.Integer,  # by default success
        'error_msg': fields.String,  # empty when success
        'result': {
            "temp_dir": fields.String,
            "message": fields.String,
        },
    }
)


class PreUploadRestful(Resource):
    _logger = SrvLoggerFactory('api_file_upload').get_logger()

    @api_file_upload_ns.expect(post_model)
    @jwt_required()
    @check_role("uploader")
    def post(self, container_id):
        '''
        This method allow to pre-upload file.
        '''
        # init resp
        _res = APIResponse()
        # get form
        _form = request.form
        file_upload_form = file_upload_form_factory(_form, container_id)
        self._logger.info(
            'Start Pre Uploading To Container: ' + str(container_id))
        self._logger.info(
            'PreUploadRestful file_upload_form: ' + str(file_upload_form.to_dict))
        if file_upload_form.resumable_identifier == "error" \
                or file_upload_form.resumable_filename == "error":
            _res.set_code(EAPIResponseCode.bad_request)
            _res.set_error_msg(
                'Invalid resumableIdentifier Or resumableFilename')
            return _res.to_dict, _res.code

        init_check_res = pre_upload_init_check(
            file_upload_form.container_id, file_upload_form.resumable_filename, self._logger)
        if not init_check_res[0]:
            _res.set_code(init_check_res[1])
            _res.set_error_msg(init_check_res[2])
            return _res.to_dict, _res.code

        # make temp directory
        try:
            temp_dir = os.path.join(
                ConfigClass.TEMP_BASE, file_upload_form.resumable_identifier)
            if not os.path.isdir(temp_dir):
                os.makedirs(temp_dir)
                self._logger.info(
                    'Succeed to create temp dir {}'.format(temp_dir))
            res = {
                "temp_dir": temp_dir,
                "message": "Succeed"
            }
            _res.set_code(EAPIResponseCode.success)
            _res.set_result(res)
        except OSError as e:
            if e.errno == errno.EEXIST:
                self._logger.warning(
                    'Folder {} exists. '.format(temp_dir))
            else:
                error_msg = 'Error when make temp dir' + str(e)
                _res.set_error_msg(error_msg)
                _res.set_code(EAPIResponseCode.forbidden)
        except Exception as e:
            self._logger.error(
                'Failed to create temp dir {}: {}'.format(temp_dir, str(e)))
            error_msg = 'Error when make temp dir' + str(e)
            _res.set_error_msg(error_msg)
            _res.set_code(EAPIResponseCode.internal_error)
        return _res.to_dict, _res.code


def pre_upload_init_check(container_id, resumable_filename, _logger):
    code = EAPIResponseCode.success
    msg = ""
    passed = True
    # Check if container exist and fetch upload path
    url = ConfigClass.NEO4J_SERVICE + 'nodes/Dataset/node/' + str(container_id)
    _logger.info(url)
    res = requests.get(url=url)
    if res.status_code != 200:
        _logger.error(
            'neo4j service: {}'.format(res.json()))
        passed = False
        code = EAPIResponseCode.not_found
        msg = 'neo4j service: {}'.format(res.json())

    datasets = res.json()
    if len(datasets) == 0:
        _logger.error(
            'neo4j service: container does not exist.')
        passed = False
        code = EAPIResponseCode.forbidden
        msg = 'Container does not exist.'
    _logger.debug(datasets)
    d_path = datasets[0].get('path', None)
    if not d_path:
        _logger.error(
            'neo4j service: cannot find the path attribute.')
        passed = False
        code = EAPIResponseCode.forbidden
        msg = 'Cannot find the path attribute.'

    # (optional) check if the folder is not existed
    path = os.path.join(
        ConfigClass.NFS_ROOT_PATH, d_path)
    path = os.path.join(path, "raw")
    if not os.path.isdir(path):
        _logger.error(
            'nfs service: folder raw does not existed!')
        passed = False
        code = EAPIResponseCode.forbidden
        msg = 'Folder raw does not existed!'
    # check if the file is already existed
    file_path = os.path.join(path, resumable_filename)
    if os.path.isfile(file_path):
        _logger.error(
            'nfs service: File %s is already existed!' % resumable_filename)
        passed = False
        code = EAPIResponseCode.forbidden
        msg = 'File %s is already existed!' % resumable_filename
    return passed, code, msg
