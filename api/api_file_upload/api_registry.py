from flask_restx import Api, Resource, fields
from resources.decorator import check_role
from config import ConfigClass
from models.api_meta_class import MetaAPI
from flask import request
import json
from .chunk_upload import ChunkUploadRestful
from .chunk_upload_on_success import ChunkUploadSuccessRestful
from .check_status import CheckUploadStateRestful, CheckUploadStatusRestfulDeprecated
from .pre_upload import PreUploadRestful
from .namespace import api_file_upload_ns

## refactoring upload
class APIFileUpload(metaclass=MetaAPI):
    def api_registry(self):
        api_file_upload_ns.add_resource(
            PreUploadRestful, '/containers/<container_id>/pre')
        api_file_upload_ns.add_resource(
            ChunkUploadRestful, '/containers/<container_id>/chunks')
        api_file_upload_ns.add_resource(
            ChunkUploadSuccessRestful, '/containers/<container_id>/on-success')
        api_file_upload_ns.add_resource(
            CheckUploadStatusRestfulDeprecated, '/containers/<container_id>/status'
        )
        api_file_upload_ns.add_resource(
            CheckUploadStateRestful, '/containers/<container_id>/upload-state'
        )

