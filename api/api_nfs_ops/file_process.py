from flask import request, current_app
from flask_restx import Api, Resource, fields
from flask_jwt import jwt_required, current_identity
from api import module_api, nfs_entity_ns
from resources.decorator import check_role
from models.api_response import APIResponse, EAPIResponseCode
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.data_providers.redis import SrvRedisSingleton, ERedisChannels
import json

class FileProcessOnCreate(Resource):
    post_model = module_api.model("file_process_on_create", {
        "project": fields.String,
        "input_path": fields.String,
        "output_path": fields.String,
        "logfile": fields.String,
        "uploader": fields.String,
        "process_pipeline": fields.String,
        "create_time": fields.String
    })
    @nfs_entity_ns.expect(post_model)
    def post(self, container_id):
        post_data = request.get_json()
        post_data_serialized = json.dumps(post_data)
        my_streamer = SrvRedisSingleton()
        res_publish = my_streamer.publish(ERedisChannels.pipeline_process_start.name, post_data_serialized)
        return {"result": str(res_publish)}, 200