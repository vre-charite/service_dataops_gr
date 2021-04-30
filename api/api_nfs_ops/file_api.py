from flask import request
import os
from flask_restx import Resource


class FileExists(Resource):
    def post(self):
        post_data = request.get_json()
        full_path = post_data.get('full_path', None)
        return {"result": os.path.isfile(full_path)}, 200
