from flask_restx import Api, Resource, fields
from config import ConfigClass
from api import minio_entity_ns
from minio import Minio

minioClient = Minio('10.3.7.1:9000',
			access_key='indoc-minio',
			secret_key='Trillian42!',
			secure=False)

from .folder_api import *
from .file_api import *

minio_entity_ns.add_resource(files, '/containers/<container_id>/files')
minio_entity_ns.add_resource(folders, '/folders')
