from flask_restx import Api, Resource, fields
from config import ConfigClass
from api import nfs_entity_ns


from .folder_api import *
from .file_api import *

nfs_entity_ns.add_resource(folders, '/folders')
nfs_entity_ns.add_resource(FileExists, '/file-exists')
