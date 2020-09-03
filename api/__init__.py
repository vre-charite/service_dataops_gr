from .module_api import module_api

## old apis
nfs_entity_ns = module_api.namespace(
    'NFS Data Operation', description='Operation on NFS', path='/v1')
minio_entity_ns = module_api.namespace(
    'MiniO Data Operation', description='Operation on MiniO', path='/v1/minio')

from .api_minio_ops import *
from .api_nfs_ops import *

## apis refactoring
from .api_file_upload.api_registry import APIFileUpload

apis = [
    APIFileUpload()
]

def api_registry(apis):
    if len(apis) > 0:
        for api_sub_module in apis:
            api_sub_module.api_registry()
    else:
        print('[Fatal]', 'No API registered.')

api_registry(apis)
