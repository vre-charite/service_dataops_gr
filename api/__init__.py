from .module_api import module_api

## old apis
nfs_entity_ns = module_api.namespace(
    'NFS Data Operation', description='Operation on NFS', path='/v1')

from .api_nfs_ops import *

## apis refactoring
from .api_file_upload.api_registry import APIFileUpload
from .api_lineage_showcase.api_registry import APILineageShowcase

apis = [
    APIFileUpload(),
    APILineageShowcase()
]

def api_registry(apis):
    if len(apis) > 0:
        for api_sub_module in apis:
            api_sub_module.api_registry()
    else:
        print('[Fatal]', 'No API registered.')

api_registry(apis)
