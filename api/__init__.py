from .module_api import module_api

## old apis
nfs_entity_ns = module_api.namespace(
    'NFS Data Operation', description='Operation on NFS', path='/v1')

from .api_nfs_ops import *

## apis refactoring
from .api_file_upload.api_registry import APIFileUpload
from .api_file_actions.api_registry import APIFileTransfer
from .api_archive.api_registry import APIArchive
from .api_tags.api_registry import APITags

apis = [
    APIFileUpload(),
    APIFileTransfer(),
    APIArchive(),
    APITags(),
]

def api_registry(apis):
    if len(apis) > 0:
        for api_sub_module in apis:
            api_sub_module.api_registry()
    else:
        print('[Fatal]', 'No API registered.')

api_registry(apis)
