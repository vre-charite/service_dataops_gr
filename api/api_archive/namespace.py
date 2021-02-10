from api import module_api

api_archive = module_api.namespace(
    'Archive', description='View contents of an archive', path='/v1')
