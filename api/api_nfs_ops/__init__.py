from flask_restx import Api, Resource, fields
from config import ConfigClass
from api import nfs_entity_ns


from .folder_api import *
from .file_api import *
from .file_copy import FileCopyRestful
from .file_meta import FileMetaRestful
from .file_tag import FileTagRestful
from .file_process import FileProcessOnCreate
from .data_tag import DataTagRestful


nfs_entity_ns.add_resource(
    totalFileCount, '/containers/<container_id>/files/count/total')
nfs_entity_ns.add_resource(
    dailyFileCount, '/containers/<container_id>/files/count/daily')
nfs_entity_ns.add_resource(file_predownload, '/containers/<container_id>/file')
nfs_entity_ns.add_resource(file, '/files/download')
nfs_entity_ns.add_resource(file_download_log, '/files/download/log')

nfs_entity_ns.add_resource(
    FileMetaRestful, '/containers/<container_id>/files/meta')
nfs_entity_ns.add_resource(processedFile, '/files/processed')
# nfs_entity_ns.add_resource(fileInfo, '/containers/<container_id>/files/meta')

nfs_entity_ns.add_resource(folders, '/folders')
nfs_entity_ns.add_resource(FileCopyRestful, '/files/copy')

nfs_entity_ns.add_resource(FileTagRestful, '/containers/<container_id>/tags')

nfs_entity_ns.add_resource(DataTagRestful, '/data/tags')

nfs_entity_ns.add_resource(FileProcessOnCreate, '/containers/<container_id>/files/process/on-create')

nfs_entity_ns.add_resource(file_download_checker, '/download-state')