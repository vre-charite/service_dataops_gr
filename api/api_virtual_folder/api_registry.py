from models.api_meta_class import MetaAPI
from .namespace import api_virtual_folder
from .virtual_folders import VirtualFolder, VirtualFolderFile, VirtualFileBulk

class APIVirtualFolder(metaclass=MetaAPI):
    def api_registry(self):
        api_virtual_folder.add_resource(
           VirtualFolder, '/vfolders'
        )
        api_virtual_folder.add_resource(
           VirtualFolderFile, '/vfolders/<folder_id>'
        )
        api_virtual_folder.add_resource(
           VirtualFileBulk, '/vfolders/<folder_id>/files'
        )
