from models.api_meta_class import MetaAPI
from .namespace import api_archive
from .archive import ArchiveList

class APIArchive(metaclass=MetaAPI):
    def api_registry(self):
        api_archive.add_resource(
           ArchiveList, '/archive'
        )
