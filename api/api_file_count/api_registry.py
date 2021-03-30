from models.api_meta_class import MetaAPI
from .namespace import api_tags_ns
from .api_file_count import FileCount

class APIFileCount(metaclass=MetaAPI):
    def api_registry(self):
        api_tags_ns.add_resource(
           FileCount, '/containers/<container_id>/files/count'
        )
