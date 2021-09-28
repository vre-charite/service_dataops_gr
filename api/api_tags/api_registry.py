
from models.api_meta_class import MetaAPI
from .namespace import api_tags_ns
from .file_tag_v2 import FileTagRestfulV2, FileTagValidate

class APITags(metaclass=MetaAPI):
    def api_registry(self):
        api_tags_ns.add_resource(
           FileTagRestfulV2, '/containers/<container_id>/tags'
        )
        api_tags_ns.add_resource(
           FileTagValidate, '/containers/<container_id>/tags/validate'
        )
