from .check_status import CheckUploadStateRestful
from models.api_meta_class import MetaAPI
from .namespace import api_file_upload_ns


class APIFileUpload(metaclass=MetaAPI):
    def api_registry(self):
        # api_file_upload_ns.add_resource(
        #     CheckUploadStateRestful, '/containers/<container_id>/upload-state'
        # )
        api_file_upload_ns.add_resource(
            CheckUploadStateRestful, '/containers/<project_geid>/upload-state'
        )


