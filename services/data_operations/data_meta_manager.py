from models.service_meta_class import MetaService
import requests
from config import ConfigClass

class SrvFileDataMgr(metaclass=MetaService):
    base_url = ConfigClass.data_ops_util_host
    def __init__(self, logger):
        self.logger = logger
    def create(self, uploader, file_name, path, file_size, desc, namespace,
            data_type, project_code, labels, generate_id, operator = None, from_parents=None):
        url = self.base_url + "/v1/filedata"
        post_json_form = {
            "uploader": uploader,
            "file_name": file_name,
            "path": path,
            "file_size": file_size,
            "description": desc,
            "namespace": namespace,
            "data_type": data_type,
            "project_code": project_code,
            "labels": labels,
            "generate_id": generate_id
        }
        self.logger.debug( 'SrvFileDataMgr post_json_form' + str(post_json_form))
        if operator:
            post_json_form['operator'] = operator
        if from_parents:
            post_json_form['parent_query'] = from_parents
        res = requests.post(url = url, json=post_json_form)
        if res.status_code == 200:
            return res.json()
        else:
            error_info = {
                "error": "create meta failed",
                "errorcode": res.status_code,
                "errorpayload": post_json_form
            }
            return error_info