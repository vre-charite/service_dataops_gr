import requests
import json
from models.service_meta_class import MetaService
from config import ConfigClass


class SrvContainerManager(metaclass=MetaService):
    def __init__(self):
        self.url = ConfigClass.NEO4J_SERVICE

    def fetch_container_by_id(self, container_id):
        my_url = self.url + "nodes/Container/node/" + str(container_id)
        res = requests.get(url=my_url)
        return json.loads(res.text)

    def list_containers(self, payload=None):
        url = ConfigClass.NEO4J_SERVICE + "nodes/Container/query"
        res = requests.post(
            url=url,
            json=payload
        )
        return json.loads(res.text)
