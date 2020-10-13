from models.service_meta_class import MetaService
from redis import StrictRedis
from config import ConfigClass

_singleton_connection = None

class SrvRedisSingleton(metaclass=MetaService):
    def __init__(self):
        self.host = ConfigClass.REDIS_HOST
        self.port = ConfigClass.REDIS_PORT
        self.db = ConfigClass.REDIS_DB
        self.pwd = ConfigClass.REDIS_PASSWORD
        self.__instance = None
        self.connect()

    def connect(self):
        global _singleton_connection
        if _singleton_connection:
            self.__instance = _singleton_connection
        else:
            self.__instance = StrictRedis(host=self.host,
                port=self.port,
                db=self.db,
                password=self.pwd)
            _singleton_connection = self.__instance

    def get_by_key(self, key: str):
        return self.__instance.get(key)

    def set_by_key(self, key: str, content: str):
        self.__instance.set(key, content)

    def mget_by_prefix(self, prefix: str):
        query='{}:*'.format(prefix)
        keys = self.__instance.keys(query)
        return self.__instance.mget(keys)