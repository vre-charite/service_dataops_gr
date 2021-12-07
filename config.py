import os
import requests
from requests.models import HTTPError
from pydantic import BaseSettings, Extra
from typing import Dict, Set, List, Any
from functools import lru_cache

SRV_NAMESPACE = os.environ.get("APP_NAME", "service_dataops_gr")
CONFIG_CENTER_ENABLED = os.environ.get("CONFIG_CENTER_ENABLED", "false")
CONFIG_CENTER_BASE_URL = os.environ.get("CONFIG_CENTER_BASE_URL", "NOT_SET")

def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == "false":
        return {}
    else:
        return vault_factory(CONFIG_CENTER_BASE_URL)

def vault_factory(config_center) -> dict:
    url = f"{config_center}/v1/utility/config/{SRV_NAMESPACE}"
    config_center_respon = requests.get(url)
    if config_center_respon.status_code != 200:
        raise HTTPError(config_center_respon.text)
    return config_center_respon.json()['result']


class Settings(BaseSettings):
    port: int = 5063
    host: str = "127.0.0.1"
    env: str = ""
    namespace: str = ""

    NFS_ROOT_PATH: str = "/data/vre-storage"
    VRE_ROOT_PATH: str = "/vre-data"

    # disk mounts
    ROOT_PATH: str = {
        "vre": "/vre-data"
    }.get(os.environ.get('namespace'), "/data/vre-storage")

    TEMP_BASE: str = "/tmp/dataops"
    DOWNLOAD_KEY: str = 'indoc101'

    # Allowed archive types
    ARCHIVE_TYPES: List[str] = [".zip"]

    # Neo4j Service
    NEO4J_SERVICE: str
    CATALOGUING_SERVICE: str
    QUEUE_SERVICE: str
    DATA_OPS_UTIL: str
    UTILITY_SERVICE: str
    EMAIL_SERVICE: str

    # minio
    MINIO_OPENID_CLIENT: str
    MINIO_ENDPOINT: str
    MINIO_HTTPS: bool = False
    KEYCLOAK_URL: str
    MINIO_TEST_PASS: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str

    # Redis Service
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_DB: str
    REDIS_PASSWORD: str

    RDS_HOST: str
    RDS_PORT: str
    RDS_DBNAME: str
    RDS_USER: str
    RDS_PWD: str
    RDS_SCHEMA_DEFAULT: str

    SQLALCHEMY_TRACK_MODIFICATIONS: bool = False

    EMAIL_SUPPORT: str = "admin@gkmc.ca"
    EMAIL_SUPPORT_PROD: str = "vre-support@charite.de"

    MAX_PREVIEW_SIZE: int = 500000

    # JWT
    JWT_AUTH_URL_RULE: None = None

    api_modules: List[str] = ["api"]

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                load_vault_settings,
                env_settings,
                init_settings,
                file_secret_settings,
            )
    

@lru_cache(1)
def get_settings():
    settings =  Settings()
    return settings

class ConfigClass(object):
    settings = get_settings()

    version = "0.1.0"
    env = settings.env
    disk_namespace = settings.namespace

    NFS_ROOT_PATH = settings.NFS_ROOT_PATH
    VRE_ROOT_PATH = settings.VRE_ROOT_PATH

    # disk mounts
    ROOT_PATH = settings.ROOT_PATH
    # TEMP_BASE = os.path.expanduser("~/tmp/flask_uploads/")
    TEMP_BASE = settings.TEMP_BASE
    DOWNLOAD_KEY = settings.DOWNLOAD_KEY

    # Allowed archive types
    ARCHIVE_TYPES = settings.ARCHIVE_TYPES

    # Neo4j Service
    NEO4J_SERVICE = settings.NEO4J_SERVICE + "/v1/neo4j/"
    NEO4J_SERVICE_V2 = settings.NEO4J_SERVICE + "/v2/neo4j/"
    CATALOGUING_SERVICE = settings.CATALOGUING_SERVICE + "/v1/"
    QUEUE_SERVICE = settings.QUEUE_SERVICE + "/v1/"
    DATA_UTILITY_SERVICE = settings.DATA_OPS_UTIL + "/v1/"
    UTILITY_SERVICE = settings.UTILITY_SERVICE + "/v1/"
    EMAIL_SERVICE = settings.EMAIL_SERVICE + "/v1/email"

    # minio
    MINIO_OPENID_CLIENT = settings.MINIO_OPENID_CLIENT
    MINIO_ENDPOINT = settings.MINIO_ENDPOINT
    MINIO_HTTPS = settings.MINIO_HTTPS
    KEYCLOAK_URL = settings.KEYCLOAK_URL
    MINIO_TEST_PASS = settings.MINIO_TEST_PASS
    MINIO_TMP_PATH = ROOT_PATH + '/tmp/'
    MINIO_ACCESS_KEY = settings.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY = settings.MINIO_SECRET_KEY

    # Redis Service
    REDIS_HOST = settings.REDIS_HOST
    REDIS_PORT = int(settings.REDIS_PORT)
    REDIS_DB = int(settings.REDIS_DB)
    REDIS_PASSWORD = settings.REDIS_PASSWORD

    RDS_HOST = settings.RDS_HOST
    RDS_PORT = settings.RDS_PORT
    RDS_DBNAME = settings.RDS_DBNAME
    RDS_USER = settings.RDS_USER
    RDS_PWD = settings.RDS_PWD
    RDS_SCHEMA_DEFAULT = settings.RDS_SCHEMA_DEFAULT
    OPS_DB_URI = f"postgresql://{RDS_USER}:{RDS_PWD}@{RDS_HOST}/{RDS_DBNAME}"

    SQLALCHEMY_DATABASE_URI = OPS_DB_URI
    SQLALCHEMY_TRACK_MODIFICATIONS = settings.SQLALCHEMY_TRACK_MODIFICATIONS

    EMAIL_SUPPORT = settings.EMAIL_SUPPORT
    if env == 'charite':
        EMAIL_SUPPORT = settings.EMAIL_SUPPORT_PROD

    MAX_PREVIEW_SIZE = settings.MAX_PREVIEW_SIZE

    # JWT
    JWT_AUTH_URL_RULE = settings.JWT_AUTH_URL_RULE
    api_modules = settings.api_modules
