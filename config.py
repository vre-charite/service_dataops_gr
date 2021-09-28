import os
import requests
from requests.models import HTTPError

# os.environ['env'] = 'test'
srv_namespace = "service_dataops_gr"
CONFIG_CENTER = "http://10.3.7.222:5062" \
    if os.environ.get('env') == "test" \
    else "http://common.utility:5062"


def vault_factory() -> dict:
    url = CONFIG_CENTER + \
        "/v1/utility/config/{}".format(srv_namespace)
    config_center_respon = requests.get(url)
    if config_center_respon.status_code != 200:
        raise HTTPError(config_center_respon.text)
    return config_center_respon.json()['result']


class ConfigClass(object):
    vault = vault_factory()
    env = os.environ.get('env')
    disk_namespace = os.environ.get('namespace')
    version = "0.1.0"
    NFS_ROOT_PATH = "/data/vre-storage"
    VRE_ROOT_PATH = "/vre-data"

    # disk mounts
    # NFS_ROOT_PATH = "./"
    # VRE_ROOT_PATH = "/vre-data"
    ROOT_PATH = {
        "vre": "/vre-data"
    }.get(os.environ.get('namespace'), "/data/vre-storage")

    # TEMP_BASE = os.path.expanduser("~/tmp/flask_uploads/")
    TEMP_BASE = "/tmp/dataops"
    DOWNLOAD_KEY = 'indoc101'

    # Allowed archive types
    ARCHIVE_TYPES = [".zip"]

    # Neo4j Service
    NEO4J_SERVICE = vault['NEO4J_SERVICE'] + "/v1/neo4j/"
    NEO4J_SERVICE_V2 = vault['NEO4J_SERVICE'] + "/v2/neo4j/"
    CATALOGUING_SERVICE = vault['CATALOGUING_SERVICE']+"/v1/"
    QUEUE_SERVICE = vault['QUEUE_SERVICE']+"/v1/"
    DATA_UTILITY_SERVICE = vault["DATA_OPS_UTIL"]+"/v1/"

    # minio
    MINIO_OPENID_CLIENT = vault['MINIO_OPENID_CLIENT']
    MINIO_ENDPOINT = vault['MINIO_ENDPOINT']
    MINIO_HTTPS = False
    KEYCLOAK_URL = vault['KEYCLOAK_URL']
    MINIO_TEST_PASS = vault['MINIO_TEST_PASS']
    MINIO_TMP_PATH = ROOT_PATH + '/tmp/'
    MINIO_ACCESS_KEY = vault['MINIO_ACCESS_KEY']
    MINIO_SECRET_KEY = vault['MINIO_SECRET_KEY']

    # Redis Service
    REDIS_HOST = vault['REDIS_HOST']
    REDIS_PORT = int(vault['REDIS_PORT'])
    REDIS_DB = int(vault['REDIS_DB'])
    REDIS_PASSWORD = vault['REDIS_PASSWORD']

    RDS_HOST = vault['RDS_HOST']
    RDS_PORT = vault['RDS_PORT']
    RDS_DBNAME = vault['RDS_DBNAME']
    RDS_USER = vault['RDS_USER']
    RDS_PWD = vault['RDS_PWD']
    RDS_SCHEMA_DEFAULT = vault['RDS_SCHEMA_DEFAULT']
    OPS_DB_URI = f"postgresql://{RDS_USER}:{RDS_PWD}@{RDS_HOST}/{RDS_DBNAME}"

    SQLALCHEMY_DATABASE_URI = OPS_DB_URI
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    MAX_PREVIEW_SIZE = 500000

    # JWT
    JWT_AUTH_URL_RULE = None

    api_modules = ["api"]

