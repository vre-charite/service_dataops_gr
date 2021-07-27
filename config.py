import os


class ConfigClass(object):

    env = os.environ.get('env')

    # NFS
    # NFS_ROOT_PATH = os.path.expanduser("~/Desktop/indoc/fake_nfs")
    NFS_ROOT_PATH = "/data/vre-storage"
    VRE_ROOT_PATH = "/vre-data"

    # TEMP_BASE = os.path.expanduser("~/tmp/flask_uploads/")
    TEMP_BASE = "/tmp/dataops"
    DOWNLOAD_KEY = 'indoc101'

    # Allowed archive types
    ARCHIVE_TYPES = [".zip"]

    # Neo4j Service
    NEO4J_SERVICE = "http://neo4j.utility:5062/v1/neo4j/"
    NEO4J_SERVICE_V2 = "http://neo4j.utility:5062/v2/neo4j/"
    CATALOGUING_SERVICE = "http://cataloguing.utility:5064/v1/"
    QUEUE_SERVICE = "http://queue-producer.greenroom:6060/v1/"

    # minio config
    MINIO_OPENID_CLIENT = "react-app"
    MINIO_ENDPOINT = "minio.minio:9000"
    MINIO_HTTPS = False
    KEYCLOAK_URL = "http://keycloak.utility:8080"
    MINIO_ACCESS_KEY = "indoc-minio"
    MINIO_SECRET_KEY = "Trillian42!"


    if env == "test":
        DATA_UTILITY_SERVICE = "http://dataops-ut.utility:5063/v1/"

        # minio config
        MINIO_ENDPOINT = "10.3.7.220"
        MINIO_HTTPS = False
        KEYCLOAK_URL = "http://10.3.7.220" # for local test ONLY



    # MINIO_OPENID_CLIENT = "react-app"
    # if env == "staging":
    #     # MINIO_ENDPOINT = "10.3.7.240:80"
    #     MINIO_ENDPOINT = "minio.minio:9000"
    #     MINIO_HTTPS = False
    #     KEYCLOAK_URL = "http://10.3.7.240:80"
    #     MINIO_TEST_PASS = "IndocStaging2021!"
    # else:
    #     MINIO_ENDPOINT = "10.3.7.220"
    #     MINIO_HTTPS = False
    #     KEYCLOAK_URL = "http://keycloak.utility:8080"
    #     # KEYCLOAK_URL = "http://10.3.7.220" # for local test ONLY
    #     MINIO_TEST_PASS = "admin"


    # Redis Service
    REDIS_HOST = "redis-master.utility"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = {
        'staging': '8EH6QmEYJN',
        'charite': 'o2x7vGQx6m'
    }.get(env, "5wCCMMC1Lk")


    RDS_HOST = "opsdb.utility"
    RDS_PORT = "5432"
    RDS_DBNAME = "INDOC_VRE"
    RDS_USER = "postgres"
    RDS_PWD = "postgres"
    RDS_SCHEMA_DEFAULT = "indoc_vre"
    if env == "test":
        RDS_HOST = '10.3.7.215'
    if env == 'charite':
        RDS_USER = "indoc_vre"
        RDS_PWD = os.environ.get('RDS_PWD')
    OPS_DB_URI= f"postgresql://{RDS_USER}:{RDS_PWD}@{RDS_HOST}/{RDS_DBNAME}"
    SQLALCHEMY_DATABASE_URI = OPS_DB_URI
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    MAX_PREVIEW_SIZE = 500000

    # JWT
    JWT_AUTH_URL_RULE = None

    api_modules = ["api"]