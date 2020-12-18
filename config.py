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

    # Neo4j Service
    NEO4J_SERVICE = "http://neo4j.utility:5062/v1/neo4j/"  # Server


    # Metadata service
    METADATA_API = "http://cataloguing.utility:5064"  # Server

    # Redis Service
    REDIS_HOST = "redis-master.utility"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = {
        'staging': '8EH6QmEYJN',
        'charite': 'o2x7vGQx6m'
    }.get(env, "5wCCMMC1Lk")

    service_queue_send_msg_url = "http://queue-producer.greenroom:6060/v1/send_message"

    # JWT
    JWT_AUTH_URL_RULE = None

    api_modules = ["api"]

# trrigger
