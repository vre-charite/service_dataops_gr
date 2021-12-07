from minio import Minio
from config import ConfigClass


class Minio_Client():
    '''
    NOTE:
        This client is different than other service
        in the dataops gr, we will create the bucket 
        so here we need the superuser-admin permission.
        we use the access key and screte to login
    '''
    def __init__(self):
        self.client = Minio(
            ConfigClass.MINIO_ENDPOINT, 
            access_key=ConfigClass.MINIO_ACCESS_KEY,
            secret_key=ConfigClass.MINIO_SECRET_KEY,
            secure=ConfigClass.MINIO_HTTPS
        )

