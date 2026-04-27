from minio import Minio
from airflow.hooks.base import BaseHook
from minio.error import S3Error


class MinioHook(BaseHook):
    def __init__(self, minio_conn_id='minio_default'):
        super().__init__()
        self.minio_conn_id = minio_conn_id
        self._client = None

    def get_client(self):
        if self._client is not None:
            return self._client

        conn = self.get_connection(self.minio_conn_id)

        endpoint = f"{conn.host}:{conn.port}"

        self._client = Minio(
            endpoint,
            access_key=conn.login,
            secret_key=conn.password,
            secure=False,
        )
        return self._client
    
    def create_bucket(self, bucket_name):
        client = self.get_client()
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                self.log.info(f"Bucket '{bucket_name}' created successfully.")
            else:
                self.log.info(f"Bucket '{bucket_name}' already exists.")
        except S3Error as e:
            self.log.error(f"Error creating bucket: {e}")
            raise
    
    def bucket_exists(self, bucket_name):
        client = self.get_client()
        try:
            return client.bucket_exists(bucket_name)
        except S3Error as e:
            self.log.error(f"Error checking bucket existence: {e}")
            raise
    
    def get_object(self, bucket: str, key: str) -> bytes:
        client = self.get_client()
        try:
            response = client.get_object(bucket, key)
            return response.read()
        except S3Error as e:
            self.log.error(f"Error fetching object from Minio: {e}")
            raise
    
    def put_object(self, bucket: str, key: str, data: bytes) -> None:
        client = self.get_client()
        if isinstance(data, str):
            data = data.encode('utf-8')
        try:
            import io
            client.put_object(bucket, 
            key, 
            io.BytesIO(data), 
            len(data))
        except S3Error as e:
            self.log.error(f"Error uploading object to Minio: {e}")
            raise