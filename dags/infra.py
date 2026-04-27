from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id = "setup_infrastructure",
    schedule = "@once",
    catchup = False,
    tags = ['rag-brain', 'setup'],
    start_date=datetime(2026, 4, 26)
)
def setup_infrastructure():

    @task
    def setup():
        from common.minio_hook import MinioHook
        from common.qdrant_hook import QdrantHook
        from common.config import QDRANT_COLLECTION, EMBEDDING_DIMENSION, MINIO_BUCKETS

        minio = MinioHook()
        
        for bucket in MINIO_BUCKETS.values():
            if not minio.bucket_exists(bucket):
                minio.create_bucket(bucket)
                print(f"Bucket '{bucket}' created.")
        
        qdrant = QdrantHook()
        if not qdrant.collection_exists(QDRANT_COLLECTION):
            qdrant.create_collection(QDRANT_COLLECTION, EMBEDDING_DIMENSION)
            print(f"Collection '{QDRANT_COLLECTION}' created.")
        
        print("Infrastructure Ready...")
    
    setup()

setup_infrastructure()