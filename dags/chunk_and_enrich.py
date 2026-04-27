from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from common.config import MINIO_BUCKETS

raw_docs_ready = Asset("s3://raw-docs/ready")

default_args = {
    'owner': 'rag-brain',
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}


@dag(
    dag_id='chunk_and_enrich',
    default_args=default_args,
    schedule= [raw_docs_ready],
    catchup=False,
    max_active_runs=5,
    tags  = ['rag-brain', 'chunking']
)
def chunk_and_enrich():

    @task
    def pull_raw_doc(**context):

        raw_docs_ready = Asset("s3://raw-docs/ready")
        asset_events = context["triggering_asset_events"]
        events = asset_events.get(raw_docs_ready, [])

        if not events:
            raise ValueError("No triggering event found for s3://raw-docs/ready")

        event = events[-1]
        file_info = event.extra
        print(f"file_info from asset event: {file_info}")

        return {
            "source_key": file_info["key"],
            "source_bucket": file_info["bucket"],
            "source_filename": file_info["source_filename"],
            "file_hash": file_info["file_hash"],
        }
    
    @task
    def chunk_document(file_info: dict):
        import json
        from common.minio_hook import MinioHook
        from common.config import CHUNK_CONFIG, MINIO_BUCKETS
        from langchain_text_splitters import RecursiveCharacterTextSplitter

        CHUNK_SIZE = CHUNK_CONFIG.get('chunk_size', 512)
        CHUNK_OVERLAP = CHUNK_CONFIG.get('chunk_overlap', 64)
        BATCH_SIZE = CHUNK_CONFIG.get("batch_size", 50)
        
        hook = MinioHook()
        raw_bytes = hook.get_object(
            bucket=file_info["source_bucket"],
            key=file_info["source_key"]
        )
        text = raw_bytes.decode("utf-8", errors="replace")
        print(f"Pulled {len(text)} chars from MinIO")

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP,
        )
        chunks = splitter.split_text(text)

        print(f"Created {len(chunks)} chunks")

        stem = file_info["source_key"].split("/")[-1].replace(".txt", "")
        bucket = MINIO_BUCKETS["chunked_docs"]
        batch_keys = []

        for i in range(0, len(chunks), BATCH_SIZE):
            batch = {
                "chunks": chunks[i:i + BATCH_SIZE],
                "source_key": file_info["source_key"],
                "source_filename": file_info["source_filename"],
                "file_hash": file_info["file_hash"],
                "start_index": i,
                "total_chunks": len(chunks),
            }
            key = f"{stem}/batch_{i // BATCH_SIZE}.json"
            hook.put_object(bucket, key, json.dumps(batch))
            batch_keys.append(key)

        print(f"Wrote {len(batch_keys)} batches to MinIO://{bucket}/{stem}/")
        return batch_keys
    
    # @task
    # def ensure_collection():
    #     from common.config import EMBEDDING_DIMENSION, QDRANT_COLLECTION
    #     from common.qdrant_hook import QdrantHook

    #     qdrant = QdrantHook()
    #     qdrant.create_collection(QDRANT_COLLECTION, EMBEDDING_DIMENSION)
    #     print(f"Collection '{QDRANT_COLLECTION} is ready...")

    @task
    def tag_metadata(batch_key: str):
        import json
        import os
        import uuid
        from datetime import datetime, timezone
        from openai import OpenAI
        from qdrant_client.models import PointStruct
        from common.minio_hook import MinioHook
        from common.qdrant_hook import QdrantHook
        from common.config import QDRANT_COLLECTION, EMBEDDING_MODEL

        minio = MinioHook()
        from common.config import MINIO_BUCKETS
        bucket = MINIO_BUCKETS['chunked_docs']
        raw = minio.get_object(bucket= bucket, key = batch_key)

        batch = json.loads(raw.decode("utf-8"))

        qdrant = QdrantHook()

        # qdrant.create_collection(QDRANT_COLLECTION, EMBEDDING_DIMENSION)

        client = OpenAI(
            api_key= os.environ["OPENAI_API_KEY"]
        )

        texts = [chunk for chunk in batch['chunks']]

        response = client.embeddings.create(
            model = EMBEDDING_MODEL,
            input = texts
        )

        embeddings = [item.embedding for item in response.data]
        print(f"Generated {len(embeddings)} embeddings for batch {batch_key}")

        points = []
        for i, (chunk_text, embedding) in enumerate(zip(texts, embeddings)):
            points.append(
                PointStruct(
                    id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{batch['file_hash']}_{batch['start_index'] + i}")),
                    vector=embedding,
                    payload={
                        "text": chunk_text,
                        "source_key": batch["source_key"],
                        "source_filename": batch["source_filename"],
                        "file_hash": batch["file_hash"],
                        "chunk_index": batch["start_index"] + i,
                        "total_chunks": batch["total_chunks"],
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
            )
        qdrant.upsert(QDRANT_COLLECTION, points)
        print(f"Upserted {len(points)} points to Qdrant collection: {QDRANT_COLLECTION}")
        return batch_key
    
    file_info  = pull_raw_doc()
    batch_keys = chunk_document(file_info)
    tag_metadata.expand(batch_key=batch_keys)


chunk_and_enrich()