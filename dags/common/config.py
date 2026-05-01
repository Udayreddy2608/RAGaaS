import os

MINIO_BUCKETS = {
    "uploads":      "uploads",
    "raw_docs":     "raw-docs",
    "chunked_docs": "chunked-docs",
    "manifests":    "manifests",
}

CHUNK_CONFIG = {
    "chunk_size": 512,
    "chunk_overlap": 64,
    "batch_size": 50
}

QDRANT_COLLECTION = "rag-brain"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSION = 1536

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-apiserver:8080")
AIRFLOW_API_URL  = f"{AIRFLOW_BASE_URL}/api/v2"
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")
