


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
EMBEDDING_MODEL= "text-embedding-3-small"
EMBEDDING_DIMENSION= 1536