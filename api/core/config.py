from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    postgres_dsn: str
 
    minio_endpoint: str
    minio_root_user: str
    minio_root_password: str
    minio_secure: bool = False
    minio_uploads_bucket: str = "uploads"
 
    airflow_api_url: str 
    airflow_username: str
    airflow_password: str
    airflow_dag_id: str = "ingest_documents"
    airflow_base_url: str = "http://airflow-apiserver:8080"
 
    qdrant_host: str
    qdrant_port: int
    qdrant_collection: str = "rag-brain"
 
    openai_api_key: str
    openai_embedding_model: str = "text-embedding-3-small"
    openai_chat_model: str      = "gpt-4o-mini"
 
    max_single_file_bytes: int = 104857600    # 100 MB
    max_zip_file_bytes: int    = 10737418240  # 10 GB
 
    class Config:
        env_file = ".env"
 
 
settings = Settings()
