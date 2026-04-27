from airflow.hooks.base import BaseHook
from qdrant_client import QdrantClient

class QdrantHook(BaseHook):
    def __init__(self, qdrant_conn_id:str = "qdrant_default") -> None:
        super().__init__()
        self.qdrant_conn_id = qdrant_conn_id
        self._client = None
    
    def get_client(self) -> QdrantClient:
        if self._client is not None:
            return self._client
        
        conn = self.get_connection(self.qdrant_conn_id)

        self._client = QdrantClient(
            host = conn.host,
            port = int(conn.port), 
        )

        return self._client
    
    def collection_exists(self, collection_name: str) -> bool:
        client = self.get_client()
        existing = client.get_collections().collections
        return any(c.name == collection_name for c in existing)

    def create_collection(self, collection_name: str, vector_size: int) -> None:
        from qdrant_client.models import Distance, VectorParams
        client = self.get_client()
        if not client.collection_exists(collection_name=collection_name):
            client.create_collection(
                 collection_name= collection_name,
                 vectors_config= VectorParams(
                     size = vector_size,
                     distance = Distance.COSINE,
                 )
             )
            self.log.info(f"Created Qdrant Collection: {collection_name}")
        else:
            self.log.info(f"Collection already exists: {collection_name}")
    
    def upsert(self, collection_name: str, points: list) -> None:
        client = self.get_client()
        client.upsert(
            collection_name= collection_name,
            points = points,
        )
        self.log.info(f"Upserted {len(points)} points to {collection_name}")  
