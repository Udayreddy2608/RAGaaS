from miniopy_async import Minio
from api.core.config import settings

client = Minio(
    endpoint= settings.minio_endpoint,
    access_key= settings.minio_root_user,
    secret_key= settings.minio_root_password,
    secure= settings.minio_secure
)

async def upload_to_minio(file, key: str, size: int, file_type: str, force: bool = False) -> dict:
    try:
        await client.put_object(
            bucket_name=settings.minio_uploads_bucket,
            object_name=key,
            data=file,
            length=size,
        )
    except Exception as e:
        raise RuntimeError(f"MinIO upload failed for {key}: {e}")
    
    return {
        "bucket": settings.minio_uploads_bucket,
        "key": key,
        "file_type": file_type,
        "force": force
    }