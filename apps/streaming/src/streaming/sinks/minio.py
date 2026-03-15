"""MinIO sink for persisting processed stream data to object storage."""
import io

from minio import Minio
from urbanpulse_core.config import settings

from logger import Logger


class MinioClient:
    """A simple MinIO client for uploading data to a MinIO bucket."""

    def __init__(self) -> None:
        self.logger = Logger("minio.client")
        self.client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )

    def upload_bytes(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload raw bytes to the specified bucket."""
        self.logger.info(f"Uploading to MinIO: bucket={bucket_name}, object={object_name}, size={len(data)} bytes")
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
        self.client.put_object(
            bucket_name,
            object_name,
            io.BytesIO(data),
            len(data),
            content_type=content_type,
        )