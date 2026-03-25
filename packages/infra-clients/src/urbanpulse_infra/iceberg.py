"""Iceberg Nessie catalog factory for MinIO-backed warehouses."""

from __future__ import annotations

from pyiceberg.catalog import Catalog, load_catalog


def get_iceberg_catalog(
    catalog_uri: str = "http://localhost:19120/iceberg/",
    minio_endpoint: str = "minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    warehouse: str = "s3://warehouse/",
) -> Catalog:
    """Create a PyIceberg REST catalog connected to Nessie's Iceberg endpoint."""
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": catalog_uri,
            "s3.endpoint": f"http://{minio_endpoint}",
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": "us-east-1",
            "warehouse": warehouse,
        },
    )
