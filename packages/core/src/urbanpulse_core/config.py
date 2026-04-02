"""Shared configuration schema and environment variable loading."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # API
    vietmap_api_key: str = ""
    dry_run: bool = False
    routes_file: str = "routes.json"

    # Kafka / Redpanda
    kafka_bootstrap_servers: str = "localhost:19092"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False

    # Iceberg
    iceberg_catalog_uri: str = "http://localhost:19120/iceberg/"

    # MLflow
    mlflow_tracking_uri: str = "http://localhost:5000"

    # Postgres (online feature store)
    postgres_dsn: str = "postgresql://urbanpulse:urbanpulse@localhost:5432/urbanpulse"

    model_config = {"env_file": ".env", "case_sensitive": False, "extra": "ignore"}


settings = Settings()
