"""DuckDB connection factory with MinIO/S3 extension bootstrapping."""

import duckdb


def get_duckdb_connection(
    minio_endpoint: str = "localhost:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection pre-configured for MinIO via the S3 extension."""
    con = duckdb.connect()
    # httpfs is bundled with DuckDB >=1.0 — LOAD is sufficient; INSTALL would
    # attempt an internet download which fails in air-gapped Docker containers.
    con.execute("LOAD httpfs")
    con.execute(f"SET s3_endpoint='{minio_endpoint}'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_access_key_id='{access_key}'")
    con.execute(f"SET s3_secret_access_key='{secret_key}'")
    return con
