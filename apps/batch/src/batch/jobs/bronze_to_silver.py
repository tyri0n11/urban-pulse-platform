"""Batch job: promotes raw bronze data to cleaned silver layer."""

import logging

import duckdb

from urbanpulse_core.config import settings
from urbanpulse_infra.duckdb import get_duckdb_connection

logger = logging.getLogger(__name__)

_BRONZE_PATH = "s3://urban-pulse/bronze/traffic-route-bronze/**/*.parquet"
_SILVER_BASE = "s3://urban-pulse/silver/traffic-route-silver"


def _get_con() -> duckdb.DuckDBPyConnection:
    return get_duckdb_connection(
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def run(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Promote bronze traffic records to silver Parquet, partitioned by date=YYYY-MM-DD.

    Returns the total number of rows in the silver layer after promotion.
    """
    if con is None:
        con = _get_con()

    try:
        # union_by_name handles schema drift between old files (congestion_heavy_ratio)
        # and new files (heavy_ratio). COALESCE picks whichever name is present.
        con.execute(f"""
            COPY (
                SELECT
                    route_id,
                    origin,
                    destination,
                    CAST(distance_meters AS DOUBLE)                                        AS distance_meters,
                    CAST(duration_ms AS DOUBLE)                                            AS duration_ms,
                    CAST(duration_minutes AS DOUBLE)                                       AS duration_minutes,
                    CAST(COALESCE(congestion_heavy_ratio,    heavy_ratio)    AS DOUBLE)    AS congestion_heavy_ratio,
                    CAST(COALESCE(congestion_moderate_ratio, moderate_ratio) AS DOUBLE)    AS congestion_moderate_ratio,
                    CAST(COALESCE(congestion_low_ratio,      low_ratio)      AS DOUBLE)    AS congestion_low_ratio,
                    CAST(COALESCE(congestion_severe_segments, severe_segments) AS INTEGER) AS congestion_severe_segments,
                    CAST(COALESCE(congestion_total_segments,  total_segments)  AS INTEGER) AS congestion_total_segments,
                    CAST(
                        REPLACE(CAST(timestamp_utc AS VARCHAR), '+00:00', '')
                        AS TIMESTAMP
                    )                                                                      AS timestamp_utc,
                    source,
                    STRFTIME('%Y-%m-%d', CAST(REPLACE(CAST(timestamp_utc AS VARCHAR), '+00:00', '') AS TIMESTAMP)) AS date
                FROM read_parquet('{_BRONZE_PATH}',
                                  union_by_name := true)
                WHERE timestamp_utc IS NOT NULL
                  AND route_id IS NOT NULL
            ) TO '{_SILVER_BASE}' (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE true)
        """)
    except duckdb.IOException as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "no such file" in msg:
            logger.info("bronze_to_silver: no bronze files yet — skipping")
            return 0
        raise

    result = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_SILVER_BASE}/**/*.parquet', hive_partitioning := true)"
    ).fetchone()
    row_count = int(result[0]) if result is not None else 0
    logger.info("bronze_to_silver: wrote %d rows → %s", row_count, _SILVER_BASE)
    return row_count


def bootstrap(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Full backfill: read all bronze data and promote to silver.

    Identical to run() — kept as a named entry point for the bootstrap Prefect flow.
    """
    logger.info("bronze_to_silver bootstrap: promoting all bronze data to silver")
    return run(con=con)
