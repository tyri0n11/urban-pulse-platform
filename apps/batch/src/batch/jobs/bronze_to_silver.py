"""Batch job: promotes raw bronze data to cleaned silver layer."""

import logging
from datetime import datetime, timedelta, timezone

import duckdb

from urbanpulse_core.config import settings
from urbanpulse_infra.duckdb import get_duckdb_connection

logger = logging.getLogger(__name__)

_BRONZE_BASE = "s3://urban-pulse/bronze/traffic-route-bronze"
_BRONZE_PATH = f"{_BRONZE_BASE}/year=2026/month=*/day=*/hour=*/*.parquet"
_SILVER_BASE = "s3://urban-pulse/silver/traffic-route-silver"
# Cast expression reused for both the timestamp_utc column and the date partition column.
_TS_CAST = "CAST(REPLACE(CAST(timestamp_utc AS VARCHAR), '+00:00', '') AS TIMESTAMP)"


def _get_con() -> duckdb.DuckDBPyConnection:
    return get_duckdb_connection(
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )

def _yesterday_bronze_path() -> str:
    """Return the bronze partition glob for yesterday (all hours)."""
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return (
        f"{_BRONZE_BASE}/year={yesterday.year}/month={yesterday.month:02d}"
        f"/day={yesterday.day:02d}/hour=*/*.parquet"
    )


def microbatch(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Promote yesterday's bronze records to silver.

    Scoped to a single day's partition — no watermark scan needed.
    Returns the number of rows written.
    """
    if con is None:
        con = _get_con()

    bronze_path = _yesterday_bronze_path()
    logger.info("bronze_to_silver microbatch: processing %s", bronze_path)

    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    route_id,
                    origin,
                    destination,
                    CAST(distance_meters AS DOUBLE)                   AS distance_meters,
                    CAST(duration_ms AS DOUBLE)                       AS duration_ms,
                    CAST(duration_minutes AS DOUBLE)                  AS duration_minutes,
                    CAST(congestion_heavy_ratio    AS DOUBLE)         AS congestion_heavy_ratio,
                    CAST(congestion_moderate_ratio AS DOUBLE)         AS congestion_moderate_ratio,
                    CAST(congestion_low_ratio      AS DOUBLE)         AS congestion_low_ratio,
                    CAST(congestion_severe_segments AS INTEGER)       AS congestion_severe_segments,
                    CAST(congestion_total_segments  AS INTEGER)       AS congestion_total_segments,
                    {_TS_CAST}                                        AS timestamp_utc,
                    source,
                    STRFTIME('%Y-%m-%d', {_TS_CAST})                  AS date
                FROM read_parquet('{bronze_path}', union_by_name := true)
                WHERE timestamp_utc IS NOT NULL
                  AND route_id IS NOT NULL
            ) TO '{_SILVER_BASE}' (FORMAT PARQUET, PARTITION_BY (date))
        """
        )
    except duckdb.IOException as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "no such file" in msg:
            logger.info(
                "bronze_to_silver microbatch: no bronze files for %s — skipping",
                bronze_path,
            )
            return 0
        raise

    result = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_SILVER_BASE}/**/*.parquet')"
    ).fetchone()
    row_count = int(result[0]) if result is not None else 0
    logger.info("bronze_to_silver microbatch: wrote %d new rows → %s", row_count, _SILVER_BASE)
    return row_count

def bootstrap(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Full backfill: ignore watermark and promote ALL bronze data to silver."""
    logger.info("bronze_to_silver bootstrap: full backfill")
    if con is None:
        con = _get_con()
    
    output_dir = f"{_SILVER_BASE}"

    try:
        con.execute(
            f"""
            COPY (
                SELECT
                    route_id,
                    origin,
                    destination,
                    CAST(distance_meters AS DOUBLE)                                        AS distance_meters,
                    CAST(duration_ms AS DOUBLE)                                            AS duration_ms,
                    CAST(duration_minutes AS DOUBLE)                                       AS duration_minutes,
                    CAST(congestion_heavy_ratio    AS DOUBLE)                              AS congestion_heavy_ratio,
                    CAST(congestion_moderate_ratio AS DOUBLE)                              AS congestion_moderate_ratio,
                    CAST(congestion_low_ratio      AS DOUBLE)                              AS congestion_low_ratio,
                    CAST(congestion_severe_segments AS INTEGER)                            AS congestion_severe_segments,
                    CAST(congestion_total_segments  AS INTEGER)                            AS congestion_total_segments,
                    {_TS_CAST}                                                             AS timestamp_utc,
                    source,
                    STRFTIME('%Y-%m-%d', {_TS_CAST})                                      AS date
                FROM read_parquet('{_BRONZE_PATH}', union_by_name := true)

            ) TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY (date))
        """
        )
    except duckdb.IOException as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "no such file" in msg:
            logger.info("bronze_to_silver bootstrap: no bronze files found")
            return 0
        raise

    result = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_dir}/**/*.parquet')"
    ).fetchone()
    row_count = int(result[0]) if result is not None else 0
    logger.info("bronze_to_silver bootstrap: wrote %d rows → %s", row_count, output_dir)
    return row_count
