"""Batch job: promotes raw bronze data to cleaned silver layer."""

import logging
from datetime import datetime, timezone

import duckdb

from urbanpulse_core.config import settings
from urbanpulse_infra.duckdb import get_duckdb_connection

logger = logging.getLogger(__name__)

_BRONZE_PATH = "s3://urban-pulse/bronze/traffic-route-bronze/year=2026/month=*/day=*/hour=*/*.parquet"
_SILVER_BASE = "s3://urban-pulse/silver/traffic-route-silver"
# _SPECIFIC_DAY = "s3://urban-pulse/bronze/traffic-route-bronze/year=2026/month=02/day=07/hour=*/*.parquet"
# Cast expression reused for both the timestamp_utc column and the date partition column.
_TS_CAST = "CAST(REPLACE(CAST(timestamp_utc AS VARCHAR), '+00:00', '') AS TIMESTAMP)"


def _get_con() -> duckdb.DuckDBPyConnection:
    return get_duckdb_connection(
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def _get_watermark(con: duckdb.DuckDBPyConnection) -> str | None:
    """Return the MAX timestamp_utc already written to silver, or None if silver is empty."""
    try:
        result = con.execute(
            f"SELECT MAX(timestamp_utc) FROM read_parquet('{_SILVER_BASE}/date=*/*.parquet',"
            " union_by_name := true)"
        ).fetchone()
        if result and result[0] is not None:
            return str(result[0])
    except duckdb.IOException:
        pass  # silver does not exist yet
    return None


def run(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Promote new bronze records (since last watermark) to silver.

    Each call writes only records newer than MAX(timestamp_utc) already in silver,
    into a timestamped run directory so file names never conflict.
    Returns the number of rows written in this run.
    """
    if con is None:
        con = _get_con()

    watermark = _get_watermark(con)
    watermark_filter = (
        f"AND {_TS_CAST} > CAST('{watermark}' AS TIMESTAMP)" if watermark else ""
    )

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
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
                    CAST(congestion_severe_segments AS INTEGER)                             AS congestion_severe_segments,
                    CAST(congestion_total_segments  AS INTEGER)                            AS congestion_total_segments,
                    {_TS_CAST}                                                             AS timestamp_utc,
                    source,
                    STRFTIME('%Y-%m-%d', {_TS_CAST})                                      AS date
                FROM read_parquet('{_BRONZE_PATH}', union_by_name := true)
                WHERE timestamp_utc IS NOT NULL
                  AND route_id IS NOT NULL
                  {watermark_filter}
            ) TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY (date))
        """
        )
    except duckdb.IOException as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "no such file" in msg:
            logger.info(
                "bronze_to_silver: no new bronze records since watermark=%s — skipping",
                watermark,
            )
            return 0
        raise

    result = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_dir}/**/*.parquet')"
    ).fetchone()
    row_count = int(result[0]) if result is not None else 0
    logger.info(
        "bronze_to_silver: wrote %d new rows → %s  (watermark was: %s)",
        row_count,
        output_dir,
        watermark,
    )
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
