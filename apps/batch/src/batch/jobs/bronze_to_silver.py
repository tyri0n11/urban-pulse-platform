"""Batch job: promotes raw bronze data to cleaned silver layer.

Micro-batch strategy: process one bronze hour partition at a time.
- Each run targets a specific (year, month, day, hour) from bronze.
- Silver is partitioned by date/hour, mirroring bronze structure.
- Idempotent: skips if the silver hour partition already exists.
- bootstrap() backfills all available bronze hours sequentially.
"""

import logging
from datetime import datetime, timedelta, timezone

import duckdb

from urbanpulse_core.config import settings
from urbanpulse_infra.duckdb import get_duckdb_connection

logger = logging.getLogger(__name__)

_BRONZE_HOUR = (
    "s3://urban-pulse/bronze/traffic-route-bronze"
    "/year={y}/month={m:02d}/day={d:02d}/hour={h:02d}/*.parquet"
)
_SILVER_HOUR = (
    "s3://urban-pulse/silver/traffic-route-silver"
    "/date={date}/hour={h:02d}"
)
_TS_CAST = "CAST(REPLACE(CAST(timestamp_utc AS VARCHAR), '+00:00', '') AS TIMESTAMP)"

_SELECT = """\
                SELECT
                    route_id,
                    origin,
                    destination,
                    CAST(distance_meters        AS DOUBLE)  AS distance_meters,
                    CAST(duration_ms            AS DOUBLE)  AS duration_ms,
                    CAST(duration_minutes       AS DOUBLE)  AS duration_minutes,
                    CAST(congestion_heavy_ratio    AS DOUBLE)  AS congestion_heavy_ratio,
                    CAST(congestion_moderate_ratio AS DOUBLE)  AS congestion_moderate_ratio,
                    CAST(congestion_low_ratio      AS DOUBLE)  AS congestion_low_ratio,
                    CAST(congestion_severe_segments AS INTEGER) AS congestion_severe_segments,
                    CAST(congestion_total_segments  AS INTEGER) AS congestion_total_segments,
                    {ts_cast}                               AS timestamp_utc,
                    source
                FROM read_parquet('{src}', union_by_name := true)
                WHERE timestamp_utc IS NOT NULL
                  AND route_id IS NOT NULL"""


def _get_con() -> duckdb.DuckDBPyConnection:
    return get_duckdb_connection(
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )


def _silver_hour_exists(con: duckdb.DuckDBPyConnection, dest: str) -> bool:
    """Return True if the silver hour partition already has files."""
    try:
        result = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{dest}/*.parquet')"
        ).fetchone()
        return result is not None and result[0] > 0
    except duckdb.IOException:
        return False


def run_hour(
    year: int,
    month: int,
    day: int,
    hour: int,
    con: duckdb.DuckDBPyConnection | None = None,
    overwrite: bool = False,
) -> int:
    """Promote one bronze hour partition to silver.

    Returns the number of rows written, or 0 if skipped (already exists).
    """
    if con is None:
        con = _get_con()

    date_str = f"{year}-{month:02d}-{day:02d}"
    src = _BRONZE_HOUR.format(y=year, m=month, d=day, h=hour)
    dest = _SILVER_HOUR.format(date=date_str, h=hour)

    if not overwrite and _silver_hour_exists(con, dest):
        logger.debug("bronze_to_silver: skip %s hour=%02d (already exists)", date_str, hour)
        return 0

    try:
        con.execute(
            f"""
            COPY (
                {_SELECT.format(ts_cast=_TS_CAST, src=src)}
            ) TO '{dest}' (FORMAT PARQUET)
            """
        )
    except duckdb.IOException as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "no such file" in msg:
            logger.debug("bronze_to_silver: no bronze data for %s hour=%02d", date_str, hour)
            return 0
        raise

    result = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{dest}/*.parquet')"
    ).fetchone()
    row_count = int(result[0]) if result is not None else 0
    logger.info("bronze_to_silver: %s hour=%02d → %d rows → %s", date_str, hour, row_count, dest)
    return row_count


def run(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Process the previous hour's bronze partition.

    Intended to be called by the Prefect hourly schedule.
    """
    if con is None:
        con = _get_con()

    now = datetime.now(timezone.utc)
    prev = now - timedelta(hours=1)
    return run_hour(prev.year, prev.month, prev.day, prev.hour, con=con)


def bootstrap(
    start: datetime | None = None,
    end: datetime | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Backfill bronze → silver hour by hour between start and end (UTC).

    Defaults: start = 2026-01-01 00:00 UTC, end = current UTC hour.
    Skips hours already present in silver (idempotent).
    """
    if con is None:
        con = _get_con()

    if start is None:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    if end is None:
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    logger.info("bronze_to_silver bootstrap: %s → %s", start.isoformat(), end.isoformat())

    total = 0
    current = start.replace(minute=0, second=0, microsecond=0)
    while current < end:
        total += run_hour(current.year, current.month, current.day, current.hour, con=con)
        current += timedelta(hours=1)

    logger.info("bronze_to_silver bootstrap: done, %d total rows written", total)
    return total
