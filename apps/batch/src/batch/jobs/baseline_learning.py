"""Batch job: computes traffic baselines for anomaly detection."""

import logging
from datetime import datetime, timezone

import duckdb

from urbanpulse_core.config import settings
from urbanpulse_infra.duckdb import get_duckdb_connection

logger = logging.getLogger(__name__)

_GOLD_HOURLY_PATH = "s3://urban-pulse/gold/traffic-route-hourly/**/*.parquet"
_GOLD_BASELINE_BASE = "s3://urban-pulse/gold/traffic-route-baseline"


def run(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Compute per-route, per-day-of-week/hour-of-day baselines for anomaly detection.

    Returns the number of rows written.
    """
    if con is None:
        con = get_duckdb_connection(
            minio_endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
        )

    now = datetime.now(timezone.utc)
    output_path = (
        f"{_GOLD_BASELINE_BASE}"
        f"/year={now.year}/month={now.month:02d}"
        f"/day={now.day:02d}"
        f"/data.parquet"
    )

    try:
        con.execute(f"""
            COPY (
                SELECT
                    route_id,
                    EXTRACT(DOW  FROM hour_utc) AS day_of_week,
                    EXTRACT(HOUR FROM hour_utc) AS hour_of_day,
                    AVG(avg_duration_minutes)    AS baseline_duration_mean,
                    STDDEV(avg_duration_minutes) AS baseline_duration_stddev,
                    AVG(avg_heavy_ratio)         AS baseline_heavy_ratio_mean,
                    COUNT(*)                     AS sample_count
                FROM read_parquet('{_GOLD_HOURLY_PATH}', hive_partitioning := true)
                GROUP BY route_id, day_of_week, hour_of_day
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
    except duckdb.IOException as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "no such file" in msg:
            logger.info("baseline_learning: no gold files yet — skipping")
            return 0
        raise

    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
    row_count = int(result[0]) if result is not None else 0
    logger.info("baseline_learning: wrote %d rows → %s", row_count, output_path)
    return row_count
