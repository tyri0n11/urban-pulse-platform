"""Batch job: aggregates silver data into gold-layer analytics tables."""

import logging
from datetime import datetime, timezone

import duckdb

from urbanpulse_core.config import settings
from urbanpulse_infra.duckdb import get_duckdb_connection

logger = logging.getLogger(__name__)

_SILVER_PATH = "s3://urban-pulse/silver/traffic-route-silver/**/*.parquet"
_GOLD_HOURLY_BASE = "s3://urban-pulse/gold/traffic-route-hourly"


def run(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Aggregate silver traffic records into gold hourly summaries.

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
        f"{_GOLD_HOURLY_BASE}"
        f"/year={now.year}/month={now.month:02d}"
        f"/day={now.day:02d}/hour={now.hour:02d}"
        f"/data.parquet"
    )

    try:
        con.execute(f"""
            COPY (
                SELECT
                    route_id,
                    origin,
                    destination,
                    DATE_TRUNC('hour', timestamp_utc)                              AS hour_utc,
                    COUNT(*)                                                        AS observation_count,
                    AVG(duration_minutes)                                           AS avg_duration_minutes,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_minutes) AS p95_duration_minutes,
                    AVG(congestion_heavy_ratio)                                     AS avg_heavy_ratio,
                    AVG(congestion_moderate_ratio)                                  AS avg_moderate_ratio,
                    MAX(congestion_severe_segments)                                 AS max_severe_segments
                FROM read_parquet('{_SILVER_PATH}', hive_partitioning := true)
                WHERE timestamp_utc IS NOT NULL
                GROUP BY route_id, origin, destination, DATE_TRUNC('hour', timestamp_utc)
            ) TO '{output_path}' (FORMAT PARQUET)
        """)
    except duckdb.IOException as exc:
        msg = str(exc).lower()
        if "no files found" in msg or "no such file" in msg:
            logger.info("silver_to_gold: no silver files yet — skipping")
            return 0
        raise

    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
    row_count = int(result[0]) if result is not None else 0
    logger.info("silver_to_gold: wrote %d rows → %s", row_count, output_path)
    return row_count
