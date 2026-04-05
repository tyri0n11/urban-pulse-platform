"""RAG indexer job — reads gold tables + anomaly history, writes to ChromaDB.

Runs every hour via Prefect. Upsert-based so re-running is always safe.

Two index passes:
  1. anomaly_events   — last 7 days of anomalous windows from PostgreSQL
  2. traffic_patterns — full gold.traffic_hourly aggregated by (route, dow, hour)
                        (refreshed weekly via the retrain flow, not every hour)
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import psycopg2

from rag.client import get_chroma_client
from rag.indexer import index_anomaly_events, index_traffic_patterns

logger = logging.getLogger(__name__)

_PG_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://urbanpulse:urbanpulse@postgres:5432/urbanpulse",
)

_ANOMALY_LOOKBACK_DAYS = 7


def _fetch_anomaly_events() -> list[dict[str, Any]]:
    """Fetch anomaly windows from PostgreSQL for the last N days."""
    since = datetime.now(timezone.utc) - timedelta(days=_ANOMALY_LOOKBACK_DAYS)
    conn = psycopg2.connect(_PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (o.route_id, o.window_start)
                    o.route_id,
                    o.window_start,
                    o.mean_heavy_ratio,
                    COALESCE(o.mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
                    COALESCE(o.max_severe_segments, 0)   AS max_severe_segments,
                    o.duration_zscore,
                    o.is_anomaly,
                    o.observation_count,
                    COALESCE(
                        CASE WHEN i.score_count > 0
                             THEN i.anomaly_count::float / i.score_count >= 0.5
                             ELSE i.iforest_anomaly
                        END,
                        false
                    ) AS iforest_anomaly,
                    COALESCE(
                        CASE WHEN i.score_count > 0
                             THEN i.both_count::float / i.score_count >= 0.5
                             ELSE i.both_anomaly
                        END,
                        false
                    ) AS both_anomaly
                FROM online_route_features o
                LEFT JOIN route_iforest_scores i
                    ON o.route_id = i.route_id
                   AND o.window_start = i.window_start
                WHERE o.window_start >= %s
                  AND (o.is_anomaly = true OR i.iforest_anomaly = true)
                ORDER BY o.route_id, o.window_start, o.updated_at DESC
                """,
                (since,),
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()

    logger.info("rag_indexer: fetched %d anomaly events from Postgres", len(rows))
    return rows


def _fetch_traffic_patterns(catalog: Any) -> list[dict[str, Any]]:
    """Aggregate gold.traffic_hourly by (route_id, dow, hour_of_day) for pattern indexing."""
    from pyiceberg.exceptions import NoSuchTableError
    try:
        gold_table = catalog.load_table("gold.traffic_hourly")
    except NoSuchTableError:
        logger.warning("rag_indexer: gold.traffic_hourly not found — skip pattern indexing")
        return []

    gold_arrow = gold_table.scan().to_arrow()
    if gold_arrow.num_rows == 0:
        return []

    con = duckdb.connect()
    con.register("gold", gold_arrow)
    result = con.execute(
        """
        SELECT
            route_id,
            CAST(EXTRACT(DOW  FROM hour_utc) AS INTEGER) AS dow,
            CAST(EXTRACT(HOUR FROM hour_utc) AS INTEGER) AS hour_of_day,
            AVG(avg_heavy_ratio)                          AS avg_heavy_ratio,
            AVG(COALESCE(avg_moderate_ratio, 0.0))        AS avg_moderate_ratio,
            AVG(avg_duration_minutes)                     AS avg_duration_minutes,
            COUNT(*)                                      AS observation_count
        FROM gold
        WHERE avg_heavy_ratio IS NOT NULL
        GROUP BY route_id, dow, hour_of_day
        ORDER BY route_id, dow, hour_of_day
        """
    ).fetchall()

    cols = ["route_id", "dow", "hour_of_day", "avg_heavy_ratio",
            "avg_moderate_ratio", "avg_duration_minutes", "observation_count"]
    rows = [dict(zip(cols, row)) for row in result]
    logger.info("rag_indexer: fetched %d traffic pattern rows from gold", len(rows))
    return rows


def run(catalog: Any, index_patterns: bool = False) -> dict[str, int]:
    """Main entry point called by Prefect task.

    Args:
        catalog:        PyIceberg catalog (passed from pipeline)
        index_patterns: if True, re-index traffic patterns (expensive full scan).
                        Set True on first run or after gold rebuild.
    """
    client = get_chroma_client()
    counts: dict[str, int] = {}

    # Always index recent anomaly events
    anomaly_rows = _fetch_anomaly_events()
    counts["anomaly_events"] = index_anomaly_events(client, anomaly_rows)

    # Pattern indexing is expensive — only when explicitly requested
    if index_patterns:
        pattern_rows = _fetch_traffic_patterns(catalog)
        counts["traffic_patterns"] = index_traffic_patterns(client, pattern_rows)

    return counts
