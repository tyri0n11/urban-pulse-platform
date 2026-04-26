"""DB access for anomaly queries (joins online_route_features + route_iforest_scores)."""

from datetime import datetime
from typing import Any

import asyncpg

from serving.utils.serializers import row_to_dict

_HISTORY_SQL_HOURS = """
    SELECT
        o.route_id, o.window_start, o.updated_at, o.observation_count,
        o.mean_duration_minutes, o.mean_heavy_ratio, o.mean_moderate_ratio,
        o.duration_zscore, o.is_anomaly, o.last_ingest_lag_ms,
        i.iforest_anomaly, i.iforest_score, i.both_anomaly
    FROM (
        SELECT DISTINCT ON (route_id, window_start)
            route_id, window_start, updated_at, observation_count,
            mean_duration_minutes, mean_heavy_ratio, mean_moderate_ratio,
            duration_zscore, is_anomaly, last_ingest_lag_ms
        FROM online_route_features
        WHERE window_start >= NOW() - ($1 * INTERVAL '1 hour')
        ORDER BY route_id, window_start, updated_at DESC
    ) o
    LEFT JOIN (
        SELECT route_id, window_start, iforest_score,
            CASE WHEN score_count > 0
                 THEN anomaly_count::float / score_count >= 0.5
                 ELSE iforest_anomaly END AS iforest_anomaly,
            CASE WHEN score_count > 0
                 THEN both_count::float / score_count >= 0.5
                 ELSE both_anomaly END    AS both_anomaly
        FROM route_iforest_scores
        WHERE window_start >= NOW() - ($1 * INTERVAL '1 hour')
          AND iforest_score IS NOT NULL
    ) i ON o.route_id = i.route_id AND o.window_start = i.window_start
    WHERE o.is_anomaly = true OR i.iforest_anomaly = true
    ORDER BY o.window_start DESC, o.route_id
"""

_HISTORY_SQL_RANGE = """
    SELECT
        o.route_id, o.window_start, o.updated_at, o.observation_count,
        o.mean_duration_minutes, o.mean_heavy_ratio, o.mean_moderate_ratio,
        o.duration_zscore, o.is_anomaly, o.last_ingest_lag_ms,
        i.iforest_anomaly, i.iforest_score, i.both_anomaly
    FROM (
        SELECT DISTINCT ON (route_id, window_start)
            route_id, window_start, updated_at, observation_count,
            mean_duration_minutes, mean_heavy_ratio, mean_moderate_ratio,
            duration_zscore, is_anomaly, last_ingest_lag_ms
        FROM online_route_features
        WHERE window_start >= $1 AND window_start <= $2
        ORDER BY route_id, window_start, updated_at DESC
    ) o
    LEFT JOIN (
        SELECT route_id, window_start, iforest_score,
            CASE WHEN score_count > 0
                 THEN anomaly_count::float / score_count >= 0.5
                 ELSE iforest_anomaly END AS iforest_anomaly,
            CASE WHEN score_count > 0
                 THEN both_count::float / score_count >= 0.5
                 ELSE both_anomaly END    AS both_anomaly
        FROM route_iforest_scores
        WHERE window_start >= $1 AND window_start <= $2
          AND iforest_score IS NOT NULL
    ) i ON o.route_id = i.route_id AND o.window_start = i.window_start
    WHERE o.is_anomaly = true OR i.iforest_anomaly = true
    ORDER BY o.window_start DESC, o.route_id
"""

_SUMMARY_SQL_HOURS = """
    SELECT
        z.hour, z.zscore_anomaly_count, z.total_routes,
        COALESCE(i.iforest_anomaly_count, 0) AS iforest_anomaly_count,
        COALESCE(i.both_anomaly_count, 0)    AS both_anomaly_count
    FROM (
        SELECT
            DATE_TRUNC('hour', window_start) AS hour,
            COUNT(*) FILTER (WHERE is_anomaly = true) AS zscore_anomaly_count,
            COUNT(DISTINCT route_id)                  AS total_routes
        FROM (
            SELECT DISTINCT ON (route_id, window_start)
                route_id, window_start, is_anomaly
            FROM online_route_features
            WHERE window_start >= NOW() - ($1 * INTERVAL '1 hour')
            ORDER BY route_id, window_start, updated_at DESC
        ) t
        GROUP BY 1
    ) z
    LEFT JOIN (
        SELECT
            DATE_TRUNC('hour', window_start) AS hour,
            COUNT(*) FILTER (
                WHERE score_count > 0 AND anomaly_count::float / score_count >= 0.5
            ) AS iforest_anomaly_count,
            COUNT(*) FILTER (
                WHERE score_count > 0 AND both_count::float / score_count >= 0.5
            ) AS both_anomaly_count
        FROM route_iforest_scores
        WHERE window_start >= NOW() - ($1 * INTERVAL '1 hour')
          AND iforest_score IS NOT NULL
        GROUP BY 1
    ) i ON z.hour = i.hour
    ORDER BY 1
"""

_SUMMARY_SQL_RANGE = """
    SELECT
        z.hour, z.zscore_anomaly_count, z.total_routes,
        COALESCE(i.iforest_anomaly_count, 0) AS iforest_anomaly_count,
        COALESCE(i.both_anomaly_count, 0)    AS both_anomaly_count
    FROM (
        SELECT
            DATE_TRUNC('hour', window_start) AS hour,
            COUNT(*) FILTER (WHERE is_anomaly = true) AS zscore_anomaly_count,
            COUNT(DISTINCT route_id)                  AS total_routes
        FROM (
            SELECT DISTINCT ON (route_id, window_start)
                route_id, window_start, is_anomaly
            FROM online_route_features
            WHERE window_start >= $1 AND window_start <= $2
            ORDER BY route_id, window_start, updated_at DESC
        ) t
        GROUP BY 1
    ) z
    LEFT JOIN (
        SELECT
            DATE_TRUNC('hour', window_start) AS hour,
            COUNT(*) FILTER (
                WHERE score_count > 0 AND anomaly_count::float / score_count >= 0.5
            ) AS iforest_anomaly_count,
            COUNT(*) FILTER (
                WHERE score_count > 0 AND both_count::float / score_count >= 0.5
            ) AS both_anomaly_count
        FROM route_iforest_scores
        WHERE window_start >= $1 AND window_start <= $2
          AND iforest_score IS NOT NULL
        GROUP BY 1
    ) i ON z.hour = i.hour
    ORDER BY 1
"""


async def fetch_history_hours(conn: asyncpg.Connection, hours: int) -> list[dict[str, Any]]:
    rows = await conn.fetch(_HISTORY_SQL_HOURS, hours)
    return [row_to_dict(r) for r in rows]


async def fetch_history_range(
    conn: asyncpg.Connection, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_HISTORY_SQL_RANGE, start, end)
    return [row_to_dict(r) for r in rows]


async def fetch_summary_hours(conn: asyncpg.Connection, hours: int) -> list[dict[str, Any]]:
    rows = await conn.fetch(_SUMMARY_SQL_HOURS, hours)
    return [row_to_dict(r) for r in rows]


async def fetch_summary_range(
    conn: asyncpg.Connection, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_SUMMARY_SQL_RANGE, start, end)
    return [row_to_dict(r) for r in rows]


async def fetch_route_history(
    conn: asyncpg.Connection, route_id: str, hours: int
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (window_start)
            route_id, window_start, updated_at, observation_count,
            mean_duration_minutes, stddev_duration_minutes,
            duration_zscore, is_anomaly, last_ingest_lag_ms
        FROM online_route_features
        WHERE route_id = $1
          AND window_start >= NOW() - ($2 * INTERVAL '1 hour')
        ORDER BY window_start DESC, updated_at DESC
        """,
        route_id,
        hours,
    )
    return [row_to_dict(r) for r in rows]


async def fetch_sse_anomalies(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Anomalies for SSE snapshot — joins pre-computed iforest scores."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (orf.route_id)
            orf.route_id, orf.window_start, orf.updated_at, orf.observation_count,
            orf.mean_duration_minutes, orf.stddev_duration_minutes, orf.last_duration_minutes,
            orf.mean_heavy_ratio,
            COALESCE(orf.mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
            COALESCE(orf.max_severe_segments, 0.0) AS max_severe_segments,
            orf.duration_zscore, orf.is_anomaly, orf.last_ingest_lag_ms,
            rif.iforest_anomaly, rif.iforest_score, rif.both_anomaly
        FROM online_route_features orf
        LEFT JOIN LATERAL (
            SELECT iforest_anomaly, iforest_score, both_anomaly
            FROM route_iforest_scores
            WHERE route_id = orf.route_id AND window_start = orf.window_start
              AND iforest_score IS NOT NULL
            LIMIT 1
        ) rif ON true
        ORDER BY orf.route_id, orf.updated_at DESC
        """
    )
    return [
        row_to_dict(r) for r in rows
        if bool(r.get("is_anomaly", False)) or bool(r.get("iforest_anomaly", False))
    ]
