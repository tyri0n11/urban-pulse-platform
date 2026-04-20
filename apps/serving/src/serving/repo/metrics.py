"""DB access for traffic metrics endpoints."""

from datetime import datetime
from typing import Any

import asyncpg

from serving.utils.serializers import row_to_dict

_HEATMAP_SQL_HOURS = """
    SELECT
        o.route_id, o.window_start, o.duration_zscore,
        o.is_anomaly, o.observation_count,
        i.iforest_anomaly, i.both_anomaly
    FROM (
        SELECT DISTINCT ON (route_id, window_start)
            route_id, window_start, duration_zscore, is_anomaly, observation_count
        FROM online_route_features
        WHERE route_id LIKE 'zone%'
          AND window_start >= NOW() - ($1 * INTERVAL '1 hour')
        ORDER BY route_id, window_start DESC, updated_at DESC
    ) o
    LEFT JOIN (
        SELECT route_id, window_start,
            CASE WHEN score_count > 0
                 THEN anomaly_count::float / score_count >= 0.5
                 ELSE iforest_anomaly END AS iforest_anomaly,
            CASE WHEN score_count > 0
                 THEN both_count::float / score_count >= 0.5
                 ELSE both_anomaly END    AS both_anomaly
        FROM route_iforest_scores
        WHERE window_start >= NOW() - ($1 * INTERVAL '1 hour')
    ) i ON o.route_id = i.route_id AND o.window_start = i.window_start
    ORDER BY o.route_id, o.window_start DESC
"""

_HEATMAP_SQL_RANGE = """
    SELECT
        o.route_id, o.window_start, o.duration_zscore,
        o.is_anomaly, o.observation_count,
        i.iforest_anomaly, i.both_anomaly
    FROM (
        SELECT DISTINCT ON (route_id, window_start)
            route_id, window_start, duration_zscore, is_anomaly, observation_count
        FROM online_route_features
        WHERE route_id LIKE 'zone%'
          AND window_start >= $1 AND window_start <= $2
        ORDER BY route_id, window_start DESC, updated_at DESC
    ) o
    LEFT JOIN (
        SELECT route_id, window_start,
            CASE WHEN score_count > 0
                 THEN anomaly_count::float / score_count >= 0.5
                 ELSE iforest_anomaly END AS iforest_anomaly,
            CASE WHEN score_count > 0
                 THEN both_count::float / score_count >= 0.5
                 ELSE both_anomaly END    AS both_anomaly
        FROM route_iforest_scores
        WHERE window_start >= $1 AND window_start <= $2
    ) i ON o.route_id = i.route_id AND o.window_start = i.window_start
    ORDER BY o.route_id, o.window_start DESC
"""


async def fetch_route_metrics(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id, window_start, updated_at,
            mean_duration_minutes, last_duration_minutes,
            mean_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
            duration_zscore, is_anomaly, observation_count
        FROM online_route_features
        WHERE route_id LIKE 'zone%'
        ORDER BY route_id, updated_at DESC
        """
    )
    return [row_to_dict(r) for r in rows]


async def fetch_route_trend(
    conn: asyncpg.Connection, route_id: str, hours: int
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (window_start)
            window_start, mean_duration_minutes, stddev_duration_minutes,
            last_duration_minutes, mean_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
            duration_zscore, observation_count, last_ingest_lag_ms
        FROM online_route_features
        WHERE route_id = $1
          AND window_start >= NOW() - ($2 * INTERVAL '1 hour')
        ORDER BY window_start DESC, updated_at DESC
        """,
        route_id,
        hours,
    )
    return [row_to_dict(r) for r in rows]


async def fetch_zone_metrics(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        WITH latest AS (
            SELECT DISTINCT ON (route_id)
                route_id, mean_duration_minutes, duration_zscore,
                is_anomaly, observation_count
            FROM online_route_features
            WHERE route_id LIKE 'zone%'
            ORDER BY route_id, updated_at DESC
        )
        SELECT
            SPLIT_PART(route_id, '_to_', 1)                   AS origin_zone,
            COUNT(*)                                           AS route_count,
            ROUND(AVG(mean_duration_minutes)::numeric, 2)     AS avg_duration_minutes,
            ROUND(MAX(ABS(duration_zscore))::numeric, 3)      AS max_abs_zscore,
            COUNT(*) FILTER (WHERE is_anomaly = true)         AS anomaly_count,
            SUM(observation_count)                            AS total_observations
        FROM latest
        GROUP BY 1
        ORDER BY max_abs_zscore DESC NULLS LAST
        """
    )
    return [row_to_dict(r) for r in rows]


async def fetch_leaderboard_raw(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Fetch all eligible routes for leaderboard — caller handles sorting + clamping."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id, window_start, updated_at,
            mean_duration_minutes, last_duration_minutes,
            duration_zscore, is_anomaly, mean_heavy_ratio,
            observation_count, last_ingest_lag_ms
        FROM online_route_features
        WHERE route_id LIKE 'zone%'
          AND duration_zscore IS NOT NULL
          AND observation_count >= 3
        ORDER BY route_id, updated_at DESC
        """
    )
    return [row_to_dict(r) for r in rows]


async def fetch_heatmap_hours(conn: asyncpg.Connection, hours: int) -> list[dict[str, Any]]:
    rows = await conn.fetch(_HEATMAP_SQL_HOURS, hours)
    return [row_to_dict(r) for r in rows]


async def fetch_heatmap_range(
    conn: asyncpg.Connection, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_HEATMAP_SQL_RANGE, start, end)
    return [row_to_dict(r) for r in rows]
