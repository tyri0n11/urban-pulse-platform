"""DB access for online_route_features table."""

from typing import Any

import asyncpg

from serving.utils.serializers import row_to_dict

_FEATURES_LATEST_SQL = """
    SELECT DISTINCT ON (route_id)
        route_id, window_start, updated_at, observation_count,
        mean_duration_minutes, stddev_duration_minutes, last_duration_minutes,
        mean_heavy_ratio, last_heavy_ratio,
        COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
        COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
        duration_zscore, is_anomaly, last_ingest_lag_ms
    FROM online_route_features
    ORDER BY route_id, updated_at DESC
"""

_FEATURE_HISTORY_SQL = """
    SELECT DISTINCT ON (window_start)
        route_id, window_start, updated_at, observation_count,
        mean_duration_minutes, stddev_duration_minutes, last_duration_minutes,
        mean_heavy_ratio,
        COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
        COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
        duration_zscore, is_anomaly, last_ingest_lag_ms
    FROM online_route_features
    WHERE route_id = $1
      AND window_start >= NOW() - ($2 * INTERVAL '1 hour')
    ORDER BY window_start DESC, updated_at DESC
"""

_ROUTES_SNAPSHOT_SQL = """
    SELECT DISTINCT ON (route_id)
        route_id, window_start, updated_at, observation_count,
        mean_duration_minutes, stddev_duration_minutes,
        duration_zscore, is_anomaly, last_ingest_lag_ms
    FROM online_route_features
    ORDER BY route_id, updated_at DESC
"""

_LAG_SQL = """
    SELECT
        COUNT(*)                                        AS active_routes,
        PERCENTILE_CONT(0.50) WITHIN GROUP
            (ORDER BY last_ingest_lag_ms)               AS p50_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP
            (ORDER BY last_ingest_lag_ms)               AS p95_ms,
        MAX(last_ingest_lag_ms)                         AS max_ms,
        AVG(last_ingest_lag_ms)                         AS mean_ms
    FROM (
        SELECT DISTINCT ON (route_id) last_ingest_lag_ms
        FROM online_route_features
        WHERE updated_at > NOW() - INTERVAL '10 minutes'
          AND last_ingest_lag_ms IS NOT NULL
        ORDER BY route_id, updated_at DESC
    ) t
"""

_RECONCILE_SQL = """
    SELECT
        o.route_id, o.window_start, o.updated_at, o.observation_count,
        o.mean_duration_minutes     AS online_mean,
        o.stddev_duration_minutes   AS online_stddev,
        o.mean_heavy_ratio,
        o.heavy_ratio_deviation,
        o.duration_zscore,
        o.is_anomaly,
        o.last_ingest_lag_ms
    FROM (
        SELECT DISTINCT ON (route_id) *
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
    ) o
    ORDER BY o.route_id
"""

_SNAPSHOT_FOR_CHAT_SQL = """
    SELECT DISTINCT ON (route_id)
        route_id, is_anomaly,
        mean_heavy_ratio,
        COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
        COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
        max_severe_segments,
        observation_count, last_ingest_lag_ms, updated_at
    FROM online_route_features
    ORDER BY route_id, updated_at DESC
"""

_EXPLAIN_ROUTE_SQL = """
    SELECT DISTINCT ON (route_id)
        route_id, is_anomaly,
        mean_heavy_ratio,
        COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
        COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
        max_severe_segments,
        observation_count, updated_at
    FROM online_route_features
    WHERE route_id = $1
    ORDER BY route_id, updated_at DESC
"""


async def fetch_all_features(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(_FEATURES_LATEST_SQL)
    return [row_to_dict(r) for r in rows]


async def fetch_route_feature(conn: asyncpg.Connection, route_id: str) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT
            route_id, window_start, updated_at, observation_count,
            mean_duration_minutes, stddev_duration_minutes, last_duration_minutes,
            mean_heavy_ratio, last_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
            duration_zscore, is_anomaly, last_ingest_lag_ms
        FROM online_route_features
        WHERE route_id = $1
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        route_id,
    )
    return row_to_dict(row) if row else None


async def fetch_feature_history(
    conn: asyncpg.Connection, route_id: str, hours: int
) -> list[dict[str, Any]]:
    rows = await conn.fetch(_FEATURE_HISTORY_SQL, route_id, hours)
    return [row_to_dict(r) for r in rows]


async def fetch_routes_snapshot(conn: asyncpg.Connection) -> dict[str, dict[str, Any]]:
    """Return latest features keyed by route_id (for SSE and /online/routes)."""
    rows = await conn.fetch(_ROUTES_SNAPSHOT_SQL)
    return {r["route_id"]: row_to_dict(r) for r in rows}


async def fetch_lag(conn: asyncpg.Connection) -> dict[str, Any]:
    row = await conn.fetchrow(_LAG_SQL)
    if row is None or row["active_routes"] == 0:
        return {"active_routes": 0, "p50_ms": None, "p95_ms": None, "max_ms": None, "mean_ms": None}
    return {
        "active_routes": row["active_routes"],
        "p50_ms": float(row["p50_ms"]) if row["p50_ms"] else None,
        "p95_ms": float(row["p95_ms"]) if row["p95_ms"] else None,
        "max_ms": float(row["max_ms"]) if row["max_ms"] else None,
        "mean_ms": float(row["mean_ms"]) if row["mean_ms"] else None,
        "slo_met": (float(row["p95_ms"]) < 20_000) if row["p95_ms"] else None,
    }


async def fetch_reconcile(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(_RECONCILE_SQL)
    return [row_to_dict(r) for r in rows]


async def fetch_for_chat_snapshot(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(_SNAPSHOT_FOR_CHAT_SQL)
    return [row_to_dict(r) for r in rows]


async def fetch_for_explain(conn: asyncpg.Connection, route_id: str) -> dict[str, Any] | None:
    row = await conn.fetchrow(_EXPLAIN_ROUTE_SQL, route_id)
    return row_to_dict(row) if row else None


async def fetch_latest_for_scoring(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Fetch latest features for all routes — used by prediction endpoints."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id, window_start, observation_count,
            mean_duration_minutes, last_duration_minutes,
            mean_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0)   AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)        AS mean_low_ratio,
            COALESCE(max_severe_segments, 0.0)   AS max_severe_segments,
            duration_zscore, is_anomaly
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
        """
    )
    return [row_to_dict(r) for r in rows]


async def fetch_route_for_scoring(
    conn: asyncpg.Connection, route_id: str
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT
            route_id, window_start, observation_count,
            mean_duration_minutes, last_duration_minutes,
            mean_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0)   AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)        AS mean_low_ratio,
            COALESCE(max_severe_segments, 0.0)   AS max_severe_segments,
            duration_zscore, is_anomaly
        FROM online_route_features
        WHERE route_id = $1
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        route_id,
    )
    return row_to_dict(row) if row else None


async def fetch_latest_for_anomaly_check(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Fetch latest features with fields needed for anomaly current endpoint."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id, window_start, updated_at, observation_count,
            mean_duration_minutes, stddev_duration_minutes, last_duration_minutes,
            mean_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0)   AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)        AS mean_low_ratio,
            COALESCE(max_severe_segments, 0.0)   AS max_severe_segments,
            duration_zscore, is_anomaly, last_ingest_lag_ms
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
        """
    )
    return [row_to_dict(r) for r in rows]
