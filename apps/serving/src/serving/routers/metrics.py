"""Router: traffic metrics endpoints.

Endpoints
---------
GET /metrics/routes
    Latest mean_duration + zscore for all routes — lightweight, for map coloring.

GET /metrics/routes/{route_id}
    Per-hour duration trend for a single route (last N hours).

GET /metrics/zones
    Aggregate mean duration per origin zone (latest hour).

GET /metrics/leaderboard
    Top N routes by absolute zscore — "most congested right now".

GET /metrics/heatmap
    duration_zscore for every (route, hour) in last 24h — matrix for heatmap.
"""

from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, Query

from serving.dependencies import get_db

router = APIRouter(prefix="/metrics", tags=["metrics"])


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    return dict(row)


@router.get("/routes")
async def route_metrics(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Lightweight snapshot of all routes — for map color coding.

    Returns route_id, mean_duration_minutes, duration_zscore, is_anomaly,
    observation_count for the current/latest window.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id,
            window_start,
            updated_at,
            mean_duration_minutes,
            last_duration_minutes,
            mean_heavy_ratio,
            duration_zscore,
            is_anomaly,
            observation_count
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
        """
    )
    return [_row_to_dict(r) for r in rows]


@router.get("/routes/{route_id}")
async def route_duration_trend(
    route_id: str,
    hours: int = Query(default=24, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Per-hour mean duration trend for a single route.

    Returns one row per window_start (hour bucket), latest snapshot per window.
    Used for the line chart on the route detail page.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (window_start)
            window_start,
            mean_duration_minutes,
            stddev_duration_minutes,
            last_duration_minutes,
            mean_heavy_ratio,
            duration_zscore,
            observation_count,
            last_ingest_lag_ms
        FROM online_route_features
        WHERE route_id = $1
          AND window_start >= NOW() - ($2 * INTERVAL '1 hour')
        ORDER BY window_start DESC, updated_at DESC
        """,
        route_id,
        hours,
    )
    return [_row_to_dict(r) for r in rows]


@router.get("/zones")
async def zone_metrics(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Aggregate mean duration grouped by origin zone (prefix of route_id).

    Extracts zone from route_id format: zone1_urban_core_to_zone2_...
    Returns per-zone average of mean_duration, max zscore, anomaly count.
    """
    rows = await conn.fetch(
        """
        WITH latest AS (
            SELECT DISTINCT ON (route_id)
                route_id,
                mean_duration_minutes,
                duration_zscore,
                is_anomaly,
                observation_count
            FROM online_route_features
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
    return [_row_to_dict(r) for r in rows]


@router.get("/leaderboard")
async def congestion_leaderboard(
    limit: int = Query(default=10, ge=1, le=50),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Top N routes with highest absolute zscore right now.

    Used for the "Most Congested Routes" widget on the dashboard.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id,
            window_start,
            updated_at,
            mean_duration_minutes,
            last_duration_minutes,
            duration_zscore,
            is_anomaly,
            mean_heavy_ratio,
            observation_count,
            last_ingest_lag_ms
        FROM online_route_features
        WHERE duration_zscore IS NOT NULL
          AND observation_count >= 3
        ORDER BY route_id, updated_at DESC
        """,
    )

    def _clamp_zscore(row: dict[str, Any]) -> dict[str, Any]:
        z = row.get("duration_zscore")
        if z is not None:
            row["duration_zscore"] = max(-30.0, min(30.0, float(z)))
        return row

    sorted_rows = sorted(
        [_clamp_zscore(_row_to_dict(r)) for r in rows],
        key=lambda x: abs(x.get("duration_zscore") or 0),
        reverse=True,
    )
    return sorted_rows[:limit]


@router.get("/heatmap")
async def zscore_heatmap(
    hours: int = Query(default=24, ge=1, le=72),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Z-score matrix: one row per (route_id, hour) for the last N hours.

    Used to render a route × time heatmap where cell color = zscore intensity.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id, window_start)
            route_id,
            window_start,
            duration_zscore,
            is_anomaly,
            observation_count
        FROM online_route_features
        WHERE window_start >= NOW() - ($1 * INTERVAL '1 hour')
        ORDER BY route_id, window_start DESC, updated_at DESC
        """,
        hours,
    )
    return [_row_to_dict(r) for r in rows]
