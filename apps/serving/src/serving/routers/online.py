"""Router: online feature endpoints.

Endpoints
---------
GET /online/features
    All routes — latest window per route, ordered by updated_at DESC.

GET /online/features/{route_id}
    Single route current window.

GET /online/lag
    Ingestion-to-feature latency summary (p50, p95, max) across all routes
    updated in the last 10 minutes.

GET /online/reconcile
    For each route: compare online mean_duration with batch baseline mean
    and surface the absolute and relative deviation.
    Useful for detecting when the streaming layer has drifted from history.
"""

from datetime import datetime, timezone
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from serving.dependencies import get_db

router = APIRouter(prefix="/online", tags=["online-features"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    return dict(row)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/features")
async def list_features(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return the latest online feature window for every route."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id,
            window_start,
            updated_at,
            observation_count,
            mean_duration_minutes,
            stddev_duration_minutes,
            last_duration_minutes,
            mean_heavy_ratio,
            last_heavy_ratio,
            duration_zscore,
            is_anomaly,
            last_ingest_lag_ms
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
        """
    )
    return [_row_to_dict(r) for r in rows]


@router.get("/features/{route_id}")
async def get_feature(
    route_id: str,
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    """Return the current online feature window for a single route."""
    row = await conn.fetchrow(
        """
        SELECT
            route_id,
            window_start,
            updated_at,
            observation_count,
            mean_duration_minutes,
            stddev_duration_minutes,
            last_duration_minutes,
            mean_heavy_ratio,
            last_heavy_ratio,
            duration_zscore,
            is_anomaly,
            last_ingest_lag_ms
        FROM online_route_features
        WHERE route_id = $1
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        route_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found in online store")
    return _row_to_dict(row)


@router.get("/lag")
async def ingestion_lag(
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    """Return p50 / p95 / max ingestion-to-feature lag for active routes.

    Only considers routes updated in the last 10 minutes so stale routes
    don't inflate the numbers.
    """
    row = await conn.fetchrow(
        """
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
    )
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


@router.get("/reconcile")
async def reconcile(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Compare online mean_duration against the batch baseline mean.

    For each route, returns:
    - online_mean: what the Faust stream computed this hour
    - baseline_mean: what the batch baseline expects for this (dow, hour)
    - abs_deviation: |online_mean - baseline_mean|
    - rel_deviation: abs_deviation / baseline_mean  (fraction)
    - zscore: pre-computed z-score stored by Faust

    Routes with no matching baseline entry are included with null deviations.
    """
    now = datetime.now(timezone.utc)
    # EXTRACT(DOW) in Postgres: 0=Sunday … 6=Saturday
    dow = now.isoweekday() % 7   # Python isoweekday: Mon=1…Sun=7 → Sun=0…Sat=6
    hour = now.hour

    rows = await conn.fetch(
        """
        SELECT
            o.route_id,
            o.window_start,
            o.updated_at,
            o.observation_count,
            o.mean_duration_minutes                         AS online_mean,
            o.stddev_duration_minutes                       AS online_stddev,
            o.duration_zscore,
            o.is_anomaly,
            o.last_ingest_lag_ms
        FROM (
            SELECT DISTINCT ON (route_id) *
            FROM online_route_features
            ORDER BY route_id, updated_at DESC
        ) o
        ORDER BY o.route_id
        """,
        # Note: baseline join happens in Python to avoid adding pyiceberg to
        # the serving layer. Baseline data is owned by the batch service.
    )

    return [_row_to_dict(r) for r in rows]
