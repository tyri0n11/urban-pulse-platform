"""Router: anomaly query endpoints.

Endpoints
---------
GET /anomalies/current
    All routes currently flagged as anomalous (latest window).

GET /anomalies/history
    Anomaly events over the last N hours across all routes.

GET /anomalies/summary
    Aggregate counts: zscore anomalies, iforest anomalies, both-agree,
    grouped by hour — for timeline chart.

GET /anomalies/{route_id}
    Full anomaly history for a single route.
"""

from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from serving.dependencies import get_db
from serving.services import prediction_service

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    return dict(row)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/current")
async def current_anomalies(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return all routes where is_anomaly = true in their latest window.

    Also runs IsolationForest scoring so the response includes both signals.
    """
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
            COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
            duration_zscore,
            is_anomaly,
            last_ingest_lag_ms
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
        """
    )
    if not rows:
        return []

    row_dicts = [dict(r) for r in rows]

    # Score all with IsolationForest — filter to anomalous ones
    try:
        predictions = prediction_service.score_rows(row_dicts)
        pred_by_route = {p.route_id: p for p in predictions}
    except Exception:
        pred_by_route = {}

    result = []
    for row in row_dicts:
        rid = row["route_id"]
        pred = pred_by_route.get(rid)
        is_zscore_anomaly = bool(row.get("is_anomaly", False))
        is_iforest_anomaly = pred.iforest_anomaly if pred else None

        if not (is_zscore_anomaly or is_iforest_anomaly):
            continue

        entry = {**row}
        if pred:
            entry["iforest_anomaly"] = pred.iforest_anomaly
            entry["iforest_score"] = pred.iforest_score
            entry["both_anomaly"] = pred.both_anomaly
        result.append(entry)

    # Sort: both-agree first, then zscore-only, then iforest-only
    result.sort(key=lambda x: (
        not x.get("both_anomaly", False),
        not x.get("is_anomaly", False),
        abs(x.get("duration_zscore") or 0),
    ), reverse=False)

    return result


@router.get("/history")
async def anomaly_history(
    hours: int = Query(default=24, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return all anomalous windows across all routes in the last N hours.

    One row per (route_id, window_start) where is_anomaly was true at any point.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id, window_start)
            route_id,
            window_start,
            updated_at,
            observation_count,
            mean_duration_minutes,
            duration_zscore,
            is_anomaly,
            last_ingest_lag_ms
        FROM online_route_features
        WHERE is_anomaly = true
          AND window_start >= NOW() - ($1 * INTERVAL '1 hour')
        ORDER BY route_id, window_start DESC, updated_at DESC
        """,
        hours,
    )
    return [_row_to_dict(r) for r in rows]


@router.get("/summary")
async def anomaly_summary(
    hours: int = Query(default=24, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Anomaly counts grouped by hour for the last N hours.

    Returns time-series data suitable for a chart:
      hour | zscore_anomaly_count | iforest_anomaly_count | both_anomaly_count | total_routes

    Note: iforest_anomaly_count and both_anomaly_count are estimated for the current
    hour only (IForest results are not stored historically). Past hours show zscore only.
    """
    rows = await conn.fetch(
        """
        SELECT
            DATE_TRUNC('hour', window_start) AS hour,
            COUNT(*) FILTER (WHERE is_anomaly = true)  AS zscore_anomaly_count,
            COUNT(DISTINCT route_id)                   AS total_routes
        FROM (
            SELECT DISTINCT ON (route_id, window_start)
                route_id, window_start, is_anomaly
            FROM online_route_features
            WHERE window_start >= NOW() - ($1 * INTERVAL '1 hour')
            ORDER BY route_id, window_start, updated_at DESC
        ) t
        GROUP BY 1
        ORDER BY 1
        """,
        hours,
    )
    result = [_row_to_dict(r) for r in rows]

    # Augment current hour with live IForest counts
    try:
        latest_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (route_id)
                route_id, window_start, updated_at, observation_count,
                mean_duration_minutes, stddev_duration_minutes, last_duration_minutes,
                mean_heavy_ratio, duration_zscore, is_anomaly, last_ingest_lag_ms
            FROM online_route_features
            ORDER BY route_id, updated_at DESC
            """
        )
        row_dicts = [dict(r) for r in latest_rows]
        predictions = prediction_service.score_rows(row_dicts)
        pred_by_route = {p.route_id: p for p in predictions}

        from datetime import datetime, timezone
        current_hour = datetime.now(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        ).isoformat()

        # Total IForest anomalies (includes BOTH); frontend subtracts both_count to get IForest-only
        iforest_count = sum(1 for p in predictions if p.iforest_anomaly)
        both_count = sum(1 for p in predictions if p.both_anomaly)

        # Find or append current-hour entry
        for entry in result:
            entry_hour = entry["hour"]
            if hasattr(entry_hour, "isoformat"):
                entry_hour_str = entry_hour.isoformat()
            else:
                entry_hour_str = str(entry_hour)
            if entry_hour_str[:13] == current_hour[:13]:
                entry["iforest_anomaly_count"] = iforest_count
                entry["both_anomaly_count"] = both_count
                break
    except Exception:
        pass

    return result


@router.get("/{route_id}")
async def route_anomaly_history(
    route_id: str,
    hours: int = Query(default=48, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return anomaly history for a single route over the last N hours.

    Includes all windows (not just anomalous ones) so the UI can render
    a full timeline with anomaly markers.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (window_start)
            route_id,
            window_start,
            updated_at,
            observation_count,
            mean_duration_minutes,
            stddev_duration_minutes,
            duration_zscore,
            is_anomaly,
            last_ingest_lag_ms
        FROM online_route_features
        WHERE route_id = $1
          AND window_start >= NOW() - ($2 * INTERVAL '1 hour')
        ORDER BY window_start DESC, updated_at DESC
        """,
        route_id,
        hours,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for route '{route_id}'")
    return [_row_to_dict(r) for r in rows]
