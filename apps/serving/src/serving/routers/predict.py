"""Router: ML model prediction endpoints.

Endpoints
---------
GET /predict/anomalies
    Score all routes (latest online window) with the IsolationForest model.
    Returns iforest_anomaly, zscore_anomaly, and both_anomaly per route.

GET /predict/anomalies/{route_id}
    Score a single route.

GET /predict/model/info
    Return metadata about the currently cached model (when it was loaded).
"""

from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from serving.dependencies import get_db
from serving.services import prediction_service

router = APIRouter(prefix="/predict", tags=["predictions"])

_FETCH_LATEST_SQL = """
    SELECT DISTINCT ON (route_id)
        route_id,
        window_start,
        observation_count,
        mean_duration_minutes,
        last_duration_minutes,
        mean_heavy_ratio,
        COALESCE(mean_moderate_ratio, 0.0)   AS mean_moderate_ratio,
        COALESCE(mean_low_ratio, 0.0)        AS mean_low_ratio,
        COALESCE(max_severe_segments, 0.0)   AS max_severe_segments,
        duration_zscore,
        is_anomaly
    FROM online_route_features
    ORDER BY route_id, updated_at DESC
"""

_FETCH_ROUTE_SQL = """
    SELECT
        route_id,
        window_start,
        observation_count,
        mean_duration_minutes,
        last_duration_minutes,
        mean_heavy_ratio,
        COALESCE(mean_moderate_ratio, 0.0)   AS mean_moderate_ratio,
        COALESCE(mean_low_ratio, 0.0)        AS mean_low_ratio,
        COALESCE(max_severe_segments, 0.0)   AS max_severe_segments,
        duration_zscore,
        is_anomaly
    FROM online_route_features
    WHERE route_id = $1
    ORDER BY updated_at DESC
    LIMIT 1
"""


@router.get("/anomalies")
async def predict_all(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Score every route with the IsolationForest model + zscore comparison."""
    rows = await conn.fetch(_FETCH_LATEST_SQL)
    if not rows:
        return []

    row_dicts = [dict(r) for r in rows]
    results = prediction_service.score_rows(row_dicts)

    return [
        {
            "route_id": r.route_id,
            "iforest_score": r.iforest_score,
            "iforest_anomaly": r.iforest_anomaly,
            "zscore_anomaly": r.zscore_anomaly,
            "both_anomaly": r.both_anomaly,
        }
        for r in results
    ]


@router.get("/anomalies/{route_id}")
async def predict_route(
    route_id: str,
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    """Score a single route with the IsolationForest model."""
    row = await conn.fetchrow(_FETCH_ROUTE_SQL, route_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found")

    results = prediction_service.score_rows([dict(row)])
    r = results[0]
    return {
        "route_id": r.route_id,
        "iforest_score": r.iforest_score,
        "iforest_anomaly": r.iforest_anomaly,
        "zscore_anomaly": r.zscore_anomaly,
        "both_anomaly": r.both_anomaly,
    }


@router.get("/model/info")
async def model_info() -> dict[str, Any]:
    """Return aggregate metadata about the currently loaded per-route models."""
    info = prediction_service.cache_info()
    return {
        "loaded": info["loaded_routes"] > 0,
        "model_uri": info["model_uri"],
        "age_seconds": info["age_seconds"],
        "ttl_seconds": info["ttl_seconds"],
        "next_refresh_seconds": info["next_refresh_seconds"],
        "loaded_routes": info["loaded_routes"],
        "total_routes_seen": info["total_routes_seen"],
        "error": None,
    }


@router.get("/model/routes")
async def model_routes_status() -> list[dict[str, Any]]:
    """Return per-route model cache status (loaded, age, error)."""
    return prediction_service.route_cache_status()
