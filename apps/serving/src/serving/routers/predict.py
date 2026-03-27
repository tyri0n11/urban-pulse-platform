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

import time
from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from serving.dependencies import get_db
from serving.services import prediction_service

router = APIRouter(prefix="/predict", tags=["predictions"])

_FETCH_LATEST_SQL = """
    SELECT DISTINCT ON (route_id)
        route_id,
        observation_count,
        mean_duration_minutes,
        last_duration_minutes,
        mean_heavy_ratio,
        duration_zscore,
        is_anomaly
    FROM online_route_features
    ORDER BY route_id, updated_at DESC
"""

_FETCH_ROUTE_SQL = """
    SELECT
        route_id,
        observation_count,
        mean_duration_minutes,
        last_duration_minutes,
        mean_heavy_ratio,
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
    """Return metadata about the currently loaded model."""
    cache = prediction_service._cache
    ttl = prediction_service.MODEL_TTL
    if cache.model is None:
        return {
            "loaded": False,
            "model_uri": None,
            "age_seconds": None,
            "ttl_seconds": ttl,
            "next_refresh_seconds": None,
            "error": cache.last_error,
        }

    age_seconds = round(time.monotonic() - cache.loaded_at, 1)
    return {
        "loaded": True,
        "model_uri": cache.model_uri,
        "age_seconds": age_seconds,
        "ttl_seconds": ttl,
        "next_refresh_seconds": max(0.0, round(ttl - age_seconds, 1)),
        "error": None,
    }
