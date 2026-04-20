"""Router: ML model prediction endpoints."""

from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from serving.controllers.predict_controller import predict_all, predict_route
from serving.dependencies import get_db
from serving.repo import predictions as predictions_repo
from serving.services import prediction_service

router = APIRouter(prefix="/predict", tags=["predictions"])


@router.get("/anomalies")
async def predict_all_routes(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await predict_all(conn)


@router.get("/anomalies/{route_id}")
async def predict_single_route(
    route_id: str,
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    result = await predict_route(conn, route_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found")
    return result


@router.get("/model/info")
async def model_info() -> dict[str, Any]:
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
    return prediction_service.route_cache_status()


@router.get("/history")
async def prediction_history(
    hours: int = Query(default=24, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await predictions_repo.fetch_history_aggregated(conn, hours)


@router.get("/history/{route_id}")
async def route_prediction_history(
    route_id: str,
    hours: int = Query(default=24, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    rows = await predictions_repo.fetch_route_ticks(conn, route_id, hours)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No prediction history for route '{route_id}'")
    return rows
