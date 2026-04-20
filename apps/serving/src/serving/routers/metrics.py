"""Router: traffic metrics endpoints."""

from datetime import datetime
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, Depends, Query

from serving.controllers.metrics_controller import get_leaderboard
from serving.dependencies import get_db
from serving.repo import metrics as metrics_repo

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/routes")
async def route_metrics(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await metrics_repo.fetch_route_metrics(conn)


@router.get("/routes/{route_id}")
async def route_duration_trend(
    route_id: str,
    hours: int = Query(default=24, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await metrics_repo.fetch_route_trend(conn, route_id, hours)


@router.get("/zones")
async def zone_metrics(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await metrics_repo.fetch_zone_metrics(conn)


@router.get("/leaderboard")
async def congestion_leaderboard(
    limit: int = Query(default=10, ge=1, le=50),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await get_leaderboard(conn, limit)


@router.get("/heatmap")
async def zscore_heatmap(
    hours: int = Query(default=24, ge=1, le=720),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if start is not None and end is not None:
        return await metrics_repo.fetch_heatmap_range(conn, start, end)
    return await metrics_repo.fetch_heatmap_hours(conn, hours)
