"""Router: anomaly query endpoints."""

from datetime import datetime
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from serving.controllers.anomaly_controller import get_current_anomalies
from serving.dependencies import get_db
from serving.repo import anomalies as anomaly_repo

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


@router.get("/current")
async def current_anomalies(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await get_current_anomalies(conn)


@router.get("/history")
async def anomaly_history(
    hours: int = Query(default=24, ge=1, le=720),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if start is not None and end is not None:
        return await anomaly_repo.fetch_history_range(conn, start, end)
    return await anomaly_repo.fetch_history_hours(conn, hours)


@router.get("/summary")
async def anomaly_summary(
    hours: int = Query(default=24, ge=1, le=720),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if start is not None and end is not None:
        return await anomaly_repo.fetch_summary_range(conn, start, end)
    return await anomaly_repo.fetch_summary_hours(conn, hours)


@router.get("/{route_id}")
async def route_anomaly_history(
    route_id: str,
    hours: int = Query(default=48, ge=1, le=168),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    rows = await anomaly_repo.fetch_route_history(conn, route_id, hours)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for route '{route_id}'")
    return rows
