"""Router: online feature endpoints."""

from typing import Any

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from serving.dependencies import get_db
from serving.repo import online as online_repo
from serving.utils.routes import load_routes_coords

router = APIRouter(prefix="/online", tags=["online-features"])


@router.get("/features")
async def list_features(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await online_repo.fetch_all_features(conn)


@router.get("/features/{route_id}")
async def get_feature(
    route_id: str,
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    row = await online_repo.fetch_route_feature(conn, route_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found in online store")
    return row


@router.get("/features/{route_id}/history")
async def get_feature_history(
    route_id: str,
    hours: int = 24,
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    if hours < 1 or hours > 168:
        raise HTTPException(status_code=422, detail="hours must be between 1 and 168")
    rows = await online_repo.fetch_feature_history(conn, route_id, hours)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history found for route '{route_id}'")
    return rows


@router.get("/routes")
async def list_routes(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    route_coords = load_routes_coords()
    online_by_id = await online_repo.fetch_routes_snapshot(conn)

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for route_id, coords in route_coords.items():
        entry: dict[str, Any] = {"route_id": route_id, **coords}
        entry.update(online_by_id.get(route_id, {}))
        result.append(entry)
        seen.add(route_id)
    for route_id, data in online_by_id.items():
        if route_id not in seen:
            result.append(data)
    return result


@router.get("/lag")
async def ingestion_lag(
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    return await online_repo.fetch_lag(conn)


@router.get("/reconcile")
async def reconcile(
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await online_repo.fetch_reconcile(conn)
