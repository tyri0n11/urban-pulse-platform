"""Router: liveness and readiness health check endpoints."""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from serving.dependencies import get_db

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(conn: asyncpg.Connection = Depends(get_db)) -> dict[str, str]:
    try:
        await conn.fetchval("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {exc}") from exc
    return {"status": "ok"}
