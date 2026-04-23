"""SSE router — streams live traffic snapshots every 10 seconds.

Endpoint: GET /events/traffic
Media type: text/event-stream
Message format: { "type": "traffic_update", "routes": [...], "anomalies": [...], "lag": {...}, "ts": "..." }
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, AsyncGenerator

import asyncpg
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from serving.repo import anomalies as anomaly_repo
from serving.repo import online as online_repo
from serving.utils.routes import load_routes_coords
from serving.utils.serializers import dumps

router = APIRouter(tags=["events"])
logger = logging.getLogger(__name__)

POLL_INTERVAL = 10


async def fetch_snapshot(pool: asyncpg.Pool) -> dict[str, Any]:
    """Fetch routes, current anomalies, and lag in one connection."""
    route_coords = load_routes_coords()

    async with pool.acquire() as conn:
        online_by_id = await online_repo.fetch_routes_snapshot(conn)

        routes: list[dict[str, Any]] = []
        seen: set[str] = set()
        for rid, coords in route_coords.items():
            entry: dict[str, Any] = {"route_id": rid, **coords}
            entry.update(online_by_id.get(rid, {}))
            routes.append(entry)
            seen.add(rid)
        for rid, data in online_by_id.items():
            if rid not in seen:
                routes.append(data)

        anomalies = await anomaly_repo.fetch_sse_anomalies(conn)
        lag = await online_repo.fetch_lag(conn)

    return {
        "type": "traffic_update",
        "routes": routes,
        "anomalies": anomalies,
        "lag": lag,
        "ts": datetime.utcnow().isoformat() + "Z",
    }


async def _event_stream(request: Request) -> AsyncGenerator[str, None]:
    pool: asyncpg.Pool = request.app.state.pg_pool

    try:
        snapshot = await fetch_snapshot(pool)
        yield f"data: {dumps(snapshot)}\n\n"
    except Exception as exc:
        logger.error("sse: initial snapshot error: %s", exc)

    while True:
        for _ in range(POLL_INTERVAL * 2):
            await asyncio.sleep(0.5)
            if await request.is_disconnected():
                logger.debug("sse: client disconnected")
                return

        try:
            snapshot = await fetch_snapshot(pool)
            yield f"data: {dumps(snapshot)}\n\n"
        except Exception as exc:
            logger.error("sse: snapshot error: %s", exc)
            yield ": error\n\n"


@router.get("/events/traffic")
async def sse_traffic(request: Request) -> StreamingResponse:
    return StreamingResponse(
        _event_stream(request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )
