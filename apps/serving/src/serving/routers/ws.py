"""SSE router — streams live traffic snapshots driven by Postgres NOTIFY.

Endpoint: GET /events/traffic
Media type: text/event-stream
Message format: { "type": "traffic_update", "routes": [...], "anomalies": [...], "lag": {...}, "ts": "..." }

The online service sends NOTIFY route_updated after each upsert. A dedicated
listener connection wakes all active SSE clients; they fetch a full snapshot.
A 30-second heartbeat fires if no notify arrives.
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

_HEARTBEAT_INTERVAL = 30  # seconds — fallback if no NOTIFY arrives

_update_event: asyncio.Event = asyncio.Event()


def _fire_update() -> None:
    global _update_event
    prev, _update_event = _update_event, asyncio.Event()
    prev.set()


async def pg_listener_loop(pool: asyncpg.Pool) -> None:
    """Background task: LISTEN route_updated → wake all SSE clients."""
    while True:
        try:
            async with pool.acquire() as conn:
                await conn.execute("LISTEN route_updated")
                logger.info("sse-listener: LISTEN route_updated active")
                while True:
                    try:
                        await asyncio.wait_for(conn.wait_for_notify(), timeout=_HEARTBEAT_INTERVAL)
                    except asyncio.TimeoutError:
                        pass
                    _fire_update()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("sse-listener: %s; reconnecting in 5s", exc)
            await asyncio.sleep(5)


async def fetch_snapshot(pool: asyncpg.Pool) -> dict[str, Any]:
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
        yield f"data: {dumps(await fetch_snapshot(pool))}\n\n"
    except Exception as exc:
        logger.error("sse: initial snapshot error: %s", exc)

    while True:
        ev = _update_event
        try:
            await asyncio.wait_for(ev.wait(), timeout=_HEARTBEAT_INTERVAL + 5)
        except asyncio.TimeoutError:
            pass

        if await request.is_disconnected():
            logger.debug("sse: client disconnected")
            return

        try:
            yield f"data: {dumps(await fetch_snapshot(pool))}\n\n"
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
