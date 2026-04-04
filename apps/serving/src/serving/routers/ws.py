"""SSE (Server-Sent Events) router — streams live traffic snapshots.

Endpoint: GET /events/traffic
Media type: text/event-stream

Each connected client gets its own polling loop that queries Postgres every
POLL_INTERVAL seconds and pushes a single "data: {...}" event.  The browser's
EventSource API handles reconnection automatically.

Message format (JSON):
    { "type": "traffic_update", "routes": [...], "anomalies": [...], "lag": {...}, "ts": "..." }
"""

import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncGenerator

import asyncpg
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from serving.services import prediction_service

router = APIRouter(tags=["events"])
logger = logging.getLogger(__name__)

POLL_INTERVAL = 15  # seconds

# Cache routes.json so we don't re-read it on every tick
_routes_coords: dict[str, dict[str, Any]] | None = None


def _default(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not JSON serializable: {type(obj)!r}")


def _dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=_default)


def _load_routes_coords() -> dict[str, dict[str, Any]]:
    global _routes_coords
    if _routes_coords is None:
        import json as _j
        routes_path = os.environ.get("ROUTES_JSON_PATH", "/app/routes.json")
        coords: dict[str, dict[str, Any]] = {}
        try:
            with open(routes_path) as f:
                for r in _j.load(f):
                    coords[r["route_id"]] = {
                        "origin": r.get("origin"),
                        "destination": r.get("destination"),
                        "origin_anchor": r.get("origin_anchor"),
                        "destination_anchor": r.get("destination_anchor"),
                    }
        except FileNotFoundError:
            pass
        _routes_coords = coords
    return _routes_coords


async def fetch_snapshot(pool: asyncpg.Pool) -> dict[str, Any]:
    """Fetch routes, current anomalies, and lag in one connection."""
    route_coords = _load_routes_coords()

    async with pool.acquire() as conn:
        route_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (route_id)
                route_id, window_start, updated_at, observation_count,
                mean_duration_minutes, stddev_duration_minutes,
                duration_zscore, is_anomaly, last_ingest_lag_ms
            FROM online_route_features
            ORDER BY route_id, updated_at DESC
            """
        )
        online_by_id: dict[str, dict[str, Any]] = {r["route_id"]: dict(r) for r in route_rows}

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

        anomaly_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (route_id)
                route_id, window_start, updated_at, observation_count,
                mean_duration_minutes, stddev_duration_minutes, last_duration_minutes,
                mean_heavy_ratio,
                COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
                COALESCE(max_severe_segments, 0.0) AS max_severe_segments,
                duration_zscore, is_anomaly, last_ingest_lag_ms
            FROM online_route_features
            ORDER BY route_id, updated_at DESC
            """
        )
        row_dicts = [dict(r) for r in anomaly_rows]

        # Run IsolationForest scoring (cached in-process, cheap after first load)
        try:
            predictions = prediction_service.score_rows(row_dicts)
            pred_by_route = {p.route_id: p for p in predictions}
        except Exception:
            pred_by_route = {}

        anomalies: list[dict[str, Any]] = []
        iforest_upsert_rows: list[tuple[Any, ...]] = []
        for row in row_dicts:
            rid = row["route_id"]
            pred = pred_by_route.get(rid)
            is_zscore = bool(row.get("is_anomaly", False))
            is_iforest = bool(pred.iforest_anomaly) if pred else False
            if pred:
                iforest_upsert_rows.append((
                    rid,
                    row["window_start"],
                    pred.iforest_anomaly,
                    pred.iforest_score,
                    pred.both_anomaly,
                ))
            if not (is_zscore or is_iforest):
                continue
            entry = dict(row)
            if pred:
                entry["iforest_anomaly"] = pred.iforest_anomaly
                entry["iforest_score"] = pred.iforest_score
                entry["both_anomaly"] = pred.both_anomaly
            anomalies.append(entry)

        if iforest_upsert_rows:
            await conn.executemany(
                """
                INSERT INTO route_iforest_scores
                    (route_id, window_start, scored_at, iforest_anomaly, iforest_score, both_anomaly,
                     score_count, anomaly_count, both_count)
                VALUES ($1, $2, NOW(), $3, $4, $5,
                        1,
                        CASE WHEN $3 THEN 1 ELSE 0 END,
                        CASE WHEN $5 THEN 1 ELSE 0 END)
                ON CONFLICT (route_id, window_start) DO UPDATE SET
                    scored_at       = NOW(),
                    iforest_anomaly = EXCLUDED.iforest_anomaly,
                    iforest_score   = EXCLUDED.iforest_score,
                    both_anomaly    = EXCLUDED.both_anomaly,
                    score_count     = route_iforest_scores.score_count + 1,
                    anomaly_count   = route_iforest_scores.anomaly_count
                                      + CASE WHEN EXCLUDED.iforest_anomaly THEN 1 ELSE 0 END,
                    both_count      = route_iforest_scores.both_count
                                      + CASE WHEN EXCLUDED.both_anomaly THEN 1 ELSE 0 END
                """,
                iforest_upsert_rows,
            )

        lag_row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)                                              AS active_routes,
                PERCENTILE_CONT(0.50) WITHIN GROUP
                    (ORDER BY last_ingest_lag_ms)                    AS p50_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP
                    (ORDER BY last_ingest_lag_ms)                    AS p95_ms,
                MAX(last_ingest_lag_ms)                              AS max_ms,
                AVG(last_ingest_lag_ms)                              AS mean_ms
            FROM (
                SELECT DISTINCT ON (route_id) last_ingest_lag_ms
                FROM online_route_features
                WHERE updated_at > NOW() - INTERVAL '10 minutes'
                  AND last_ingest_lag_ms IS NOT NULL
                ORDER BY route_id, updated_at DESC
            ) t
            """
        )

    if lag_row and lag_row["active_routes"] > 0:
        lag: dict[str, Any] = {
            "active_routes": int(lag_row["active_routes"]),
            "p50_ms": float(lag_row["p50_ms"]) if lag_row["p50_ms"] is not None else None,
            "p95_ms": float(lag_row["p95_ms"]) if lag_row["p95_ms"] is not None else None,
            "max_ms": float(lag_row["max_ms"]) if lag_row["max_ms"] is not None else None,
            "mean_ms": float(lag_row["mean_ms"]) if lag_row["mean_ms"] is not None else None,
            "slo_met": (float(lag_row["p95_ms"]) < 20_000) if lag_row["p95_ms"] is not None else None,
        }
    else:
        lag = {"active_routes": 0, "p50_ms": None, "p95_ms": None, "max_ms": None, "mean_ms": None}

    return {
        "type": "traffic_update",
        "routes": routes,
        "anomalies": anomalies,
        "lag": lag,
        "ts": datetime.utcnow().isoformat() + "Z",
    }


async def _event_stream(request: Request) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted events until client disconnects."""
    pool: asyncpg.Pool = request.app.state.pg_pool

    # Send initial snapshot immediately on connect
    try:
        snapshot = await fetch_snapshot(pool)
        yield f"data: {_dumps(snapshot)}\n\n"
    except Exception as exc:
        logger.error("sse: initial snapshot error: %s", exc)

    import asyncio
    while True:
        # Sleep in small increments so we can detect disconnects quickly
        for _ in range(POLL_INTERVAL * 2):  # check every 0.5s
            await asyncio.sleep(0.5)
            if await request.is_disconnected():
                logger.debug("sse: client disconnected")
                return

        try:
            snapshot = await fetch_snapshot(pool)
            yield f"data: {_dumps(snapshot)}\n\n"
        except Exception as exc:
            logger.error("sse: snapshot error: %s", exc)
            # yield a keepalive comment so the connection stays open
            yield ": error\n\n"


@router.get("/events/traffic")
async def sse_traffic(request: Request) -> StreamingResponse:
    """Stream live traffic updates as Server-Sent Events."""
    return StreamingResponse(
        _event_stream(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
