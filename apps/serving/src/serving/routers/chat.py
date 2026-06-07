"""Router: conversational AI assistant about Urban Pulse system state (SSE)."""

import json
import logging
from typing import Any, AsyncGenerator

import asyncpg
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from serving.controllers.chat_controller import fetch_system_snapshot, build_user_prompt
from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt
from serving.repo import interactions as interactions_repo
from serving.schemas.chat import ChatRequest
from serving.services.llm_service import stream_ollama_chat

router = APIRouter(prefix="/chat", tags=["chat"])
logger = logging.getLogger(__name__)

_SYSTEM = build_system_prompt(
    "You are Urban Pulse AI, an intelligent assistant embedded in the Urban Pulse "
    "real-time monitoring dashboard for Ho Chi Minh City metro region. "
    "You receive a live system snapshot that includes: "
    "(1) current congestion metrics and anomaly alerts for all monitored routes, "
    "(2) current weather conditions for HCMC from Open-Meteo. "
    "Congestion is measured by heavy_ratio (% road segments heavily congested), "
    "moderate_ratio (% moderately slow), and severe_segments (worst spots). "
    "When asked about weather, use the weather data in the snapshot to answer directly — "
    "never say weather data is unavailable if it is present in the snapshot. "
    "Answer questions concisely. Reference real road names (Xa lộ Hà Nội, QL13, etc.) "
    "and HCMC district names when relevant. "
    "Do not add greetings or sign-offs. "
    "Keep answers under 4 sentences unless the user explicitly asks for more detail."
)


def _build_chat_figure(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    """Build priority bar chart data from live snapshot. Returns None if no anomalies."""
    anomalies = snapshot.get("anomalies", [])
    top_congested = snapshot.get("top_congested", [])
    if not anomalies and not top_congested:
        return None

    # Merge: anomalies first (both-signal confirmed), then top congested not already listed
    seen: set[str] = set()
    bars: list[dict[str, Any]] = []
    for a in anomalies:
        seen.add(a["route"])
        bars.append({
            "route": a["route"],
            "peak_z": round(a.get("heavy_pct", 0.0), 1),
            "signal": a.get("signal", "zscore"),
            "anomaly": True,
        })
    for r in top_congested:
        if r["route"] not in seen:
            bars.append({
                "route": r["route"],
                "peak_z": round(r.get("heavy_pct", 0.0), 1),
                "signal": "none",
                "anomaly": False,
            })

    bars.sort(key=lambda x: x["peak_z"], reverse=True)
    return {
        "type": "priority_bar",
        "title_vi": "Tình trạng tuyến đường hiện tại",
        "title_en": "Current Route Status",
        "snapshot_time": snapshot.get("snapshot_time", ""),
        "x_unit": "%",
        "bars": bars[:8],
    }


@router.post("")
async def chat(
    req: ChatRequest,
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    snapshot = await fetch_system_snapshot(conn)
    user_prompt = build_user_prompt(snapshot, req.message, req.lang)
    logger.info("chat: lang=%s msg_len=%d anomalies=%d", req.lang, len(req.message), snapshot["anomaly_count"])

    history = [{"role": m.role, "content": m.content} for m in req.history[-20:]]

    log_id = await interactions_repo.log_interaction(
        conn, query_type="chat", query=req.message, lang=req.lang,
    )
    full_text: list[str] = []
    figure = _build_chat_figure(snapshot)

    async def _stream_and_log() -> AsyncGenerator[str, None]:
        if figure:
            yield f"data: {json.dumps({'figure': figure})}\n\n"
        async for event in stream_ollama_chat(
            _SYSTEM, history, user_prompt, temperature=0.4
        ):
            try:
                data = json.loads(event.removeprefix("data: ").strip())
            except Exception:
                yield event
                continue
            if "chunk" in data:
                full_text.append(data["chunk"])
            yield event
        if log_id and full_text:
            await interactions_repo.update_response(conn, log_id, "".join(full_text))

    return StreamingResponse(
        _stream_and_log(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/context")
async def chat_context(
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    return await fetch_system_snapshot(conn)
