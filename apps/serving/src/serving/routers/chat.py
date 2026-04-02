"""Router: conversational AI assistant about Urban Pulse system state.

Endpoint
--------
POST /chat
    Body: {"message": "...", "lang": "vi"}
    Fetches a live system snapshot from Postgres, injects it as context,
    and streams an LLM response via Ollama SSE.

GET /chat/context
    Returns the raw system snapshot (useful for debugging / FE preloading).
"""

import json
import logging
import os
from typing import Any, AsyncGenerator

import asyncpg
import httpx
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt

router = APIRouter(prefix="/chat", tags=["chat"])
logger = logging.getLogger(__name__)

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

# Built once at import — grounding + geo knowledge + assistant role
_SYSTEM = build_system_prompt(
    "You are Urban Pulse AI, a traffic assistant embedded in the Urban Pulse "
    "real-time monitoring dashboard for Ho Chi Minh City metro region. "
    "You receive a live system snapshot below with current anomaly alerts and metrics. "
    "Answer questions concisely. Reference real road names (Xa lộ Hà Nội, QL13, etc.) "
    "and HCMC district names when relevant. "
    "Do not add greetings or sign-offs. "
    "Keep answers under 4 sentences unless the user explicitly asks for more detail."
)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    lang: str = Field(default="vi", pattern="^(vi|en)$")


async def _fetch_system_snapshot(conn: asyncpg.Connection) -> dict[str, Any]:
    """Fetch a concise current-state summary from Postgres."""

    # Latest features per route
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (route_id)
            route_id,
            mean_duration_minutes,
            duration_zscore,
            is_anomaly,
            mean_heavy_ratio,
            heavy_ratio_deviation,
            p95_to_mean_ratio,
            max_severe_segments,
            observation_count,
            last_ingest_lag_ms,
            updated_at
        FROM online_route_features
        ORDER BY route_id, updated_at DESC
        """
    )

    if not rows:
        return {"total_routes": 0, "anomalies": [], "top_congested": [], "avg_lag_ms": 0}

    row_list = [dict(r) for r in rows]

    # Run IForest scoring (best-effort)
    iforest_by_route: dict[str, bool] = {}
    try:
        from serving.services.prediction_service import score_rows
        preds = score_rows(row_list)
        iforest_by_route = {p.route_id: p.iforest_anomaly for p in preds}
    except Exception:
        pass

    # Summarise each route
    def _signal(row: dict[str, Any]) -> str:
        z = row.get("is_anomaly", False)
        i = iforest_by_route.get(row["route_id"], False)
        if z and i:
            return "BOTH"
        if z:
            return "ZSCORE"
        if i:
            return "IFOREST"
        return "normal"

    def _short_name(route_id: str) -> str:
        parts = route_id.split("_to_")
        if len(parts) != 2:
            return route_id
        fmt = lambda s: s.replace(f"zone{s[4]}_", "", 1).replace("_", " ").title() if s.startswith("zone") else s.replace("_", " ").title()
        # simpler approach
        def clean(s: str) -> str:
            import re
            return re.sub(r"^zone\d+_", "", s).replace("_", " ").title()
        return f"{clean(parts[0])} → {clean(parts[1])}"

    anomalies = []
    for row in row_list:
        sig = _signal(row)
        if sig != "normal":
            anomalies.append({
                "route": _short_name(row["route_id"]),
                "signal": sig,
                "zscore": round(row["duration_zscore"] or 0, 2),
                "mean_min": round(row["mean_duration_minutes"] or 0, 1),
                "heavy_dev": round((row["heavy_ratio_deviation"] or 0) * 100, 1),
                "severe_seg": row["max_severe_segments"] or 0,
            })

    # Sort by |zscore| desc
    anomalies.sort(key=lambda x: abs(x["zscore"]), reverse=True)

    top_congested = sorted(
        row_list,
        key=lambda r: abs(r["duration_zscore"] or 0),
        reverse=True,
    )[:5]

    avg_lag = int(
        sum(r["last_ingest_lag_ms"] or 0 for r in row_list) / max(len(row_list), 1)
    )

    return {
        "total_routes": len(row_list),
        "anomaly_count": len(anomalies),
        "anomalies": anomalies[:8],
        "top_congested": [
            {
                "route": _short_name(r["route_id"]),
                "zscore": round(r["duration_zscore"] or 0, 2),
                "mean_min": round(r["mean_duration_minutes"] or 0, 1),
            }
            for r in top_congested
        ],
        "avg_lag_ms": avg_lag,
    }


def _build_user_prompt(snapshot: dict[str, Any], message: str, lang: str) -> str:
    """User-turn prompt: live snapshot + question. Geo context lives in system prompt."""
    anomaly_count = snapshot["anomaly_count"]
    total = snapshot["total_routes"]
    avg_lag = snapshot["avg_lag_ms"]

    lines = [
        "=== LIVE SNAPSHOT (real-time) ===",
        f"Đang giám sát {total} tuyến · Độ trễ cảm biến TB: {avg_lag} ms",
        f"Bất thường hiện tại: {anomaly_count}/{total} tuyến",
        "",
    ]

    if snapshot["anomalies"]:
        lines.append("Tuyến bất thường (sắp xếp theo |z-score|):")
        for a in snapshot["anomalies"]:
            lines.append(
                f"  [{a['signal']}] {a['route']}: z={a['zscore']:+.2f}, "
                f"avg {a['mean_min']} phút, xe nặng lệch {a['heavy_dev']:+.1f}%, "
                f"đoạn nặng={a['severe_seg']}"
            )
    else:
        lines.append("Không có bất thường tại thời điểm này.")

    if snapshot["top_congested"]:
        lines.append("")
        lines.append("Top tắc nghẽn:")
        for r in snapshot["top_congested"]:
            lines.append(f"  {r['route']}: z={r['zscore']:+.2f}, avg {r['mean_min']} phút")

    lang_note = "Trả lời bằng tiếng Việt." if lang == "vi" else "Respond in English."
    lines += ["", "=== END SNAPSHOT ===", "", lang_note, "", f"Câu hỏi: {message}"]

    return "\n".join(lines)


async def _stream_ollama(system: str, prompt: str) -> AsyncGenerator[str, None]:
    """Stream SSE chunks using separate system field for stronger grounding."""
    payload = {
        "model": _MODEL,
        "system": system,   # Ollama treats this as persistent context
        "prompt": prompt,
        "stream": True,
        "options": {"temperature": 0.4, "num_predict": 300},
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream("POST", f"{_OLLAMA_URL}/api/generate", json=payload) as resp:
            if resp.status_code != 200:
                yield f"data: {json.dumps({'error': f'Ollama {resp.status_code}'})}\n\n"
                return
            async for line in resp.aiter_lines():
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                chunk = data.get("response", "")
                if chunk:
                    yield f"data: {json.dumps({'chunk': chunk})}\n\n"
                if data.get("done"):
                    yield f"data: {json.dumps({'done': True})}\n\n"
                    return


@router.post("")
async def chat(
    req: ChatRequest,
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    """Stream a context-aware LLM response about current Urban Pulse state."""
    snapshot = await _fetch_system_snapshot(conn)
    user_prompt = _build_user_prompt(snapshot, req.message, req.lang)
    logger.info("chat: lang=%s msg_len=%d anomalies=%d", req.lang, len(req.message), snapshot["anomaly_count"])

    return StreamingResponse(
        _stream_ollama(_SYSTEM, user_prompt),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/context")
async def chat_context(
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    """Return current system snapshot (for debug / FE preload)."""
    return await _fetch_system_snapshot(conn)
