"""Router: LLM-powered anomaly explanation.

Endpoint
--------
GET /anomalies/{route_id}/explain?lang=vi
    Streams a natural-language explanation of why a route is anomalous.
    Uses Ollama (local LLM) with structured prompt built from Postgres features.
    Response: text/event-stream  — each chunk: `data: {"chunk": "..."}\\n\\n`
              final chunk:       `data: {"done": true}\\n\\n`

GET /anomalies/{route_id}/explain/status
    Returns whether Ollama is reachable and which model is loaded.
"""

import json
import logging
import os
import time
from typing import Any, AsyncGenerator

import asyncpg
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt

router = APIRouter(prefix="/anomalies", tags=["explain"])
logger = logging.getLogger(__name__)

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
_CACHE_TTL = 900  # 15 min — anomaly context doesn't change fast

# Simple in-process cache: route_id → (ts, explanation_text)
_cache: dict[str, tuple[float, str]] = {}

# Full system prompt: grounding + geo knowledge + analyst role
# Build once at import time — geo knowledge is static
_SYSTEM = build_system_prompt(
    "You are a traffic analyst for Ho Chi Minh City metro region. "
    "Given real-time congestion sensor data for an anomalous route, write a concise 2–3 sentence explanation "
    "of WHY the route is anomalous — reference the specific road names, the type of traffic "
    "(container trucks, commuters, industrial freight), and what the congestion ratios imply. "
    "Focus on heavy_ratio and moderate_ratio values — interpret them as percentage of road segments congested. "
    "Be specific about the numbers. Do not use bullet points. Do not add greetings or sign-offs."
)


def _build_user_prompt(row: dict[str, Any], lang: str) -> str:
    """Build the user-turn prompt (data only — geo context is in system prompt)."""
    heavy = row.get("mean_heavy_ratio") or 0.0
    moderate = row.get("mean_moderate_ratio") or 0.0
    low = row.get("mean_low_ratio") or 0.0
    severe = row.get("max_severe_segments") or 0
    obs = row.get("observation_count") or 0
    is_z = row.get("is_anomaly", False)
    is_if = row.get("iforest_anomaly", False)
    origin = row.get("origin") or row.get("route_id", "unknown")
    dest = row.get("destination") or ""

    signal = []
    if is_z and is_if:
        signal.append("BOTH Z-Score lẫn IsolationForest (độ tin cậy cao nhất)")
    elif is_z:
        signal.append("Z-Score (heavy_ratio > 30% — tắc nặng bất thường)")
    elif is_if:
        signal.append("IsolationForest (cấu trúc congestion bất thường đa chiều)")

    lang_instruction = "Trả lời bằng tiếng Việt." if lang == "vi" else "Respond in English."

    return (
        f"=== DỮ LIỆU TẮC NGHẼN BẤT THƯỜNG ===\n"
        f"Tuyến: {origin} → {dest}\n"
        f"Tỉ lệ đoạn TẮC NẶNG (heavy_ratio): {heavy:.1%}\n"
        f"Tỉ lệ đoạn CHẬM VỪA (moderate_ratio): {moderate:.1%}\n"
        f"Tỉ lệ đoạn ÍT TẮC (low_ratio): {low:.1%}\n"
        f"Đoạn NGHIÊM TRỌNG NHẤT (severe_segments): {severe}\n"
        f"Số quan sát trong cửa sổ: {obs}\n"
        f"Tín hiệu bất thường: {', '.join(signal) if signal else 'none'}\n\n"
        f"{lang_instruction}\n"
        f"Dựa vào kiến thức về hành lang này và mẫu giao thông điển hình, "
        f"hãy giải thích trong 2–3 câu tại sao tuyến đường này đang bất thường:"
    )


async def _stream_ollama(system: str, prompt: str) -> AsyncGenerator[str, None]:
    """Stream SSE chunks from Ollama generate API using separate system field."""
    payload = {
        "model": _MODEL,
        "system": system,   # separate system context — model weights this strongly
        "prompt": prompt,
        "stream": True,
        "options": {"temperature": 0.3, "num_predict": 200},
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream("POST", f"{_OLLAMA_URL}/api/generate", json=payload) as resp:
            if resp.status_code != 200:
                raise HTTPException(502, f"Ollama returned {resp.status_code}")
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


async def _fetch_route_row(
    route_id: str,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    """Fetch latest feature row + run IForest score for the route."""
    row = await conn.fetchrow(
        """
        SELECT DISTINCT ON (route_id)
            route_id, is_anomaly,
            mean_heavy_ratio,
            COALESCE(mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
            COALESCE(mean_low_ratio, 0.0)      AS mean_low_ratio,
            max_severe_segments,
            observation_count, updated_at
        FROM online_route_features
        WHERE route_id = $1
        ORDER BY route_id, updated_at DESC
        """,
        route_id,
    )
    if row is None:
        raise HTTPException(404, f"No data for route '{route_id}'")

    result = dict(row)

    # Try to get origin/destination from routes.json (loaded by ws.py)
    from serving.routers.ws import _load_routes_coords
    coords = _load_routes_coords()
    meta = coords.get(route_id, {})
    result["origin"] = meta.get("origin", route_id)
    result["destination"] = meta.get("destination", "")

    # IForest score (best-effort — no model = skip)
    try:
        from serving.services.prediction_service import score_rows
        preds = score_rows([result])
        if preds:
            result["iforest_anomaly"] = preds[0].iforest_anomaly
    except Exception:
        result["iforest_anomaly"] = False

    return result


@router.get("/{route_id}/explain")
async def explain_anomaly(
    route_id: str,
    lang: str = Query(default="vi", pattern="^(vi|en)$"),
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    """Stream a natural-language explanation of a route anomaly via local LLM."""
    # Serve from cache if fresh
    cached = _cache.get(f"{route_id}:{lang}")
    if cached and (time.monotonic() - cached[0]) < _CACHE_TTL:
        cached_text = cached[1]

        async def _replay() -> AsyncGenerator[str, None]:
            yield f"data: {json.dumps({'chunk': cached_text, 'cached': True})}\n\n"
            yield f"data: {json.dumps({'done': True, 'cached': True})}\n\n"

        return StreamingResponse(_replay(), media_type="text/event-stream",
                                  headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    row = await _fetch_route_row(route_id, conn)
    user_prompt = _build_user_prompt(row, lang)
    logger.info("explain: route=%s lang=%s model=%s", route_id, lang, _MODEL)

    full_text: list[str] = []

    async def _stream_and_cache() -> AsyncGenerator[str, None]:
        async for chunk_event in _stream_ollama(_SYSTEM, user_prompt):
            yield chunk_event
            try:
                data = json.loads(chunk_event.removeprefix("data: ").strip())
                if "chunk" in data:
                    full_text.append(data["chunk"])
            except Exception:
                pass
        _cache[f"{route_id}:{lang}"] = (time.monotonic(), "".join(full_text))

    return StreamingResponse(
        _stream_and_cache(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/explain/status")
async def explain_status() -> dict[str, Any]:
    """Check Ollama availability and loaded model."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{_OLLAMA_URL}/api/tags")
            tags = resp.json()
            models = [m["name"] for m in tags.get("models", [])]
            model_ready = any(_MODEL in m for m in models)
            return {
                "ollama_reachable": True,
                "model": _MODEL,
                "model_ready": model_ready,
                "available_models": models,
            }
    except Exception as exc:
        return {"ollama_reachable": False, "model": _MODEL, "model_ready": False, "error": str(exc)}
