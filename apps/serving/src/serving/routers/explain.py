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
from serving.routers.chat import _fetch_current_weather
from rag.client import get_chroma_client
from rag.retriever import retrieve_for_route, format_chunks_for_prompt

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
    "Given real-time congestion sensor data, current weather conditions, and historical RAG context "
    "for an anomalous route, write a concise 2–3 sentence explanation of WHY the route is anomalous. "
    "Reference the specific road names, the type of traffic (container trucks, commuters, industrial freight), "
    "and what the congestion ratios imply. "
    "If weather data is present (rain, storm, fog), explicitly mention whether it likely contributes. "
    "Focus on heavy_ratio and moderate_ratio values — interpret them as percentage of road segments congested. "
    "Be specific about the numbers. Do not use bullet points. Do not add greetings or sign-offs."
)


def _build_user_prompt(
    row: dict[str, Any],
    lang: str,
    rag_context: str = "",
    weather: dict[str, Any] | None = None,
) -> str:
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
        signal.append("Z-Score (heavy_ratio bất thường so với baseline lịch sử)")
    elif is_if:
        signal.append("IsolationForest (cấu trúc congestion bất thường đa chiều)")

    lang_instruction = "Trả lời bằng tiếng Việt." if lang == "vi" else "Respond in English."

    parts = [
        f"=== DỮ LIỆU TẮC NGHẼN BẤT THƯỜNG ===\n"
        f"Tuyến: {origin} → {dest}\n"
        f"Tỉ lệ đoạn TẮC NẶNG (heavy_ratio): {heavy:.1%}\n"
        f"Tỉ lệ đoạn CHẬM VỪA (moderate_ratio): {moderate:.1%}\n"
        f"Tỉ lệ đoạn ÍT TẮC (low_ratio): {low:.1%}\n"
        f"Đoạn NGHIÊM TRỌNG NHẤT (severe_segments): {severe}\n"
        f"Số quan sát trong cửa sổ: {obs}\n"
        f"Tín hiệu bất thường: {', '.join(signal) if signal else 'none'}"
    ]

    # Live weather — always present even if RAG hasn't indexed yet
    if weather:
        temp = weather.get("temperature_c")
        rain = weather.get("rain_mm") or weather.get("precipitation_mm") or 0.0
        wind = weather.get("wind_speed_kmh")
        desc = weather.get("weather_desc", "")
        parts.append(
            f"\n=== THỜI TIẾT HIỆN TẠI TP.HCM (Open-Meteo) ===\n"
            f"Điều kiện: {desc}"
            + (f", {temp:.1f}°C" if temp is not None else "")
            + (f", mưa {rain:.1f} mm" if float(rain) > 0 else ", không mưa")
            + (f", gió {wind:.1f} km/h" if wind is not None else "")
        )

    if rag_context:
        parts.append(f"\n{rag_context}")

    parts.append(
        f"\n{lang_instruction}\n"
        f"Dựa vào dữ liệu tắc nghẽn, thời tiết hiện tại, mẫu giao thông lịch sử "
        f"và context ở trên (nếu có), hãy giải thích trong 2–3 câu tại sao tuyến "
        f"đường này đang bất thường — đề cập cụ thể đến thời tiết nếu có khả năng liên quan:"
    )
    return "\n".join(parts)


async def _stream_ollama(system: str, prompt: str) -> AsyncGenerator[str, None]:
    """Stream SSE chunks from Ollama generate API using separate system field."""
    payload = {
        "model": _MODEL,
        "system": system,   # separate system context — model weights this strongly
        "prompt": prompt,
        "stream": True,
        "options": {"temperature": 0.3, "num_predict": 200},
    }
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
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
    except httpx.TimeoutException:
        logger.warning("explain: Ollama timeout for model=%s", _MODEL)
        yield f"data: {json.dumps({'error': 'LLM timeout — Ollama mất quá lâu để phản hồi'})}\n\n"
    except httpx.HTTPError as exc:
        logger.error("explain: Ollama connection error: %s", exc)
        yield f"data: {json.dumps({'error': 'Không thể kết nối tới Ollama'})}\n\n"


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

    # Live weather + RAG retrieval — both best-effort, never block the response
    weather = await _fetch_current_weather()

    rag_context = ""
    retrieved_chunks: list[Any] = []
    try:
        from datetime import timezone
        window_start = row.get("updated_at")
        if window_start and hasattr(window_start, "astimezone"):
            window_start = window_start.astimezone(timezone.utc)
            hour = window_start.hour
            dow = (window_start.weekday() + 1) % 7
        else:
            from datetime import datetime
            now = datetime.now(timezone.utc)
            hour, dow = now.hour, (now.weekday() + 1) % 7

        chroma = get_chroma_client()
        retrieved_chunks = retrieve_for_route(chroma, route_id, hour=hour, dow=dow)
        rag_context = format_chunks_for_prompt(retrieved_chunks)
    except Exception as exc:
        logger.debug("explain: RAG retrieval failed (non-fatal) — %s", exc)

    user_prompt = _build_user_prompt(row, lang, rag_context=rag_context, weather=weather)
    logger.info("explain: route=%s lang=%s model=%s rag_chunks=%d",
                route_id, lang, _MODEL, len(retrieved_chunks))

    log_id: int | None = None
    try:
        chunk_data = [{"text": c.text, "score": c.score, "collection": c.collection}
                      for c in retrieved_chunks]
        log_id = await conn.fetchval(
            """
            INSERT INTO rag_interaction_log
                (query_type, route_id, query, retrieved_chunks, lang)
            VALUES ('explain', $1, $1, $2::jsonb, $3)
            RETURNING id
            """,
            route_id, json.dumps(chunk_data) if chunk_data else None, lang,
        )
    except Exception as exc:
        logger.debug("explain: failed to log interaction — %s", exc)

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
        if log_id and full_text:
            try:
                await conn.execute(
                    "UPDATE rag_interaction_log SET response = $1 WHERE id = $2",
                    "".join(full_text), log_id,
                )
            except Exception:
                pass

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
