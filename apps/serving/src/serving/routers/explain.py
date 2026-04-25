"""Router: LLM-powered anomaly explanation (SSE)."""

import json
import logging
import time
from typing import Any, AsyncGenerator

import asyncpg
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from serving.controllers.explain_controller import (
    fetch_route_data,
    fetch_rag_context,
    build_explain_prompt,
)
from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt
from serving.repo import interactions as interactions_repo
from serving.services.llm_service import stream_ollama, check_ollama_status
from serving.utils.weather import fetch_current_weather

router = APIRouter(prefix="/anomalies", tags=["explain"])
logger = logging.getLogger(__name__)

_CACHE_TTL = 900  # 15 min
_cache: dict[str, tuple[float, str]] = {}

_SYSTEM = build_system_prompt(
    "You are a traffic analyst for Ho Chi Minh City metro region. "
    "Given real-time congestion sensor data, weather, and historical RAG context for an anomalous route, "
    "produce a structured analysis with exactly 3 sections using ### headings. "
    "CRITICAL — anomaly interpretation: "
    "heavy_ratio and moderate_ratio are compared to THIS ROUTE's historical baseline at THIS SPECIFIC hour and day-of-week, not to absolute thresholds. "
    "A heavy_ratio of 6% can be highly anomalous if the baseline for this route at this hour is 0.5%. "
    "Never say a ratio is 'high' or 'low' in absolute terms — always compare to the route's own baseline. "
    "IsolationForest anomalies mean the combination of time-of-day and congestion pattern is unusual. "
    "Always write 'Zone' (never 'Zona'). Be specific with numbers. No greetings or sign-offs."
)


@router.get("/{route_id}/explain")
async def explain_anomaly(
    route_id: str,
    lang: str = Query(default="vi", pattern="^(vi|en)$"),
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    # Serve from cache if fresh
    cache_key = f"{route_id}:{lang}"
    cached = _cache.get(cache_key)
    if cached and (time.monotonic() - cached[0]) < _CACHE_TTL:
        cached_text = cached[1]

        async def _replay() -> AsyncGenerator[str, None]:
            yield f"data: {json.dumps({'chunk': cached_text, 'cached': True})}\n\n"
            yield f"data: {json.dumps({'done': True, 'cached': True})}\n\n"

        return StreamingResponse(
            _replay(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    row = await fetch_route_data(route_id, conn)
    weather = await fetch_current_weather()
    rag_context, retrieved_chunks = await fetch_rag_context(route_id, row)

    user_prompt = build_explain_prompt(row, lang, rag_context=rag_context, weather=weather)
    logger.info("explain: route=%s lang=%s rag_chunks=%d", route_id, lang, len(retrieved_chunks))

    log_id = await interactions_repo.log_interaction(
        conn,
        query_type="explain",
        query=route_id,
        lang=lang,
        route_id=route_id,
        chunks=[{"text": c.text, "score": c.score, "collection": c.collection}
                for c in retrieved_chunks] if retrieved_chunks else None,
    )
    full_text: list[str] = []

    async def _stream_and_cache() -> AsyncGenerator[str, None]:
        async for event in stream_ollama(_SYSTEM, user_prompt, temperature=0.3, num_predict=400):
            yield event
            try:
                data = json.loads(event.removeprefix("data: ").strip())
                if "chunk" in data:
                    full_text.append(data["chunk"])
            except Exception:
                pass
        _cache[cache_key] = (time.monotonic(), "".join(full_text))
        if log_id and full_text:
            await interactions_repo.update_response(conn, log_id, "".join(full_text))

    return StreamingResponse(
        _stream_and_cache(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/explain/status")
async def explain_status() -> dict[str, Any]:
    return await check_ollama_status()
