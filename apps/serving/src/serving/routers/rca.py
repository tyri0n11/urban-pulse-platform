"""Router: RAG-powered Root Cause Analysis (RCA) endpoint.

Endpoints
---------
POST /rca
    Free-text question about traffic. Retrieves relevant context from ChromaDB
    (anomaly history + traffic patterns + external events) then streams LLM answer.
    Body: {"message": "...", "route_id": "...", "lang": "vi"}

POST /rca/feedback
    Log thumbs-up/down for a previous RCA response (for fine-tuning data collection).
    Body: {"log_id": 123, "feedback": 1}

GET /rca/logs
    Return recent interaction logs (for debugging / dataset export).
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

import asyncpg
import httpx
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt
from rag.client import get_chroma_client
from rag.retriever import retrieve_for_route, retrieve_for_query, format_chunks_for_prompt

router = APIRouter(prefix="/rca", tags=["rca"])
logger = logging.getLogger(__name__)

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

_SYSTEM = build_system_prompt(
    "You are Urban Pulse RCA (Root Cause Analysis) AI — a senior traffic analyst for Ho Chi Minh City. "
    "You receive real-time anomaly data AND historical context retrieved from a traffic knowledge base. "
    "Your job is to identify the most likely ROOT CAUSE of the anomaly — not just describe symptoms. "
    "Reference specific roads (Xa lộ Hà Nội, QL13, Võ Văn Kiệt...), vehicle types, "
    "time-of-day patterns, and whether this matches historical precedent. "
    "If external context (weather, events) is provided, factor it in explicitly. "
    "Be concise and analytical. No greetings or sign-offs."
)


class RCARequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    route_id: str | None = Field(default=None)
    lang: str = Field(default="vi", pattern="^(vi|en)$")


class FeedbackRequest(BaseModel):
    log_id: int
    feedback: int = Field(..., ge=-1, le=1)


async def _stream_ollama(system: str, prompt: str) -> AsyncGenerator[str, None]:
    payload = {
        "model": _MODEL,
        "system": system,
        "prompt": prompt,
        "stream": True,
        "options": {"temperature": 0.3, "num_predict": 400},
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
        yield f"data: {json.dumps({'error': 'LLM timeout'})}\n\n"
    except httpx.HTTPError as exc:
        logger.error("rca: Ollama error: %s", exc)
        yield f"data: {json.dumps({'error': 'Không thể kết nối tới Ollama'})}\n\n"


@router.post("")
async def root_cause_analysis(
    req: RCARequest,
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    """RAG-powered RCA: retrieve historical context then stream LLM analysis."""
    chroma = get_chroma_client()
    chunks = []

    # Route-specific retrieval if route_id provided
    if req.route_id:
        now = datetime.now(timezone.utc)
        hour = now.hour
        dow = (now.weekday() + 1) % 7
        try:
            chunks = retrieve_for_route(
                chroma, req.route_id, hour=hour, dow=dow,
                n_anomaly=4, n_pattern=3, n_external=2,
            )
        except Exception as exc:
            logger.debug("rca: route retrieval failed — %s", exc)

    # Fallback to free-text retrieval
    if not chunks:
        try:
            chunks = retrieve_for_query(chroma, req.message, n_results=5)
        except Exception as exc:
            logger.debug("rca: free-text retrieval failed — %s", exc)

    rag_context = format_chunks_for_prompt(chunks)
    lang_note = "Trả lời bằng tiếng Việt." if req.lang == "vi" else "Respond in English."

    prompt = "\n".join(filter(None, [
        rag_context,
        f"{lang_note}",
        f"Câu hỏi phân tích: {req.message}",
    ]))

    # Log interaction for fine-tuning data collection
    chunk_data = [{"text": c.text, "score": c.score, "collection": c.collection}
                  for c in chunks]
    log_id: int | None = None
    try:
        log_id = await conn.fetchval(
            """
            INSERT INTO rag_interaction_log
                (query_type, route_id, query, retrieved_chunks, lang)
            VALUES ('rca', $1, $2, $3::jsonb, $4)
            RETURNING id
            """,
            req.route_id, req.message, json.dumps(chunk_data), req.lang,
        )
    except Exception as exc:
        logger.debug("rca: failed to log interaction — %s", exc)

    full_text: list[str] = []

    async def _stream_and_log() -> AsyncGenerator[str, None]:
        async for event in _stream_ollama(_SYSTEM, prompt):
            yield event
            try:
                data = json.loads(event.removeprefix("data: ").strip())
                if "chunk" in data:
                    full_text.append(data["chunk"])
            except Exception:
                pass
        # Persist the full response for fine-tuning
        if log_id and full_text:
            try:
                await conn.execute(
                    "UPDATE rag_interaction_log SET response = $1 WHERE id = $2",
                    "".join(full_text), log_id,
                )
            except Exception:
                pass
        if log_id:
            yield f"data: {json.dumps({'done': True, 'log_id': log_id})}\n\n"

    logger.info("rca: route=%s chunks=%d", req.route_id, len(chunks))
    return StreamingResponse(
        _stream_and_log(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.post("/feedback")
async def submit_feedback(
    req: FeedbackRequest,
    conn: asyncpg.Connection = Depends(get_db),
) -> dict[str, Any]:
    """Record thumbs-up (+1) or thumbs-down (-1) for an RCA response."""
    result = await conn.execute(
        "UPDATE rag_interaction_log SET feedback = $1 WHERE id = $2",
        req.feedback, req.log_id,
    )
    return {"log_id": req.log_id, "feedback": req.feedback, "updated": result == "UPDATE 1"}


@router.get("/logs")
async def get_logs(
    limit: int = Query(default=20, ge=1, le=100),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return recent RAG interaction logs for debugging and fine-tuning export."""
    rows = await conn.fetch(
        """
        SELECT id, ts, query_type, route_id, query, response, feedback, lang
        FROM rag_interaction_log
        ORDER BY ts DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]
