"""Router: RAG-powered Root Cause Analysis endpoints."""

import json
import logging
from typing import Any, AsyncGenerator

import asyncpg
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from serving.controllers.rca_controller import retrieve_context, build_rca_prompt, chunks_to_log_data
from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt
from serving.repo import interactions as interactions_repo
from serving.schemas.rca import FeedbackRequest, RCARequest
from serving.services.llm_service import stream_ollama

router = APIRouter(prefix="/rca", tags=["rca"])
logger = logging.getLogger(__name__)

_SYSTEM = build_system_prompt(
    "You are Urban Pulse RCA (Root Cause Analysis) AI — a senior traffic analyst for Ho Chi Minh City. "
    "You receive real-time anomaly data AND historical context retrieved from a traffic knowledge base. "
    "Your job is to identify the most likely ROOT CAUSE of the anomaly — not just describe symptoms. "
    "Reference specific roads (Xa lộ Hà Nội, QL13, Võ Văn Kiệt...), vehicle types, "
    "time-of-day patterns, and whether this matches historical precedent. "
    "If external context (weather, events) is provided, factor it in explicitly. "
    "Be concise and analytical. No greetings or sign-offs."
)


@router.post("")
async def root_cause_analysis(
    req: RCARequest,
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    chunks, rag_context = await retrieve_context(req.message, req.route_id)
    prompt = build_rca_prompt(rag_context, req.message, req.lang)

    log_id = await interactions_repo.log_interaction(
        conn,
        query_type="rca",
        query=req.message,
        lang=req.lang,
        route_id=req.route_id,
        chunks=chunks_to_log_data(chunks) if chunks else None,
    )

    logger.info("rca: route=%s chunks=%d", req.route_id, len(chunks))
    full_text: list[str] = []

    async def _stream_and_log() -> AsyncGenerator[str, None]:
        async for event in stream_ollama(_SYSTEM, prompt, temperature=0.3, num_predict=400):
            try:
                data = json.loads(event.removeprefix("data: ").strip())
            except Exception:
                yield event
                continue
            if data.get("done"):
                continue  # suppress inner done — emit our own with log_id
            if "chunk" in data:
                full_text.append(data["chunk"])
            yield event
        if log_id and full_text:
            await interactions_repo.update_response(conn, log_id, "".join(full_text))
        done_payload: dict[str, Any] = {"done": True}
        if log_id:
            done_payload["log_id"] = log_id
        yield f"data: {json.dumps(done_payload)}\n\n"

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
    result = await interactions_repo.update_feedback(conn, req.log_id, req.feedback)
    return {"log_id": req.log_id, "feedback": req.feedback, "updated": result == "UPDATE 1"}


@router.get("/logs")
async def get_logs(
    limit: int = Query(default=20, ge=1, le=100),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict[str, Any]]:
    return await interactions_repo.fetch_recent_logs(conn, limit)
