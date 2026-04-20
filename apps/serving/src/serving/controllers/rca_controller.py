"""RCA controller — RAG retrieval and prompt assembly for root cause analysis."""

from datetime import datetime, timezone
from typing import Any


async def retrieve_context(
    message: str, route_id: str | None
) -> tuple[list[Any], str]:
    """Retrieve RAG context; returns (chunks, formatted_rag_context)."""
    from rag.client import get_chroma_client
    from rag.retriever import retrieve_for_route, retrieve_for_query, format_chunks_for_prompt
    import logging

    logger = logging.getLogger(__name__)
    chunks: list[Any] = []

    if route_id:
        now = datetime.now(timezone.utc)
        hour = now.hour
        dow = (now.weekday() + 1) % 7
        try:
            chroma = get_chroma_client()
            chunks = retrieve_for_route(
                chroma, route_id, hour=hour, dow=dow,
                n_anomaly=4, n_pattern=3, n_external=2,
            )
        except Exception as exc:
            logger.debug("rca: route retrieval failed — %s", exc)

    if not chunks:
        try:
            chroma = get_chroma_client()
            chunks = retrieve_for_query(chroma, message, n_results=5)
        except Exception as exc:
            logger.debug("rca: free-text retrieval failed — %s", exc)

    return chunks, format_chunks_for_prompt(chunks)


def build_rca_prompt(rag_context: str, message: str, lang: str) -> str:
    lang_note = "Trả lời bằng tiếng Việt." if lang == "vi" else "Respond in English."
    return "\n".join(filter(None, [rag_context, lang_note, f"Câu hỏi phân tích: {message}"]))


def chunks_to_log_data(chunks: list[Any]) -> list[dict[str, Any]]:
    return [{"text": c.text, "score": c.score, "collection": c.collection} for c in chunks]
