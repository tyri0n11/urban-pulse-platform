"""RAG retriever — queries ChromaDB collections at serving time.

Used by explain.py, chat.py, and rca.py to fetch relevant context
before calling Ollama.
"""

import logging
from dataclasses import dataclass
from typing import Any

import chromadb

from rag.collections import ANOMALY_EVENTS, TRAFFIC_PATTERNS, EXTERNAL_CONTEXT, get_or_create_collections

logger = logging.getLogger(__name__)


@dataclass
class RetrievedChunk:
    text: str
    score: float          # cosine distance (lower = more similar)
    collection: str
    metadata: dict[str, Any]


def retrieve_for_route(
    client: chromadb.HttpClient,
    route_id: str,
    hour: int,
    dow: int,
    n_anomaly: int = 3,
    n_pattern: int = 2,
    n_external: int = 2,
) -> list[RetrievedChunk]:
    """Retrieve relevant context chunks for a specific route at a given time.

    Returns a ranked list of chunks from all three collections:
      - n_anomaly: past anomaly events for this route at similar time
      - n_pattern: typical traffic patterns for this route at this hour/dow
      - n_external: external context (weather, events) at this time

    Args:
        route_id: e.g. "zone1_urban_core_to_zone4_southern_port"
        hour: UTC hour (0–23)
        dow: SQL day-of-week (Sun=0, Sat=6)
    """
    collections = get_or_create_collections(client)
    results: list[RetrievedChunk] = []

    query_text = (
        f"Tuyến {route_id.replace('_', ' ')} "
        f"bất thường lúc {hour:02d}:00 UTC, "
        f"ngày trong tuần {dow}"
    )

    # --- anomaly_events: filter by route_id for precision ---
    try:
        res = collections[ANOMALY_EVENTS].query(
            query_texts=[query_text],
            n_results=n_anomaly,
            where={"route_id": route_id},
        )
        for doc, dist, meta in zip(
            res["documents"][0], res["distances"][0], res["metadatas"][0]
        ):
            results.append(RetrievedChunk(
                text=doc, score=dist, collection=ANOMALY_EVENTS, metadata=meta,
            ))
    except Exception as exc:
        logger.debug("retrieve: anomaly_events query failed — %s", exc)

    # --- traffic_patterns: filter by route_id + hour/dow ---
    try:
        res = collections[TRAFFIC_PATTERNS].query(
            query_texts=[query_text],
            n_results=n_pattern,
            where={"route_id": route_id},
        )
        for doc, dist, meta in zip(
            res["documents"][0], res["distances"][0], res["metadatas"][0]
        ):
            results.append(RetrievedChunk(
                text=doc, score=dist, collection=TRAFFIC_PATTERNS, metadata=meta,
            ))
    except Exception as exc:
        logger.debug("retrieve: traffic_patterns query failed — %s", exc)

    # --- external_context: by time only (weather/events affect all routes) ---
    try:
        res = collections[EXTERNAL_CONTEXT].query(
            query_texts=[query_text],
            n_results=n_external,
            where={"hour": hour},
        )
        for doc, dist, meta in zip(
            res["documents"][0], res["distances"][0], res["metadatas"][0]
        ):
            results.append(RetrievedChunk(
                text=doc, score=dist, collection=EXTERNAL_CONTEXT, metadata=meta,
            ))
    except Exception as exc:
        logger.debug("retrieve: external_context query failed — %s", exc)

    results.sort(key=lambda c: c.score)
    logger.info(
        "retrieve: route=%s hour=%d dow=%d → %d chunks",
        route_id, hour, dow, len(results),
    )
    return results


def retrieve_for_query(
    client: chromadb.HttpClient,
    query: str,
    n_results: int = 5,
) -> list[RetrievedChunk]:
    """Free-text retrieval across all collections — used by /chat and /rca.

    No where-filter: searches all documents by semantic similarity to query.
    """
    collections = get_or_create_collections(client)
    results: list[RetrievedChunk] = []

    for col_name, col in collections.items():
        try:
            res = col.query(query_texts=[query], n_results=n_results)
            for doc, dist, meta in zip(
                res["documents"][0], res["distances"][0], res["metadatas"][0]
            ):
                results.append(RetrievedChunk(
                    text=doc, score=dist, collection=col_name, metadata=meta,
                ))
        except Exception as exc:
            logger.debug("retrieve: %s query failed — %s", col_name, exc)

    results.sort(key=lambda c: c.score)
    return results[:n_results]


def format_chunks_for_prompt(chunks: list[RetrievedChunk]) -> str:
    """Render retrieved chunks as a prompt section for the LLM."""
    if not chunks:
        return ""

    lines = ["=== CONTEXT TỪ LỊCH SỬ & DỮ LIỆU NỀN ==="]
    for i, chunk in enumerate(chunks, 1):
        source = {
            ANOMALY_EVENTS: "Sự kiện lịch sử",
            TRAFFIC_PATTERNS: "Pattern điển hình",
            EXTERNAL_CONTEXT: "Context bên ngoài",
        }.get(chunk.collection, chunk.collection)
        lines.append(f"\n[{i}] {source}:")
        lines.append(chunk.text)
    lines.append("=== END CONTEXT ===")
    return "\n".join(lines)
