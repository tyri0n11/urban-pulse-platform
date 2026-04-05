"""RAG indexer — writes documents into ChromaDB collections.

Called by the Prefect batch pipeline (apps/batch) every hour.
Upsert-based so re-running is always safe (idempotent).
"""

import logging
from typing import Any

import chromadb

from rag.collections import ANOMALY_EVENTS, TRAFFIC_PATTERNS, EXTERNAL_CONTEXT, get_or_create_collections
from rag.documents import format_anomaly_event, format_traffic_pattern, format_external_event

logger = logging.getLogger(__name__)

_UPSERT_BATCH = 20  # Keep batches small so Ollama embedding stays within timeout


def _upsert_batch(
    collection: chromadb.Collection,
    ids: list[str],
    documents: list[str],
    metadatas: list[dict[str, Any]],
) -> None:
    for i in range(0, len(ids), _UPSERT_BATCH):
        collection.upsert(
            ids=ids[i:i + _UPSERT_BATCH],
            documents=documents[i:i + _UPSERT_BATCH],
            metadatas=metadatas[i:i + _UPSERT_BATCH],
        )


def index_anomaly_events(
    client: chromadb.HttpClient,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert anomaly event rows into the anomaly_events collection.

    Returns the number of documents upserted.
    """
    collections = get_or_create_collections(client)
    col = collections[ANOMALY_EVENTS]

    ids, docs, metas = [], [], []
    for row in rows:
        try:
            doc_id, text, meta = format_anomaly_event(row)
            ids.append(doc_id)
            docs.append(text)
            metas.append(meta)
        except Exception as exc:
            logger.warning("indexer: skip anomaly row — %s", exc)

    if ids:
        _upsert_batch(col, ids, docs, metas)
        logger.info("indexer: upserted %d anomaly events", len(ids))

    return len(ids)


def index_traffic_patterns(
    client: chromadb.HttpClient,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert gold.traffic_hourly rows as baseline pattern documents.

    Caller should pre-aggregate by (route_id, dow, hour_of_day) before passing.
    Returns the number of documents upserted.
    """
    collections = get_or_create_collections(client)
    col = collections[TRAFFIC_PATTERNS]

    ids, docs, metas = [], [], []
    for row in rows:
        try:
            doc_id, text, meta = format_traffic_pattern(row)
            ids.append(doc_id)
            docs.append(text)
            metas.append(meta)
        except Exception as exc:
            logger.warning("indexer: skip pattern row — %s", exc)

    if ids:
        _upsert_batch(col, ids, docs, metas)
        logger.info("indexer: upserted %d traffic patterns", len(ids))

    return len(ids)


def index_external_events(
    client: chromadb.HttpClient,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert gold.context_hourly rows as external context documents."""
    collections = get_or_create_collections(client)
    col = collections[EXTERNAL_CONTEXT]

    ids, docs, metas = [], [], []
    for row in rows:
        try:
            doc_id, text, meta = format_external_event(row)
            ids.append(doc_id)
            docs.append(text)
            metas.append(meta)
        except Exception as exc:
            logger.warning("indexer: skip external row — %s", exc)

    if ids:
        _upsert_batch(col, ids, docs, metas)
        logger.info("indexer: upserted %d external context docs", len(ids))

    return len(ids)
