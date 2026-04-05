"""ChromaDB collection definitions for Urban Pulse RAG.

Three collections:
  anomaly_events    — historical anomaly windows (PostgreSQL + route_iforest_scores)
  traffic_patterns  — per-route, per-(dow, hour) baseline patterns (Iceberg gold)
  external_context  — weather, events, news (Iceberg gold.context_hourly, Phase 2)

Each collection uses OllamaEmbeddingFunction so vectors are always produced
by the same model at both index time and query time.
"""

import chromadb

from rag.embedder import OllamaEmbeddingFunction

ANOMALY_EVENTS = "anomaly_events"
TRAFFIC_PATTERNS = "traffic_patterns"
EXTERNAL_CONTEXT = "external_context"


def get_or_create_collections(
    client: chromadb.HttpClient,
) -> dict[str, chromadb.Collection]:
    """Return all three collections, creating them if they don't exist."""
    ef = OllamaEmbeddingFunction()
    return {
        ANOMALY_EVENTS: client.get_or_create_collection(
            name=ANOMALY_EVENTS,
            embedding_function=ef,
            metadata={"hnsw:space": "cosine"},
        ),
        TRAFFIC_PATTERNS: client.get_or_create_collection(
            name=TRAFFIC_PATTERNS,
            embedding_function=ef,
            metadata={"hnsw:space": "cosine"},
        ),
        EXTERNAL_CONTEXT: client.get_or_create_collection(
            name=EXTERNAL_CONTEXT,
            embedding_function=ef,
            metadata={"hnsw:space": "cosine"},
        ),
    }
