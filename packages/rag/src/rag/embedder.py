"""Ollama-backed embedding function compatible with ChromaDB EmbeddingFunction interface.

Uses the /api/embed endpoint (Ollama >= 0.1.26).
Default model: nomic-embed-text (768-dim, fast, runs on CPU).
"""

import logging
import os
from typing import cast

import httpx
from chromadb import EmbeddingFunction, Documents, Embeddings

logger = logging.getLogger(__name__)

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")


class OllamaEmbeddingFunction(EmbeddingFunction[Documents]):
    """Calls Ollama /api/embed to produce document embeddings."""

    def __init__(
        self,
        model: str = _EMBED_MODEL,
        ollama_url: str = _OLLAMA_URL,
    ) -> None:
        self.model = model
        self.url = ollama_url

    def __call__(self, input: Documents) -> Embeddings:
        """Embed a batch of texts. ChromaDB calls this for both index and query."""
        response = httpx.post(
            f"{self.url}/api/embed",
            json={"model": self.model, "input": list(input)},
            timeout=120.0,
        )
        response.raise_for_status()
        data = response.json()
        embeddings: list[list[float]] = data["embeddings"]
        logger.debug("embedded %d texts with %s", len(embeddings), self.model)
        return cast(Embeddings, embeddings)


def pull_embed_model(ollama_url: str = _OLLAMA_URL, model: str = _EMBED_MODEL) -> None:
    """Pull the embedding model if not already present. Call once at startup."""
    try:
        resp = httpx.post(
            f"{ollama_url}/api/pull",
            json={"name": model, "stream": False},
            timeout=300.0,
        )
        resp.raise_for_status()
        logger.info("rag: embed model '%s' ready", model)
    except Exception as exc:
        logger.warning("rag: could not pull embed model '%s': %s", model, exc)
