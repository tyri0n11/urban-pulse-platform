"""ChromaDB HTTP client factory."""

import os

import chromadb


def get_chroma_client() -> chromadb.HttpClient:
    """Return a ChromaDB HTTP client pointed at the configured host."""
    host = os.getenv("CHROMA_HOST", "chromadb")
    port = int(os.getenv("CHROMA_PORT", "8000"))
    return chromadb.HttpClient(host=host, port=port)
