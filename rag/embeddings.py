"""Voyage embedding wrapper for RAG document and query embeddings.

Uses Voyage voyage-3-lite (1024 dimensions), optimized for retrieval on
financial text. Batch support up to 128 texts per call.

Requires: VOYAGE_API_KEY environment variable.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        import voyageai
        _client = voyageai.Client(api_key=os.environ.get("VOYAGE_API_KEY"))
    return _client


def embed_texts(
    texts: list[str],
    input_type: str = "document",
    model: str = "voyage-3-lite",
    batch_size: int = 128,
) -> list[list[float]]:
    """Embed a batch of text chunks.

    Args:
        texts: List of text strings to embed.
        input_type: 'document' for storage, 'query' for retrieval.
        model: Voyage model name.
        batch_size: Max texts per API call (Voyage limit is 128).

    Returns:
        List of embedding vectors (each 1024 floats for voyage-3-lite).
    """
    client = _get_client()
    all_embeddings = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        result = client.embed(batch, model=model, input_type=input_type)
        all_embeddings.extend(result.embeddings)

    return all_embeddings


def embed_query(query: str, model: str = "voyage-3-lite") -> list[float]:
    """Embed a single query for retrieval."""
    return embed_texts([query], input_type="query", model=model)[0]
