"""Semantic retrieval over RAG document store.

Provides filtered vector similarity search with metadata pre-filtering
(ticker, doc_type, date range). Used by the qual analyst `query_filings` tool.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RetrievalResult:
    content: str
    ticker: str
    doc_type: str
    filed_date: date
    section_label: str | None
    similarity: float


def retrieve(
    query: str,
    tickers: list[str] | None = None,
    doc_types: list[str] | None = None,
    min_date: date | None = None,
    top_k: int = 10,
) -> list[RetrievalResult]:
    """Retrieve the most relevant chunks for a natural language query.

    1. Embeds the query via Voyage (input_type='query')
    2. Builds SQL with metadata pre-filters (ticker, doc_type, date)
    3. Cosine similarity search via pgvector HNSW index
    4. Returns top_k results with content, metadata, and similarity scores

    Args:
        query: Natural language search query.
        tickers: Filter to these stock symbols (optional).
        doc_types: Filter to these doc types, e.g. ['10-K', '10-Q'] (optional).
        min_date: Only return documents filed on or after this date (optional).
        top_k: Maximum number of results to return.

    Returns:
        List of RetrievalResult sorted by similarity (highest first).
    """
    from rag.embeddings import embed_query
    from rag.db import get_connection

    query_vec = embed_query(query)

    # Build WHERE clauses for metadata pre-filtering
    conditions = []
    params: list = [str(query_vec)]  # first param is the query vector

    if tickers:
        conditions.append("d.ticker = ANY(%s)")
        params.append(tickers)
    if doc_types:
        conditions.append("d.doc_type = ANY(%s)")
        params.append(doc_types)
    if min_date:
        conditions.append("d.filed_date >= %s")
        params.append(min_date)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT c.content, d.ticker, d.doc_type, d.filed_date, c.section_label,
               1 - (c.embedding <=> %s::vector) AS similarity
        FROM rag.chunks c
        JOIN rag.documents d ON c.document_id = d.id
        {where}
        ORDER BY c.embedding <=> %s::vector
        LIMIT %s
    """
    # query vector appears twice (in SELECT for similarity score and ORDER BY)
    params.extend([str(query_vec), top_k])

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    results = []
    for row in rows:
        content, ticker, doc_type, filed_date, section_label, similarity = row
        results.append(RetrievalResult(
            content=content,
            ticker=ticker,
            doc_type=doc_type,
            filed_date=filed_date,
            section_label=section_label,
            similarity=round(float(similarity), 4),
        ))

    logger.info(
        "RAG retrieve: query=%r tickers=%s top_k=%d → %d results",
        query[:60], tickers, top_k, len(results),
    )
    return results


def document_exists(ticker: str, doc_type: str, filed_date: date, source: str) -> bool:
    """Check if a document has already been ingested (dedup)."""
    from rag.db import execute_query

    rows = execute_query(
        "SELECT 1 FROM rag.documents WHERE ticker=%s AND doc_type=%s AND filed_date=%s AND source=%s LIMIT 1",
        (ticker, doc_type, filed_date, source),
    )
    return len(rows) > 0


def ingest_document(
    ticker: str,
    sector: str | None,
    doc_type: str,
    source: str,
    filed_date: date,
    title: str | None,
    url: str | None,
    chunks: list[dict],
) -> str | None:
    """Ingest a document and its embedded chunks into the RAG store.

    Args:
        ticker: Stock symbol.
        sector: GICS sector (optional).
        doc_type: '10-K', '10-Q', 'earnings_transcript', 'thesis'.
        source: 'sec_edgar', 'fmp', 'alpha_engine'.
        filed_date: Date the document was filed/published.
        title: Document title (optional).
        url: Source URL (optional).
        chunks: List of dicts with keys: content, section_label, embedding.

    Returns:
        Document UUID on success, None on failure.
    """
    from rag.db import get_connection

    if document_exists(ticker, doc_type, filed_date, source):
        logger.debug("Skipping duplicate: %s %s %s", ticker, doc_type, filed_date)
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Insert document
            cur.execute(
                """INSERT INTO rag.documents (ticker, sector, doc_type, source, filed_date, title, url)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (ticker, sector, doc_type, source, filed_date, title, url),
            )
            doc_id = cur.fetchone()[0]

            # Insert chunks
            chunk_params = [
                (doc_id, i, c["content"], c.get("section_label"), str(c["embedding"]))
                for i, c in enumerate(chunks)
            ]
            from psycopg2.extras import execute_batch
            execute_batch(
                cur,
                """INSERT INTO rag.chunks (document_id, chunk_index, content, section_label, embedding)
                   VALUES (%s, %s, %s, %s, %s::vector)""",
                chunk_params,
                page_size=100,
            )

    logger.info("Ingested %s %s %s: %d chunks", ticker, doc_type, filed_date, len(chunks))
    return str(doc_id)
