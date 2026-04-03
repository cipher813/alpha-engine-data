"""Ingest earnings call transcripts from FMP into the RAG vector store.

Downloads transcript text via FMP API, splits into prepared remarks and Q&A
sections, chunks, embeds via Voyage, and stores in Neon pgvector.

Usage:
    # Ingest recent transcripts for specific tickers
    python -m rag.pipelines.ingest_earnings_transcripts --tickers AAPL,MSFT

    # Backfill last 8 quarters from signals
    python -m rag.pipelines.ingest_earnings_transcripts --from-signals --quarters 8
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from datetime import date

import requests

logger = logging.getLogger(__name__)

_FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"


def _fetch_transcript(ticker: str, year: int, quarter: int) -> dict | None:
    """Fetch a single earnings call transcript from FMP.

    Returns dict with keys: date, content, or None if not found.
    """
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY not set")
        return None

    url = f"{_FMP_BASE_URL}/earning_call_transcript/{ticker}"
    params = {"year": year, "quarter": quarter, "apikey": api_key}

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        return data[0]  # first (most recent) transcript for this quarter
    except Exception as e:
        logger.warning("FMP transcript fetch failed for %s Q%d %d: %s", ticker, quarter, year, e)
        return None


def _split_transcript_sections(text: str) -> dict[str, str]:
    """Split transcript text into prepared remarks and Q&A sections.

    Detects section boundaries by common patterns: "Operator", "Question-and-Answer",
    "Q&A Session", etc.
    """
    sections = {}

    # Try to find Q&A boundary
    qa_patterns = [
        r"(?i)question.and.answer\s*session",
        r"(?i)q\s*&\s*a\s*session",
        r"(?i)\boperator\b.*(?:first|next)\s+question",
    ]

    qa_start = None
    for pattern in qa_patterns:
        match = re.search(pattern, text)
        if match:
            qa_start = match.start()
            break

    if qa_start and qa_start > 200:
        sections["prepared_remarks"] = text[:qa_start].strip()
        sections["qa_session"] = text[qa_start:].strip()
    else:
        # No clear Q&A boundary — treat entire transcript as prepared remarks
        sections["prepared_remarks"] = text.strip()

    # Truncate very long sections
    for key in sections:
        if len(sections[key]) > 60000:
            sections[key] = sections[key][:60000]

    return {k: v for k, v in sections.items() if len(v) > 100}


def _chunk_text(text: str, chunk_size: int = 400, overlap: int = 50) -> list[str]:
    """Split text into overlapping chunks by approximate token count."""
    words = text.split()
    words_per_chunk = int(chunk_size / 1.3)
    overlap_words = int(overlap / 1.3)

    chunks = []
    start = 0
    while start < len(words):
        end = start + words_per_chunk
        chunk = " ".join(words[start:end])
        if chunk.strip():
            chunks.append(chunk)
        start = end - overlap_words
        if start >= len(words):
            break

    return chunks


def _quarter_dates(n_quarters: int) -> list[tuple[int, int]]:
    """Generate (year, quarter) tuples going back n_quarters from now."""
    today = date.today()
    current_q = (today.month - 1) // 3 + 1
    current_y = today.year

    quarters = []
    y, q = current_y, current_q
    for _ in range(n_quarters):
        quarters.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return quarters


def ingest_ticker(
    ticker: str,
    sector: str | None = None,
    n_quarters: int = 8,
    dry_run: bool = False,
) -> int:
    """Ingest earnings transcripts for a single ticker.

    Returns number of transcripts ingested.
    """
    from rag.embeddings import embed_texts
    from rag.retrieval import ingest_document, document_exists

    ingested = 0

    for year, quarter in _quarter_dates(n_quarters):
        # Approximate filed date for dedup
        q_end_month = quarter * 3
        filed_date = date(year, q_end_month, 28)

        if document_exists(ticker, "earnings_transcript", filed_date, "fmp"):
            logger.debug("Already ingested: %s Q%d %d", ticker, quarter, year)
            continue

        if dry_run:
            logger.info("[DRY RUN] Would ingest %s Q%d %d transcript", ticker, quarter, year)
            ingested += 1
            continue

        transcript = _fetch_transcript(ticker, year, quarter)
        if not transcript:
            continue
        time.sleep(0.3)  # FMP rate limiting

        content = transcript.get("content", "")
        if len(content) < 200:
            continue

        transcript_date_str = transcript.get("date", "")
        if transcript_date_str:
            try:
                filed_date = date.fromisoformat(transcript_date_str[:10])
            except ValueError:
                pass

        sections = _split_transcript_sections(content)
        if not sections:
            continue

        # Chunk and embed
        all_chunks = []
        for section_label, section_text in sections.items():
            for chunk_text in _chunk_text(section_text):
                all_chunks.append({
                    "content": chunk_text,
                    "section_label": section_label,
                })

        if not all_chunks:
            continue

        embeddings = embed_texts([c["content"] for c in all_chunks])
        for chunk, emb in zip(all_chunks, embeddings):
            chunk["embedding"] = emb

        doc_id = ingest_document(
            ticker=ticker,
            sector=sector,
            doc_type="earnings_transcript",
            source="fmp",
            filed_date=filed_date,
            title=f"{ticker} Q{quarter} {year} Earnings Call",
            url=None,
            chunks=all_chunks,
        )
        if doc_id:
            ingested += 1

    return ingested


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingest earnings transcripts into RAG store")
    parser.add_argument("--tickers", type=str, help="Comma-separated ticker list")
    parser.add_argument("--from-signals", action="store_true", help="Load tickers from latest signals.json")
    parser.add_argument("--quarters", type=int, default=8, help="Number of quarters to backfill")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    elif args.from_signals:
        import boto3
        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket="alpha-engine-research", Prefix="signals/", Delimiter="/")
        prefixes = sorted([p["Prefix"] for p in resp.get("CommonPrefixes", [])])
        if not prefixes:
            logger.error("No signals found on S3")
            return
        obj = s3.get_object(Bucket="alpha-engine-research", Key=f"{prefixes[-1]}signals.json")
        data = json.loads(obj["Body"].read())
        tickers = [s["ticker"] for s in data.get("universe", []) if s.get("ticker")]
        logger.info("Loaded %d tickers from signals", len(tickers))
    else:
        parser.error("Provide --tickers or --from-signals")
        return

    total = 0
    for ticker in tickers:
        n = ingest_ticker(ticker, n_quarters=args.quarters, dry_run=args.dry_run)
        total += n

    logger.info("Total: %d transcripts ingested for %d tickers", total, len(tickers))


if __name__ == "__main__":
    main()
