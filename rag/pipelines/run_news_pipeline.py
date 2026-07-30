"""Gate A — news pipeline orchestrator CLI.

Runs the full Wave 1 news producer chain on Saturday SF:

  1. Fetch via NewsAggregator (Polygon + GDELT + Yahoo RSS, dedup +
     trust-weighted)
  2. Run NewsNLPPipeline (Loughran-McDonald sentiment + Anthropic-Haiku
     event extraction)
  3. Write structured aggregates parquet to
     s3://alpha-engine-research/data/news_aggregates/{date}.parquet
  4. Ingest article narrative into the RAG corpus via
     nousergon_lib.rag.ingest_document (one document per
     (ticker, article); idempotent via document_exists)

All inputs are sized by --hours; default 168 (7 days) so the Saturday
SF firing captures the prior week's news. Each step graceful-degrades
on individual ticker failures (matches the canonical pipeline
ergonomics of ingest_8k_filings et al.).

Usage::

    # Saturday SF invocation
    python -m rag.pipelines.run_news_pipeline --from-signals

    # Ad-hoc for a specific population
    python -m rag.pipelines.run_news_pipeline --tickers AAPL,MSFT \\
        --hours 48 --aggregate-date 2026-05-17

    # Skip RAG ingest (smoke test the parquet writer only)
    python -m rag.pipelines.run_news_pipeline --from-signals --skip-rag
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--tickers", type=str,
        help="Comma-separated ticker list.",
    )
    grp.add_argument(
        "--from-signals", action="store_true",
        help="Load tickers from the scanner decision set (universe-membership cuts.scanner_candidates)",
    )
    parser.add_argument(
        "--hours", type=int, default=168,
        help="Lookback window in hours (default 168 = 7 days).",
    )
    parser.add_argument(
        "--aggregate-date", type=str, default=None,
        help="Date stamp for the structured aggregates parquet "
             "(default: today UTC).",
    )
    parser.add_argument(
        "--bucket", type=str, default="alpha-engine-research",
    )
    parser.add_argument(
        "--skip-rag", action="store_true",
        help="Skip RAG-corpus ingest step (useful for smoke testing).",
    )
    parser.add_argument(
        "--skip-nlp", action="store_true",
        help="Skip NLP pipeline step (writes empty streams + still "
             "produces aggregates parquet with zero sentiment / events).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + log but don't write parquet or ingest to RAG.",
    )
    args = parser.parse_args()

    # ── Resolve tickers + aggregate_date ─────────────────────────
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        from rag.pipelines._rag_scope import load_rag_scope_tickers
        tickers = load_rag_scope_tickers(bucket=args.bucket)
    if not tickers:
        logger.error("[run_news_pipeline] no tickers — aborting")
        return 1
    logger.info("[run_news_pipeline] running for %d tickers", len(tickers))

    if args.aggregate_date:
        agg_date = date.fromisoformat(args.aggregate_date)
    else:
        agg_date = datetime.now(timezone.utc).date()

    # ── Step 1: build aggregator + fetch ─────────────────────────
    logger.info("[run_news_pipeline] step 1/4 — fetch via multi-source aggregator")
    from collectors.news_aggregator_async import AsyncNewsAggregator
    from collectors.news_sources.gdelt import GdeltNewsAdapter
    from collectors.news_sources.polygon import PolygonNewsAdapter
    from collectors.news_sources.yahoo_rss import YahooRssNewsAdapter

    # config#2938 ruling 1 — the WEEKLY corpus gets FULL coverage: size the
    # Polygon budget from the LIVE universe so the ~5-req/min sweep COMPLETES
    # (the adapter guard is only a SIGKILL backstop). GDELT keeps its own tight
    # default (its throttle-degrades-this-adapter posture, config#2813). The
    # budget is kept below the RAGIngestion SSM executionTimeout so the rest of
    # the step (NLP + RAG ingest) always runs — see fetch_budget for the
    # derivation and its lockstep with nousergon-data's step_function.json.
    from collectors.news_sources.fetch_budget import weekly_news_max_fetch_seconds
    poly_budget = weekly_news_max_fetch_seconds(len(tickers))
    logger.info(
        "[run_news_pipeline] weekly Polygon news budget = %ds for %d tickers",
        poly_budget, len(tickers),
    )
    # config-I5703: ASYNC aggregator, matching collectors/daily_news.py. The
    # sync NewsAggregator sums its sources sequentially — its own docstring
    # says "Sequential per-source today; PR D adds async parallel fan-in" — and
    # PR D landed as AsyncNewsAggregator wired into the DAILY path only
    # (2026-06-09, after the sync version TimedOut every weekday producing
    # nothing, ROADMAP L4573). The weekly path, which is the one on a decision
    # pipeline's critical path, never got the fix. Polygon is the long pole
    # either way; this recovers the GDELT + Yahoo wall-clock that was being
    # added to it rather than overlapped.
    aggregator = AsyncNewsAggregator(sources=[
        PolygonNewsAdapter(max_fetch_seconds=poly_budget),
        GdeltNewsAdapter(ticker_name_map=_load_ticker_name_map()),
        YahooRssNewsAdapter(),
    ])
    # AsyncNewsAggregator.fetch is a coroutine — drive it from this sync entry
    # point, same shape as collectors/daily_news.py.
    import functools

    import anyio
    articles = anyio.run(
        functools.partial(aggregator.fetch, tickers, hours=args.hours)
    )
    logger.info(
        "[run_news_pipeline] step 1 — %d aggregated articles "
        "(across %d source-variants)",
        len(articles),
        sum(len(a.variants) for a in articles),
    )

    # ── Step 2: NLP ──────────────────────────────────────────────
    if args.skip_nlp:
        logger.info("[run_news_pipeline] step 2/4 — SKIPPED (--skip-nlp)")
        from collectors.nlp.pipeline import NewsNLPOutput
        nlp_output = NewsNLPOutput()
    else:
        logger.info("[run_news_pipeline] step 2/4 — NLP pipeline (rule-based, no LLM)")
        nlp_output = _run_nlp(articles)
        logger.info(
            "[run_news_pipeline] step 2 — sentiment_scores=%d "
            "event_flags=%d entity_mentions=%d (%d/%d articles processed)",
            len(nlp_output.sentiment_scores),
            len(nlp_output.event_flags),
            len(nlp_output.entity_mentions),
            nlp_output.n_articles_processed,
            nlp_output.n_articles_processed + nlp_output.n_articles_failed,
        )

    # ── Step 3: structured aggregates parquet ────────────────────
    if args.dry_run:
        logger.info(
            "[run_news_pipeline] step 3/4 — SKIPPED (--dry-run); "
            "would write aggregates for %s", agg_date,
        )
    else:
        logger.info("[run_news_pipeline] step 3/4 — structured aggregates")
        from data.derived.news_aggregates import aggregate_and_write
        import boto3
        s3 = boto3.client("s3")
        key, df = aggregate_and_write(
            articles=articles,
            nlp_output=nlp_output,
            aggregate_date=agg_date,
            aggregator=aggregator,
            s3_client=s3,
            bucket=args.bucket,
        )
        logger.info(
            "[run_news_pipeline] step 3 — wrote %d rows to s3://%s/%s",
            len(df), args.bucket, key,
        )

    # ── Step 4: RAG ingest ───────────────────────────────────────
    if args.skip_rag or args.dry_run:
        logger.info(
            "[run_news_pipeline] step 4/4 — SKIPPED (--skip-rag or --dry-run)",
        )
    else:
        logger.info("[run_news_pipeline] step 4/4 — RAG corpus ingest")
        from rag.pipelines.ingest_news import ingest_articles
        ticker_to_sector = _load_ticker_sector_map(tickers)
        stats = ingest_articles(
            articles=articles,
            filed_date=agg_date,
            ticker_to_sector=ticker_to_sector,
        )
        logger.info("[run_news_pipeline] step 4 — RAG ingest stats: %s", stats)

    logger.info("[run_news_pipeline] complete")
    return 0


def _run_nlp(articles):
    """Instantiate the default NLP pipeline (LM sentiment + rule-based
    event extraction) and run over the article set.

    Event extraction uses :class:`RuleBasedEventExtractor` — deterministic
    classification from Polygon/GDELT/Benzinga vendor tags + title-keyword
    regex against the ``DEFAULT_EVENT_CATEGORIES`` taxonomy. Zero
    LLM calls, zero API spend, zero new dependencies.

    Replaced the prior LLM-backed event extractor 2026-05-25 per
    ``[[preference_llm_calls_confined_to_research_module]]`` after the
    audit found the Haiku output was aggregated to scalar/list summaries
    before any research consumer touched it (rich structured per-article
    output was wasted). See PR body for the deeper rationale.
    """
    from collectors.nlp.loughran_mcdonald import LoughranMcDonaldScorer
    from collectors.nlp.pipeline import NewsNLPPipeline
    from collectors.nlp.rule_based_event_extraction import RuleBasedEventExtractor

    pipeline = NewsNLPPipeline(
        sentiment_scorers=[LoughranMcDonaldScorer()],
        event_extractors=[RuleBasedEventExtractor()],
    )
    return pipeline.process(articles)


def _load_ticker_name_map() -> dict[str, str]:
    """Build a {ticker: company_name} map for GDELT query construction.

    Reads from the SEC company_tickers.json file (already cached by
    the other EDGAR pipelines). Tolerates missing entries — GDELT
    adapter falls back to using the ticker symbol verbatim.
    """
    try:
        import requests
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "AlphaEngine research@nousergon.ai"},
            timeout=10,
        )
        if resp.status_code != 200:
            return {}
        out: dict[str, str] = {}
        for entry in resp.json().values():
            ticker = (entry.get("ticker") or "").upper()
            name = entry.get("title") or ""
            if ticker and name:
                out[ticker] = name
        return out
    except Exception as e:
        logger.warning("[run_news_pipeline] ticker→name map fetch failed: %s", e)
        return {}


def _load_ticker_sector_map(tickers: list[str]) -> dict[str, str]:
    """Build a {ticker: sector} map for RAG ingest's sector tagging.

    Reads sectors from ``signals/latest.json``. This is a legitimate use of
    the wide ``universe`` array — it is a per-name ATTRIBUTE lookup, not a
    scope decision (config-I5700 changed only the latter), and the sizing
    envelope covering the whole board is exactly what makes it a good sector
    source. Missing entries leave sector=None for that ticker (acceptable per
    the RAG ingest contract).

    config-I5703: reads the ``latest.json`` POINTER rather than listing and
    sorting prefixes. ``list_objects_v2`` caps ``CommonPrefixes`` at 1000, so
    the old scan silently returns the 1000th-oldest date once the partition
    count crosses a page — a stale sector map with no error.
    """
    try:
        from rag.pipelines._rag_scope import DEFAULT_BUCKET
        import boto3
        import json
        s3 = boto3.client("s3")
        obj = s3.get_object(
            Bucket=DEFAULT_BUCKET,
            Key="signals/latest.json",
        )
        data = json.loads(obj["Body"].read())
        out: dict[str, str] = {}
        for entry in data.get("universe", []):
            if isinstance(entry, dict):
                ticker = entry.get("ticker")
                sector = entry.get("sector")
                if ticker and sector:
                    out[ticker.upper()] = sector
        return out
    except Exception as e:
        logger.warning(
            "[run_news_pipeline] ticker→sector map fetch failed: %s", e,
        )
        return {}


if __name__ == "__main__":
    raise SystemExit(main())
