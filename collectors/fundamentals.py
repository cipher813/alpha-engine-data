"""
collectors/fundamentals.py — FMP quarterly fundamental data collection.

Fetches P/E, P/B, D/E, revenue growth, FCF yield, gross margin, ROE,
current ratio for all universe tickers from Financial Modeling Prep.

Runs weekly in DataPhase1. Cached to S3 at archive/fundamentals/{date}.json.
Daily pipeline reads the cached file (fundamentals are quarterly — don't
change within a week).

FMP free tier: 250 req/day. Each ticker uses 2 calls (key-metrics-ttm,
income-statement). ~30 promoted tickers = ~60 calls, well within budget.
Rate limited at 4 req/sec to stay under FMP's 5/sec limit.
"""

from __future__ import annotations

import json
import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/api/v3"
_TIMEOUT = 10
_RATE_LIMIT_DELAY = 0.25  # 4 req/sec


def _fmp_get(endpoint: str, api_key: str, params: dict | None = None) -> dict | list:
    url = f"{_FMP_BASE}/{endpoint}"
    p = {"apikey": api_key}
    if params:
        p.update(params)
    resp = requests.get(url, params=p, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _clip(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


# Neutral values for tickers where FMP returns nothing
NEUTRAL = {
    "pe_ratio": 0.0,
    "pb_ratio": 0.0,
    "debt_to_equity": 0.0,
    "revenue_growth_yoy": 0.0,
    "fcf_yield": 0.0,
    "gross_margin": 0.0,
    "roe": 0.0,
    "current_ratio": 0.0,
}


def _fetch_single_ticker(ticker: str, api_key: str) -> dict:
    """Fetch and normalize fundamental data for a single ticker."""
    metrics = _fmp_get(f"key-metrics-ttm/{ticker}", api_key)
    time.sleep(_RATE_LIMIT_DELAY)

    if not isinstance(metrics, list) or not metrics:
        return NEUTRAL.copy()

    m = metrics[0]
    pe_raw = _safe_float(m.get("peRatioTTM"))
    pb_raw = _safe_float(m.get("pbRatioTTM"))
    roe_raw = _safe_float(m.get("roeTTM"))
    fcf_yield_raw = _safe_float(m.get("freeCashFlowYieldTTM"))
    current_ratio_raw = _safe_float(m.get("currentRatioTTM"))
    de_raw = _safe_float(m.get("debtToEquityTTM"))

    income = _fmp_get(f"income-statement/{ticker}", api_key, params={"period": "quarter", "limit": 5})
    time.sleep(_RATE_LIMIT_DELAY)

    revenue_growth = 0.0
    gross_margin = 0.0

    if isinstance(income, list) and len(income) >= 5:
        recent_rev = _safe_float(income[0].get("revenue"))
        year_ago_rev = _safe_float(income[4].get("revenue"))
        if year_ago_rev > 0:
            revenue_growth = (recent_rev - year_ago_rev) / year_ago_rev
        gross_profit = _safe_float(income[0].get("grossProfit"))
        if recent_rev > 0:
            gross_margin = gross_profit / recent_rev
    elif isinstance(income, list) and income:
        recent_rev = _safe_float(income[0].get("revenue"))
        gross_profit = _safe_float(income[0].get("grossProfit"))
        if recent_rev > 0:
            gross_margin = gross_profit / recent_rev

    return {
        "pe_ratio": _clip(pe_raw / 30.0, -3.0, 3.0),
        "pb_ratio": _clip(pb_raw / 5.0, -3.0, 3.0),
        "debt_to_equity": _clip(de_raw / 2.0, -3.0, 3.0),
        "revenue_growth_yoy": _clip(revenue_growth, -1.0, 2.0),
        "fcf_yield": _clip(fcf_yield_raw, -0.5, 0.5),
        "gross_margin": _clip(gross_margin, 0.0, 1.0),
        "roe": _clip(roe_raw, -1.0, 1.0),
        "current_ratio": _clip(current_ratio_raw / 3.0, 0.0, 3.0),
    }


def collect(
    bucket: str,
    tickers: list[str],
    run_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Fetch fundamentals for all tickers and cache to S3.

    Returns summary dict with counts.
    """
    import boto3

    api_key = os.environ.get("FMP_API_KEY", "")
    if not api_key:
        logger.warning("FMP_API_KEY not set — skipping fundamentals collection")
        return {"status": "skipped", "reason": "no_api_key"}

    logger.info("Fetching fundamentals for %d tickers from FMP...", len(tickers))
    t0 = time.time()

    results: dict[str, dict] = {}
    n_ok = 0
    n_err = 0

    for ticker in tickers:
        try:
            data = _fetch_single_ticker(ticker, api_key)
            results[ticker] = data
            if data != NEUTRAL:
                n_ok += 1
        except Exception as e:
            logger.debug("Fundamental fetch failed for %s: %s", ticker, e)
            results[ticker] = NEUTRAL.copy()
            n_err += 1

    elapsed = time.time() - t0
    logger.info(
        "Fundamentals fetched in %.1fs: %d OK, %d errors, %d total",
        elapsed, n_ok, n_err, len(results),
    )

    if dry_run:
        logger.info("[dry-run] Would write fundamentals for %d tickers", len(results))
        return {"status": "ok", "n_tickers": len(results), "n_ok": n_ok, "dry_run": True}

    # Write to S3
    s3 = boto3.client("s3")
    key = f"archive/fundamentals/{run_date}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(results, default=str),
        ContentType="application/json",
    )
    logger.info("Fundamentals cached to s3://%s/%s", bucket, key)

    return {
        "status": "ok",
        "n_tickers": len(results),
        "n_ok": n_ok,
        "n_errors": n_err,
        "elapsed_seconds": round(elapsed, 1),
        "s3_key": key,
    }
