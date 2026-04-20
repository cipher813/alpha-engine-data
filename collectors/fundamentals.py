"""
collectors/fundamentals.py — FMP quarterly fundamental data collection.

Fetches P/E, P/B, D/E, revenue growth, FCF yield, gross margin, ROE,
current ratio for all universe tickers from Financial Modeling Prep.

Runs weekly in DataPhase1. Cached to S3 at archive/fundamentals/{date}.json.
Daily pipeline reads the cached file (fundamentals are quarterly — don't
change within a week).

Endpoint contract
-----------------
FMP sunset the v3 API on 2025-08-31. All calls target the `/stable`
endpoint with query-string tickers, e.g.:

    /stable/key-metrics-ttm?symbol=AAPL   # FCF yield, current ratio, ROE
    /stable/ratios-ttm?symbol=AAPL        # P/E, P/B, debt/equity
    /stable/income-statement?symbol=AAPL&period=quarter&limit=5

v3 crammed P/E / P/B / D/E into ``key-metrics-ttm`` alongside the
efficiency ratios; /stable split them across two endpoints and renamed
a few fields (``roeTTM`` → ``returnOnEquityTTM``).

Failure semantics
-----------------
Per-ticker errors are logged at WARNING and fall through to NEUTRAL
values, but the collector hard-fails (``status="error"``) if fewer than
``_MIN_OK_RATIO`` of tickers produced real (non-NEUTRAL) data — which
catches a recurrence of the silent-zeros bug that went undetected for
two weeks in April 2026 when the v3 endpoints started returning 403.
Matches the ``short_interest`` collector's guard.
"""

from __future__ import annotations

import json
import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/stable"
_TIMEOUT = 10
_RATE_LIMIT_DELAY = 0.25  # 4 req/sec

# Minimum fraction of requested tickers that must produce real fundamentals
# (at least one non-zero field) for the run to be considered OK. Below
# this threshold the endpoint is probably broken (sunsetted, paid-tier,
# quota hit) — don't let a silently-zeroed output flow into the predictor
# feature store and research scoring.
_MIN_OK_RATIO = 0.90


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
    metrics = _fmp_get("key-metrics-ttm", api_key, params={"symbol": ticker})
    time.sleep(_RATE_LIMIT_DELAY)

    if not isinstance(metrics, list) or not metrics:
        return NEUTRAL.copy()

    m = metrics[0]
    # roeTTM renamed on /stable. Accept both for forward-compat.
    roe_raw = _safe_float(m.get("returnOnEquityTTM") or m.get("roeTTM"))
    fcf_yield_raw = _safe_float(m.get("freeCashFlowYieldTTM"))
    current_ratio_raw = _safe_float(m.get("currentRatioTTM"))

    # P/E, P/B, D/E moved to /stable/ratios-ttm (v3 had them in key-metrics).
    ratios = _fmp_get("ratios-ttm", api_key, params={"symbol": ticker})
    time.sleep(_RATE_LIMIT_DELAY)
    if isinstance(ratios, list) and ratios:
        r = ratios[0]
        pe_raw = _safe_float(r.get("priceToEarningsRatioTTM") or r.get("peRatioTTM"))
        pb_raw = _safe_float(r.get("priceToBookRatioTTM") or r.get("pbRatioTTM"))
        de_raw = _safe_float(r.get("debtToEquityRatioTTM") or r.get("debtToEquityTTM"))
    else:
        pe_raw = pb_raw = de_raw = 0.0

    income = _fmp_get(
        "income-statement",
        api_key,
        params={"symbol": ticker, "period": "quarter", "limit": 5},
    )
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

    Returns summary dict with counts. ``status="error"`` if the ok_ratio
    gate is breached — downstream orchestrator treats the phase as failed.
    """
    import boto3

    api_key = os.environ.get("FMP_API_KEY", "")
    if not api_key:
        # Preflight is expected to catch this earlier; hard-fail here too
        # so a missing key can never land as "0 OK / N errors / all-zeros".
        return {
            "status": "error",
            "error": "FMP_API_KEY not set — refusing to write all-NEUTRAL fundamentals",
        }

    logger.info("Fetching fundamentals for %d tickers from FMP (%s)...", len(tickers), _FMP_BASE)
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
            logger.warning("Fundamental fetch failed for %s: %s", ticker, e)
            results[ticker] = NEUTRAL.copy()
            n_err += 1

    elapsed = time.time() - t0
    ok_ratio = n_ok / max(len(tickers), 1)
    logger.info(
        "Fundamentals fetched in %.1fs: %d populated, %d errors, %d total (ok_ratio=%.1f%%)",
        elapsed, n_ok, n_err, len(results), ok_ratio * 100,
    )

    if ok_ratio < _MIN_OK_RATIO:
        msg = (
            f"only {n_ok}/{len(tickers)} tickers ({ok_ratio:.1%}) had populated "
            f"fundamentals — below {_MIN_OK_RATIO:.0%} threshold. FMP endpoint "
            f"likely sunsetted, moved to paid tier, or quota exhausted. Refusing "
            f"to write a mostly-zero fundamentals snapshot that would silently "
            f"degrade the predictor + research scoring layers."
        )
        logger.error(msg)
        return {
            "status": "error",
            "error": msg,
            "n_tickers": len(results),
            "n_ok": n_ok,
            "n_errors": n_err,
            "elapsed_seconds": round(elapsed, 1),
        }

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
