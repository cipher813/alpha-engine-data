"""
collectors/alternative.py — Alternative data collector for promoted tickers.

Phase 2 collector: runs AFTER research produces signals.json to fetch
alternative data for the ~25-30 promoted tickers (buy candidates + tracked).

Data sources:
  - Analyst rating (Finnhub ``/stock/recommendation`` — free tier)
  - Analyst price target (yfinance ``Ticker.info.targetMeanPrice`` — free)
  - Earnings surprises (Finnhub ``/stock/earnings`` — free tier)
  - EPS estimates (FMP ``/stable/analyst-estimates?period=annual`` — free tier)
  - Options flow (yfinance)
  - Insider trading (SEC EDGAR Form 4)
  - Institutional 13F (edgartools)
  - News headlines (Yahoo RSS + EDGAR 8-K)

Provider notes (updated 2026-04-22)
-----------------------------------
FMP v3 sunsetted on 2025-08-31; all FMP calls go to /stable with
query-string tickers. On /stable, these endpoints are paid-tier (HTTP
402 / 403 on free), so the corresponding features are sourced elsewhere:

  - ``grades-consensus`` → Finnhub ``/stock/recommendation``
  - ``price-target-consensus`` → yfinance ``Ticker.info`` exposes
    ``targetMeanPrice`` + ``numberOfAnalystOpinions`` on free. Finnhub's
    ``/stock/price-target`` is paid-only. yfinance ``.info`` is the same
    per-ticker pattern ``short_interest.py`` uses.
  - ``earnings-surprises-bulk`` → Finnhub ``/stock/earnings`` (richer
    historical data anyway)
  - ``analyst-estimates?period=quarter`` → FMP ``period=annual`` still
    works on free tier.

Output: one JSON file per ticker at market_data/weekly/{date}/alternative/{TICKER}.json
plus a manifest at market_data/weekly/{date}/alternative/manifest.json.
"""

from __future__ import annotations

import json
import logging
import os
import time
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import boto3
import requests

logger = logging.getLogger(__name__)


def collect(
    bucket: str,
    s3_prefix: str,
    run_date: str | None = None,
    signals_key: str | None = None,
    tickers: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Fetch alternative data for promoted tickers and write to S3.

    Either pass `tickers` directly or provide `signals_key` to read
    promoted tickers from the latest signals.json.

    Args:
        bucket: S3 bucket
        s3_prefix: market_data/ prefix
        run_date: YYYY-MM-DD (defaults to today)
        signals_key: S3 key for signals.json (auto-detected if None)
        tickers: explicit ticker list (overrides signals_key)
        dry_run: validate without writing

    Returns:
        dict with status, tickers_processed, tickers_failed, errors
    """
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = boto3.client("s3")

    # Resolve ticker list
    if not tickers:
        tickers = _load_promoted_tickers(s3, bucket, signals_key, run_date)
    if not tickers:
        logger.warning("No promoted tickers found — skipping alternative data")
        return {"status": "skipped", "reason": "no tickers"}

    logger.info("Collecting alternative data for %d tickers", len(tickers))

    if dry_run:
        return {
            "status": "ok_dry_run",
            "tickers": len(tickers),
            "ticker_list": tickers[:10],
        }

    succeeded = 0
    failed = 0
    errors = []

    for ticker in tickers:
        try:
            data = _fetch_all_alternative(ticker, run_date, bucket)
            key = f"{s3_prefix}weekly/{run_date}/alternative/{ticker}.json"
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data, indent=2, default=str),
                ContentType="application/json",
            )
            succeeded += 1
            logger.info("Alternative data: %s -> s3://%s/%s", ticker, bucket, key)
        except Exception as e:
            failed += 1
            errors.append({"ticker": ticker, "error": str(e)})
            logger.warning("Alternative data failed for %s: %s", ticker, e)

    # Write manifest
    manifest = {
        "run_date": run_date,
        "tickers_requested": len(tickers),
        "tickers_succeeded": succeeded,
        "tickers_failed": failed,
        "errors": errors[:20],
    }
    manifest_key = f"{s3_prefix}weekly/{run_date}/alternative/manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2, default=str),
        ContentType="application/json",
    )

    status = "ok" if failed == 0 else "partial"
    return {
        "status": status,
        "tickers_processed": succeeded,
        "tickers_failed": failed,
        "errors": errors[:20],
    }


def load_from_s3(
    bucket: str,
    s3_prefix: str,
    ticker: str,
    run_date: str | None = None,
) -> dict | None:
    """Load alternative data for a single ticker from S3."""
    s3 = boto3.client("s3")
    if not run_date:
        run_date = _get_latest_date(s3, bucket, s3_prefix)
    if not run_date:
        return None
    try:
        key = f"{s3_prefix}weekly/{run_date}/alternative/{ticker}.json"
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


# -- Ticker resolution -------------------------------------------------------

def _load_promoted_tickers(
    s3, bucket: str, signals_key: str | None, run_date: str
) -> list[str]:
    """Extract promoted tickers from the latest signals.json."""
    if not signals_key:
        signals_key = f"signals/{run_date}/signals.json"

    try:
        obj = s3.get_object(Bucket=bucket, Key=signals_key)
        signals = json.loads(obj["Body"].read())
    except Exception:
        # Try previous trading days
        for days_back in range(1, 8):
            dt = date.fromisoformat(run_date) - timedelta(days=days_back)
            try_key = f"signals/{dt}/signals.json"
            try:
                obj = s3.get_object(Bucket=bucket, Key=try_key)
                signals = json.loads(obj["Body"].read())
                logger.info("Using signals from %s (fallback)", dt)
                break
            except Exception:
                continue
        else:
            return []

    tickers = set()

    # Buy candidates
    for candidate in signals.get("buy_candidates", []):
        t = candidate.get("ticker") or candidate.get("symbol")
        if t:
            tickers.add(t)

    # Tracked universe (currently held + watchlist)
    for entry in signals.get("universe", []):
        t = entry.get("ticker") or entry.get("symbol")
        if t:
            tickers.add(t)

    return sorted(tickers)


def _get_latest_date(s3, bucket: str, s3_prefix: str) -> str | None:
    """Get the most recent weekly date from latest_weekly.json."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=f"{s3_prefix}latest_weekly.json")
        return json.loads(obj["Body"].read()).get("date")
    except Exception:
        return None


# -- Per-ticker alternative data aggregation ----------------------------------

def _fetch_all_alternative(ticker: str, run_date: str, bucket: str) -> dict:
    """Fetch all alternative data sources for a single ticker."""
    result = {
        "ticker": ticker,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    # 1. Analyst consensus (FMP)
    result["analyst_consensus"] = _fetch_analyst(ticker)

    # 2. EPS revisions (FMP)
    result["eps_revision"] = _fetch_revisions(ticker, bucket, run_date)

    # 3. Options flow (yfinance)
    result["options_flow"] = _fetch_options(ticker, run_date)

    # 4. Insider activity (SEC EDGAR)
    result["insider_activity"] = _fetch_insider(ticker, run_date)

    # 5. Institutional 13F (edgartools)
    result["institutional"] = _fetch_institutional(ticker)

    # 6. News (Yahoo RSS + EDGAR 8-K)
    result["news"] = _fetch_news(ticker)

    return result


# -- Individual fetchers (self-contained, no cross-repo imports) -------------

# ---- FMP + Finnhub rate limiters ----

_FMP_STABLE = "https://financialmodelingprep.com/stable"
_fmp_lock = threading.Lock()
_fmp_last_call = 0.0
_fmp_daily_count = 0
_FMP_DAILY_LIMIT = 250
_FMP_MIN_INTERVAL = 1.0

_FINNHUB_BASE = "https://finnhub.io/api/v1"
_finnhub_lock = threading.Lock()
_finnhub_last_call = 0.0
# Finnhub free tier: 60 req/min — give ourselves a small margin.
_FINNHUB_MIN_INTERVAL = 1.1


def _fmp_get(endpoint: str, params: dict | None = None) -> dict | list:
    """Rate-limited FMP /stable API call.

    Returns ``[]`` if ``FMP_API_KEY`` is missing, the daily budget is
    exhausted, or a 429 trips the per-minute limit. All other errors
    propagate — the caller's try/except must log at WARNING so silent
    endpoint sunsets (the 2026-04 incident) can't hide.
    """
    global _fmp_last_call, _fmp_daily_count
    api_key = os.environ.get("FMP_API_KEY", "")
    if not api_key:
        return []

    url = f"{_FMP_STABLE}/{endpoint}"
    p = {"apikey": api_key}
    if params:
        p.update(params)

    with _fmp_lock:
        if _fmp_daily_count >= _FMP_DAILY_LIMIT:
            logger.warning("FMP daily budget exhausted (%d calls)", _FMP_DAILY_LIMIT)
            return []
        now = time.monotonic()
        wait = _FMP_MIN_INTERVAL - (now - _fmp_last_call)
        if wait > 0:
            time.sleep(wait)
        _fmp_last_call = time.monotonic()
        _fmp_daily_count += 1

    resp = requests.get(url, params=p, timeout=10)
    if resp.status_code == 429:
        with _fmp_lock:
            _fmp_daily_count = _FMP_DAILY_LIMIT
        return []
    resp.raise_for_status()
    return resp.json()


def _finnhub_get(endpoint: str, params: dict | None = None) -> dict | list:
    """Rate-limited Finnhub API call. Returns ``[]`` on missing key."""
    global _finnhub_last_call
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        return []

    url = f"{_FINNHUB_BASE}/{endpoint}"
    p = {"token": api_key}
    if params:
        p.update(params)

    with _finnhub_lock:
        now = time.monotonic()
        wait = _FINNHUB_MIN_INTERVAL - (now - _finnhub_last_call)
        if wait > 0:
            time.sleep(wait)
        _finnhub_last_call = time.monotonic()

    resp = requests.get(url, params=p, timeout=10)
    if resp.status_code == 429:
        logger.warning("Finnhub 429 rate-limited on %s", endpoint)
        return []
    resp.raise_for_status()
    return resp.json()


# ---- 1. Analyst consensus ----

def _fetch_analyst(ticker: str) -> dict:
    """Fetch analyst rating + target price + earnings surprises.

    Sources: Finnhub ``/stock/recommendation`` for rating + bull/bear
    analyst counts; yfinance ``Ticker.info`` for the consensus price
    target (Finnhub's ``/stock/price-target`` and FMP's
    ``/stable/price-target-consensus`` are both paid-tier); Finnhub
    ``/stock/earnings`` for historical surprises.
    """
    result = {
        "rating": None,
        "target_price": None,
        "num_analysts": None,
        "earnings_surprises": [],
    }

    # Finnhub analyst recommendation: list of {buy, hold, sell, strongBuy,
    # strongSell, period, symbol}. Most recent is first.
    try:
        data = _finnhub_get("stock/recommendation", {"symbol": ticker})
        if isinstance(data, list) and data:
            latest = data[0]
            totals = {k: latest.get(k, 0) or 0 for k in ("strongBuy", "buy", "hold", "sell", "strongSell")}
            total = sum(totals.values())
            bullish = totals["strongBuy"] + totals["buy"]
            bearish = totals["sell"] + totals["strongSell"]
            if total > 0:
                if bullish > bearish and bullish >= totals["hold"]:
                    result["rating"] = "Buy"
                elif bearish > bullish:
                    result["rating"] = "Sell"
                else:
                    result["rating"] = "Hold"
                result["num_analysts"] = total
    except Exception as e:
        logger.warning("Finnhub recommendation failed for %s: %s", ticker, e)

    # Finnhub historical earnings surprises: list of {actual, estimate,
    # surprise, surprisePercent, period, quarter, symbol, year}, most
    # recent first. Richer than FMP /stable/earnings (which is the
    # forward-looking calendar).
    try:
        data = _finnhub_get("stock/earnings", {"symbol": ticker})
        if isinstance(data, list) and data:
            surprises = []
            for entry in data[:4]:
                actual = entry.get("actual")
                estimated = entry.get("estimate")
                surprise_pct = entry.get("surprisePercent")
                if surprise_pct is None and actual is not None and estimated not in (None, 0):
                    surprise_pct = round((actual - estimated) / abs(estimated) * 100, 2)
                surprises.append({
                    "date": entry.get("period", ""),
                    "actual": actual,
                    "estimated": estimated,
                    "surprise_pct": surprise_pct,
                })
            result["earnings_surprises"] = surprises
    except Exception as e:
        logger.warning("Finnhub earnings failed for %s: %s", ticker, e)

    # yfinance Ticker.info: ``targetMeanPrice`` is the consensus price target;
    # ``numberOfAnalystOpinions`` is the count of analysts covering the name
    # (a different denominator than the Finnhub rating counts above — Finnhub's
    # ``num_analysts`` is the sum of current-period rating buckets, used for
    # the Buy/Hold/Sell classification; yfinance's count is the analyst
    # coverage universe for the price-target aggregation). Populate
    # ``num_analysts`` from yfinance only if Finnhub did not supply one.
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        target = info.get("targetMeanPrice")
        if target is not None:
            result["target_price"] = round(float(target), 2)
        if result["num_analysts"] is None:
            n = info.get("numberOfAnalystOpinions")
            if n is not None:
                result["num_analysts"] = int(n)
    except Exception as e:
        logger.warning("yfinance target_price failed for %s: %s", ticker, e)

    return result


# ---- 2. EPS revisions ----

def _fetch_revisions(ticker: str, bucket: str, run_date: str) -> dict:
    """Fetch current EPS estimate and compute revision vs prior week."""
    result = {
        "current_estimate": None,
        "revision_4w": None,
        "streak": 0,
    }

    # /stable requires period=annual on free tier; quarter is 402 paid.
    try:
        data = _fmp_get(
            "analyst-estimates",
            params={"symbol": ticker, "period": "annual", "limit": 1},
        )
        if isinstance(data, list) and data:
            # /stable renames the field vs v3 — accept either shape.
            result["current_estimate"] = (
                data[0].get("epsAvg")
                or data[0].get("estimatedEpsAvg")
            )
    except Exception as e:
        logger.warning("EPS estimate failed for %s: %s", ticker, e)

    # Load prior snapshot for revision comparison
    try:
        s3 = boto3.client("s3")
        today = datetime.strptime(run_date, "%Y-%m-%d")
        for days_ago in range(7, 15):
            check_date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            try:
                key = f"archive/revisions/{check_date}.json"
                obj = s3.get_object(Bucket=bucket, Key=key)
                prior = json.loads(obj["Body"].read())
                prior_eps = prior.get(ticker, {}).get("eps_current", 0.0)
                if prior_eps and result["current_estimate"]:
                    result["revision_4w"] = round(
                        (result["current_estimate"] - prior_eps) / abs(prior_eps) * 100, 2
                    )
                break
            except Exception:
                continue
    except Exception:
        pass

    return result


# ---- 3. Options flow ----

def _fetch_options(ticker: str, run_date: str) -> dict:
    """Fetch options-derived signals from yfinance."""
    result = {
        "put_call_ratio": None,
        "iv_rank": None,
        "expected_move_pct": None,
    }

    try:
        import yfinance
        import numpy as np

        t = yfinance.Ticker(ticker)
        expiries = t.options
        if not expiries:
            return result

        # Select nearest expiry with 15-60 DTE, prefer ~30 DTE
        today = datetime.strptime(run_date, "%Y-%m-%d")
        best_exp = None
        best_dte = float("inf")
        for exp_str in expiries:
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
                dte = (exp_date - today).days
                if 15 <= dte <= 60 and abs(dte - 30) < abs(best_dte - 30):
                    best_exp = exp_str
                    best_dte = dte
            except ValueError:
                continue

        if not best_exp:
            # Fallback: nearest expiry > 7 DTE
            for exp_str in expiries:
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
                    if (exp_date - today).days > 7:
                        best_exp = exp_str
                        best_dte = (exp_date - today).days
                        break
                except ValueError:
                    continue

        if not best_exp:
            return result

        chain = t.option_chain(best_exp)
        calls, puts = chain.calls, chain.puts

        # Put/call ratio
        put_oi = puts["openInterest"].sum() if "openInterest" in puts.columns else 0
        call_oi = calls["openInterest"].sum() if "openInterest" in calls.columns else 0
        result["put_call_ratio"] = round(put_oi / max(call_oi, 1), 3)

        # ATM IV
        info = t.info if hasattr(t, "info") else {}
        price = info.get("regularMarketPrice") or info.get("previousClose", 0)
        if not price:
            hist = t.history(period="1d")
            price = float(hist["Close"].iloc[-1]) if not hist.empty else 0

        if price > 0 and "strike" in calls.columns and "impliedVolatility" in calls.columns:
            strikes = calls["strike"].values
            if len(strikes) > 0:
                atm_idx = np.abs(strikes - price).argmin()
                atm_iv = float(calls.iloc[atm_idx]["impliedVolatility"])

                # Average with put ATM IV
                if "strike" in puts.columns and "impliedVolatility" in puts.columns:
                    put_strikes = puts["strike"].values
                    if len(put_strikes) > 0:
                        put_atm_idx = np.abs(put_strikes - price).argmin()
                        atm_iv = (atm_iv + float(puts.iloc[put_atm_idx]["impliedVolatility"])) / 2

                # IV rank approximation via realized vol
                try:
                    hist = t.history(period="1y")
                    if not hist.empty and len(hist) >= 30:
                        returns = hist["Close"].pct_change().dropna()
                        rolling_vol = returns.rolling(20).std() * np.sqrt(252)
                        rolling_vol = rolling_vol.dropna()
                        if len(rolling_vol) >= 10:
                            result["iv_rank"] = round(
                                float((rolling_vol < atm_iv).sum() / len(rolling_vol) * 100), 1
                            )
                except Exception:
                    pass

                # Expected move
                if atm_iv > 0 and best_dte > 0:
                    result["expected_move_pct"] = round(
                        atm_iv * np.sqrt(best_dte / 365) * 100, 2
                    )

    except ImportError:
        logger.debug("yfinance/numpy not available for options data")
    except Exception as e:
        logger.warning("Options fetch failed for %s: %s", ticker, e)

    return result


# ---- 4. Insider activity ----

_EDGAR_BASE = "https://data.sec.gov"
_SEC_RATE_DELAY = 0.25


def _fetch_insider(ticker: str, run_date: str) -> dict:
    """Fetch insider trading data from SEC EDGAR Form 4."""
    result = {
        "cluster_buying": False,
        "net_shares_30d": 0,
        "transactions": [],
    }

    identity = os.environ.get("EDGAR_IDENTITY", "")
    if not identity:
        return result

    headers = {"User-Agent": identity, "Accept": "application/json"}
    today = datetime.strptime(run_date, "%Y-%m-%d")

    # Look up CIK
    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=headers, timeout=15,
        )
        resp.raise_for_status()
        cik = None
        for entry in resp.json().values():
            if entry.get("ticker", "").upper() == ticker.upper():
                cik = str(entry["cik_str"]).zfill(10)
                break
        if not cik:
            return result
        time.sleep(_SEC_RATE_DELAY)
    except Exception:
        return result

    # Get Form 4 filings
    try:
        resp = requests.get(
            f"{_EDGAR_BASE}/submissions/CIK{cik}.json",
            headers=headers, timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        time.sleep(_SEC_RATE_DELAY)
    except Exception:
        return result

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])

    # Count insider buys/sells in last 30 days from filing metadata
    buyers_30d = set()
    net_shares = 0
    transactions = []

    start_date = today - timedelta(days=90)
    for i, form in enumerate(forms):
        if form != "4" or i >= len(dates):
            continue
        filing_date = dates[i]
        try:
            fd = datetime.strptime(filing_date, "%Y-%m-%d")
        except ValueError:
            continue
        if fd < start_date:
            break

        transactions.append({
            "date": filing_date,
            "days_ago": (today - fd).days,
            "form": "4",
        })

    result["transactions"] = transactions[:10]

    return result


# ---- 5. Institutional 13F ----

def _fetch_institutional(ticker: str) -> dict:
    """Fetch institutional accumulation signal from 13F filings."""
    result = {
        "accumulation": False,
        "funds_increasing": 0,
        "funds_decreasing": 0,
    }

    identity = os.environ.get("EDGAR_IDENTITY", "")
    if not identity:
        return result

    try:
        from edgar import set_identity, Company
        set_identity(identity)

        company = Company(ticker)
        filings = company.get_filings(form="13F-HR").latest(5)
        if not filings or len(filings) == 0:
            return result

        n_accumulating = 0
        n_decreasing = 0

        try:
            latest_filing = filings[0]
            thirteen_f = latest_filing.obj()

            if hasattr(thirteen_f, 'previous_holding_report'):
                prev = thirteen_f.previous_holding_report()
                if prev is not None:
                    current_holdings = {
                        h.cusip: h.value for h in thirteen_f.holdings
                    } if hasattr(thirteen_f, 'holdings') else {}
                    prev_holdings = {
                        h.cusip: h.value for h in prev.holdings
                    } if hasattr(prev, 'holdings') else {}

                    for cusip, current_value in current_holdings.items():
                        prev_value = prev_holdings.get(cusip, 0)
                        if current_value and prev_value:
                            if current_value > prev_value:
                                n_accumulating += 1
                            elif current_value < prev_value:
                                n_decreasing += 1
        except Exception as e:
            logger.warning("13F comparison failed for %s: %s", ticker, e)

        result["funds_increasing"] = n_accumulating
        result["funds_decreasing"] = n_decreasing
        result["accumulation"] = n_accumulating >= 3

    except ImportError:
        logger.debug("edgartools not available for 13F data")
    except Exception as e:
        logger.warning("Institutional fetch failed for %s: %s", ticker, e)

    return result


# ---- 6. News ----

def _fetch_news(ticker: str) -> dict:
    """Fetch news from Yahoo RSS and EDGAR 8-K."""
    result = {"articles": [], "sec_filings_8k": []}

    # Yahoo RSS
    try:
        import feedparser
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
        feed = feedparser.parse(url)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
        for entry in feed.entries[:10]:
            try:
                pub = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub:
                    pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
                else:
                    pub_dt = datetime.now(timezone.utc)
                if pub_dt < cutoff:
                    continue
                result["articles"].append({
                    "headline": entry.get("title", "").strip(),
                    "source": entry.get("source", {}).get("title", "Yahoo Finance"),
                    "url": entry.get("link", ""),
                    "published_utc": pub_dt.isoformat(),
                })
            except Exception:
                continue
    except ImportError:
        logger.debug("feedparser not available for news")
    except Exception as e:
        logger.warning("Yahoo RSS failed for %s: %s", ticker, e)

    # EDGAR 8-K
    try:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start_date = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
        url = (
            f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22"
            f"&dateRange=custom&startdt={start_date}&enddt={end_date}&forms=8-K"
        )
        headers = {"User-Agent": "alpha-engine-data/1.0", "Accept-Encoding": "gzip"}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for hit in data.get("hits", {}).get("hits", [])[:5]:
            src = hit.get("_source", {})
            result["sec_filings_8k"].append({
                "title": src.get("display_names", [ticker])[0],
                "date": src.get("file_date", ""),
                "form_type": src.get("form_type", "8-K"),
            })
    except Exception as e:
        logger.warning("EDGAR 8-K failed for %s: %s", ticker, e)

    return result
