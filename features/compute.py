"""
features/compute.py — Standalone feature computation for the full universe.

Decouples feature computation from the predictor module entirely. Loads
price + macro data from S3 (slim cache + daily_closes delta), computes all
53 features for every ticker in the universe, and writes dated Parquet
snapshots to S3.

NO imports from alpha-engine-predictor. All S3 loading is self-contained.

Usage:
    python -m features.compute                          # today's date
    python -m features.compute --date 2026-04-03        # specific date
    python -m features.compute --dry-run                # compute but skip S3 write
    python -m features.compute --source full            # use full 10y cache (Saturday)

Data sources:
    Prices:       predictor/price_cache_slim/*.parquet + predictor/daily_closes/{date}.parquet
    Macro:        SPY, VIX, TNX, IRX, GLD, USO, VIX3M (from slim cache)
    Sector map:   predictor/price_cache/sector_map.json
    Fundamentals: archive/fundamentals/{date}.json (cached by prior inference)
    Alt data:     market_data/weekly/{latest}/alternative/{TICKER}.json (from DataPhase2)
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from features.feature_engineer import FEATURES, MIN_ROWS_FOR_FEATURES, compute_features
from features.registry import upload_registry
from features.writer import write_feature_snapshot

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_BUCKET = "alpha-engine-research"
FEATURE_STORE_PREFIX = "features/"

# Split detection: single-day return beyond ±45% triggers yfinance re-fetch
# (matches predictor's config.SPLIT_RETURN_THRESHOLD)
_SPLIT_RETURN_THRESHOLD = 0.45

# Tickers that are macro/index series, not stocks
_SKIP_TICKERS = {
    "SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
    "^VIX", "^VIX3M", "^TNX", "^IRX",
}
# Sector ETFs to skip (not individual stocks)
_SECTOR_ETF_PREFIXES = {"XL"}


def _is_sector_etf(ticker: str) -> bool:
    return len(ticker) == 3 and ticker[:2] in _SECTOR_ETF_PREFIXES


# ── S3 data loading (self-contained, no predictor imports) ───────────────────

def _load_sector_map(s3, bucket: str) -> dict[str, str]:
    """Load ticker -> sector ETF mapping from S3."""
    try:
        obj = s3.get_object(Bucket=bucket, Key="predictor/price_cache/sector_map.json")
        return json.loads(obj["Body"].read())
    except Exception as exc:
        log.warning("Failed to load sector_map.json: %s", exc)
        return {}


def _load_parquet_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    """Download a single parquet file from S3 and return as DataFrame.

    Normalizes to timezone-naive UTC DatetimeIndex, matching the predictor's
    ``_load_ticker_parquet`` behaviour so that downstream reindex/join calls
    don't raise TypeError when mixing tz-aware and tz-naive indices.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buf, engine="pyarrow")
    # Ensure DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        else:
            df.index = pd.to_datetime(df.index)
    # Normalize timezone: convert to UTC then strip tz info (match predictor)
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)
    # Ensure sorted
    if isinstance(df.index, pd.DatetimeIndex) and not df.index.is_monotonic_increasing:
        df = df.sort_index()
    return df


def _load_slim_cache(s3, bucket: str) -> dict[str, pd.DataFrame]:
    """
    Load all parquets from predictor/price_cache_slim/ using concurrent downloads.

    Returns dict of ticker -> DataFrame (OHLCV with DatetimeIndex).
    """
    prefix = "predictor/price_cache_slim/"

    # List all parquet keys
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        log.warning("No parquets found in s3://%s/%s", bucket, prefix)
        return {}

    log.info("Downloading %d slim cache parquets...", len(keys))

    price_data: dict[str, pd.DataFrame] = {}
    errors = 0

    def _download(key: str) -> tuple[str, pd.DataFrame | None]:
        ticker = key.split("/")[-1].replace(".parquet", "")
        try:
            df = _load_parquet_from_s3(s3, bucket, key)
            if df.empty:
                return ticker, None
            return ticker, df
        except Exception:
            return ticker, None

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_download, k): k for k in keys}
        for fut in as_completed(futures):
            ticker, df = fut.result()
            if df is not None:
                price_data[ticker] = df
            else:
                errors += 1

    log.info("Slim cache loaded: %d tickers OK, %d errors", len(price_data), errors)
    return price_data


def _load_full_cache(s3, bucket: str) -> dict[str, pd.DataFrame]:
    """
    Load all parquets from predictor/price_cache/ (full 10y cache).

    Same approach as slim but from the full cache prefix.
    """
    prefix = "predictor/price_cache/"

    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and "slim" not in key:
                keys.append(key)

    if not keys:
        log.warning("No parquets found in s3://%s/%s", bucket, prefix)
        return {}

    log.info("Downloading %d full cache parquets...", len(keys))

    price_data: dict[str, pd.DataFrame] = {}
    errors = 0

    def _download(key: str) -> tuple[str, pd.DataFrame | None]:
        ticker = key.split("/")[-1].replace(".parquet", "")
        try:
            df = _load_parquet_from_s3(s3, bucket, key)
            if df.empty:
                return ticker, None
            return ticker, df
        except Exception:
            return ticker, None

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_download, k): k for k in keys}
        for fut in as_completed(futures):
            ticker, df = fut.result()
            if df is not None:
                price_data[ticker] = df
            else:
                errors += 1

    log.info("Full cache loaded: %d tickers OK, %d errors", len(price_data), errors)
    return price_data


def _safe_last_date(idx: pd.Index) -> pd.Timestamp | None:
    """Return the normalized last date from a DatetimeIndex, or None if empty/NaT."""
    if idx is None or idx.empty:
        return None
    last = idx.max()
    if pd.isna(last):
        return None
    return pd.Timestamp(last).normalize()


def _load_delta_from_daily_closes(
    s3, bucket: str, start_date: pd.Timestamp, end_date: pd.Timestamp,
) -> dict[str, list[dict]]:
    """
    Load daily_closes parquets for every trading day in (start_date, end_date].

    The daily_closes format has index=ticker (string) and columns including
    date, open, high, low, close, adj_close, volume (all lowercase).

    Returns dict: ticker -> list of row dicts with capitalized OHLCV keys.
    """
    delta_dates = [
        d.strftime("%Y-%m-%d")
        for d in pd.bdate_range(start_date + pd.Timedelta(days=1), end_date)
    ]

    if not delta_dates:
        return {}

    log.info(
        "Loading daily_closes delta: %d trading days (%s -> %s)",
        len(delta_dates), delta_dates[0], delta_dates[-1],
    )

    ticker_rows: dict[str, list[dict]] = {}

    for d in delta_dates:
        key = f"predictor/daily_closes/{d}.parquet"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            day_df = pd.read_parquet(buf, engine="pyarrow")
            # daily_closes: index=ticker (str), columns=[date, open, high, low, close, adj_close, volume]
            for ticker, row in day_df.iterrows():
                if ticker not in ticker_rows:
                    ticker_rows[ticker] = []
                ticker_rows[ticker].append({
                    "date":   pd.Timestamp(d),
                    "Open":   float(row.get("open",      row.get("Open",      np.nan))),
                    "High":   float(row.get("high",      row.get("High",      np.nan))),
                    "Low":    float(row.get("low",       row.get("Low",       np.nan))),
                    "Close":  float(row.get("close",     row.get("Close",     np.nan))),
                    "Volume": int(row.get("volume",      row.get("Volume",    0))),
                })
        except Exception:
            log.debug("daily_closes/%s.parquet not found in S3 (non-trading day?)", d)

    n_tickers = len(ticker_rows)
    n_rows = sum(len(v) for v in ticker_rows.values())
    log.info("Delta loaded: %d rows across %d tickers", n_rows, n_tickers)
    return ticker_rows


def _apply_daily_delta(
    s3, bucket: str, date_str: str, price_data: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], set[str]]:
    """
    Append daily_closes delta rows to price DataFrames.

    Matches the predictor's ``load_price_data_from_cache`` behaviour:
    1. Loads ALL daily_closes files between the slim cache's last date and
       the target date (not just the target date's file).
    2. Uses ``duplicated(keep='last')`` so delta rows override cache rows
       on the same date.
    3. Detects splits (>45% single-day return) and returns those tickers
       for yfinance re-fetch.

    Returns (updated_price_data, split_tickers).
    """
    # Find the slim cache's last date (most common across all tickers)
    candidate_dates = [_safe_last_date(df.index) for df in price_data.values()]
    valid_dates = [d for d in candidate_dates if d is not None]
    if not valid_dates:
        return price_data, set()

    slim_last_date = max(valid_dates)
    today = pd.Timestamp(date_str).normalize()

    # Load all delta files between slim cache last date and target date
    ticker_rows = _load_delta_from_daily_closes(s3, bucket, slim_last_date, today)

    if not ticker_rows:
        log.info("No daily_closes delta files found — using cache as-is")
        return price_data, set()

    split_tickers: set[str] = set()
    n_updated = 0

    for ticker, slim_df in list(price_data.items()):
        base_cols = ["Open", "High", "Low", "Close", "Volume"]
        base = slim_df[[c for c in base_cols if c in slim_df.columns]].copy()

        delta = ticker_rows.get(ticker, [])
        if not delta:
            price_data[ticker] = base
            continue

        # Build delta DataFrame with capitalized columns (matches slim cache schema)
        delta_df = pd.DataFrame(
            [{k: r[k] for k in ["Open", "High", "Low", "Close", "Volume"]} for r in delta],
            index=pd.DatetimeIndex([r["date"] for r in delta]),
        )

        combined = pd.concat([base, delta_df])
        # keep="last" so delta rows win on duplicate dates (matches predictor)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()

        # Split detection: any single-day Close return beyond threshold
        returns = combined["Close"].pct_change().dropna()
        if (returns.abs() > _SPLIT_RETURN_THRESHOLD).any():
            log.info(
                "Split/large-move detected in %s — will re-fetch from yfinance", ticker,
            )
            split_tickers.add(ticker)
        else:
            price_data[ticker] = combined
            n_updated += 1

    log.info("Applied daily delta: %d tickers updated, %d splits detected", n_updated, len(split_tickers))
    return price_data, split_tickers


_MACRO_SLIM_KEYS = {
    "SPY": "SPY",
    "VIX": "VIX",     # stored as VIX, yfinance ticker is ^VIX
    "VIX3M": "VIX3M", # stored as VIX3M, yfinance ticker is ^VIX3M
    "TNX": "TNX",     # stored as TNX, yfinance ticker is ^TNX
    "IRX": "IRX",
    "GLD": "GLD",
    "USO": "USO",
}


def _extract_macro(
    price_data: dict[str, pd.DataFrame],
    slim_data: dict[str, pd.DataFrame],
    today: pd.Timestamp,
) -> tuple[dict[str, pd.Series], list[str]]:
    """
    Extract macro series (SPY, VIX, TNX, IRX, GLD, USO, VIX3M) and sector ETFs
    from the price data dict.

    Also detects stale macro series (last date >1 day behind target) that need
    yfinance re-fetch, matching the predictor's behaviour.

    Returns (macro_dict, stale_yf_tickers).
    """
    macro: dict[str, pd.Series] = {}
    stale_macros: list[str] = []

    for key, stem in _MACRO_SLIM_KEYS.items():
        source = price_data.get(stem) if stem in price_data else slim_data.get(stem)
        if source is not None and "Close" in source.columns:
            last_date = _safe_last_date(source.index)
            if last_date is None:
                log.warning("Macro series %s has empty/NaT index — skipping", key)
                continue
            if (today - last_date).days > 1:
                yf_sym = f"^{stem}" if stem in ("VIX", "VIX3M", "TNX", "IRX") else stem
                stale_macros.append(yf_sym)
            macro[key] = source["Close"].dropna()
        else:
            log.debug("Macro series %s not in price data or slim cache", key)

    # Sector ETFs
    for stem, df in slim_data.items():
        if stem.startswith("XL") and "Close" in df.columns:
            source = price_data.get(stem) if stem in price_data else df
            last_date = _safe_last_date(source.index)
            if last_date is None:
                continue
            if (today - last_date).days > 1:
                stale_macros.append(stem)
            macro[stem] = source["Close"].dropna()

    return macro, stale_macros


def _yf_fetch_tickers(tickers: list[str], period: str = "2y") -> dict[str, pd.DataFrame]:
    """
    Fetch price data from yfinance for a list of tickers.

    Used as fallback for split-detected and missing tickers, matching the
    predictor's ``fetch_today_prices`` behaviour.
    """
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance not installed — cannot re-fetch split/stale tickers")
        return {}

    log.info("Fetching %d tickers from yfinance (period=%s)...", len(tickers), period)
    result: dict[str, pd.DataFrame] = {}

    batch_size = 50
    batches = [tickers[i : i + batch_size] for i in range(0, len(tickers), batch_size)]

    for batch in batches:
        try:
            if len(batch) == 1:
                raw = yf.download(
                    batch[0], period=period, interval="1d",
                    auto_adjust=True, progress=False,
                )
                raw.index = pd.to_datetime(raw.index)
                if hasattr(raw.index, "tz") and raw.index.tz is not None:
                    raw.index = raw.index.tz_convert("UTC").tz_localize(None)
                raw = raw.dropna(subset=["Close"])
                result[batch[0]] = raw
            else:
                raw = yf.download(
                    tickers=batch, period=period, interval="1d",
                    auto_adjust=True, progress=False,
                    group_by="ticker", threads=True,
                )
                for ticker in batch:
                    try:
                        df = raw[ticker].copy()
                        df.index = pd.to_datetime(df.index)
                        if hasattr(df.index, "tz") and df.index.tz is not None:
                            df.index = df.index.tz_convert("UTC").tz_localize(None)
                        df = df.dropna(subset=["Close"])
                        result[ticker] = df
                    except (KeyError, AttributeError):
                        result[ticker] = pd.DataFrame()
        except Exception as exc:
            log.warning("yfinance batch fetch failed: %s", exc)

    n_ok = sum(1 for df in result.values() if not df.empty)
    log.info("yfinance fetch complete: %d / %d succeeded", n_ok, len(tickers))
    return result


def _load_prices_and_macro(
    s3, bucket: str, date_str: str, source: str = "slim",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.Series]]:
    """
    Load price data and macro series from S3 cache.

    Matches the predictor's ``load_price_data_from_cache`` behaviour:
    1. Download slim (or full) cache parquets with timezone normalization.
    2. Load ALL daily_closes delta files between cache last date and target.
    3. Merge with keep='last' so delta rows override cache.
    4. Detect splits (>45% single-day return) and re-fetch from yfinance.
    5. Build macro dict, detect stale series, re-fetch from yfinance.

    Parameters
    ----------
    s3 : boto3 S3 client
    bucket : S3 bucket name
    date_str : Target date (YYYY-MM-DD)
    source : "slim" (2y slim cache, default) or "full" (10y full cache)

    Returns
    -------
    (price_data, macro) — dict of ticker->DataFrame, dict of key->Series
    """
    today = pd.Timestamp(date_str).normalize()

    if source == "slim":
        slim_data = _load_slim_cache(s3, bucket)
    elif source == "full":
        slim_data = _load_full_cache(s3, bucket)
    else:
        log.error("Unknown source: %s (expected 'slim' or 'full')", source)
        return {}, {}

    if not slim_data:
        return {}, {}

    # Start with a copy of slim data as the working price_data
    price_data = dict(slim_data)

    # Apply daily delta (loads all files, handles splits)
    price_data, split_tickers = _apply_daily_delta(s3, bucket, date_str, price_data)

    # Re-fetch split tickers from yfinance (with clean adjusted prices)
    if split_tickers:
        log.info(
            "Re-fetching %d split/large-move tickers from yfinance: %s",
            len(split_tickers), sorted(split_tickers)[:10],
        )
        # Map bare macro tickers to yfinance caret format
        _CARET_MAP = {"VIX": "^VIX", "VIX3M": "^VIX3M", "TNX": "^TNX", "IRX": "^IRX"}
        yf_tickers = [_CARET_MAP.get(t, t) for t in sorted(split_tickers)]
        fresh = _yf_fetch_tickers(yf_tickers)
        for yf_sym, df in fresh.items():
            if not df.empty:
                cache_key = yf_sym.lstrip("^")
                price_data[cache_key] = df

    # Extract macro series (with staleness detection)
    macro, stale_macros = _extract_macro(price_data, slim_data, today)

    # Re-fetch stale macro series from yfinance
    if stale_macros:
        log.info("Re-fetching %d stale macro series from yfinance: %s", len(stale_macros), stale_macros[:10])
        fresh_macro = _yf_fetch_tickers(stale_macros)
        for yf_sym, fresh_df in fresh_macro.items():
            if fresh_df.empty:
                continue
            cache_key = yf_sym.lstrip("^")
            # Find the macro dict key
            macro_key = cache_key
            if cache_key in _MACRO_SLIM_KEYS.values():
                macro_key = next(k for k, v in _MACRO_SLIM_KEYS.items() if v == cache_key)
            if "Close" in fresh_df.columns:
                macro[macro_key] = fresh_df["Close"].dropna()
                price_data[cache_key] = fresh_df
        log.info("Macro refresh complete: %d series updated", len(fresh_macro))

    return price_data, macro


def _load_cached_fundamentals(s3, bucket: str, date_str: str) -> dict[str, dict]:
    """Load cached fundamental data from S3 (written by prior inference)."""
    # Try exact date, then scan for most recent
    for key in [
        f"archive/fundamentals/{date_str}.json",
    ]:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read())
            log.info("Loaded cached fundamentals from s3://%s/%s (%d tickers)", bucket, key, len(data))
            return data
        except Exception:
            pass

    # Scan for most recent fundamentals file
    try:
        resp = s3.list_objects_v2(
            Bucket=bucket, Prefix="archive/fundamentals/", MaxKeys=100,
        )
        keys = sorted(
            [c["Key"] for c in resp.get("Contents", []) if c["Key"].endswith(".json")],
            reverse=True,
        )
        if keys:
            obj = s3.get_object(Bucket=bucket, Key=keys[0])
            data = json.loads(obj["Body"].read())
            log.info("Loaded cached fundamentals from s3://%s/%s (%d tickers)", bucket, keys[0], len(data))
            return data
    except Exception as exc:
        log.warning("Failed to scan for cached fundamentals: %s", exc)

    log.info("No cached fundamentals found — fundamental features will use defaults")
    return {}


def _load_cached_alternative(s3, bucket: str) -> dict[str, dict]:
    """Load cached alternative data from the most recent DataPhase2 output."""
    try:
        # Find latest weekly date
        obj = s3.get_object(Bucket=bucket, Key="market_data/latest_weekly.json")
        latest = json.loads(obj["Body"].read())
        latest_date = latest.get("date", "")
        prefix = f"market_data/weekly/{latest_date}/alternative/"

        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=200)
        contents = resp.get("Contents", [])

        alt_data: dict[str, dict] = {}
        for item in contents:
            key = item["Key"]
            if key.endswith("manifest.json") or not key.endswith(".json"):
                continue
            ticker = key.split("/")[-1].replace(".json", "")
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                ticker_data = json.loads(obj["Body"].read())
                alt_data[ticker] = {
                    "earnings": {
                        "surprise_pct": ticker_data.get("eps_revision", {}).get("surprise_pct",
                                        ticker_data.get("analyst_consensus", {}).get("surprise_pct", 0.0)),
                        "days_since_earnings": ticker_data.get("eps_revision", {}).get("days_since_earnings", 0.0),
                    },
                    "revisions": {
                        "eps_revision_4w": ticker_data.get("eps_revision", {}).get("revision_4w", 0.0),
                        "revision_streak": ticker_data.get("eps_revision", {}).get("streak", 0),
                    },
                    "options": {
                        "put_call_ratio": ticker_data.get("options_flow", {}).get("put_call_ratio"),
                        "iv_rank": ticker_data.get("options_flow", {}).get("iv_rank"),
                        "atm_iv": ticker_data.get("options_flow", {}).get("expected_move_pct"),
                    },
                }
            except Exception:
                pass

        if alt_data:
            log.info("Loaded cached alternative data for %d tickers from %s", len(alt_data), latest_date)
        return alt_data

    except Exception as exc:
        log.debug("No cached alternative data available: %s", exc)
        return {}


# ── Main computation ─────────────────────────────────────────────────────────

def compute_and_write(
    date_str: str,
    bucket: str = DEFAULT_BUCKET,
    source: str = "slim",
    dry_run: bool = False,
) -> dict:
    """
    Compute all 53 features for the full universe and write to S3.

    Returns summary dict with counts and timing.
    """
    import boto3

    s3 = boto3.client("s3")
    t0 = time.time()

    # ── 1. Load data ─────────────────────────────────────────────────────────
    price_data, macro = _load_prices_and_macro(s3, bucket, date_str, source)
    if not price_data:
        log.error("No price data loaded — cannot compute features")
        return {"status": "error", "error": "no_price_data"}

    sector_map = _load_sector_map(s3, bucket)
    fundamentals = _load_cached_fundamentals(s3, bucket, date_str)
    alt_data = _load_cached_alternative(s3, bucket)

    t_load = time.time() - t0
    log.info(
        "Data loaded in %.1fs: %d tickers, %d macro series, %d sector mappings, "
        "%d fundamentals, %d alt data",
        t_load, len(price_data), len(macro), len(sector_map),
        len(fundamentals), len(alt_data),
    )

    # ── 2. Compute features for each ticker ──────────────────────────────────
    store_rows: list[dict] = []
    n_ok = 0
    n_skip = 0
    n_err = 0

    # Filter to stock tickers only
    universe_tickers = [
        t for t in price_data
        if t not in _SKIP_TICKERS
        and not _is_sector_etf(t)
        and price_data[t] is not None
        and len(price_data[t]) >= MIN_ROWS_FOR_FEATURES
    ]

    log.info("Computing features for %d tickers...", len(universe_tickers))

    # Extract macro series once
    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")

    for ticker in universe_tickers:
        try:
            df = price_data[ticker]
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None

            # Get alt data for this ticker (if available)
            ticker_alt = alt_data.get(ticker, {})
            earnings_data = ticker_alt.get("earnings")
            revision_data = ticker_alt.get("revisions")
            options_data = ticker_alt.get("options")
            fundamental_data = fundamentals.get(ticker)

            featured_df = compute_features(
                df,
                spy_series=spy_series,
                vix_series=vix_series,
                sector_etf_series=sector_etf_series,
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                earnings_data=earnings_data,
                revision_data=revision_data,
                options_data=options_data,
                fundamental_data=fundamental_data,
            )

            if featured_df.empty:
                n_skip += 1
                continue

            latest = featured_df.iloc[-1]
            row = {"ticker": ticker}
            for f in FEATURES:
                val = latest[f] if f in latest.index else 0.0
                row[f] = float(val) if pd.notna(val) else 0.0
            store_rows.append(row)
            n_ok += 1

        except Exception as exc:
            log.debug("Feature computation failed for %s: %s", ticker, exc)
            n_err += 1

    t_compute = time.time() - t0 - t_load
    log.info(
        "Feature computation complete in %.1fs: %d OK, %d skipped, %d errors",
        t_compute, n_ok, n_skip, n_err,
    )

    if not store_rows:
        log.error("No features computed — nothing to write")
        return {"status": "error", "error": "no_features_computed"}

    # ── 3. Write to S3 ───────────────────────────────────────────────────────
    features_df = pd.DataFrame(store_rows)

    if dry_run:
        log.info(
            "[dry-run] Would write feature snapshot: %d tickers, %d features, date=%s",
            len(features_df), len(FEATURES), date_str,
        )
        summary = {
            "groups": {
                g: len(features_df)
                for g in ["technical", "macro", "interaction", "alternative", "fundamental"]
            },
        }
    else:
        summary = write_feature_snapshot(
            date_str, features_df, bucket,
            prefix=FEATURE_STORE_PREFIX,
        )
        upload_registry(bucket, prefix=FEATURE_STORE_PREFIX)
        log.info(
            "Feature snapshot + registry written to s3://%s/%s%s/",
            bucket, FEATURE_STORE_PREFIX, date_str,
        )

    t_total = time.time() - t0

    result = {
        "status": "ok",
        "date": date_str,
        "tickers_computed": n_ok,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "groups_written": summary,
        "load_seconds": round(t_load, 1),
        "compute_seconds": round(t_compute, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
    }

    log.info("Feature store compute complete: %s", json.dumps(result, default=str))
    return result


# ── CLI entry point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compute and write feature store snapshots to S3",
    )
    parser.add_argument(
        "--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Target date (YYYY-MM-DD, default: today UTC)",
    )
    parser.add_argument(
        "--source", choices=["slim", "full"], default="slim",
        help="Price data source: slim (2y cache, default) or full (10y cache)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute features but skip S3 write",
    )
    parser.add_argument(
        "--bucket", default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = compute_and_write(
        date_str=args.date,
        bucket=args.bucket,
        source=args.source,
        dry_run=args.dry_run,
    )

    if result["status"] != "ok":
        log.error("Feature compute failed: %s", result.get("error"))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
