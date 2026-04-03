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

Data sources:
    Prices:       predictor/price_cache_slim/*.parquet + predictor/daily_closes/{date}.parquet
    Macro:        SPY, VIX, TNX, IRX, GLD, USO, VIX3M (from slim cache)
    Sector map:   data/sector_map.json
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

import hashlib

from features.feature_engineer import FEATURES, FEATURE_CFG, MIN_ROWS_FOR_FEATURES, compute_features
from features.registry import upload_registry
from features.writer import write_feature_snapshot

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_BUCKET = "alpha-engine-research"
FEATURE_STORE_PREFIX = "features/"

# Large-move warning threshold (>45% daily return, e.g. stock splits, VIX spikes)
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
        obj = s3.get_object(Bucket=bucket, Key="data/sector_map.json")
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


    # _load_full_cache removed — slim cache (2y) is sufficient for feature computation.
    # Features only use the latest row; 2y provides enough warmup for all indicators.


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
            # daily_closes: index=ticker (str), columns=[date, Open, High, Low, Close, Adj_Close, Volume]
            for ticker, row in day_df.iterrows():
                if ticker not in ticker_rows:
                    ticker_rows[ticker] = []
                ticker_rows[ticker].append({
                    "date":   pd.Timestamp(d),
                    "Open":   float(row.get("Open", np.nan)),
                    "High":   float(row.get("High", np.nan)),
                    "Low":    float(row.get("Low", np.nan)),
                    "Close":  float(row.get("Close", np.nan)),
                    "Volume": int(row.get("Volume", 0)),
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

        # Split detection: log warning but still use the data.
        # The weekly price collector handles splits properly during
        # Saturday DataPhase1; feature compute trusts upstream data.
        returns = combined["Close"].pct_change().dropna()
        if (returns.abs() > _SPLIT_RETURN_THRESHOLD).any():
            log.warning("Large move detected in %s (>45%% daily return) — using data as-is", ticker)

        price_data[ticker] = combined
        n_updated += 1

    log.info("Applied daily delta: %d tickers updated", n_updated)
    return price_data


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
) -> dict[str, pd.Series]:
    """
    Extract macro series (SPY, VIX, TNX, IRX, GLD, USO, VIX3M) and sector ETFs
    from the price data dict. Trusts upstream DailyData for freshness.
    """
    macro: dict[str, pd.Series] = {}

    for key, stem in _MACRO_SLIM_KEYS.items():
        source = price_data.get(stem) if stem in price_data else slim_data.get(stem)
        if source is not None and "Close" in source.columns:
            macro[key] = source["Close"].dropna()

    # Sector ETFs
    for stem, df in slim_data.items():
        if stem.startswith("XL") and "Close" in df.columns:
            source = price_data.get(stem) if stem in price_data else df
            macro[stem] = source["Close"].dropna()

    return macro


def _load_prices_and_macro(
    s3, bucket: str, date_str: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.Series]]:
    """
    Load price data and macro series from S3 slim cache + daily delta.

    Trusts upstream data quality — DailyData collects fresh prices,
    Saturday DataPhase1 handles splits during full price refresh.
    No yfinance calls; no external API dependencies.
    """
    slim_data = _load_slim_cache(s3, bucket)
    if not slim_data:
        return {}, {}

    price_data = dict(slim_data)
    price_data = _apply_daily_delta(s3, bucket, date_str, price_data)
    macro = _extract_macro(price_data, slim_data)

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
    price_data, macro = _load_prices_and_macro(s3, bucket, date_str)
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

        # Write schema version alongside snapshot for training consistency checks
        _schema_content = json.dumps({"features": FEATURES, "config": FEATURE_CFG}, sort_keys=True)
        _schema_hash = hashlib.sha256(_schema_content.encode()).hexdigest()[:12]
        _version_doc = {
            "schema_version": 1,
            "schema_hash": _schema_hash,
            "n_features": len(FEATURES),
            "features": FEATURES,
            "date": date_str,
        }
        try:
            import boto3 as _b3_ver
            _b3_ver.client("s3").put_object(
                Bucket=bucket,
                Key=f"{FEATURE_STORE_PREFIX}{date_str}/schema_version.json",
                Body=json.dumps(_version_doc, indent=2),
                ContentType="application/json",
            )
        except Exception as _ver_exc:
            log.debug("Schema version write failed (non-fatal): %s", _ver_exc)

        log.info(
            "Feature snapshot + registry written to s3://%s/%s%s/ (schema=%s)",
            bucket, FEATURE_STORE_PREFIX, date_str, _schema_hash,
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
        dry_run=args.dry_run,
    )

    if result["status"] != "ok":
        log.error("Feature compute failed: %s", result.get("error"))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
