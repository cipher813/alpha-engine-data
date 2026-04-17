"""
builders/backfill.py — Historical backfill of ArcticDB universe from S3 price cache.

Loads the full 10-year price cache from S3, computes all 53 features for every
ticker's full history, and writes each ticker as a symbol in the ArcticDB
universe library. Also writes macro features to the macro library.

This is a one-time migration script (Phase 1 of the unified data layer plan).
After initial backfill, the weekly Saturday pipeline rebuilds from fresh data,
and the daily weekday pipeline appends new rows.

Usage:
    python -m builders.backfill                          # full backfill
    python -m builders.backfill --dry-run                # compute but skip ArcticDB write
    python -m builders.backfill --ticker AAPL            # single ticker (for testing)
    python -m builders.backfill --validate               # backfill + spot-check validation
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

import boto3
import numpy as np
import pandas as pd

from features.feature_engineer import (
    FEATURES,
    MIN_ROWS_FOR_FEATURES,
    compute_features,
)
from features.compute import (
    DEFAULT_BUCKET,
    _SKIP_TICKERS,
    _is_sector_etf,
    _load_parquet_from_s3,
    _load_sector_map,
    _load_cached_fundamentals,
    _load_cached_alternative,
)
from store.arctic_store import get_universe_lib, get_macro_lib

log = logging.getLogger(__name__)

# OHLCV columns to keep alongside features in ArcticDB.
# VWAP added 2026-04-17 — backfill source (``predictor/price_cache/*.parquet``)
# is OHLCV only, so historical VWAP is populated via the ``(H+L+C)/3``
# typical-price proxy below (same proxy used in ``collectors/daily_closes.py:293``
# when yfinance is the source and polygon's true ``vw`` isn't available).
# Production daily_append uses polygon's true VWAP when available — proxy is
# only used for historical backfill rows predating polygon adoption.
OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume", "VWAP"]


def _ensure_vwap_column(df: pd.DataFrame) -> pd.DataFrame:
    """Populate VWAP column with ``(H+L+C)/3`` typical-price proxy if absent.

    The legacy ``predictor/price_cache/`` parquets used by backfill are OHLCV
    only. Populating a proxy here ensures ArcticDB's VWAP column has coverage
    across the full 10y history, so downstream consumers never encounter
    unexpected NaN for pre-polygon dates.
    """
    if "VWAP" in df.columns:
        return df
    required = {"High", "Low", "Close"}
    if not required.issubset(df.columns):
        return df
    out = df.copy()
    out["VWAP"] = ((out["High"] + out["Low"] + out["Close"]) / 3.0).round(4)
    return out


def _load_full_cache(s3, bucket: str, prefix: str = "predictor/price_cache/") -> dict[str, pd.DataFrame]:
    """Load all 10-year price cache parquets from S3 (concurrent)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        log.error("No parquets found in s3://%s/%s", bucket, prefix)
        return {}

    log.info("Downloading %d full cache parquets from s3://%s/%s ...", len(keys), bucket, prefix)

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


def _extract_macro_series(price_data: dict[str, pd.DataFrame]) -> dict[str, pd.Series]:
    """Extract macro/ETF Close series from price data."""
    macro_keys = {
        "SPY": "SPY", "VIX": "VIX", "VIX3M": "VIX3M",
        "TNX": "TNX", "IRX": "IRX", "GLD": "GLD", "USO": "USO",
    }
    macro: dict[str, pd.Series] = {}
    for key, stem in macro_keys.items():
        df = price_data.get(stem)
        if df is not None and "Close" in df.columns:
            macro[key] = df["Close"].dropna()

    # Sector ETFs
    for stem, df in price_data.items():
        if stem.startswith("XL") and len(stem) <= 4 and "Close" in df.columns:
            macro[stem] = df["Close"].dropna()

    return macro


def _build_macro_features_df(macro: dict[str, pd.Series]) -> pd.DataFrame:
    """Build a DataFrame of macro features (one row per date) for the macro library."""
    vix = macro.get("VIX")
    tnx = macro.get("TNX")
    irx = macro.get("IRX")
    gld = macro.get("GLD")
    uso = macro.get("USO")
    vix3m = macro.get("VIX3M")
    spy = macro.get("SPY")

    if vix is None or spy is None:
        log.warning("Missing VIX or SPY — macro features will be incomplete")
        return pd.DataFrame()

    # Build on the VIX index (available for all trading dates)
    idx = vix.index
    df = pd.DataFrame(index=idx)

    df["vix_level"] = (vix.reindex(idx) / 20.0).astype("float32")
    if tnx is not None:
        df["yield_10y"] = (tnx.reindex(idx) / 10.0).astype("float32")
    if tnx is not None and irx is not None:
        df["yield_curve_slope"] = ((tnx.reindex(idx) - irx.reindex(idx)) / 10.0).astype("float32")
    if gld is not None:
        df["gold_mom_5d"] = gld.reindex(idx).pct_change(5).astype("float32")
    if uso is not None:
        df["oil_mom_5d"] = uso.reindex(idx).pct_change(5).astype("float32")
    if vix3m is not None:
        vix_r = vix.reindex(idx)
        vix3m_r = vix3m.reindex(idx)
        df["vix_term_slope"] = ((vix3m_r - vix_r) / vix_r.clip(lower=1.0)).astype("float32")

    # Cross-sectional dispersion placeholder (requires per-ticker returns, set to 0)
    df["xsect_dispersion"] = np.float32(0.0)

    df = df.dropna(subset=["vix_level"])
    df.index.name = "date"
    return df


def backfill(
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
    ticker_filter: str | None = None,
    validate: bool = False,
) -> dict:
    """
    Run the full historical backfill: load 10y prices, compute features, write to ArcticDB.

    Args:
        bucket: S3 bucket name
        dry_run: compute but skip ArcticDB writes
        ticker_filter: if set, only process this single ticker (for testing)
        validate: if True, run spot-check validation after backfill

    Returns:
        Summary dict with counts and timing.
    """
    s3 = boto3.client("s3")
    t0 = time.time()

    # ── 1. Load data ─────────────────────────────────────────────────────────
    log.info("Loading full 10-year price cache...")
    price_data = _load_full_cache(s3, bucket)
    if not price_data:
        return {"status": "error", "error": "no_price_data"}

    macro = _extract_macro_series(price_data)
    sector_map = _load_sector_map(s3, bucket)

    # Use today's date for fundamentals/alt data lookup
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fundamentals = _load_cached_fundamentals(s3, bucket, today_str)
    alt_data = _load_cached_alternative(s3, bucket)

    t_load = time.time() - t0
    log.info(
        "Data loaded in %.1fs: %d tickers, %d macro series, %d sector mappings",
        t_load, len(price_data), len(macro), len(sector_map),
    )

    # ── 2. Filter to stock tickers ───────────────────────────────────────────
    universe_tickers = [
        t for t in price_data
        if t not in _SKIP_TICKERS
        and not _is_sector_etf(t)
        and price_data[t] is not None
        and len(price_data[t]) >= MIN_ROWS_FOR_FEATURES
    ]

    if ticker_filter:
        if ticker_filter not in universe_tickers:
            log.error("Ticker %s not found in universe (or insufficient history)", ticker_filter)
            return {"status": "error", "error": f"ticker_not_found: {ticker_filter}"}
        universe_tickers = [ticker_filter]

    log.info("Computing features for %d tickers...", len(universe_tickers))

    # ── 3. Extract macro series ──────────────────────────────────────────────
    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")

    # ── 4. Compute features and write to ArcticDB ────────────────────────────
    if not dry_run:
        universe_lib = get_universe_lib(bucket)
        macro_lib = get_macro_lib(bucket)

    n_ok = 0
    n_skip = 0
    n_err = 0
    t_compute_start = time.time()

    for i, ticker in enumerate(universe_tickers):
        try:
            df = _ensure_vwap_column(price_data[ticker])
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None

            ticker_alt = alt_data.get(ticker, {})

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
                earnings_data=ticker_alt.get("earnings"),
                revision_data=ticker_alt.get("revisions"),
                options_data=ticker_alt.get("options"),
                fundamental_data=fundamentals.get(ticker),
            )

            if featured_df.empty:
                n_skip += 1
                continue

            # Keep OHLCV + all feature columns
            keep_cols = [c for c in OHLCV_COLS if c in featured_df.columns] + \
                        [f for f in FEATURES if f in featured_df.columns]
            symbol_df = featured_df[keep_cols].copy()

            # Ensure float32 for feature columns (OHLCV stays float64/int64)
            for f in FEATURES:
                if f in symbol_df.columns:
                    symbol_df[f] = symbol_df[f].astype("float32")

            symbol_df.index.name = "date"

            if not dry_run:
                universe_lib.write(ticker, symbol_df)

            n_ok += 1

            if (i + 1) % 100 == 0:
                log.info("Progress: %d / %d tickers processed (%d OK)", i + 1, len(universe_tickers), n_ok)

        except Exception as exc:
            log.warning("Failed to compute features for %s: %s", ticker, exc)
            n_err += 1

    t_compute = time.time() - t_compute_start

    # ── 5. Write macro features ──────────────────────────────────────────────
    macro_df = _build_macro_features_df(macro)
    if not macro_df.empty and not dry_run:
        macro_lib.write("features", macro_df)
        log.info("Wrote macro features: %d dates", len(macro_df))

    # Write raw macro series (SPY, VIX, etc.) for consumers that need them
    if not dry_run:
        for key in ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]:
            series = macro.get(key)
            if series is not None:
                macro_series_df = pd.DataFrame({"Close": series}, index=series.index)
                macro_series_df.index.name = "date"
                macro_lib.write(key, macro_series_df)

        # Write sector ETFs
        for key in macro:
            if key.startswith("XL"):
                sector_df = pd.DataFrame({"Close": macro[key]}, index=macro[key].index)
                sector_df.index.name = "date"
                macro_lib.write(key, sector_df)

    # ── 6. Snapshot ──────────────────────────────────────────────────────────
    if not dry_run:
        snapshot_name = f"backfill-{today_str}"
        try:
            universe_lib.snapshot(snapshot_name)
            log.info("Created snapshot: %s", snapshot_name)
        except Exception as exc:
            log.warning("Snapshot creation failed (non-fatal): %s", exc)

    t_total = time.time() - t0

    result = {
        "status": "ok",
        "tickers_written": n_ok,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "macro_dates": len(macro_df) if not macro_df.empty else 0,
        "load_seconds": round(t_load, 1),
        "compute_seconds": round(t_compute, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
    }

    log.info("Backfill complete: %s", json.dumps(result, default=str))

    # ── 7. Validation (optional) ─────────────────────────────────────────────
    if validate and not dry_run:
        _run_validation(universe_lib, price_data, macro, sector_map, fundamentals, alt_data)

    return result


def _run_validation(
    universe_lib,
    price_data: dict[str, pd.DataFrame],
    macro: dict[str, pd.Series],
    sector_map: dict[str, str],
    fundamentals: dict[str, dict],
    alt_data: dict[str, dict],
):
    """Spot-check: recompute features inline for 10 tickers and compare to ArcticDB."""
    symbols = universe_lib.list_symbols()
    check_tickers = sorted(symbols)[:10]

    log.info("Running validation on %d tickers: %s", len(check_tickers), check_tickers)

    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")

    passed = 0
    failed = 0

    for ticker in check_tickers:
        try:
            stored = universe_lib.read(ticker).data

            df = price_data[ticker]
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None
            ticker_alt = alt_data.get(ticker, {})

            recomputed = compute_features(
                df,
                spy_series=spy_series,
                vix_series=vix_series,
                sector_etf_series=sector_etf_series,
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                earnings_data=ticker_alt.get("earnings"),
                revision_data=ticker_alt.get("revisions"),
                options_data=ticker_alt.get("options"),
                fundamental_data=fundamentals.get(ticker),
            )

            # Compare row counts
            if len(stored) != len(recomputed):
                log.warning(
                    "FAIL %s: row count mismatch (stored=%d, recomputed=%d)",
                    ticker, len(stored), len(recomputed),
                )
                failed += 1
                continue

            # Compare feature values on last 10 rows
            feature_cols = [f for f in FEATURES if f in stored.columns and f in recomputed.columns]
            tail_stored = stored[feature_cols].tail(10).values
            tail_recomputed = recomputed[feature_cols].tail(10).values.astype("float32")

            if np.allclose(tail_stored, tail_recomputed, atol=1e-5, equal_nan=True):
                log.info("PASS %s: features match (%d rows, %d features)", ticker, len(stored), len(feature_cols))
                passed += 1
            else:
                max_diff = np.nanmax(np.abs(tail_stored - tail_recomputed))
                log.warning("FAIL %s: max feature diff = %.6f", ticker, max_diff)
                failed += 1

        except Exception as exc:
            log.warning("FAIL %s: validation error: %s", ticker, exc)
            failed += 1

    log.info("Validation complete: %d passed, %d failed", passed, failed)


def main():
    parser = argparse.ArgumentParser(description="Backfill ArcticDB universe from S3 price cache")
    parser.add_argument("--dry-run", action="store_true", help="Compute but skip ArcticDB writes")
    parser.add_argument("--ticker", default=None, help="Process single ticker (for testing)")
    parser.add_argument("--validate", action="store_true", help="Run spot-check validation after backfill")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"S3 bucket (default: {DEFAULT_BUCKET})")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = backfill(
        bucket=args.bucket,
        dry_run=args.dry_run,
        ticker_filter=args.ticker,
        validate=args.validate,
    )

    if result["status"] != "ok":
        log.error("Backfill failed: %s", result.get("error"))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
