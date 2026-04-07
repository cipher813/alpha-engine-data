"""
builders/daily_append.py — Append today's OHLCV + features to ArcticDB universe.

Reads today's daily_closes from S3 (already written by daily_closes.py),
loads recent history from ArcticDB for feature warmup, computes today's
features, and appends a single row per ticker to the universe library.

Usage:
    python -m builders.daily_append                          # today
    python -m builders.daily_append --date 2026-04-07        # specific date
    python -m builders.daily_append --dry-run                # compute but skip write
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
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
    _load_sector_map,
    _load_cached_fundamentals,
    _load_cached_alternative,
)
from store.arctic_store import get_universe_lib, get_macro_lib

log = logging.getLogger(__name__)

OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]


def _load_daily_closes(s3, bucket: str, date_str: str) -> dict[str, dict]:
    """Load today's daily_closes parquet from S3. Returns {ticker: {Open, High, Low, Close, Volume}}."""
    key = f"predictor/daily_closes/{date_str}.parquet"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        df = pd.read_parquet(buf, engine="pyarrow")

        records = {}
        for ticker, row in df.iterrows():
            records[str(ticker)] = {
                "Open": float(row.get("Open", np.nan)),
                "High": float(row.get("High", np.nan)),
                "Low": float(row.get("Low", np.nan)),
                "Close": float(row.get("Close", np.nan)),
                "Volume": int(row.get("Volume", 0)),
            }
        log.info("Loaded daily closes for %s: %d tickers", date_str, len(records))
        return records
    except Exception as exc:
        log.error("Failed to load daily_closes/%s.parquet: %s", date_str, exc)
        return {}


def daily_append(
    date_str: str | None = None,
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
) -> dict:
    """
    Append today's features to ArcticDB universe.

    For each ticker:
    1. Read recent history from ArcticDB (tail ~300 rows for feature warmup)
    2. Append today's OHLCV row
    3. Compute features for the combined series
    4. Extract the last row (today) and append to ArcticDB

    Returns summary dict.
    """
    s3 = boto3.client("s3")
    date_str = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_ts = pd.Timestamp(date_str)
    t0 = time.time()

    # ── 1. Load today's OHLCV ────────────────────────────────────────────────
    closes = _load_daily_closes(s3, bucket, date_str)
    if not closes:
        return {"status": "error", "error": "no daily_closes for date"}

    # ── 2. Load supporting data ──────────────────────────────────────────────
    sector_map = _load_sector_map(s3, bucket)
    fundamentals = _load_cached_fundamentals(s3, bucket, date_str)
    alt_data = _load_cached_alternative(s3, bucket)

    if not dry_run:
        universe_lib = get_universe_lib(bucket)
        macro_lib = get_macro_lib(bucket)
    else:
        universe_lib = None
        macro_lib = None

    # ── 3. Load macro series from ArcticDB ───────────────────────────────────
    macro: dict[str, pd.Series] = {}
    macro_keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]

    if not dry_run:
        for key in macro_keys:
            try:
                mdf = macro_lib.read(key).data
                if "Close" in mdf.columns:
                    series = mdf["Close"].dropna()
                    # Append today's close if available
                    ticker_close = closes.get(key)
                    if ticker_close and not np.isnan(ticker_close["Close"]):
                        new_row = pd.Series(
                            {"Close": ticker_close["Close"]},
                            name=today_ts,
                        )
                        series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                        series = series[~series.index.duplicated(keep="last")]
                    macro[key] = series
            except Exception:
                log.debug("Macro series %s not in ArcticDB", key)

        # Sector ETFs
        for sym in macro_lib.list_symbols():
            if sym.startswith("XL"):
                try:
                    mdf = macro_lib.read(sym).data
                    if "Close" in mdf.columns:
                        series = mdf["Close"].dropna()
                        ticker_close = closes.get(sym)
                        if ticker_close and not np.isnan(ticker_close["Close"]):
                            series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                            series = series[~series.index.duplicated(keep="last")]
                        macro[sym] = series
                except Exception:
                    pass

    t_load = time.time() - t0
    log.info("Data loaded in %.1fs: %d closes, %d macro series", t_load, len(closes), len(macro))

    # ── 4. Compute features and append ───────────────────────────────────────
    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")

    # Filter to stock tickers only
    stock_tickers = [
        t for t in closes
        if t not in _SKIP_TICKERS and not _is_sector_etf(t)
    ]

    n_ok = 0
    n_skip = 0
    n_err = 0

    for ticker in stock_tickers:
        try:
            # Read recent history from ArcticDB (need ~265 rows for feature warmup)
            if dry_run:
                n_skip += 1
                continue

            try:
                hist = universe_lib.read(ticker).data
            except Exception:
                log.debug("Ticker %s not in ArcticDB — skipping", ticker)
                n_skip += 1
                continue

            if len(hist) < MIN_ROWS_FOR_FEATURES:
                n_skip += 1
                continue

            # Check if today already exists
            if today_ts in hist.index:
                n_skip += 1
                continue

            # Build today's OHLCV row
            bar = closes[ticker]
            if np.isnan(bar["Close"]):
                n_skip += 1
                continue

            new_row = pd.DataFrame(
                [{col: bar.get(col, np.nan) for col in OHLCV_COLS}],
                index=pd.DatetimeIndex([today_ts]),
            )

            # Combine history OHLCV + today's bar for feature computation
            hist_ohlcv = hist[[c for c in OHLCV_COLS if c in hist.columns]]
            combined = pd.concat([hist_ohlcv, new_row])
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()

            # Compute features on the combined series
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None
            ticker_alt = alt_data.get(ticker, {})

            featured = compute_features(
                combined,
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

            if featured.empty or today_ts not in featured.index:
                n_skip += 1
                continue

            # Extract today's row with OHLCV + features
            keep_cols = [c for c in OHLCV_COLS if c in featured.columns] + \
                        [f for f in FEATURES if f in featured.columns]
            today_row = featured.loc[[today_ts], keep_cols].copy()

            for f in FEATURES:
                if f in today_row.columns:
                    today_row[f] = today_row[f].astype("float32")

            today_row.index.name = "date"

            universe_lib.append(ticker, today_row)
            n_ok += 1

        except Exception as exc:
            log.debug("Failed to append %s: %s", ticker, exc)
            n_err += 1

    # ── 5. Update macro series ───────────────────────────────────────────────
    if not dry_run:
        for key in macro_keys:
            bar = closes.get(key)
            if bar and not np.isnan(bar.get("Close", np.nan)):
                try:
                    new_row = pd.DataFrame(
                        [{"Close": bar["Close"]}],
                        index=pd.DatetimeIndex([today_ts]),
                    )
                    new_row.index.name = "date"
                    macro_lib.append(key, new_row)
                except Exception as exc:
                    log.debug("Failed to append macro %s: %s", key, exc)

        # Sector ETFs
        for sym in closes:
            if sym.startswith("XL") and len(sym) <= 4:
                bar = closes[sym]
                if not np.isnan(bar.get("Close", np.nan)):
                    try:
                        new_row = pd.DataFrame(
                            [{"Close": bar["Close"]}],
                            index=pd.DatetimeIndex([today_ts]),
                        )
                        new_row.index.name = "date"
                        macro_lib.append(sym, new_row)
                    except Exception as exc:
                        log.debug("Failed to append macro %s: %s", sym, exc)

    t_total = time.time() - t0

    result = {
        "status": "ok",
        "date": date_str,
        "tickers_appended": n_ok,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "load_seconds": round(t_load, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
    }

    log.info("Daily append complete: %s", json.dumps(result, default=str))
    return result


def main():
    parser = argparse.ArgumentParser(description="Append daily features to ArcticDB universe")
    parser.add_argument("--date", default=None, help="Target date (YYYY-MM-DD, default: today UTC)")
    parser.add_argument("--dry-run", action="store_true", help="Compute but skip ArcticDB writes")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"S3 bucket (default: {DEFAULT_BUCKET})")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = daily_append(
        date_str=args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        bucket=args.bucket,
        dry_run=args.dry_run,
    )

    if result["status"] != "ok":
        log.error("Daily append failed: %s", result.get("error"))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
