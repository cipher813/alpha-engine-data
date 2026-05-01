"""
refresh_stale_universe_tickers.py — One-shot refresh of stuck universe tickers.

Some tickers have stale parquet+ArcticDB entries from before the most recent
weekly Saturday SF (or because daily_append silently skips them). This is a
recurring data-quality issue; this script is the manual unblock until the
underlying daily_append silent-skip is fixed.

Pulls 10y from yfinance, writes parquet to S3, computes features against
real macro/fundamentals context, writes feature rows to ArcticDB universe
library. Caps processing to the explicit ticker list — avoids loading the
909-ticker full cache that ``builders.backfill`` does.

Usage:
    python -m scripts.refresh_stale_universe_tickers PAYC ASGN LW GTM MOH HOLX KMPR MTCH
"""
from __future__ import annotations

import io
import logging
import os
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

BUCKET = "alpha-engine-research"
PREFIX = "predictor/price_cache/"


def refresh_parquets(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """yfinance batch download → write parquet to S3 → return DataFrames."""
    import yfinance as yf
    s3 = boto3.client("s3")
    out: dict[str, pd.DataFrame] = {}

    log.info("Downloading 10y from yfinance for %d tickers", len(tickers))
    df = yf.download(
        tickers, period="10y", auto_adjust=True, progress=False, group_by="ticker",
    )

    for t in tickers:
        try:
            if len(tickers) == 1:
                tdf = df.copy()
            else:
                tdf = df[t].copy()
            tdf = tdf.dropna(how="all")
            if tdf.empty:
                log.warning("yfinance returned empty for %s — skipping", t)
                continue
            tdf.index = pd.to_datetime(tdf.index).tz_localize(None).normalize()
            tdf = tdf[["Open", "High", "Low", "Close", "Volume"]].dropna(how="all")

            # Write parquet to S3
            buf = io.BytesIO()
            tdf.to_parquet(buf, engine="pyarrow")
            s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}{t}.parquet", Body=buf.getvalue())
            last = tdf.index.max().date()
            log.info("Wrote parquet %s: %d rows, last_date=%s", t, len(tdf), last)
            out[t] = tdf
        except Exception as e:
            log.error("Failed to refresh %s: %s", t, e)
    return out


def write_to_arcticdb(parquets: dict[str, pd.DataFrame]) -> None:
    """Compute features + write to ArcticDB universe lib using daily_append helpers."""
    from features.feature_engineer import compute_features
    from store.arctic_store import get_universe_lib, get_macro_lib

    universe_lib = get_universe_lib(BUCKET)
    macro_lib = get_macro_lib(BUCKET)

    # Load macro context from ArcticDB (SPY, VIX, sector ETFs, etc.)
    macro_symbols = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
                     "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP",
                     "XLRE", "XLU", "XLV", "XLY"]
    macro: dict[str, pd.Series] = {}
    for sym in macro_symbols:
        try:
            macro[sym] = macro_lib.read(sym).data["Close"]
        except Exception as e:
            log.warning("Could not load macro %s: %s", sym, e)

    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    vix3m_series = macro.get("VIX3M")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")

    # Sector map
    s3 = boto3.client("s3")
    try:
        sm_obj = s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}sector_map.json")
        import json as _json
        sector_map = _json.loads(sm_obj["Body"].read())
    except Exception as e:
        log.warning("Could not load sector_map: %s", e)
        sector_map = {}

    for ticker, df in parquets.items():
        try:
            sector_etf_sym = sector_map.get(ticker)
            sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None

            log.info("Computing features for %s (%d OHLCV rows)", ticker, len(df))
            featured = compute_features(
                df,
                spy_series=spy_series,
                vix_series=vix_series,
                sector_etf_series=sector_etf_series,
                tnx_series=tnx_series,
                irx_series=irx_series,
                gld_series=gld_series,
                uso_series=uso_series,
                vix3m_series=vix3m_series,
                earnings_data=None,
                revision_data=None,
                options_data=None,
                fundamental_data=None,
            )

            # Write the full featured frame in one shot — full rewrite per ticker.
            universe_lib.write(ticker, featured, prune_previous_versions=True)
            last = featured.index.max().date()
            log.info("Wrote ArcticDB %s: %d rows, last_date=%s", ticker, len(featured), last)
        except Exception as e:
            log.error("Failed to write ArcticDB for %s: %s", ticker, e)


def verify(tickers: list[str], min_date: str = "2026-04-20") -> int:
    """Verify last_date >= min_date for each ticker."""
    from store.arctic_store import get_universe_lib
    universe_lib = get_universe_lib(BUCKET)
    n_fail = 0
    cutoff = pd.Timestamp(min_date)
    for t in tickers:
        try:
            d = universe_lib.read(t).data
            last = d.index.max()
            ok = last >= cutoff
            sigil = "✓" if ok else "❌"
            log.info("%s %s last_date=%s rows=%d", sigil, t, last.date(), len(d))
            if not ok:
                n_fail += 1
        except Exception as e:
            log.error("❌ %s read failed: %s", t, e)
            n_fail += 1
    return n_fail


def main() -> int:
    tickers = sys.argv[1:]
    if not tickers:
        log.error("Usage: python -m scripts.refresh_stale_universe_tickers TICKER1 TICKER2 ...")
        return 1
    log.info("Refreshing %d tickers: %s", len(tickers), ", ".join(tickers))
    parquets = refresh_parquets(tickers)
    if not parquets:
        log.error("No parquets refreshed — abort")
        return 1
    write_to_arcticdb(parquets)
    n_fail = verify(tickers)
    if n_fail:
        log.error("Verification: %d/%d tickers stale after refresh", n_fail, len(tickers))
        return 1
    log.info("Refresh complete: all %d tickers fresh.", len(tickers))
    return 0


if __name__ == "__main__":
    sys.exit(main())
