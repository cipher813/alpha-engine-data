"""
prices.py — Refresh stale price cache parquets and upload to S3.

Extracted from alpha-engine-predictor/training/train_handler.py:refresh_price_cache().

For each <ticker>.parquet in the S3 price cache, checks if the last date is stale
(more than staleness_threshold_days business days behind). If stale, downloads a
full history from yfinance and replaces the parquet entirely.

Why full replace (not append): yfinance auto_adjust=True retroactively adjusts the
entire price history on splits/dividends. Appending creates a discontinuity at the
splice point. Full rewrite guarantees internal consistency.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# Tickers that require a leading caret in yfinance
_CARET_SYMBOLS = {"VIX", "VIX3M", "TNX", "IRX"}

# Always-download tickers (benchmarks, macro, sector ETFs)
_ALWAYS_DOWNLOAD = [
    "SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC",
]


def collect(
    bucket: str,
    tickers: list[str],
    s3_prefix: str = "predictor/price_cache/",
    fetch_period: str = "10y",
    staleness_threshold_days: int = 3,
    batch_size: int = 50,
    dry_run: bool = False,
) -> dict:
    """
    Download price cache from S3, refresh stale parquets via yfinance, upload back.

    Args:
        bucket: S3 bucket name
        tickers: full universe of tickers to maintain
        s3_prefix: S3 key prefix for price cache parquets
        fetch_period: yfinance period string for full refresh
        staleness_threshold_days: business days before a parquet is stale
        batch_size: tickers per yfinance batch download
        dry_run: if True, identify stale tickers but don't fetch/upload

    Returns:
        dict with status, refreshed count, errors
    """
    s3 = boto3.client("s3")

    # Ensure always-download tickers are included
    all_tickers = list(dict.fromkeys(tickers + _ALWAYS_DOWNLOAD))

    with tempfile.TemporaryDirectory() as tmpdir:
        local_dir = Path(tmpdir)

        # Download existing parquets from S3
        downloaded = _download_cache(s3, bucket, s3_prefix, local_dir)
        logger.info("Downloaded %d existing parquets from S3", downloaded)

        # Identify stale tickers
        stale = _find_stale(local_dir, all_tickers, staleness_threshold_days)

        if not stale:
            logger.info("Price cache is current — no refresh needed")
            return {"status": "ok", "refreshed": 0, "stale": 0, "total": len(all_tickers)}

        if dry_run:
            logger.info("[dry-run] %d stale tickers would be refreshed", len(stale))
            return {"status": "ok_dry_run", "stale": len(stale), "total": len(all_tickers)}

        # Refresh stale tickers
        refreshed, failed_tickers = _refresh_stale(
            s3, bucket, s3_prefix, local_dir, stale, fetch_period, batch_size,
        )

        return {
            "status": "ok" if not failed_tickers else "partial",
            "refreshed": refreshed,
            "stale": len(stale),
            "failed": len(failed_tickers),
            "failed_tickers": failed_tickers[:20],  # cap for manifest size
            "total": len(all_tickers),
        }


def _download_cache(s3, bucket: str, prefix: str, local_dir: Path) -> int:
    """Download all parquets from S3 prefix to local_dir."""
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            filename = key.split("/")[-1]
            local_path = local_dir / filename
            s3.download_file(bucket, key, str(local_path))
            count += 1
    return count


def _find_stale(
    local_dir: Path,
    all_tickers: list[str],
    staleness_threshold_days: int,
) -> list[tuple[str, Path | None]]:
    """
    Identify tickers that need refreshing.

    Returns list of (ticker, existing_parquet_path_or_None).
    """
    today = pd.Timestamp.now().normalize()
    stale: list[tuple[str, Path | None]] = []

    existing_files = {p.stem: p for p in local_dir.glob("*.parquet") if p.stem != "sector_map"}

    for ticker in all_tickers:
        parquet_path = existing_files.get(ticker)
        if parquet_path is None:
            # New ticker, no existing data
            stale.append((ticker, None))
            continue

        try:
            df = pd.read_parquet(parquet_path)
            if df.empty:
                stale.append((ticker, parquet_path))
                continue
            last_ts = pd.Timestamp(df.index.max())
            if last_ts.tzinfo is not None:
                last_ts = last_ts.tz_convert("UTC").tz_localize(None)
            bdays_lag = len(pd.bdate_range(last_ts, today)) - 1
            if bdays_lag >= staleness_threshold_days:
                stale.append((ticker, parquet_path))
        except Exception as e:
            logger.warning("Could not check staleness for %s: %s", ticker, e)

    return stale


def _refresh_stale(
    s3,
    bucket: str,
    s3_prefix: str,
    local_dir: Path,
    stale: list[tuple[str, Path | None]],
    fetch_period: str,
    batch_size: int,
) -> tuple[int, list[str]]:
    """Batch-fetch stale tickers from yfinance and upload to S3."""
    logger.info("Refreshing %d stale tickers (period=%s) ...", len(stale), fetch_period)

    refreshed = 0
    failed_tickers: list[str] = []

    for batch_start in range(0, len(stale), batch_size):
        batch = stale[batch_start : batch_start + batch_size]
        ticker_names = [t for t, _ in batch]
        yf_symbols = [f"^{t}" if t in _CARET_SYMBOLS else t for t in ticker_names]

        try:
            tickers_arg = yf_symbols[0] if len(yf_symbols) == 1 else yf_symbols
            raw = yf.download(
                tickers=tickers_arg,
                period=fetch_period,
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
            is_multi = isinstance(raw.columns, pd.MultiIndex)
        except Exception as e:
            logger.warning("yfinance batch download failed for %s...: %s", ticker_names[:3], e)
            failed_tickers.extend(ticker_names)
            continue

        for ticker, existing_path in batch:
            yf_sym = f"^{ticker}" if ticker in _CARET_SYMBOLS else ticker
            try:
                new_df = (raw[yf_sym] if is_multi else raw).copy()
                if "Close" not in new_df.columns or new_df.empty:
                    failed_tickers.append(ticker)
                    continue
                new_df = new_df.dropna(subset=["Close"])
                if new_df.empty:
                    failed_tickers.append(ticker)
                    continue

                # Normalize index to timezone-naive
                idx = pd.to_datetime(new_df.index)
                if idx.tz is not None:
                    idx = idx.tz_convert("UTC").tz_localize(None)
                new_df.index = idx
                new_df = new_df.sort_index()

                # Write locally
                parquet_path = local_dir / f"{ticker}.parquet"
                new_df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")

                # Upload to S3
                s3_key = f"{s3_prefix}{ticker}.parquet"
                s3.upload_file(str(parquet_path), bucket, s3_key)
                refreshed += 1

            except Exception as e:
                logger.warning("Refresh failed for %s: %s", ticker, e)
                failed_tickers.append(ticker)

        pct = 100 * min(batch_start + batch_size, len(stale)) / len(stale)
        logger.info(
            "Refresh batch %d/%d done — %.0f%% complete",
            batch_start // batch_size + 1,
            -(-len(stale) // batch_size),
            pct,
        )

    logger.info("Price cache refresh: %d / %d tickers updated", refreshed, len(stale))
    return refreshed, failed_tickers
