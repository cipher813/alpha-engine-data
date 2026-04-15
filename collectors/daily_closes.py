"""
collectors/daily_closes.py — Daily OHLCV archive for all tracked tickers.

Writes one parquet per trading day at predictor/daily_closes/{date}.parquet.
The predictor inference Lambda uses these to bridge the gap between the
weekly slim cache and today's prices, avoiding a full 2-year yfinance fetch.

Data source priority:
  1. polygon.io grouped-daily (1 API call, covers all US stocks + ETFs)
  2. FRED (covers the 4 index tickers not on polygon free tier: VIX, VIX3M, TNX, IRX)
  3. yfinance batch download (fallback for whatever remains)

Schema: index=ticker (str), columns=[date, Open, High, Low, Close, Adj_Close, Volume, VWAP]
"""

from __future__ import annotations

import io
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

_YFINANCE_BATCH_SIZE = 100
_YFINANCE_BATCH_DELAY = 2  # seconds between batches

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
_FRED_TIMEOUT = 15

# Map our ArcticDB ticker key (after stripping ^) to FRED series id.
# Both yfinance (^VIX, ^TNX, ...) and FRED (VIXCLS, DGS10, ...) publish
# these in the same scale (raw index level for VIX/VIX3M, percent for
# TNX/IRX), so no conversion is needed before appending to ArcticDB.
_FRED_INDEX_MAP = {
    "VIX": "VIXCLS",
    "VIX3M": "VXVCLS",
    "TNX": "DGS10",
    "IRX": "DTB3",
}


def collect(
    bucket: str,
    tickers: list[str],
    run_date: str | None = None,
    s3_prefix: str = "predictor/daily_closes/",
    dry_run: bool = False,
) -> dict:
    """
    Fetch today's OHLCV for all tickers and write to S3.

    Args:
        bucket: S3 bucket name
        tickers: list of ticker symbols to capture
        run_date: YYYY-MM-DD (defaults to today)
        s3_prefix: S3 key prefix for daily closes
        dry_run: if True, fetch but don't write to S3

    Returns:
        dict with status, tickers_captured, method breakdown
    """
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = boto3.client("s3")

    # Check if already written for this date
    key = f"{s3_prefix}{run_date}.parquet"
    if not dry_run:
        from botocore.exceptions import ClientError
        try:
            s3.head_object(Bucket=bucket, Key=key)
            logger.info("Daily closes already exist for %s — skipping", run_date)
            return {"status": "ok", "tickers_captured": 0, "skipped": True}
        except ClientError as exc:
            err_code = exc.response.get("Error", {}).get("Code")
            if err_code not in ("404", "NoSuchKey"):
                # Auth failure, throttling, or network — not "file doesn't exist".
                # Don't silently paper over it.
                raise
            # 404/NoSuchKey: expected case — file doesn't exist, proceed to write.

    if not tickers:
        return {"status": "error", "error": "no tickers provided"}

    records: list[dict] = []

    # ── Step 1: Try polygon.io grouped-daily (1 API call for all US stocks) ──
    polygon_count = 0
    try:
        from polygon_client import polygon_client
        grouped = polygon_client().get_grouped_daily(run_date)
        if grouped:
            for ticker in tickers:
                store_ticker = ticker.lstrip("^")
                g = grouped.get(store_ticker)
                if g:
                    records.append({
                        "ticker": store_ticker,
                        "date": run_date,
                        "Open": round(g["open"], 4),
                        "High": round(g["high"], 4),
                        "Low": round(g["low"], 4),
                        "Close": round(g["close"], 4),
                        "Adj_Close": round(g["close"], 4),
                        "Volume": int(g["volume"]),
                        "VWAP": round(g["vwap"], 4) if g.get("vwap") else None,
                    })
            polygon_count = len(records)
            logger.info("Polygon grouped-daily: %d/%d tickers", polygon_count, len(tickers))
    except Exception as e:
        logger.warning("Polygon grouped-daily failed: %s — falling back to yfinance", e)

    # ── Step 2: FRED fallback for index tickers ──────────────────────────────
    # VIX/VIX3M/TNX/IRX are not on polygon free tier. FRED has same-scale
    # equivalents (VIXCLS/VXVCLS/DGS10/DTB3) that typically publish T-1 values
    # by the time the daily pipeline runs at 6:05 AM PT.
    captured_tickers = {r["ticker"] for r in records}
    fred_missing = [
        t for t in tickers
        if t.lstrip("^") not in captured_tickers and t.lstrip("^") in _FRED_INDEX_MAP
    ]
    fred_count = 0
    if fred_missing:
        fred_count = _fetch_fred_closes(fred_missing, run_date, records)

    # ── Step 3: yfinance fallback for anything still missing ─────────────────
    captured_tickers = {r["ticker"] for r in records}
    missing = [t for t in tickers if t.lstrip("^") not in captured_tickers]
    yfinance_count = 0

    if missing:
        yfinance_count = _fetch_yfinance_closes(missing, run_date, records)

    if not records:
        logger.warning("No closes captured for %s", run_date)
        return {"status": "error", "error": "no data fetched", "tickers_captured": 0}

    closes_df = pd.DataFrame(records).set_index("ticker")
    logger.info(
        "Daily closes: %d tickers for %s (polygon=%d, fred=%d, yfinance=%d)",
        len(closes_df), run_date, polygon_count, fred_count, yfinance_count,
    )

    if dry_run:
        return {
            "status": "ok_dry_run",
            "tickers_captured": len(closes_df),
            "polygon": polygon_count,
            "fred": fred_count,
            "yfinance": yfinance_count,
        }

    # ── Step 3: Write to S3 ──────────────────────────────────────────────────
    try:
        buf = io.BytesIO()
        closes_df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
        buf.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info("Written to s3://%s/%s (%d tickers)", bucket, key, len(closes_df))
        return {
            "status": "ok",
            "tickers_captured": len(closes_df),
            "polygon": polygon_count,
            "fred": fred_count,
            "yfinance": yfinance_count,
        }
    except Exception as e:
        logger.error("Failed to write daily closes: %s", e)
        return {"status": "error", "error": str(e), "tickers_captured": len(closes_df)}


def _fetch_fred_closes(
    tickers: list[str],
    date_str: str,
    records: list[dict],
) -> int:
    """Fetch the latest close for index tickers from FRED.

    Serves the 4 index symbols not on polygon free tier (VIX, VIX3M, TNX, IRX).
    Takes the most recent non-missing observation for each series — typically
    T-1 when the daily pipeline runs at 6:05 AM PT.
    """
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        logger.warning("FRED_API_KEY not set — skipping FRED fallback for %d tickers", len(tickers))
        return 0

    count = 0
    for ticker in tickers:
        store_ticker = ticker.lstrip("^")
        series_id = _FRED_INDEX_MAP.get(store_ticker)
        if not series_id:
            continue
        try:
            params = {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 5,
            }
            resp = requests.get(_FRED_BASE, params=params, timeout=_FRED_TIMEOUT)
            resp.raise_for_status()
            obs = resp.json().get("observations", [])
            latest = next((o for o in obs if o.get("value", ".") != "."), None)
            if latest is None:
                logger.warning("FRED %s → %s: no non-missing observation", store_ticker, series_id)
                continue
            close = float(latest["value"])
            records.append({
                "ticker": store_ticker,
                "date": date_str,
                "Open": round(close, 4),
                "High": round(close, 4),
                "Low": round(close, 4),
                "Close": round(close, 4),
                "Adj_Close": round(close, 4),
                "Volume": 0,
                "VWAP": round(close, 4),
            })
            count += 1
        except Exception as e:
            logger.warning("FRED fetch failed for %s (%s): %s", store_ticker, series_id, e)

    logger.info("FRED fallback: %d/%d index tickers captured", count, len(tickers))
    return count


def _fetch_yfinance_closes(
    tickers: list[str],
    date_str: str,
    records: list[dict],
) -> int:
    """Fetch closes from yfinance for tickers not covered by polygon."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available for daily closes fallback")
        return 0

    count = 0
    batches = [tickers[i:i + _YFINANCE_BATCH_SIZE]
               for i in range(0, len(tickers), _YFINANCE_BATCH_SIZE)]

    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(_YFINANCE_BATCH_DELAY)
        try:
            tickers_arg = batch[0] if len(batch) == 1 else batch
            raw = yf.download(
                tickers=tickers_arg,
                period="5d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=True,
            )
            is_multi = isinstance(raw.columns, pd.MultiIndex)

            for ticker in batch:
                try:
                    df = (raw[ticker] if is_multi else raw).copy()
                    df.index = pd.to_datetime(df.index)
                    if df.index.tz is not None:
                        df.index = df.index.tz_convert("UTC").tz_localize(None)
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue

                    last = df.iloc[-1]
                    store_ticker = ticker.lstrip("^")
                    adj_close = float(last["Adj Close"]) if "Adj Close" in df.columns else float(last["Close"])
                    high = float(last["High"])
                    low = float(last["Low"])
                    close = float(last["Close"])
                    # Typical-price proxy for VWAP: (H + L + C) / 3. yfinance
                    # does not expose true intraday VWAP for free, so we fall
                    # back to the standard pre-tick-data proxy. Executor entry
                    # triggers use this as a prior-day reference level, not
                    # an intraday VWAP — the proxy is accurate enough for
                    # that purpose and materially better than None.
                    vwap_proxy = round((high + low + close) / 3.0, 4)
                    records.append({
                        "ticker": store_ticker,
                        "date": date_str,
                        "Open": round(float(last["Open"]), 4),
                        "High": round(high, 4),
                        "Low": round(low, 4),
                        "Close": round(close, 4),
                        "Adj_Close": round(adj_close, 4),
                        "Volume": int(last["Volume"]) if pd.notna(last.get("Volume")) else 0,
                        "VWAP": vwap_proxy,
                    })
                    count += 1
                except Exception as e:
                    logger.warning("yfinance close extract failed for %s: %s", ticker, e)
        except Exception as e:
            logger.warning("yfinance batch failed: %s", e)

    logger.info("yfinance fallback: %d/%d tickers captured", count, len(tickers))
    return count
