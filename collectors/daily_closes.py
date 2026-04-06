"""
collectors/daily_closes.py — Daily OHLCV archive for all tracked tickers.

Writes one parquet per trading day at predictor/daily_closes/{date}.parquet.
The predictor inference Lambda uses these to bridge the gap between the
weekly slim cache and today's prices, avoiding a full 2-year yfinance fetch.

Data source priority: polygon.io grouped-daily (1 API call for all US stocks),
then yfinance batch download for any tickers polygon missed.

Schema: index=ticker (str), columns=[date, Open, High, Low, Close, Adj_Close, Volume, VWAP]
"""

from __future__ import annotations

import io
import logging
import time
from datetime import datetime, timezone

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

_YFINANCE_BATCH_SIZE = 100
_YFINANCE_BATCH_DELAY = 2  # seconds between batches


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
        try:
            s3.head_object(Bucket=bucket, Key=key)
            logger.info("Daily closes already exist for %s — skipping", run_date)
            return {"status": "ok", "tickers_captured": 0, "skipped": True}
        except Exception:
            pass  # Not found — proceed

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

    # ── Step 2: yfinance fallback for missing tickers ────────────────────────
    captured_tickers = {r["ticker"] for r in records}
    missing = [t for t in tickers if t.lstrip("^") not in captured_tickers]
    yfinance_count = 0

    if missing:
        yfinance_count = _fetch_yfinance_closes(missing, run_date, records)

    if not records:
        logger.warning("No closes captured for %s", run_date)
        return {"status": "error", "error": "no data fetched", "tickers_captured": 0}

    closes_df = pd.DataFrame(records).set_index("ticker")
    logger.info("Daily closes: %d tickers for %s (polygon=%d, yfinance=%d)",
                len(closes_df), run_date, polygon_count, yfinance_count)

    if dry_run:
        return {
            "status": "ok_dry_run",
            "tickers_captured": len(closes_df),
            "polygon": polygon_count,
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
            "yfinance": yfinance_count,
        }
    except Exception as e:
        logger.error("Failed to write daily closes: %s", e)
        return {"status": "error", "error": str(e), "tickers_captured": len(closes_df)}


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
                    records.append({
                        "ticker": store_ticker,
                        "date": date_str,
                        "Open": round(float(last["Open"]), 4),
                        "High": round(float(last["High"]), 4),
                        "Low": round(float(last["Low"]), 4),
                        "Close": round(float(last["Close"]), 4),
                        "Adj_Close": round(adj_close, 4),
                        "Volume": int(last["Volume"]) if pd.notna(last.get("Volume")) else 0,
                        "VWAP": None,
                    })
                    count += 1
                except Exception as e:
                    logger.debug("yfinance close extract failed for %s: %s", ticker, e)
        except Exception as e:
            logger.warning("yfinance batch failed: %s", e)

    logger.info("yfinance fallback: %d/%d tickers captured", count, len(tickers))
    return count
