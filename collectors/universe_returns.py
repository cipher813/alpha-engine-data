"""
collectors/universe_returns.py — Full-population forward-return tracking.

Uses polygon.io grouped-daily endpoint to fetch OHLCV for the entire US market
in a single API call per date. Computes 5d/10d forward returns for every ticker,
SPY benchmark returns, and sector ETF returns for sector-relative analysis.

This is the denominator for all lift calculations in the backtester evaluation
framework: scanner filter lift, sector team lift, CIO lift, predictor lift,
execution lift.

Target table: universe_returns in research.db (~900 rows/date, ~47K rows/year).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# -- Sector ETF mapping ------------------------------------------------------

_SECTOR_TO_ETF = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
    "Basic Materials": "XLB",
}

_ETF_TO_SECTOR = {v: k for k, v in _SECTOR_TO_ETF.items()}
_SECTOR_ETFS = set(_SECTOR_TO_ETF.values())
_SKIP_TICKERS = _SECTOR_ETFS | {"SPY", "VIX", "^VIX", "^TNX", "^IRX"}

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS universe_returns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    eval_date TEXT NOT NULL,
    sector TEXT,
    close_price REAL,
    return_5d REAL,
    return_10d REAL,
    spy_return_5d REAL,
    spy_return_10d REAL,
    beat_spy_5d INTEGER,
    beat_spy_10d INTEGER,
    sector_etf TEXT,
    sector_etf_return_5d REAL,
    beat_sector_5d INTEGER,
    UNIQUE(ticker, eval_date)
)
"""


def collect(
    bucket: str,
    db_path: str,
    signals_prefix: str = "signals",
    sector_map_key: str = "predictor/price_cache/sector_map.json",
    dry_run: bool = False,
) -> dict:
    """
    Populate universe_returns table with forward returns for all ~900 S&P stocks.

    Reads signal dates from S3, identifies dates missing from universe_returns,
    fetches grouped-daily prices via polygon.io, and inserts rows.

    Args:
        bucket: S3 bucket name
        db_path: path to local research.db
        signals_prefix: S3 prefix for signals (e.g. "signals")
        sector_map_key: S3 key for sector map JSON
        dry_run: if True, compute but don't write to DB

    Returns:
        dict with status, dates_processed, rows_inserted, errors
    """
    from polygon_client import polygon_client

    try:
        client = polygon_client()
    except ValueError as e:
        logger.warning("Polygon client init failed: %s", e)
        return {"status": "error", "error": str(e)}

    # List signal dates from S3
    s3 = boto3.client("s3")
    eval_dates = _list_signal_dates(s3, bucket, signals_prefix)
    if not eval_dates:
        logger.warning("No signal dates found in s3://%s/%s/", bucket, signals_prefix)
        return {"status": "ok", "dates_processed": 0, "rows_inserted": 0, "skipped": 0}

    # Load sector map
    sector_map = _load_sector_map(s3, bucket, sector_map_key)

    # Ensure table exists
    _ensure_table(db_path)
    existing = _get_existing_dates(db_path)

    dates_to_process = [d for d in eval_dates if d not in existing]
    if not dates_to_process:
        logger.info("All %d eval_dates already in universe_returns", len(eval_dates))
        return {
            "status": "ok" if not dry_run else "ok_dry_run",
            "dates_processed": 0,
            "rows_inserted": 0,
            "skipped": len(eval_dates),
        }

    logger.info(
        "Processing %d eval_dates for universe_returns (%d already exist)",
        len(dates_to_process), len(existing),
    )

    total_inserted = 0
    errors = []

    for eval_date in dates_to_process:
        try:
            rows = _build_rows_for_date(eval_date, client, sector_map)
            if not rows:
                errors.append({"date": eval_date, "error": "no rows computed"})
                continue

            if not dry_run:
                inserted = _insert_rows(db_path, rows)
                total_inserted += inserted
                logger.info("universe_returns: %s -> %d rows inserted", eval_date, inserted)
            else:
                total_inserted += len(rows)
                logger.info("universe_returns (dry-run): %s -> %d rows computed", eval_date, len(rows))
        except Exception as e:
            logger.warning("universe_returns: failed for %s: %s", eval_date, e)
            errors.append({"date": eval_date, "error": str(e)})

    # Upload updated research.db back to S3
    if not dry_run and total_inserted > 0:
        try:
            s3.upload_file(db_path, bucket, "research.db")
            logger.info("Uploaded research.db to s3://%s/research.db", bucket)
        except Exception as e:
            logger.warning("Failed to upload research.db: %s", e)

    status = "ok" if not errors else "partial"
    if dry_run:
        status = "ok_dry_run"

    return {
        "status": status,
        "dates_processed": len(dates_to_process),
        "rows_inserted": total_inserted,
        "errors": errors[:20],
    }


# -- Signal date listing -----------------------------------------------------

def _list_signal_dates(s3, bucket: str, prefix: str) -> list[str]:
    """List all signal dates from S3 (YYYY-MM-DD directories under prefix/)."""
    dates = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            # prefix/2026-03-28/ -> 2026-03-28
            part = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                dates.append(part)
    dates.sort()
    return dates


# -- Sector map loading ------------------------------------------------------

def _load_sector_map(s3, bucket: str, key: str) -> dict[str, str] | None:
    """Load ticker -> sector ETF mapping from S3."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        logger.warning("Could not load sector_map from s3://%s/%s: %s", bucket, key, e)
        return None


# -- DB helpers ---------------------------------------------------------------

def _ensure_table(db_path: str) -> None:
    """Create universe_returns table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(_CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()


def _get_existing_dates(db_path: str) -> set[str]:
    """Return set of eval_dates already populated."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT DISTINCT eval_date FROM universe_returns").fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def _insert_rows(db_path: str, rows: list[dict]) -> int:
    """Insert rows into universe_returns, skipping duplicates."""
    conn = sqlite3.connect(db_path)
    try:
        inserted = 0
        for row in rows:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO universe_returns "
                    "(ticker, eval_date, sector, close_price, return_5d, return_10d, "
                    "spy_return_5d, spy_return_10d, beat_spy_5d, beat_spy_10d, "
                    "sector_etf, sector_etf_return_5d, beat_sector_5d) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        row["ticker"], row["eval_date"], row["sector"],
                        row["close_price"], row["return_5d"], row["return_10d"],
                        row["spy_return_5d"], row["spy_return_10d"],
                        row["beat_spy_5d"], row["beat_spy_10d"],
                        row["sector_etf"], row["sector_etf_return_5d"],
                        row["beat_sector_5d"],
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        return inserted
    finally:
        conn.close()


# -- Row building (polygon.io) -----------------------------------------------

def _build_rows_for_date(
    eval_date: str,
    polygon_client,
    sector_map: dict[str, str] | None,
) -> list[dict]:
    """Build universe_returns rows for a single eval_date."""
    eval_dt = date.fromisoformat(eval_date)
    fwd_5d = _add_business_days(eval_dt, 5)
    fwd_10d = _add_business_days(eval_dt, 10)

    # Check that forward dates are in the past (returns can be computed)
    today = date.today()
    if fwd_5d >= today:
        logger.debug("Skipping %s: 5d forward date %s is in the future", eval_date, fwd_5d)
        return []

    has_10d = fwd_10d < today

    # Fetch grouped-daily prices for eval_date and forward dates
    prices_t0 = polygon_client.get_grouped_daily(eval_date)
    prices_5d = polygon_client.get_grouped_daily(str(fwd_5d))
    prices_10d = polygon_client.get_grouped_daily(str(fwd_10d)) if has_10d else {}

    if not prices_t0:
        logger.warning("No prices for eval_date %s — may be a non-trading day", eval_date)
        # Try next business day
        next_day = _add_business_days(eval_dt, 1)
        prices_t0 = polygon_client.get_grouped_daily(str(next_day))
        if not prices_t0:
            return []

    # SPY benchmark
    spy_t0 = prices_t0.get("SPY", {}).get("close")
    spy_5d = prices_5d.get("SPY", {}).get("close")
    spy_10d = prices_10d.get("SPY", {}).get("close") if has_10d else None

    spy_ret_5d = _pct_return(spy_t0, spy_5d)
    spy_ret_10d = _pct_return(spy_t0, spy_10d) if has_10d else None

    # Sector ETF returns
    sector_etf_returns_5d: dict[str, float | None] = {}
    for etf in _SECTOR_ETFS:
        etf_t0 = prices_t0.get(etf, {}).get("close")
        etf_5d = prices_5d.get(etf, {}).get("close")
        sector_etf_returns_5d[etf] = _pct_return(etf_t0, etf_5d)

    # Build rows for all tickers
    rows = []
    for ticker, bar in prices_t0.items():
        if ticker in _SKIP_TICKERS:
            continue

        close_t0 = bar.get("close")
        if close_t0 is None or close_t0 <= 0:
            continue

        close_5d = prices_5d.get(ticker, {}).get("close")
        close_10d = prices_10d.get(ticker, {}).get("close") if has_10d else None

        ret_5d = _pct_return(close_t0, close_5d)
        ret_10d = _pct_return(close_t0, close_10d) if has_10d else None

        # Sector classification
        sector_etf = sector_map.get(ticker) if sector_map else None
        sector = _ETF_TO_SECTOR.get(sector_etf, "") if sector_etf else ""
        etf_ret_5d = sector_etf_returns_5d.get(sector_etf) if sector_etf else None

        rows.append({
            "ticker": ticker,
            "eval_date": eval_date,
            "sector": sector,
            "close_price": round(close_t0, 2),
            "return_5d": round(ret_5d, 4) if ret_5d is not None else None,
            "return_10d": round(ret_10d, 4) if ret_10d is not None else None,
            "spy_return_5d": round(spy_ret_5d, 4) if spy_ret_5d is not None else None,
            "spy_return_10d": round(spy_ret_10d, 4) if spy_ret_10d is not None else None,
            "beat_spy_5d": int(ret_5d > spy_ret_5d) if ret_5d is not None and spy_ret_5d is not None else None,
            "beat_spy_10d": int(ret_10d > spy_ret_10d) if ret_10d is not None and spy_ret_10d is not None else None,
            "sector_etf": sector_etf,
            "sector_etf_return_5d": round(etf_ret_5d, 4) if etf_ret_5d is not None else None,
            "beat_sector_5d": int(ret_5d > etf_ret_5d) if ret_5d is not None and etf_ret_5d is not None else None,
        })

    return rows


# -- Helpers ------------------------------------------------------------------

def _pct_return(price_start: float | None, price_end: float | None) -> float | None:
    """Compute percentage return (as decimal, e.g. 0.05 = 5%)."""
    if price_start is None or price_end is None or price_start <= 0:
        return None
    return (price_end / price_start) - 1.0


def _add_business_days(start: date, n: int) -> date:
    """Add n business days to a date (skipping weekends)."""
    current = start
    added = 0
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon-Fri
            added += 1
    return current
