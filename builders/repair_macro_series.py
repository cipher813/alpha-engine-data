"""Repair a truncated macro series in the ArcticDB ``macro`` library and its
price-cache parquet, from the canonical upstream source.

Origin: alpha-engine-config-I9256 / I9255. ``macro/VIX3M`` fell from 2509 rows
to 16 across the 2026-08-22 and 2026-08-29 Saturday backfills because
``reference/price_cache/VIX3M.parquet`` had itself been overwritten with a
1-row yfinance response. ``builders/backfill.py`` then rewrote the ArcticDB
symbol wholesale from that parquet plus the ~16-trading-day ``daily_closes``
delta, which is why the surviving window slid forward one week at a time.

This module is the AUTOMATED repair for that class — a committed, idempotent,
re-runnable CLI rather than an operator procedure. It:

  1. fetches full history for each named symbol from the canonical source
     (yfinance; ``^``-prefixed for the index tickers, matching
     ``collectors/prices.py::_CARET_SYMBOLS``),
  2. UNIONS it with whatever the price-cache parquet and the ArcticDB symbol
     already hold — existing rows are never dropped, fetched rows win on
     overlapping dates,
  3. refuses to write anything that would leave either store with fewer rows
     than it started with,
  4. reads both back and reports the row count actually observed.

Run IN-REGION (AGENTS.md: manual one-off production data-repo writes run on an
EC2 box in the bucket's region, never from the laptop). ``--dry-run`` is
read-only and safe anywhere.

Usage::

    python -m builders.repair_macro_series --symbols VIX3M
    python -m builders.repair_macro_series --symbols VIX3M --dry-run
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys

import boto3
import pandas as pd

from builders._price_cache_writeboth import (
    price_cache_read_prefixes,
    price_cache_write_prefixes,
)
from collectors.prices import _CARET_SYMBOLS
from store.arctic_store import DEFAULT_BUCKET, get_macro_lib

log = logging.getLogger(__name__)

# Fetch window. Matches ``collectors/prices.py``'s production ``fetch_period``
# so a repaired series has exactly the span the weekly refresh would maintain.
DEFAULT_FETCH_PERIOD = "10y"

_OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]


def _yf_symbol(symbol: str) -> str:
    return f"^{symbol}" if symbol in _CARET_SYMBOLS else symbol


def fetch_full_history(symbol: str, period: str = DEFAULT_FETCH_PERIOD) -> pd.DataFrame:
    """Fetch ``period`` of daily OHLCV for ``symbol``. Raises on an empty answer."""
    import yfinance as yf

    from nousergon_lib.yfinance_quiet import yf_quiet

    @yf_quiet
    def _dl() -> pd.DataFrame:
        return yf.download(
            _yf_symbol(symbol),
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=False,
        )

    raw = _dl()
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    if raw is None or raw.empty or "Close" not in raw.columns:
        raise RuntimeError(
            f"Repair fetch for {symbol} ({_yf_symbol(symbol)}) returned no usable rows"
        )
    out = raw[[c for c in _OHLCV_COLS if c in raw.columns]].copy()
    out = out.dropna(subset=["Close"])
    idx = pd.to_datetime(out.index)
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    out.index = idx.normalize()
    out = out[~out.index.duplicated(keep="last")].sort_index()
    out.index.name = "date"
    if out.empty:
        raise RuntimeError(f"Repair fetch for {symbol} produced an empty frame after cleaning")
    return out


def union_no_shrink(existing: pd.DataFrame | None, fetched: pd.DataFrame, label: str) -> pd.DataFrame:
    """Union ``existing`` and ``fetched`` (fetched wins on overlap); never shrink.

    Raises if the union somehow holds fewer rows than ``existing`` — that would
    mean a column/index contract violation, and a repair that loses rows is the
    defect it is repairing.
    """
    if existing is None or existing.empty:
        return fetched
    common = [c for c in fetched.columns if c in existing.columns]
    if not common:
        # Existing store keeps a narrower schema (the macro lib is Close-only).
        # Project the fetch onto the existing columns so the union is well-formed.
        common = [c for c in existing.columns if c in fetched.columns]
    if not common:
        raise RuntimeError(
            f"{label}: existing columns {list(existing.columns)} and fetched columns "
            f"{list(fetched.columns)} do not overlap — refusing to write"
        )
    merged = pd.concat([existing[common], fetched[common]])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    merged.index.name = existing.index.name or "date"
    if len(merged) < len(existing):
        raise RuntimeError(
            f"{label}: union produced {len(merged)} rows from an existing {len(existing)} — "
            "refusing to write a shrinking repair"
        )
    return merged


def _read_parquet(s3, bucket: str, s3_prefix: str, symbol: str) -> pd.DataFrame | None:
    for prefix in price_cache_read_prefixes(s3_prefix):
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"{prefix}{symbol}.parquet")
        except s3.exceptions.NoSuchKey:
            continue
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        df.index = pd.to_datetime(df.index).normalize()
        return df
    return None


def repair_symbol(
    symbol: str,
    bucket: str = DEFAULT_BUCKET,
    s3_prefix: str = "predictor/price_cache/",
    period: str = DEFAULT_FETCH_PERIOD,
    dry_run: bool = False,
) -> dict:
    """Repair one macro symbol end to end. Returns a per-symbol result dict."""
    s3 = boto3.client("s3")
    macro_lib = get_macro_lib(bucket)

    fetched = fetch_full_history(symbol, period=period)

    existing_pq = _read_parquet(s3, bucket, s3_prefix, symbol)
    pq_before = 0 if existing_pq is None else len(existing_pq)
    merged_pq = union_no_shrink(existing_pq, fetched, f"price_cache.{symbol}")

    try:
        existing_arctic = macro_lib.read(symbol).data
        existing_arctic.index = pd.to_datetime(existing_arctic.index).normalize()
    except Exception:
        existing_arctic = None
    arctic_before = 0 if existing_arctic is None else len(existing_arctic)

    close_frame = pd.DataFrame({"Close": fetched["Close"]}, index=fetched.index)
    close_frame.index.name = "date"
    merged_arctic = union_no_shrink(existing_arctic, close_frame, f"macro.{symbol}")

    result = {
        "symbol": symbol,
        "fetched_rows": len(fetched),
        "parquet_rows_before": pq_before,
        "parquet_rows_planned": len(merged_pq),
        "arctic_rows_before": arctic_before,
        "arctic_rows_planned": len(merged_arctic),
        "first_date": str(merged_arctic.index.min().date()),
        "last_date": str(merged_arctic.index.max().date()),
        "dry_run": dry_run,
    }

    if dry_run:
        result["status"] = "ok_dry_run"
        return result

    buf = io.BytesIO()
    merged_pq.to_parquet(buf, engine="pyarrow", compression="snappy")
    body = buf.getvalue()
    for prefix in price_cache_write_prefixes(s3_prefix):
        s3.put_object(Bucket=bucket, Key=f"{prefix}{symbol}.parquet", Body=body)

    macro_lib.write(symbol, merged_arctic)

    readback = macro_lib.read(symbol).data
    result["arctic_rows_after"] = len(readback)
    result["arctic_first_after"] = str(pd.Timestamp(readback.index.min()).date())
    result["arctic_last_after"] = str(pd.Timestamp(readback.index.max()).date())
    if len(readback) < len(merged_arctic):
        raise RuntimeError(
            f"macro.{symbol}: readback {len(readback)} rows < written {len(merged_arctic)}"
        )
    pq_after = _read_parquet(s3, bucket, s3_prefix, symbol)
    result["parquet_rows_after"] = 0 if pq_after is None else len(pq_after)
    result["status"] = "ok"
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", required=True,
                        help="comma-separated macro symbols, e.g. VIX3M or VIX3M,HYOAS")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--s3-prefix", default="predictor/price_cache/")
    parser.add_argument("--period", default=DEFAULT_FETCH_PERIOD)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    results = []
    for symbol in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        res = repair_symbol(
            symbol,
            bucket=args.bucket,
            s3_prefix=args.s3_prefix,
            period=args.period,
            dry_run=args.dry_run,
        )
        log.info("repair %s: %s", symbol, json.dumps(res, default=str))
        results.append(res)

    print(json.dumps({"results": results}, default=str, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
