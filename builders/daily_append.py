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

OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume", "VWAP"]


def _load_daily_closes(s3, bucket: str, date_str: str) -> dict[str, dict]:
    """Load today's daily_closes parquet from S3. Raises if the file is missing or unreadable.

    VWAP (2026-04-17): extracted from the parquet alongside OHLCV so it materializes
    into ArcticDB as a first-class column. Source is polygon grouped-daily's `vw`
    field when available, else `(H+L+C)/3` typical-price proxy — both are populated
    upstream in ``collectors/daily_closes.py``. Missing VWAP for a ticker becomes
    ``NaN`` in the output (not an error); downstream consumers handle NaN.
    """
    key = f"predictor/daily_closes/{date_str}.parquet"
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buf, engine="pyarrow")

    records = {}
    for ticker, row in df.iterrows():
        vwap_raw = row.get("VWAP")
        records[str(ticker)] = {
            "Open": float(row.get("Open", np.nan)),
            "High": float(row.get("High", np.nan)),
            "Low": float(row.get("Low", np.nan)),
            "Close": float(row.get("Close", np.nan)),
            "Volume": int(row.get("Volume", 0)),
            "VWAP": float(vwap_raw) if pd.notna(vwap_raw) else np.nan,
        }
    if not records:
        raise RuntimeError(
            f"daily_closes/{date_str}.parquet loaded zero tickers — upstream daily_closes collection is broken"
        )
    log.info("Loaded daily closes for %s: %d tickers", date_str, len(records))
    return records


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
    # _load_daily_closes raises on missing/empty file; no need for status-return guard.
    closes = _load_daily_closes(s3, bucket, date_str)

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
            except Exception as exc:
                raise RuntimeError(
                    f"Macro series {key} unreadable from ArcticDB — features depend on all macro inputs: {exc}"
                ) from exc
            if "Close" not in mdf.columns:
                raise RuntimeError(
                    f"Macro series {key} has no Close column — ArcticDB schema drift"
                )
            series = mdf["Close"].dropna()
            ticker_close = closes.get(key)
            if ticker_close and not np.isnan(ticker_close["Close"]):
                series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                series = series[~series.index.duplicated(keep="last")]
            macro[key] = series

        # Sector ETFs — every XL* in the macro library must read cleanly.
        # Missing any one corrupts sector-relative features for stocks in
        # that sector.
        for sym in macro_lib.list_symbols():
            if sym.startswith("XL"):
                try:
                    mdf = macro_lib.read(sym).data
                except Exception as exc:
                    raise RuntimeError(
                        f"Sector ETF {sym} unreadable from ArcticDB: {exc}"
                    ) from exc
                if "Close" not in mdf.columns:
                    raise RuntimeError(
                        f"Sector ETF {sym} has no Close column — ArcticDB schema drift"
                    )
                series = mdf["Close"].dropna()
                ticker_close = closes.get(sym)
                if ticker_close and not np.isnan(ticker_close["Close"]):
                    series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                    series = series[~series.index.duplicated(keep="last")]
                macro[sym] = series

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

    n_ok = 0       # fully-featured rows (all FEATURES finite)
    n_skip = 0     # legitimate skips (dry_run, NaN close from upstream)
    n_err = 0      # ArcticDB read failures
    n_partial = 0  # rows written with ≥1 NaN feature (short-history, etc.)

    for ticker in stock_tickers:
        try:
            # Read recent history from ArcticDB (need ~265 rows for feature warmup)
            if dry_run:
                n_skip += 1
                continue

            try:
                hist = universe_lib.read(ticker).data
            except Exception as exc:
                log.warning("Ticker %s not in ArcticDB: %s", ticker, exc)
                n_err += 1
                continue

            # Re-running daily_append for the same date MUST overwrite the
            # existing row, not skip it. universe_lib.update() is idempotent
            # (same-date rows replace instead of accumulate), so there's
            # nothing to guard against.
            #
            # Prior to 2026-04-18, a `today_ts in hist.index: skip` guard
            # silently no-op'd every re-run. That masked the 2026-04-17
            # label incident: after Polygon returned T-1 data stamped as T
            # in the morning DailyData run, a re-run with fresh polygon
            # data couldn't repair the poisoned rows. Removing the guard
            # restores the idempotency guarantee that update() provides.

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

            # Compute features on the combined series. `compute_features`
            # returns rows with NaN for features whose rolling-window
            # warmup exceeds the available history (short-history tickers
            # get ATR-14 computed on ≥14 rows, while 252-day features
            # stay NaN). Rows are never dropped — see 2026-04-21 docstring
            # in features/feature_engineer.py.
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

            if today_ts not in featured.index:
                # Only possible if combined had a genuine upstream data
                # issue (today's row disappeared during feature compute).
                log.warning(
                    "Ticker %s: today_ts missing from featured frame — "
                    "unexpected after compute_features stopped dropping rows",
                    ticker,
                )
                n_err += 1
                continue

            # Extract today's row with OHLCV + every feature that has
            # a column in the featured frame. Features that failed to
            # compute arrive as NaN and are written as NaN — first-class
            # support for partial coverage.
            keep_cols = [c for c in OHLCV_COLS if c in featured.columns] + \
                        [f for f in FEATURES if f in featured.columns]
            today_row = featured.loc[[today_ts], keep_cols].copy()

            # Per-ticker coverage observability: count NaN features now
            # so the eventual log + counter reflects exactly what's
            # being written. Silent partial coverage is forbidden
            # (feedback_no_silent_fails). Increment is deferred until
            # after universe_lib.update() so an exception rolls back
            # cleanly into n_err.
            nan_features = [
                f for f in FEATURES
                if f in today_row.columns and today_row[f].isna().iloc[0]
            ]

            # Match stored schema dtype per-column. ArcticDB rejects
            # updates whose column dtypes don't match the existing
            # version; stored dtype varies across tickers (some Volume
            # int64, some float64 depending on backfill vintage).
            # hist.dtypes[col] is authoritative by construction.
            # Feature columns that aren't yet in storage default to
            # float32 — matches the predictor training schema.
            for col in today_row.columns:
                if col in hist.columns:
                    today_row[col] = today_row[col].astype(hist.dtypes[col])
                elif col in FEATURES:
                    today_row[col] = today_row[col].astype("float32")

            today_row.index.name = "date"

            # Use update() rather than append() so a re-run on the same
            # date overwrites instead of accumulating duplicate rows.
            # 2026-04-15: diagnosed as root cause of the predictor training
            # failure where 904/909 tickers had duplicate date rows when
            # read back from ArcticDB. update() is idempotent — same-date
            # rows replace instead of append, regardless of race conditions.
            universe_lib.update(ticker, today_row)

            # Increment coverage counter + emit the partial-features log
            # only after the write landed.
            if nan_features:
                log.warning(
                    "partial-features ticker=%s rows=%d nan=%d/%d features=%s",
                    ticker, len(hist), len(nan_features), len(FEATURES),
                    nan_features,
                )
                n_partial += 1
            else:
                n_ok += 1

        except Exception as exc:
            log.warning("Failed to append %s: %s", ticker, exc)
            n_err += 1

    # ── 5. Update macro series ───────────────────────────────────────────────
    # update() semantics: same-date rows overwrite instead of appending. See
    # commentary at the universe_lib.update() call above for the 2026-04-15
    # duplicate-row diagnosis.
    #
    # Per feedback_hard_fail_until_stable: count which keys got updated vs
    # silently skipped due to missing closes data, verify the writes actually
    # landed, and raise with a named reject list if anything went missing.
    # Previous behavior: if closes.get(key) returned None (upstream collection
    # gap), the update was silently skipped. Combined with stock tickers all
    # hitting the "today already exists" skip path after a backfill, a run
    # could return status="ok" with ZERO data actually written. 2026-04-15
    # 08:39 PT manual rerun reproduced this — Step Function marked SUCCEEDED,
    # macro/SPY stayed at 4/10 for 5 days until an inference-side preflight
    # caught it.
    macro_missing_from_closes: list[str] = []
    macro_updated: list[str] = []
    sector_updated: list[str] = []

    if not dry_run:
        for key in macro_keys:
            bar = closes.get(key)
            if bar is None or np.isnan(bar.get("Close", np.nan)):
                macro_missing_from_closes.append(key)
                continue
            try:
                new_row = pd.DataFrame(
                    [{"Close": bar["Close"]}],
                    index=pd.DatetimeIndex([today_ts]),
                )
                new_row.index.name = "date"
                macro_lib.update(key, new_row)
                macro_updated.append(key)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to update macro {key} bar for {date_str}: {exc}"
                ) from exc

        # Sector ETFs — iterate the expected list explicitly rather than
        # filtering closes.keys(), so a missing XL* key surfaces as a loud
        # reject instead of a silent skip.
        sector_etfs = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
                       "XLP", "XLRE", "XLU", "XLV", "XLY"]
        for sym in sector_etfs:
            bar = closes.get(sym)
            if bar is None or np.isnan(bar.get("Close", np.nan)):
                macro_missing_from_closes.append(sym)
                continue
            try:
                new_row = pd.DataFrame(
                    [{"Close": bar["Close"]}],
                    index=pd.DatetimeIndex([today_ts]),
                )
                new_row.index.name = "date"
                macro_lib.update(sym, new_row)
                sector_updated.append(sym)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to update sector ETF {sym} bar for {date_str}: {exc}"
                ) from exc

        # Hard-fail on any missing key — macro inputs are not optional.
        # downstream feature compute + predictor preflight both depend on
        # these being fresh.
        if macro_missing_from_closes:
            raise RuntimeError(
                f"Macro/sector-ETF keys missing from today's daily_closes "
                f"parquet: {macro_missing_from_closes}. Upstream daily_closes "
                f"collection (polygon → FRED → yfinance fallback chain) did "
                f"not produce bars for these tickers on {date_str}. Macro "
                f"data is critical for downstream inference (SPY for "
                f"return_vs_spy_5d, VIX for vix_level, sector ETFs for "
                f"sector-relative features). Fix the upstream collection "
                f"before claiming pipeline success."
            )

        # Verify writes landed. The update() call above is fire-and-forget
        # (no return value surfaces a success flag), so we read back the
        # latest row for each key we claimed to update. If the readback
        # doesn't show today's date, something between update() and commit
        # silently dropped the write — the 2026-04-15 failure mode where
        # Step Function reported SUCCEEDED and macro stayed 5 days stale.
        verification_failures: list[tuple[str, str]] = []
        for key in macro_updated + sector_updated:
            try:
                readback = macro_lib.read(key).data
            except Exception as exc:
                verification_failures.append((key, f"readback error: {exc}"))
                continue
            if readback.empty:
                verification_failures.append((key, "readback empty"))
                continue
            last_ts = pd.Timestamp(readback.index[-1]).normalize()
            if last_ts != today_ts.normalize():
                verification_failures.append(
                    (key, f"last date {last_ts.date()} != expected {today_ts.date()}")
                )
        if verification_failures:
            raise RuntimeError(
                f"Macro update verification failed for {date_str}: "
                f"{verification_failures}. update() calls completed without "
                f"exception but readback shows stale data. Investigate "
                f"ArcticDB commit / consistency semantics."
            )

    t_total = time.time() - t0

    result = {
        "status": "ok",
        "date": date_str,
        "tickers_appended": n_ok,
        "tickers_partial": n_partial,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "load_seconds": round(t_load, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
    }

    log.info(
        "ArcticDB daily_append: stocks n_ok=%d n_partial=%d n_skip=%d n_err=%d (of %d) | "
        "macro_updated=%d sector_updated=%d | %.1fs total",
        n_ok, n_partial, n_skip, n_err, len(stock_tickers),
        len(macro_updated) if not dry_run else 0,
        len(sector_updated) if not dry_run else 0,
        t_total,
    )

    # Hard-fail on high error rate. ``n_ok == 0`` alone is NOT a failure
    # signal — it correctly occurs when every ticker hit the
    # "today already in ArcticDB" skip path (a second same-day invocation,
    # or a Step Function retry that runs after the first one succeeded).
    # The real silent-fail we're guarding against (ArcticDB-wide auth /
    # connectivity failure making every read throw) now registers as
    # ``n_err`` rather than ``n_skip`` after PR #24, so the 5% error-rate
    # threshold catches it without false positives on no-op reruns.
    # dry_run is exempt because it short-circuits the per-ticker loop.
    if not dry_run:
        err_rate = n_err / max(len(stock_tickers), 1)
        if err_rate > 0.05:
            raise RuntimeError(
                f"ArcticDB daily_append error rate {err_rate:.1%} exceeds 5% threshold "
                f"(n_ok={n_ok} n_err={n_err} of {len(stock_tickers)}) — treating as pipeline failure"
            )

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
