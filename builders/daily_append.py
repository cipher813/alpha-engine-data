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
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

from botocore.exceptions import ClientError

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
from arcticdb.version_store.library import ReadRequest
from arcticdb_ext.version_store import DataError

from store.arctic_store import get_universe_lib, get_macro_lib
from store.parquet_loader import load_parquet_from_s3

log = logging.getLogger(__name__)

OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume", "VWAP"]
PRICE_CACHE_PREFIX = "predictor/price_cache/"


def _write_row_backfill_safe(
    lib,
    symbol: str,
    new_row: pd.DataFrame,
    existing_series: pd.DataFrame | None = None,
) -> str:
    """Write a single-date row to ArcticDB, handling both append and backfill cases.

    Returns the mode used: ``"append"`` (target_date > all existing dates,
    used update() — fast) or ``"backfill"`` (target_date is in the middle
    of an existing series, used read+splice+write() — necessary because
    update() requires monotonic insertion at the head).

    The backfill path is ~10-100x slower per ticker (full series read +
    full rewrite vs. single-row update) but fires only for rare backfill
    operations like the 2026-04-24 historical VWAP repair after the
    polygon outage.
    """
    target_ts = new_row.index[0]

    # If caller already has the existing series (the per-ticker loop in
    # daily_append already reads `hist` for feature warmup), reuse it
    # instead of double-reading.
    if existing_series is None:
        try:
            existing_series = lib.read(symbol).data
        except Exception:
            # Symbol doesn't exist yet — first write is always an append.
            lib.write(symbol, new_row, prune_previous_versions=True)
            return "append"

    if existing_series.empty or target_ts > existing_series.index.max():
        # Append at head — fast path. update() is idempotent for same-date
        # rows (replaces in place rather than appending duplicates).
        lib.update(symbol, new_row)
        return "append"

    # Backfill — splice new_row into existing series, write back full
    # series. Required because ArcticDB's update() refuses non-monotonic
    # insertion ("index must be monotonic increasing or decreasing").
    # Same-date rows are deduped with keep="last" so the new row wins
    # over any existing row at target_ts (matches update() semantics).
    combined = pd.concat([existing_series, new_row])
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    lib.write(symbol, combined, prune_previous_versions=True)
    return "backfill"


def _emit_missing_from_closes_metric(count: int) -> None:
    """Emit ``AlphaEngine/Data/missing_from_closes_count`` gauge.

    Best-effort: CloudWatch errors WARN but don't fail the pipeline — the
    hard-fail above the threshold is the load-bearing path, the metric +
    alarm catches slow drift below the threshold (1-2 silently-missing
    tickers per day adds up to a regression like the 2026-04-25 incident
    if uncaught). Pattern mirrors ``_emit_admission_refused_metric`` in
    alpha-engine/executor/signal_reader.py.
    """
    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "missing_from_closes_count",
                "Value": float(count),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        log.warning(
            "CloudWatch missing_from_closes_count metric failed: %s. "
            "Not blocking daily_append — the threshold check above already "
            "surfaced the count.",
            exc,
        )


def _load_parquet_warmup(s3, bucket: str, ticker: str) -> pd.DataFrame | None:
    """Load a ticker's 10y price-cache parquet for feature warmup.

    Returns None when the parquet doesn't exist (new constituent that hasn't
    been picked up by the weekly backfill yet). Hard-fails on any other
    error shape — NoSilentFails.
    """
    key = f"{PRICE_CACHE_PREFIX}{ticker}.parquet"
    try:
        df = load_parquet_from_s3(s3, bucket, key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return None
        raise RuntimeError(
            f"parquet-warmup read failed for {ticker} (bucket={bucket}, "
            f"key={key}): {exc}"
        ) from exc

    if df.empty or "Close" not in df.columns:
        raise RuntimeError(
            f"parquet-warmup for {ticker}: parquet exists but invalid shape "
            f"(empty={df.empty}, cols={list(df.columns)[:6]})"
        )
    return df


def _load_daily_closes(s3, bucket: str, date_str: str) -> dict[str, dict]:
    """Load today's daily_closes parquet from S3. Raises if the file is missing or unreadable.

    VWAP semantics (per the 2026-04-17 Phase 7 VWAP centralization decision,
    refined by the 2026-04-23 split-by-source PR):

      * Polygon grouped-daily (collected via ``daily_closes --source polygon_only``
        in the morning enrichment pass) → true volume-weighted VWAP from
        polygon's ``vw`` field.

      * yfinance EOD pass (``daily_closes --source yfinance_only``, ~1:05 PM PT)
        → VWAP=None. yfinance does not expose true VWAP and the (H+L+C)/3
        typical-price proxy was explicitly REJECTED in 2026-04-17 because it
        misrepresented arithmetic typical price as volume-weighted VWAP.
        Morning polygon enrichment overwrites the row to fill VWAP.

      * FRED fallback for indices (VIX/VIX3M/TNX/IRX) → VWAP=None. Single
        daily Close value with no trade distribution to weight.

    Missing VWAP becomes ``NaN`` in the output (not an error); downstream
    consumers (executor's ``load_daily_vwap``) handle NaN by walking back up
    to 5 trading days for a populated value.
    """
    key = f"staging/daily_closes/{date_str}.parquet"
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

    # ── 2a. Update macro / sector-ETF series in ArcticDB ─────────────────────
    # This block was previously the final write step (old "step 5") and ran
    # AFTER the universe-coverage guard at step 2b. That ordering coupled
    # macro/SPY freshness to stock-coverage correctness: a 7-stock universe
    # gap on 2026-04-27 raised at the guard before SPY ever got written, which
    # then hard-failed the EOD reconcile (alpha against stale SPY is by-design
    # rejected) and blacked out the EOD email + alpha tracking for the day.
    #
    # Macro keys are a fixed list of ~18 well-known tickers (SPY, VIX, sector
    # ETFs); their freshness has nothing to do with whether 5 or 50 stocks
    # went missing in the universe collection. Doing the macro write FIRST
    # decouples the two concerns: macro lands in ArcticDB regardless of
    # downstream stock-side issues, and the universe guard still raises
    # non-zero so operators get paged on the universe gap. Net effect on
    # 2026-04-27-style failures: EOD email goes out, daily-data still exits 1.
    #
    # update() semantics: same-date rows overwrite instead of appending. See
    # _write_row_backfill_safe — it routes append vs backfill correctly.
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

    # Track per-symbol write mode (append vs backfill) so the verification
    # check below can apply the right correctness assertion. Append: last
    # readback row should equal target_ts. Backfill: target_ts should be
    # in the readback index (could be anywhere in the middle).
    macro_write_modes: dict[str, str] = {}

    macro_keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]

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
                mode = _write_row_backfill_safe(macro_lib, key, new_row)
                macro_updated.append(key)
                macro_write_modes[key] = mode
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
                mode = _write_row_backfill_safe(macro_lib, sym, new_row)
                sector_updated.append(sym)
                macro_write_modes[sym] = mode
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

        # Verify writes landed. The update() / write() calls above are
        # fire-and-forget (no return value surfaces a success flag), so
        # we read back each key and assert target_ts is present. The
        # check is mode-aware:
        #   - append mode: last readback row should equal target_ts
        #     (catches the 2026-04-15 silent-stale failure where SSM
        #     reported SUCCEEDED but macro/SPY stayed 5 days behind)
        #   - backfill mode: target_ts should be IN the readback index,
        #     anywhere (last date is naturally future relative to
        #     target_ts when we backfill an old date)
        target_ts_norm = today_ts.normalize()
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
            mode = macro_write_modes.get(key, "append")
            if mode == "backfill":
                # Target date should be present somewhere in the series.
                index_norm = pd.DatetimeIndex(readback.index).normalize()
                if target_ts_norm not in index_norm:
                    verification_failures.append(
                        (key, f"backfill target {target_ts_norm.date()} not in readback index "
                              f"(last={pd.Timestamp(readback.index[-1]).date()})")
                    )
            else:
                last_ts = pd.Timestamp(readback.index[-1]).normalize()
                if last_ts != target_ts_norm:
                    verification_failures.append(
                        (key, f"last date {last_ts.date()} != expected {target_ts_norm.date()}")
                    )
        if verification_failures:
            raise RuntimeError(
                f"Macro update verification failed for {date_str}: "
                f"{verification_failures}. update()/write() calls completed without "
                f"exception but readback shows the row is missing. Investigate "
                f"ArcticDB commit / consistency semantics."
            )

    # ── 2b. Detect tickers that exist in ArcticDB universe but are missing
    #        from today's daily_closes parquet ─────────────────────────────────
    # Without this guard, the line ~274 ``stock_tickers = [t for t in closes ...]``
    # filter silently drops every ArcticDB symbol absent from today's closes —
    # no counter increments, no WARN log. That class was the recurring "8
    # tickers regressed to 4/01" failure mode (ROADMAP 2026-04-25 P1) — daily
    # closes upstream stops returning a ticker, daily_append silently no-ops
    # writes for it across many weekdays, and the regression only surfaces
    # when an unrelated freshness preflight catches it days later.
    #
    # Runs AFTER the macro write at step 2a so a stock-universe gap can't
    # block macro/SPY freshness — see the rationale on the 2a header for the
    # 2026-04-27 EOD blackout that motivated decoupling these two concerns.
    # This guard still raises non-zero on threshold violations; pipelines
    # exit 1 and operators get paged. Threshold default (5) matches the
    # absolute count of the 2026-04-25 regression; env-overridable so prod
    # can tune without a redeploy.
    n_missing_from_closes = 0
    if not dry_run:
        try:
            arctic_stock_symbols = set(universe_lib.list_symbols())
        except Exception as exc:
            raise RuntimeError(
                f"Could not list ArcticDB universe symbols (needed for "
                f"missing-from-closes check): {exc}"
            ) from exc
        # closes contains everything: stocks + macro keys + sector ETFs.
        # Reduce to the stock set so the diff is apples-to-apples with
        # universe_lib's contents (which holds only stocks). Same predicate
        # as the line ~274 stock_tickers filter — keep them in lockstep.
        closes_stock_keys = {
            t for t in closes
            if t not in _SKIP_TICKERS and not _is_sector_etf(t)
        }
        missing_from_closes = sorted(arctic_stock_symbols - closes_stock_keys)
        n_missing_from_closes = len(missing_from_closes)
        # Always emit a metric — silent regression in the slow-drift band
        # (1-2 tickers) won't trip the hard-fail but is still observable.
        _emit_missing_from_closes_metric(n_missing_from_closes)
        threshold = int(
            os.environ.get("DAILY_APPEND_MISSING_THRESHOLD", "5")
        )
        if n_missing_from_closes > threshold:
            raise RuntimeError(
                f"daily_append: {n_missing_from_closes} tickers in ArcticDB "
                f"universe missing from today's daily_closes parquet "
                f"(threshold={threshold}). Missing: {missing_from_closes}. "
                f"Either upstream daily_closes collection stopped emitting "
                f"these tickers (most common — polygon/yfinance hiccup or "
                f"ticker-listing change), or these tickers are legitimately "
                f"delisted and need to be pruned from ArcticDB universe "
                f"(see ROADMAP P2 'ArcticDB universe pruning'). "
                f"Override threshold via DAILY_APPEND_MISSING_THRESHOLD env "
                f"var if you've already triaged the list."
            )
        elif n_missing_from_closes > 0:
            log.warning(
                "daily_append: %d tickers in ArcticDB universe missing from "
                "closes (below %d hard-fail threshold) — %s",
                n_missing_from_closes, threshold, missing_from_closes,
            )

    # ── 3. Load macro series from ArcticDB into in-memory dict ───────────────
    # Read-only on ArcticDB. Builds a multi-day series per macro key for the
    # per-stock feature loop (return_vs_spy_5d, vix_level, etc., which need
    # context not just today's value). After step 2a's write, today's row is
    # already in ArcticDB — the pd.concat below is now redundant in the happy
    # path, but kept defensively so this block remains correct if step 2a is
    # ever skipped or factored out.
    macro: dict[str, pd.Series] = {}

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

    n_ok = 0              # fully-featured rows (all FEATURES finite)
    n_skip = 0            # legitimate skips (dry_run, NaN close from upstream)
    n_err = 0             # ArcticDB read failures
    n_partial = 0         # rows written with ≥1 NaN feature (short-history, etc.)
    n_parquet_warmup = 0  # rows whose feature compute used parquet-enriched context

    # ── 4a. Batch-read every ticker's full universe history upfront ──────────
    # Replaces the prior per-ticker `universe_lib.read(ticker)` loop. ArcticDB's
    # read_batch parallelizes the underlying S3 round-trips internally, cutting
    # ~900 sequential reads at ~0.3-0.5s each (5-7 minutes wall time) down to a
    # single batched call. The full series (no date_range slice) is required
    # because `_write_row_backfill_safe` rewrites the full symbol on the
    # backfill path, and most MorningEnrich runs hit backfill (target_ts is
    # the prior trading day, already written by post-close DailyData).
    # Missing symbols come back as DataError objects (not exceptions) — they're
    # filtered into n_err with the same semantics as the old per-ticker
    # `try/except Exception` branch.
    hists_by_ticker: dict[str, pd.DataFrame] = {}
    if not dry_run and stock_tickers:
        t_read0 = time.time()
        read_results = universe_lib.read_batch(
            [ReadRequest(symbol=t) for t in stock_tickers]
        )
        for ticker, result in zip(stock_tickers, read_results):
            if isinstance(result, DataError):
                log.warning(
                    "Ticker %s not in ArcticDB: %s",
                    ticker, result.exception_string,
                )
                n_err += 1
                continue
            hists_by_ticker[ticker] = result.data
        log.info(
            "Batched universe read: %d/%d tickers in %.1fs",
            len(hists_by_ticker), len(stock_tickers), time.time() - t_read0,
        )

    # ── 4b. Phase 1 — sequential compute pass ────────────────────────────────
    # Per-ticker feature compute stays sequential so we don't have to reason
    # about pandas/numpy thread safety on the shared macro series. The
    # bottleneck this PR targets is the I/O-bound write phase below; CPU
    # parallelism is a separate (higher-risk) lever to pull later if needed.
    write_tasks: list[tuple[str, pd.DataFrame, pd.DataFrame, list[str]]] = []
    for ticker in stock_tickers:
        try:
            # Read recent history from ArcticDB (need ~265 rows for feature warmup)
            if dry_run:
                n_skip += 1
                continue

            hist = hists_by_ticker.get(ticker)
            if hist is None:
                # Ticker was missing from ArcticDB — already counted into
                # n_err during the batch read above, skip silently.
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

            # Warmup context — ArcticDB by default; parquet-enriched when the
            # ArcticDB history is too short for full feature warmup.
            #
            # Before this change, short-history tickers (new listings, spinoffs,
            # recent constituent adds) accumulated feature coverage one day at
            # a time — features with 252-day rolling windows stayed NaN for
            # up to a year after the ticker entered ArcticDB, even though the
            # weekly backfill's 10y parquet held the full series. That state
            # routinely caused manual polygon backfills (8 tickers in one day,
            # 2026-04-22) just to unblock downstream consumers.
            #
            # When len(hist) is below the feature-warmup threshold we union
            # the ticker's `predictor/price_cache/{T}.parquet` (full 10y
            # adjusted OHLCV, rebuilt every Saturday by backfill.py) with
            # ArcticDB by date. ArcticDB wins on overlapping dates because
            # daily_append writes there every weekday — it's fresher than a
            # parquet that can be up to 6 days old. Full-history tickers
            # (~99% of the universe on a steady-state day) skip the parquet
            # read entirely.
            #
            # `hist` (the original ArcticDB read) remains authoritative for
            # the write schema (dtype matching via hist.dtypes[col] at the
            # update() call below). Only the feature-compute context is
            # enriched.
            warmup_source = hist
            if len(hist) < MIN_ROWS_FOR_FEATURES:
                parquet_df = _load_parquet_warmup(s3, bucket, ticker)
                if parquet_df is None:
                    log.warning(
                        "short-history-no-parquet ticker=%s arctic_rows=%d "
                        "— falling through to NaN-feature degrade",
                        ticker, len(hist),
                    )
                else:
                    parquet_ohlcv = parquet_df[
                        [c for c in OHLCV_COLS if c in parquet_df.columns]
                    ]
                    arctic_ohlcv = hist[
                        [c for c in OHLCV_COLS if c in hist.columns]
                    ]
                    warmup_source = pd.concat([parquet_ohlcv, arctic_ohlcv])
                    warmup_source = warmup_source[
                        ~warmup_source.index.duplicated(keep="last")
                    ].sort_index()
                    n_parquet_warmup += 1
                    log.info(
                        "parquet-warmup ticker=%s arctic_rows=%d "
                        "parquet_rows=%d stitched_rows=%d",
                        ticker, len(hist), len(parquet_df), len(warmup_source),
                    )

            # Combine warmup OHLCV + today's bar for feature computation
            hist_ohlcv = warmup_source[
                [c for c in OHLCV_COLS if c in warmup_source.columns]
            ]
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

            # Defer the actual ArcticDB write — collected here so Phase 2
            # can run them in parallel via a thread pool. The previous
            # sequential per-ticker `_write_row_backfill_safe` call took
            # ~300-400ms × 900 = ~5 minutes wall time, the residual half
            # of the budget after the read_batch optimization in PR #99.
            write_tasks.append((ticker, today_row, hist, nan_features))

        except Exception as exc:
            log.warning("Failed to compute %s: %s", ticker, exc)
            n_err += 1

    # ── 4c. Phase 2 — parallel writes via ThreadPoolExecutor ─────────────────
    # ArcticDB writes against different symbols are independent, and the
    # underlying S3 round-trip releases the GIL on every network op, so a
    # thread pool is a clean fit for the per-ticker write fan-out. Worker
    # count is overridable via env var so prod can tune without a redeploy
    # if the boto3 connection-pool ceiling changes.
    write_workers = int(os.environ.get("DAILY_APPEND_WRITE_WORKERS", "16"))

    def _do_write(task):
        _ticker, _today_row, _hist, _nan_features = task
        try:
            _write_row_backfill_safe(
                universe_lib, _ticker, _today_row, existing_series=_hist
            )
            return ("ok", _ticker, _nan_features, len(_hist), None)
        except Exception as exc:  # pragma: no cover — covered by err test
            return ("err", _ticker, None, len(_hist), exc)

    if write_tasks:
        t_write0 = time.time()
        with ThreadPoolExecutor(max_workers=write_workers) as pool:
            write_results = list(pool.map(_do_write, write_tasks))
        log.info(
            "Threadpooled writes: %d tickers in %.1fs (workers=%d)",
            len(write_tasks), time.time() - t_write0, write_workers,
        )

        # Aggregation runs on the main thread — counter mutation stays
        # single-threaded so we don't need locks.
        for status, ticker, nan_features, hist_rows, exc in write_results:
            if status == "err":
                log.warning("Failed to append %s: %s", ticker, exc)
                n_err += 1
                continue
            if nan_features:
                log.warning(
                    "partial-features ticker=%s rows=%d nan=%d/%d features=%s",
                    ticker, hist_rows, len(nan_features), len(FEATURES),
                    nan_features,
                )
                n_partial += 1
            else:
                n_ok += 1

    t_total = time.time() - t0

    result = {
        "status": "ok",
        "date": date_str,
        "tickers_appended": n_ok,
        "tickers_partial": n_partial,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "tickers_parquet_warmup": n_parquet_warmup,
        "tickers_missing_from_closes": n_missing_from_closes,
        "load_seconds": round(t_load, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
    }

    log.info(
        "ArcticDB daily_append: stocks n_ok=%d n_partial=%d n_skip=%d n_err=%d "
        "n_parquet_warmup=%d n_missing_from_closes=%d (of %d) | "
        "macro_updated=%d sector_updated=%d | %.1fs total",
        n_ok, n_partial, n_skip, n_err, n_parquet_warmup,
        n_missing_from_closes, len(stock_tickers),
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
