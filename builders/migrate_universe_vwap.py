"""builders/migrate_universe_vwap.py — normalize VWAP column position in ArcticDB universe.

Background (2026-04-27 EOD-email blackout investigation):
---------------------------------------------------------
Universe ArcticDB had heterogeneous schemas:
  - 816 symbols (~90%): 64 cols, no VWAP at all
  - 88  symbols (~10%): 65 cols, VWAP at idx=64 (appended at end)

``builders/daily_append`` writes via
``OHLCV_COLS = [Open, High, Low, Close, Volume, VWAP]``, so the per-stock row
has VWAP at idx=5 (between Volume and the feature block). ArcticDB's
``update()`` requires the argument's column order to match the stored version
exactly — both schema variants above mismatch the canonical write path, so
every per-stock write failed today with::

    The columns (names and types) in the argument are not identical to that
    of the existing version: UPDATE
    -FD<name=rsi_14, type=FLOAT32, idx=6>
    +FD<name=VWAP,   type=FLOAT64, idx=6>
    +FD<name=rsi_14, type=FLOAT32, idx=7>

This blocked daily_append step 4 entirely, which left every held ticker
without a 2026-04-27 row in universe_lib, which then hard-failed EOD
reconcile's per-position close lookup. (The structural fix in PR #104
already decoupled macro/SPY freshness from this — SPY landed cleanly. The
remaining surface is the per-stock universe write.)

Operational design (yfinance EOD → polygon morning):
----------------------------------------------------
- yfinance EOD post-close hook writes daily_closes parquet with VWAP=NaN
  (yfinance does not expose true volume-weighted VWAP).
- polygon morning enrichment overwrites the parquet with real VWAP values
  (true volume-weighted VWAP from polygon grouped-daily).
- daily_append runs end-of-day and writes whatever VWAP is in the parquet
  to ArcticDB universe — NaN initially, real values after morning re-run.

For that flow to work, VWAP must be a first-class column in the universe
schema with a stable position. This migration normalizes every symbol to::

    [Open, High, Low, Close, Volume, VWAP] + FEATURES

(canonical OHLCV_COLS order followed by the feature block). NaN-fills VWAP
historically for the 816 symbols that didn't have it. Repositions VWAP for
the 88 symbols that had it appended at the end.

Idempotent — symbols already in canonical order are skipped.

Usage::

    python -m builders.migrate_universe_vwap                  # dry-run
    python -m builders.migrate_universe_vwap --apply          # actually write
    python -m builders.migrate_universe_vwap --apply --tickers AAPL,MO  # subset
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
import numpy as np

from features.compute import DEFAULT_BUCKET
from store.arctic_store import get_universe_lib

log = logging.getLogger(__name__)

OHLCV_COLS_CANONICAL = ["Open", "High", "Low", "Close", "Volume", "VWAP"]
AUDIT_PREFIX = "builders/migrate_universe_vwap_audit/"
DEFAULT_WORKERS = 16


def _canonical_column_order(existing_cols: list[str]) -> list[str]:
    """Return the canonical column ordering for a universe symbol.

    OHLCV_COLS_CANONICAL first (with VWAP inserted at idx=5 if missing),
    then every existing non-OHLCV column in its current relative order
    (i.e. the feature block stays as-is). Drops nothing.
    """
    ohlcv_set = set(OHLCV_COLS_CANONICAL)
    feature_block = [c for c in existing_cols if c not in ohlcv_set]
    return list(OHLCV_COLS_CANONICAL) + feature_block


def _is_canonical(existing_cols: list[str]) -> bool:
    """True iff existing column order already matches the canonical layout."""
    if "VWAP" not in existing_cols:
        return False
    canonical = _canonical_column_order(existing_cols)
    return list(existing_cols) == canonical


def _write_audit(s3, bucket: str, summary: dict) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{AUDIT_PREFIX}{ts}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(summary, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("Wrote audit to s3://%s/%s", bucket, key)


def migrate_universe_vwap(
    *,
    bucket: str = DEFAULT_BUCKET,
    apply: bool = False,
    tickers_override: list[str] | None = None,
) -> dict:
    """Normalize universe symbols to canonical [OHLCV+VWAP, FEATURES] ordering.

    Parameters
    ----------
    bucket
        S3 bucket holding ArcticDB.
    apply
        If True, actually write the reordered/added-VWAP frames. Default
        False (dry-run; counts only).
    tickers_override
        Subset of symbols to migrate (rest are left alone). Useful for
        canary runs and one-off repairs. ``None`` = every symbol in the
        universe library.

    Returns
    -------
    summary dict with the action plan and outcome.
    """
    s3 = boto3.client("s3")
    universe_lib = get_universe_lib(bucket)

    arctic_symbols = sorted(universe_lib.list_symbols())
    log.info("ArcticDB universe holds %d symbols", len(arctic_symbols))

    if tickers_override is not None:
        targets = sorted(set(tickers_override) & set(arctic_symbols))
        ignored = sorted(set(tickers_override) - set(arctic_symbols))
        if ignored:
            log.warning(
                "Skipping %d tickers from --tickers override that aren't in "
                "ArcticDB: %s",
                len(ignored), ignored,
            )
    else:
        targets = arctic_symbols

    migrated: list[dict] = []
    already_canonical: list[str] = []
    errors: list[dict] = []

    # Each per-symbol op is read → transform → write — all S3-bound, GIL
    # released on every network call. Mirror daily_append's Phase 2 fan-out:
    # one ThreadPoolExecutor across all symbols, env-overridable worker count
    # so prod can tune without a redeploy. The first apply run on the full
    # 904-symbol universe at sequential speed (~8/min) hit the SSM 1-hour
    # ceiling at 60% complete; threading at 16 workers brings it to ~10 min.
    workers = int(os.environ.get("MIGRATE_UNIVERSE_VWAP_WORKERS", str(DEFAULT_WORKERS)))

    def _migrate_one(ticker: str) -> dict:
        """Read → transform → (optionally) write a single symbol.

        Returns a per-ticker outcome dict so the main thread can aggregate.
        Exceptions are captured into the dict (not raised) so one bad symbol
        never aborts the batch.
        """
        try:
            df = universe_lib.read(ticker).data
        except Exception as exc:
            return {"ticker": ticker, "outcome": "read_error", "error": str(exc)}

        existing_cols = list(df.columns)
        if _is_canonical(existing_cols):
            return {"ticker": ticker, "outcome": "already_canonical"}

        if "VWAP" not in df.columns:
            df["VWAP"] = np.nan  # float64 by default — matches daily_append dtype

        canonical = _canonical_column_order(list(df.columns))
        df = df[canonical]

        record = {
            "ticker": ticker,
            "outcome": "migrated",
            "rows": len(df),
            "had_vwap": "VWAP" in existing_cols,
            "previous_vwap_idx": (
                existing_cols.index("VWAP") if "VWAP" in existing_cols else None
            ),
            "new_vwap_idx": canonical.index("VWAP"),
        }
        if apply:
            try:
                universe_lib.write(ticker, df, prune_previous_versions=True)
            except Exception as exc:
                return {"ticker": ticker, "outcome": "write_error", "error": str(exc)}
        return record

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_migrate_one, targets))
    elapsed = time.time() - t0
    log.info(
        "Threadpooled migration: %d targets in %.1fs (workers=%d)",
        len(targets), elapsed, workers,
    )

    # Aggregation runs on the main thread — counter mutation stays
    # single-threaded so we don't need locks (mirrors daily_append).
    for r in results:
        outcome = r["outcome"]
        if outcome == "already_canonical":
            already_canonical.append(r["ticker"])
        elif outcome == "migrated":
            log_prefix = "MIGRATED" if apply else "DRY-RUN would migrate"
            log.info(
                "%s ticker=%s rows=%d previous_vwap_idx=%s -> new_vwap_idx=%d",
                log_prefix, r["ticker"], r["rows"],
                r["previous_vwap_idx"], r["new_vwap_idx"],
            )
            migrated.append(r)
        elif outcome == "read_error":
            log.error("Could not read %s: %s", r["ticker"], r["error"])
            errors.append({"ticker": r["ticker"], "stage": "read", "error": r["error"]})
        elif outcome == "write_error":
            log.error("Failed to write %s: %s", r["ticker"], r["error"])
            errors.append({"ticker": r["ticker"], "stage": "write", "error": r["error"]})
        else:
            raise RuntimeError(f"unexpected outcome={outcome!r} for {r['ticker']}")

    summary = {
        "status": "ok" if not errors else "partial",
        "applied": apply,
        "arctic_universe_size": len(arctic_symbols),
        "targets_count": len(targets),
        "migrated_count": len(migrated),
        "already_canonical_count": len(already_canonical),
        "errors_count": len(errors),
        "elapsed_seconds": round(elapsed, 1),
        "workers": workers,
        "migrated": migrated,
        "already_canonical": already_canonical,
        "errors": errors,
    }

    log.info(
        "migrate_universe_vwap: applied=%s targets=%d migrated=%d "
        "already_canonical=%d errors=%d elapsed=%.1fs workers=%d",
        apply, len(targets), len(migrated), len(already_canonical),
        len(errors), elapsed, workers,
    )

    _write_audit(s3, bucket, summary)

    return summary


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually rewrite. Default dry-run.",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated subset of tickers to migrate (default: all).",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET})",
    )
    args = parser.parse_args()

    tickers_override = (
        [t.strip() for t in args.tickers.split(",") if t.strip()]
        if args.tickers
        else None
    )

    result = migrate_universe_vwap(
        bucket=args.bucket,
        apply=args.apply,
        tickers_override=tickers_override,
    )
    print(json.dumps(result, indent=2, default=str))
    if result["errors_count"] > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
