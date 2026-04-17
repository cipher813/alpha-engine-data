"""
Data-module postflight: consumer-contract checks run at the end of DataPhase1
before the health marker is written.

Producer owns the consumer contract. Every downstream freshness/shape check
(research's ``PriceFetchError`` / ``MacroFetchError``, predictor's
``_verify_arctic_fresh``, backtester's ArcticDB reads) must have a matching
producer-side postflight that fails FIRST. This eliminates blast-radius
fan-out — one ``alpha-engine-saturday-sf-failed`` alarm at DataPhase1 instead
of 3× downstream cold-starts each reporting the same root cause — and avoids
compute waste on downstream Lambda invocations / spot-EC2 bootstraps doomed
to fail at preflight.

Contract encoded here is the union of:

  1. Predictor `inference/stages/load_prices.py::_verify_arctic_fresh`
     (SPY last-row ≥ run_date - 1)
  2. Research `data/fetchers/price_fetcher.py::_load_constituents_from_s3`
     (``constituents.json`` HEAD + parse + ≥ 800 tickers)
  3. Research `data/fetchers/macro_fetcher.py::fetch_macro_data`
     (``macro.json`` HEAD + parse + ``fed_funds_rate`` populated)
  4. Research preflight `_check_arcticdb_universe`
     (universe library reachable, sample tickers fresh)

Failure semantics: each check raises ``PostflightError`` (a ``RuntimeError``
subclass) with a specific named message. The caller in
``weekly_collector._finalize()`` catches, flips ``results["status"]`` to
``"postflight_failed"``, writes the health marker accordingly, and lets
``main()``'s SystemExit(1) propagate through SSM → Step Function
HandleFailure → CloudWatch alarm.
"""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


class PostflightError(RuntimeError):
    """Raised when a DataPhase1 output fails a consumer contract check."""
    pass


# Universe sample size for freshness checks. 20 tickers balances confidence
# in detecting partial writes (one missing ticker in 900 is ~0.1% — sampling
# 20 gives ~20% chance of catching a single-ticker miss per run, but catches
# any systematic write failure that drops >5% of tickers with near-certainty).
_UNIVERSE_SAMPLE_SIZE = 20

# Max staleness of a sampled universe ticker relative to SPY's last row.
# >2d tolerates weekend-adjacent partial writes but catches genuine staleness.
_UNIVERSE_MAX_STALE_VS_SPY_DAYS = 2

# Minimum SPY freshness: last row must be ≥ run_date - 1 calendar day.
# DataPhase1 for run_date=<Saturday> expects SPY to have <Friday>'s close,
# which is run_date - 1 calendar day. Tighter than the preflight check that
# tolerates weekend runs (Saturday 00:00 UTC ≈ Friday 5-8 PM PT).
_MACRO_SPY_MAX_STALE_DAYS = 1

# Minimum constituent count for a valid constituents.json payload.
# Matches research's ``fetch_sp500_sp400_with_sectors`` contract
# (S&P 500 + S&P 400 ≈ 900 tickers; we tolerate down to 800 for membership
# churn + deduplication margin).
_MIN_CONSTITUENTS = 800


class DataPostflight:
    """Producer-side consumer-contract checks for DataPhase1 output.

    Parameters
    ----------
    bucket : str
        S3 bucket hosting market_data/ + ArcticDB + health/ prefixes.
    run_date : str
        YYYY-MM-DD stamp identifying the Saturday pipeline run.
    market_prefix : str
        S3 key prefix for market_data (typically ``"market_data/"``).
    phase : int
        Phase number (only Phase 1 is gated today; Phase 2 gets its own
        postflight when the alternative-data contract is encoded).
    """

    def __init__(
        self,
        bucket: str,
        run_date: str,
        market_prefix: str,
        phase: int,
    ) -> None:
        self.bucket = bucket
        self.run_date = run_date
        self.market_prefix = market_prefix
        self.phase = phase
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        # Lazy-initialized handles (set on first use to keep __init__ cheap).
        self._s3 = None
        self._universe_lib = None
        self._macro_lib = None

    # ── Lazy handles ─────────────────────────────────────────────────────────

    def _s3_client(self):
        if self._s3 is None:
            import boto3
            self._s3 = boto3.client("s3", region_name=self.region)
        return self._s3

    def _open_arctic_libs(self) -> "tuple[Any, Any]":
        if self._universe_lib is None or self._macro_lib is None:
            import arcticdb as adb
            uri = (
                f"s3s://s3.{self.region}.amazonaws.com:{self.bucket}"
                "?path_prefix=arcticdb&aws_auth=true"
            )
            try:
                arctic = adb.Arctic(uri)
                self._universe_lib = arctic.get_library("universe")
                self._macro_lib = arctic.get_library("macro")
            except Exception as exc:
                raise PostflightError(
                    f"ArcticDB unreachable at {uri}: {exc}"
                ) from exc
        return self._universe_lib, self._macro_lib

    # ── Checks ───────────────────────────────────────────────────────────────

    def _check_macro_spy_fresh(self) -> None:
        """Consumer: predictor ``_verify_arctic_fresh``.

        SPY lives in the ArcticDB macro library. For a Saturday run_date, SPY's
        last row must be ≥ run_date - 1 (= the prior Friday's close).
        """
        _, macro_lib = self._open_arctic_libs()
        try:
            df = macro_lib.read("SPY", columns=["Close"]).data
        except Exception as exc:
            raise PostflightError(
                f"ArcticDB macro.SPY unreadable: {exc} — universe_returns or "
                f"daily_append did not write SPY this run."
            ) from exc

        if df is None or df.empty:
            raise PostflightError(
                "ArcticDB macro.SPY has zero rows — daily_append has never written."
            )

        last_ts = pd.Timestamp(df.index[-1])
        if last_ts.tzinfo is not None:
            last_ts = last_ts.tz_convert("UTC").tz_localize(None)
        last_date = last_ts.normalize()
        expected = pd.Timestamp(self.run_date).normalize()
        stale_days = (expected - last_date).days
        if stale_days > _MACRO_SPY_MAX_STALE_DAYS:
            raise PostflightError(
                f"ArcticDB macro.SPY last_date={last_date.date()} is "
                f"{stale_days}d stale for run_date={expected.date()} "
                f"(>{_MACRO_SPY_MAX_STALE_DAYS}d threshold). Predictor's "
                f"_verify_arctic_fresh will reject this."
            )
        log.info(
            "postflight: ArcticDB macro.SPY last_date=%s (%dd ≤ %dd)",
            last_date.date(), stale_days, _MACRO_SPY_MAX_STALE_DAYS,
        )

    def _check_universe_sample_fresh(self) -> None:
        """Consumer: research preflight + predictor per-ticker ArcticDB reads.

        Samples ``_UNIVERSE_SAMPLE_SIZE`` tickers from the universe library and
        asserts each has a last-row date within ``_UNIVERSE_MAX_STALE_VS_SPY_DAYS``
        of SPY's last row. Catches partial writes where most tickers landed
        but some didn't — the symptom that would otherwise surface as
        per-ticker error rate in research's ``PriceFetchError`` check at
        Lambda runtime.
        """
        universe_lib, macro_lib = self._open_arctic_libs()

        # SPY last date serves as the staleness reference (already validated above).
        spy_last = pd.Timestamp(macro_lib.read("SPY", columns=["Close"]).data.index[-1])
        if spy_last.tzinfo is not None:
            spy_last = spy_last.tz_convert("UTC").tz_localize(None)
        spy_last = spy_last.normalize()

        symbols = list(universe_lib.list_symbols())
        # Filter sector ETFs + macro symbols out of the stock sample.
        macro_syms = {"SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"}
        sector_prefixes = ("XL",)  # XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY
        stock_syms = [
            s for s in symbols
            if s not in macro_syms and not s.startswith(sector_prefixes)
        ]
        if len(stock_syms) < _UNIVERSE_SAMPLE_SIZE:
            raise PostflightError(
                f"ArcticDB universe has only {len(stock_syms)} non-macro symbols "
                f"(expected ≥ {_UNIVERSE_SAMPLE_SIZE}). Backfill has not run or "
                f"universe library is empty."
            )

        # Deterministic sample on run_date to keep the check reproducible
        # for debugging (same run_date → same sample → same failure mode).
        rng = random.Random(self.run_date)
        sample = rng.sample(stock_syms, _UNIVERSE_SAMPLE_SIZE)

        stale_tickers: list[tuple[str, int]] = []
        for sym in sample:
            try:
                df = universe_lib.read(sym, columns=["Close"]).data
            except Exception as exc:
                raise PostflightError(
                    f"ArcticDB universe.{sym} read failed: {exc}"
                ) from exc
            if df is None or df.empty:
                stale_tickers.append((sym, 9999))
                continue
            last_ts = pd.Timestamp(df.index[-1])
            if last_ts.tzinfo is not None:
                last_ts = last_ts.tz_convert("UTC").tz_localize(None)
            stale = (spy_last - last_ts.normalize()).days
            if stale > _UNIVERSE_MAX_STALE_VS_SPY_DAYS:
                stale_tickers.append((sym, stale))

        if stale_tickers:
            raise PostflightError(
                f"ArcticDB universe sample has {len(stale_tickers)}/{_UNIVERSE_SAMPLE_SIZE} "
                f"tickers >{_UNIVERSE_MAX_STALE_VS_SPY_DAYS}d stale vs SPY "
                f"({spy_last.date()}): {stale_tickers[:5]}"
                + (" ..." if len(stale_tickers) > 5 else "")
                + ". daily_append partial-write suspected — downstream reads "
                "will silently drop stale tickers."
            )
        log.info(
            "postflight: universe sample %d/%d tickers fresh (within %dd of SPY %s)",
            len(sample) - len(stale_tickers), len(sample),
            _UNIVERSE_MAX_STALE_VS_SPY_DAYS, spy_last.date(),
        )

    def _check_macro_json_contract(self) -> None:
        """Consumer: research ``macro_fetcher.fetch_macro_data``.

        Asserts ``market_data/weekly/<run_date>/macro.json`` exists, is
        parseable JSON, and has a populated ``fed_funds_rate`` field.
        """
        key = f"{self.market_prefix}weekly/{self.run_date}/macro.json"
        data = self._fetch_json(key, name="macro.json")
        if data.get("fed_funds_rate") is None:
            raise PostflightError(
                f"s3://{self.bucket}/{key} missing 'fed_funds_rate' — "
                f"research's MacroFetchError will reject this. Upstream "
                f"collector produced a malformed output."
            )
        log.info("postflight: macro.json OK (fed_funds_rate=%s)", data["fed_funds_rate"])

    def _check_constituents_json_contract(self) -> None:
        """Consumer: research ``price_fetcher.fetch_sp500_sp400_with_sectors``.

        Asserts ``market_data/weekly/<run_date>/constituents.json`` exists,
        is parseable JSON, has a ``tickers`` array of ≥ 800 symbols, and
        has a ``sector_map`` dict covering them.
        """
        key = f"{self.market_prefix}weekly/{self.run_date}/constituents.json"
        data = self._fetch_json(key, name="constituents.json")
        tickers = data.get("tickers", [])
        if not tickers or len(tickers) < _MIN_CONSTITUENTS:
            raise PostflightError(
                f"s3://{self.bucket}/{key} has {len(tickers)} tickers "
                f"(expected ≥ {_MIN_CONSTITUENTS}). Research's PriceFetchError "
                f"will reject this on ingest."
            )
        if not isinstance(data.get("sector_map"), dict):
            raise PostflightError(
                f"s3://{self.bucket}/{key} missing 'sector_map' dict — "
                f"research scanner requires ticker→sector mapping."
            )
        log.info(
            "postflight: constituents.json OK (%d tickers, %d sector_map entries)",
            len(tickers), len(data["sector_map"]),
        )

    def _check_latest_weekly_pointer(self) -> None:
        """Pointer must roll forward to ``run_date``.

        The #1 class of bug hiding between a successful collector run and a
        consumer that reads yesterday's data: the per-date artifacts write
        successfully but the ``latest_weekly.json`` pointer doesn't update.
        Downstream reads stale data while upstream reports ``status=ok``.
        """
        key = f"{self.market_prefix}latest_weekly.json"
        data = self._fetch_json(key, name="latest_weekly.json")
        ptr_date = data.get("date")
        if ptr_date != self.run_date:
            raise PostflightError(
                f"s3://{self.bucket}/{key} has date={ptr_date!r} but run_date="
                f"{self.run_date!r}. Pointer did not roll forward — consumers "
                f"would read yesterday's data while upstream reports success."
            )
        expected_prefix = f"{self.market_prefix}weekly/{self.run_date}/"
        if data.get("s3_prefix") != expected_prefix:
            raise PostflightError(
                f"s3://{self.bucket}/{key} has s3_prefix={data.get('s3_prefix')!r} "
                f"but expected {expected_prefix!r}."
            )
        log.info("postflight: latest_weekly.json pointer OK (date=%s)", ptr_date)

    def _check_health_marker_matches(self, expected_status: str) -> None:
        """The health marker is written AFTER this postflight — so we can't
        cross-check its content here without a chicken-and-egg.

        Instead, this is a placeholder for a Phase 7 follow-up: assert that
        the last-seen ``health/data_phase1.json`` matches the in-process
        ``results["status"]`` flowing through ``_finalize``. For now we skip
        — the in-process status is what we're writing into the marker next,
        so they are tautologically equal at this point in the flow.
        """
        log.debug(
            "postflight: health marker consistency check skipped (tautological at this phase)"
        )

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _fetch_json(self, key: str, name: str) -> dict:
        s3 = self._s3_client()
        try:
            obj = s3.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            raise PostflightError(
                f"s3://{self.bucket}/{key} unreadable: {exc} — "
                f"{name} did not write or pointer is broken."
            ) from exc
        try:
            return json.loads(obj["Body"].read())
        except Exception as exc:
            raise PostflightError(
                f"s3://{self.bucket}/{key} is not valid JSON: {exc}"
            ) from exc

    # ── Entry point ──────────────────────────────────────────────────────────

    def run(self) -> None:
        """Run every check in sequence. Fail on the first contract violation."""
        if self.phase != 1:
            log.info(
                "postflight: phase=%d is not gated today (only Phase 1). Skipping.",
                self.phase,
            )
            return

        # Ordered so the cheapest + most-likely-to-fail checks run first.
        self._check_latest_weekly_pointer()
        self._check_macro_json_contract()
        self._check_constituents_json_contract()
        self._check_macro_spy_fresh()
        self._check_universe_sample_fresh()
        log.info("postflight: all DataPhase1 consumer contracts satisfied")
