"""Regression tests for the missing-from-closes hard-fail in builders/daily_append.py.

Background (ROADMAP 2026-04-25 P1, "daily_append silent-skip bug"):
8 tickers (PAYC, ASGN, LW, GTM, MOH, KMPR, MTCH, HOLX) had been polygon-backfilled
on 2026-04-22 but regressed back to last_date=4/01 by 2026-04-25 — daily_append
was silently skipping them across the intervening weekdays. Root cause: the
``stock_tickers = [t for t in closes if ...]`` filter at the top of the per-ticker
loop silently drops any ticker missing from today's daily_closes parquet. No
counter, no log — the only signal was a freshness preflight catching the
staleness days later.

The fix mirrors the existing macro/sector hard-fail (which raises on any
macro key absent from closes): compare ArcticDB universe symbols against
closes keys, hard-fail above a small threshold, WARN below.

These tests lock the hard-fail invariants. Source-text patterns + functional
behavior — same style as the rest of the daily_append test suite.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


# ── 1. Source-text invariants ──────────────────────────────────────────────────


def test_missing_from_closes_check_present():
    """The check must compute the diff between universe_lib symbols and
    closes keys, with the same stock-vs-macro filter the loop uses."""
    src = _source()
    assert "missing_from_closes" in src, (
        "Expected missing_from_closes computation — silent-skip class is "
        "open without it."
    )
    assert "universe_lib.list_symbols()" in src, (
        "Diff must compare against ArcticDB universe symbols, not a hardcoded "
        "list — universe drifts as constituents change."
    )


def test_missing_from_closes_hardfail_above_threshold():
    """Above the threshold, the run must raise — silent-skip is forbidden."""
    src = _source()
    assert "DAILY_APPEND_MISSING_THRESHOLD" in src, (
        "Threshold must be env-overridable — prod tuning shouldn't require "
        "a code change + redeploy."
    )
    # Hard-fail message keywords — checks the raise path is wired.
    assert "missing from today's daily_closes parquet" in src
    assert "raise RuntimeError" in src


def test_missing_from_closes_metric_emitted():
    """A CloudWatch gauge must fire on every run for slow-drift observability —
    1-2 missing tickers per day is below the hard-fail but a real regression
    over weeks. Pattern mirrors _emit_admission_refused_metric."""
    src = _source()
    assert "_emit_missing_from_closes_metric" in src
    assert "AlphaEngine/Data" in src
    assert "missing_from_closes_count" in src


def test_missing_from_closes_metric_helper_swallows_errors():
    """The metric emit must never block daily_append — CloudWatch errors
    (IAM, network) WARN-log only. Same hard-fail-until-stable carve-out as
    other observability emits."""
    src = _source()
    # Find the helper body and confirm it swallows exceptions.
    helper_start = src.find("def _emit_missing_from_closes_metric")
    assert helper_start != -1, "Helper function not found"
    helper_end = src.find("\ndef ", helper_start + 1)
    helper = src[helper_start:helper_end]
    assert "except Exception" in helper, (
        "Metric helper must swallow exceptions — observability must not "
        "break the load-bearing pipeline path."
    )


def test_missing_from_closes_counter_in_summary():
    """The result dict and summary log must include the counter so
    downstream consumers (Step Function output, dashboards) can observe it."""
    src = _source()
    assert "tickers_missing_from_closes" in src, (
        "Result dict must include tickers_missing_from_closes so SF + "
        "dashboards see the counter."
    )
    assert "n_missing_from_closes=" in src, (
        "Summary log must include n_missing_from_closes for log-aggregation "
        "tooling."
    )


def test_macro_write_runs_before_universe_coverage_guard():
    """Macro / sector-ETF write must execute BEFORE the universe-coverage
    guard so a stock-universe gap can't blackout SPY/VIX freshness.

    Regression for 2026-04-27: 7 stock tickers (PAYC, ASGN, LW, GTM, MOH,
    KMPR, MTCH) went missing from daily_closes; the universe-coverage guard
    raised at threshold>5 BEFORE the macro write block ran, so SPY never
    landed in ArcticDB for the day. The EOD reconcile then hard-failed on
    stale SPY (by design — alpha against stale SPY is meaningless) and the
    EOD email did not go out. Independent macro freshness + loud universe
    failure is the intent: macro writes first, universe guard still raises
    on threshold violations, pipeline still exits non-zero.

    Locks the source-order invariant: the offset of the macro write site
    must precede the offset of the missing-from-closes raise.
    """
    src = _source()
    macro_write_idx = src.find("_write_row_backfill_safe(macro_lib, key, new_row)")
    guard_raise_idx = src.find("missing from today's daily_closes parquet")
    assert macro_write_idx != -1, "macro write call site not found"
    assert guard_raise_idx != -1, "missing-from-closes raise not found"
    assert macro_write_idx < guard_raise_idx, (
        f"Macro write at offset {macro_write_idx} must precede the "
        f"missing-from-closes guard at offset {guard_raise_idx}. The "
        f"reordered design ensures SPY/VIX/sector freshness is independent "
        f"of stock-universe coverage — a regression here resurrects the "
        f"2026-04-27 EOD-email blackout."
    )


def test_macro_write_does_not_block_on_universe_coverage(monkeypatch):
    """Functional: even when stock universe coverage trips the hard-fail,
    the macro write must have completed first (SPY/VIX/sector ETFs land
    in ArcticDB before the guard raises).

    Direct simulation of the 2026-04-27 failure mode: 10 stocks missing
    from closes (well above threshold 5), but macro keys + sector ETFs are
    all present. The function must raise on the universe guard, but the
    macro_lib must have received its writes first.
    """
    from builders import daily_append as _da
    from builders.daily_append import daily_append

    universe = [f"TKR{i}" for i in range(12)] + ["AAPL", "MSFT"]
    universe_lib, macro_lib = _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        closes_tickers=["AAPL", "MSFT"],
    )

    # Spy on the macro write helper so we can confirm it ran before the raise.
    write_calls: list[tuple] = []

    def _spy_write(lib, sym, df, existing_series=None):
        write_calls.append((lib, sym))
        return "append"

    monkeypatch.setattr(_da, "_write_row_backfill_safe", _spy_write)

    with pytest.raises(RuntimeError, match=r"missing from today's daily_closes"):
        daily_append(date_str="2026-04-28")

    # Macro keys (7) + sector ETFs (11) = 18 expected writes BEFORE the
    # universe-coverage raise. If the macro write were still ordered
    # AFTER the guard, write_calls would be empty.
    macro_keys = {"SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"}
    sector_etfs = {"XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
                   "XLP", "XLRE", "XLU", "XLV", "XLY"}
    written_syms = {sym for _, sym in write_calls}
    assert macro_keys.issubset(written_syms), (
        f"Macro keys not all written before universe-guard raise. "
        f"Missing: {macro_keys - written_syms}. write_calls={write_calls}"
    )
    assert sector_etfs.issubset(written_syms), (
        f"Sector ETFs not all written before universe-guard raise. "
        f"Missing: {sector_etfs - written_syms}. write_calls={write_calls}"
    )


# ── 2. Functional: end-to-end behavior ─────────────────────────────────────────


def _stub_closes(tickers: list[str], date_str: str = "2026-04-28") -> dict:
    """Build a closes dict mirroring _load_daily_closes output shape."""
    return {
        t: {
            "Open": 100.0, "High": 101.0, "Low": 99.0,
            "Close": 100.0, "Volume": 1_000_000, "VWAP": 100.0,
        }
        for t in tickers
    }


def _patch_targets(monkeypatch, *, universe_symbols: list[str], closes_tickers: list[str]):
    """Common patch surface for the daily_append entrypoint.

    Stubs the data-load layer + per-ticker loop helpers so the function
    reaches the result return. The per-ticker loop's compute + write are
    mocked to no-op success — these tests are about the pre-loop missing-
    from-closes check, not the loop body.

    Macro keys + sector ETFs are always present in closes so they pass
    their own hard-fails (which fire BEFORE the per-ticker loop matters here).
    """
    from builders import daily_append as _da

    macro_keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]
    sector_etfs = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
                   "XLP", "XLRE", "XLU", "XLV", "XLY"]

    closes = _stub_closes(closes_tickers + macro_keys + sector_etfs)

    # read_batch returns success-shaped MagicMocks (NOT DataError instances)
    # carrying enough history for compute_features to find a row at today_ts.
    # Pattern matches tests/test_daily_append_read_batch.py's fake_read_batch.
    hist_dates = pd.date_range("2024-01-01", periods=300, freq="B")
    hist_df = pd.DataFrame(
        {
            "Open": 100.0, "High": 101.0, "Low": 99.0,
            "Close": 100.0, "Volume": 1_000_000, "VWAP": 100.0,
        },
        index=hist_dates,
    )
    hist_df.index.name = "date"

    universe_lib = MagicMock()
    universe_lib.list_symbols.return_value = universe_symbols
    universe_lib.read_batch.return_value = [
        MagicMock(spec=[], data=hist_df.copy()) for _ in closes_tickers
    ]
    # _scan_universe_and_emit_freshness_receipt calls tail(sym, n=1) per
    # symbol after the daily writes. Mock it to return today's row so the
    # post-write freshness scan passes (these tests are about the pre-loop
    # missing-from-closes check, not the post-write scan, which has its
    # own dedicated test_daily_append_universe_freshness.py).
    today_row = pd.DataFrame(
        {"Close": [100.0]},
        index=[pd.Timestamp(datetime.now(timezone.utc).date())],
    )
    universe_lib.tail.return_value = MagicMock(spec=[], data=today_row)

    macro_lib = MagicMock()
    # macro reads return a frame with a "Close" column so macro-load passes.
    # Long enough history so the macro update verification's "last_ts ==
    # target_ts" check passes after _write_row_backfill_safe.
    macro_df = hist_df[["Close"]].copy()
    # Append today_ts so the verification readback sees target as last.
    macro_df.loc[pd.Timestamp("2026-04-28")] = 100.0
    macro_lib.read.return_value = MagicMock(data=macro_df)
    macro_lib.list_symbols.return_value = sector_etfs

    monkeypatch.setattr(_da, "_load_daily_closes", lambda *a, **k: closes)
    monkeypatch.setattr(_da, "_load_sector_map", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_fundamentals", lambda *a, **k: {})
    monkeypatch.setattr(_da, "_load_cached_alternative", lambda *a, **k: {})
    monkeypatch.setattr(_da, "get_universe_lib", lambda *a, **k: universe_lib)
    monkeypatch.setattr(_da, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_da, "_emit_missing_from_closes_metric", MagicMock())

    # compute_features returns a frame containing today_ts with a minimal
    # FEATURES subset populated. The loop extracts the row at today_ts
    # and only writes columns that exist in the featured frame.
    from features.feature_engineer import FEATURES

    def _fake_compute_features(combined, **_):
        out = combined.copy()
        # Add a small subset of FEATURES so the write extraction has columns
        # to copy. NaN-only is fine — the n_partial path is exercised but
        # n_err stays at zero (which is what these tests need).
        for f in list(FEATURES)[:3]:
            out[f] = 0.5
        return out

    monkeypatch.setattr(_da, "compute_features", _fake_compute_features)

    # _write_row_backfill_safe is the inner write helper; stub to no-op.
    monkeypatch.setattr(
        _da, "_write_row_backfill_safe",
        lambda lib, sym, df, existing_series=None: "append",
    )

    # Disable boto3 client construction outside the metric helper —
    # daily_append calls boto3.client("s3") at function entry.
    mock_s3 = MagicMock()
    monkeypatch.setattr("builders.daily_append.boto3.client", lambda *a, **k: mock_s3)

    return universe_lib, macro_lib


def test_no_missing_passes(monkeypatch):
    """Universe == closes → run proceeds normally, no raise, no WARN."""
    from builders.daily_append import daily_append
    _patch_targets(
        monkeypatch,
        universe_symbols=["AAPL", "MSFT"],
        closes_tickers=["AAPL", "MSFT"],
    )

    result = daily_append(date_str="2026-04-28")
    assert result["status"] == "ok"
    assert result["tickers_missing_from_closes"] == 0


def test_below_threshold_warns_does_not_raise(monkeypatch, caplog):
    """1-5 missing → WARN log + counter, no raise. Slow-drift class."""
    import logging
    from builders.daily_append import daily_append

    _patch_targets(
        monkeypatch,
        # Universe has 4 tickers, closes only has 2 → 2 missing (PAYC, ASGN).
        universe_symbols=["AAPL", "MSFT", "PAYC", "ASGN"],
        closes_tickers=["AAPL", "MSFT"],
    )

    with caplog.at_level(logging.WARNING, logger="builders.daily_append"):
        result = daily_append(date_str="2026-04-28")

    assert result["status"] == "ok"
    assert result["tickers_missing_from_closes"] == 2
    assert any(
        "missing from" in r.message and "PAYC" in r.message and "ASGN" in r.message
        for r in caplog.records
    ), f"Expected WARN naming PAYC + ASGN; got: {[r.message for r in caplog.records]}"


def test_above_threshold_raises(monkeypatch):
    """>5 missing (default threshold) → RuntimeError with named tickers."""
    from builders.daily_append import daily_append

    universe = [f"TKR{i}" for i in range(10)] + ["AAPL", "MSFT"]
    # closes only has 2 of the 12 → 10 missing → above threshold (5).
    with pytest.raises(RuntimeError, match=r"missing from today's daily_closes"):
        _patch_targets(
            monkeypatch,
            universe_symbols=universe,
            closes_tickers=["AAPL", "MSFT"],
        )
        daily_append(date_str="2026-04-28")


def test_threshold_env_override(monkeypatch):
    """DAILY_APPEND_MISSING_THRESHOLD env var raises the bar so a triaged
    universe with >5 known-delisted symbols can still run."""
    from builders.daily_append import daily_append

    # 8 missing — would normally raise (default threshold 5).
    universe = [f"TKR{i}" for i in range(8)] + ["AAPL"]
    monkeypatch.setenv("DAILY_APPEND_MISSING_THRESHOLD", "10")

    _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        closes_tickers=["AAPL"],
    )

    # Should NOT raise — operator has explicitly raised the threshold.
    result = daily_append(date_str="2026-04-28")
    assert result["status"] == "ok"
    assert result["tickers_missing_from_closes"] == 8


def test_dry_run_skips_check(monkeypatch):
    """Dry-run skips the check entirely — universe_lib is None and the
    purpose is to compute features without writing or hard-failing on
    upstream data shape."""
    from builders.daily_append import daily_append

    # Universe has 100 tickers, closes has 0 → would normally hard-fail.
    # Dry-run should skip the check.
    _patch_targets(
        monkeypatch,
        universe_symbols=[f"TKR{i}" for i in range(100)],
        closes_tickers=[],
    )

    result = daily_append(date_str="2026-04-28", dry_run=True)
    assert result["status"] == "ok"
    assert result["tickers_missing_from_closes"] == 0
    assert result["dry_run"] is True


def test_metric_emitted_even_when_zero(monkeypatch):
    """The CloudWatch gauge fires on every non-dry-run, even at zero. Without
    a baseline data point, downstream alarms can't distinguish 'no data yet'
    from 'genuinely zero'."""
    from builders import daily_append as _da
    from builders.daily_append import daily_append

    metric_emit = MagicMock()
    _patch_targets(
        monkeypatch,
        universe_symbols=["AAPL", "MSFT"],
        closes_tickers=["AAPL", "MSFT"],
    )
    monkeypatch.setattr(_da, "_emit_missing_from_closes_metric", metric_emit)

    daily_append(date_str="2026-04-28")
    metric_emit.assert_called_once_with(0)
