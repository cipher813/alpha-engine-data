"""Tests for builders/migrate_universe_vwap.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from builders.migrate_universe_vwap import (
    OHLCV_COLS_CANONICAL,
    _canonical_column_order,
    _is_canonical,
    migrate_universe_vwap,
)


def _stock_frame(cols: list[str], rows: int = 5) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=rows, freq="B")
    return pd.DataFrame(
        {c: np.linspace(1.0, 2.0, rows) for c in cols},
        index=idx,
    )


# ── _canonical_column_order / _is_canonical ──────────────────────────────────


def test_canonical_order_inserts_vwap_at_idx5():
    """OHLCV_COLS_CANONICAL puts VWAP at index 5."""
    assert OHLCV_COLS_CANONICAL.index("VWAP") == 5


def test_canonical_order_with_features_puts_vwap_idx5():
    """Existing layout with VWAP at end gets relocated to idx=5."""
    existing = ["Open", "High", "Low", "Close", "Volume",
                "rsi_14", "macd_cross", "VWAP"]
    canonical = _canonical_column_order(existing)
    assert canonical[5] == "VWAP"
    assert canonical[:6] == OHLCV_COLS_CANONICAL
    # Feature block preserved in its relative order, with VWAP removed
    # from the suffix and re-inserted in the OHLCV block.
    assert canonical[6:] == ["rsi_14", "macd_cross"]


def test_canonical_order_with_no_vwap_inserts_it():
    existing = ["Open", "High", "Low", "Close", "Volume", "rsi_14", "macd_cross"]
    canonical = _canonical_column_order(existing)
    assert canonical == [
        "Open", "High", "Low", "Close", "Volume", "VWAP",
        "rsi_14", "macd_cross",
    ]


def test_is_canonical_recognizes_correct_layout():
    assert _is_canonical([
        "Open", "High", "Low", "Close", "Volume", "VWAP",
        "rsi_14", "macd_cross",
    ])


def test_is_canonical_rejects_appended_vwap():
    assert not _is_canonical([
        "Open", "High", "Low", "Close", "Volume",
        "rsi_14", "macd_cross", "VWAP",
    ])


def test_is_canonical_rejects_missing_vwap():
    assert not _is_canonical([
        "Open", "High", "Low", "Close", "Volume",
        "rsi_14", "macd_cross",
    ])


# ── migrate_universe_vwap (functional) ──────────────────────────────────────


def _patch_libs(monkeypatch, tickers_to_frames: dict[str, pd.DataFrame]):
    """Stub out the universe lib + s3 client so the migration runs in-memory."""
    from builders import migrate_universe_vwap as _m

    universe_lib = MagicMock()
    universe_lib.list_symbols.return_value = list(tickers_to_frames.keys())

    # Track the in-memory state across read/write so the test can verify
    # that reorder + repeat-call is idempotent.
    state = {t: df.copy() for t, df in tickers_to_frames.items()}

    def _read(ticker):
        result = MagicMock()
        result.data = state[ticker].copy()
        return result

    def _write(ticker, df, prune_previous_versions=False):
        state[ticker] = df.copy()
        return None

    universe_lib.read.side_effect = _read
    universe_lib.write.side_effect = _write

    monkeypatch.setattr(_m, "get_universe_lib", lambda *a, **k: universe_lib)
    monkeypatch.setattr(_m, "boto3", MagicMock())

    # Don't actually upload audit JSON
    monkeypatch.setattr(_m, "_write_audit", MagicMock())

    return universe_lib, state


def test_migration_dry_run_makes_no_writes(monkeypatch):
    frames = {
        "AAPL": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "rsi_14", "macd_cross",
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_vwap(apply=False)
    assert result["migrated_count"] == 1
    assert universe_lib.write.call_count == 0
    # In-memory state still without VWAP
    assert "VWAP" not in state["AAPL"].columns


def test_migration_apply_inserts_vwap_at_idx5(monkeypatch):
    frames = {
        "AAPL": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "rsi_14", "macd_cross",
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_vwap(apply=True)
    assert result["migrated_count"] == 1
    assert result["errors_count"] == 0
    assert universe_lib.write.call_count == 1
    final = state["AAPL"]
    assert list(final.columns)[:6] == OHLCV_COLS_CANONICAL
    assert list(final.columns) == [
        "Open", "High", "Low", "Close", "Volume", "VWAP",
        "rsi_14", "macd_cross",
    ]
    # VWAP starts as float64 NaN
    assert final["VWAP"].dtype == np.float64
    assert final["VWAP"].isna().all()


def test_migration_apply_relocates_vwap_from_end(monkeypatch):
    """Symbols that have VWAP appended at idx=last get it moved to idx=5."""
    frames = {
        "MO": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "rsi_14", "macd_cross", "VWAP",
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_vwap(apply=True)
    assert result["migrated_count"] == 1
    final = state["MO"]
    assert list(final.columns) == [
        "Open", "High", "Low", "Close", "Volume", "VWAP",
        "rsi_14", "macd_cross",
    ]
    # Reorder must preserve existing VWAP values, not NaN them out
    assert not final["VWAP"].isna().any()


def test_migration_skips_already_canonical(monkeypatch):
    frames = {
        "GOOG": _stock_frame([
            "Open", "High", "Low", "Close", "Volume", "VWAP",
            "rsi_14", "macd_cross",
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_vwap(apply=True)
    assert result["migrated_count"] == 0
    assert result["already_canonical_count"] == 1
    assert universe_lib.write.call_count == 0


def test_migration_idempotent(monkeypatch):
    """Running twice must not change the second-run result."""
    frames = {
        "AAPL": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "rsi_14", "macd_cross",
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    first = migrate_universe_vwap(apply=True)
    second = migrate_universe_vwap(apply=True)
    assert first["migrated_count"] == 1
    assert second["migrated_count"] == 0
    assert second["already_canonical_count"] == 1


def test_migration_tickers_override_filters_to_subset(monkeypatch):
    frames = {
        "AAPL": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "rsi_14",
        ]),
        "MSFT": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "rsi_14",
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_vwap(apply=True, tickers_override=["AAPL"])
    assert result["migrated_count"] == 1
    assert result["targets_count"] == 1
    assert "VWAP" in state["AAPL"].columns
    # MSFT untouched
    assert "VWAP" not in state["MSFT"].columns


def test_migration_records_errors_without_aborting(monkeypatch):
    """One symbol blowing up on write must not stop the rest."""
    from builders import migrate_universe_vwap as _m

    frames = {
        "AAPL": _stock_frame([
            "Open", "High", "Low", "Close", "Volume", "rsi_14",
        ]),
        "BREAKS": _stock_frame([
            "Open", "High", "Low", "Close", "Volume", "rsi_14",
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)

    def _selective_write(ticker, df, prune_previous_versions=False):
        if ticker == "BREAKS":
            raise RuntimeError("simulated arctic write failure")
        state[ticker] = df.copy()

    universe_lib.write.side_effect = _selective_write

    result = migrate_universe_vwap(apply=True)
    assert result["migrated_count"] == 1  # AAPL succeeded
    assert result["errors_count"] == 1
    assert result["errors"][0]["ticker"] == "BREAKS"
    assert result["status"] == "partial"


def test_migration_preserves_feature_block_order(monkeypatch):
    """The feature block must keep its existing relative ordering — only
    the OHLCV+VWAP prefix gets normalized."""
    frames = {
        "AAPL": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "feat_z", "feat_a", "feat_m",  # deliberately not alphabetic
        ]),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    migrate_universe_vwap(apply=True)
    final = state["AAPL"]
    assert list(final.columns) == [
        "Open", "High", "Low", "Close", "Volume", "VWAP",
        "feat_z", "feat_a", "feat_m",
    ]


# ── threading invariants ─────────────────────────────────────────────────────


def test_migration_uses_threadpool_executor():
    """Source-text invariant: the per-symbol fan-out must use
    ThreadPoolExecutor.

    Locks the speedup. The first prod run hit the SSM 1-hour ceiling at
    sequential speed (~8 symbols/min over 904 universe symbols, ~120 min
    needed); a regression to single-threaded would re-introduce that
    timeout class. Mirrors daily_append's Phase 2 fan-out pattern.
    """
    from pathlib import Path
    src = Path(__file__).parent.parent.joinpath(
        "builders", "migrate_universe_vwap.py"
    ).read_text()
    assert "from concurrent.futures import ThreadPoolExecutor" in src, (
        "ThreadPoolExecutor import missing — fan-out has been reverted to "
        "sequential and will hit SSM's 1-hour timeout on a full-universe run."
    )
    assert "ThreadPoolExecutor(max_workers=workers)" in src, (
        "ThreadPoolExecutor not used — see daily_append.py Phase 2 for the "
        "canonical fan-out pattern."
    )


def test_migration_workers_env_overridable():
    """Worker count must be tunable via env var without a redeploy —
    mirrors DAILY_APPEND_WRITE_WORKERS so prod can react if the boto3
    connection-pool ceiling changes."""
    from pathlib import Path
    src = Path(__file__).parent.parent.joinpath(
        "builders", "migrate_universe_vwap.py"
    ).read_text()
    assert "MIGRATE_UNIVERSE_VWAP_WORKERS" in src, (
        "Env-var override missing — prod can't tune worker count without "
        "a code change + redeploy."
    )


def test_migration_threaded_all_writes_succeed(monkeypatch):
    """Functional: with N>1 symbols, all writes complete and all results
    aggregate correctly. Catches a regression where the threaded result
    aggregation drops some symbols (e.g. iterating a generator twice)."""
    frames = {
        f"TKR{i}": _stock_frame([
            "Open", "High", "Low", "Close", "Volume",
            "rsi_14", "macd_cross",
        ])
        for i in range(20)
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)
    result = migrate_universe_vwap(apply=True)
    assert result["migrated_count"] == 20
    assert result["errors_count"] == 0
    assert universe_lib.write.call_count == 20
    # Every symbol now has VWAP at idx=5
    for ticker in frames:
        assert list(state[ticker].columns)[5] == "VWAP"


def test_migration_threaded_summary_includes_elapsed_and_workers(monkeypatch):
    """Summary must include elapsed_seconds + workers for ops observability —
    SSM-timeout-vs-finish requires knowing both."""
    frames = {
        "AAPL": _stock_frame([
            "Open", "High", "Low", "Close", "Volume", "rsi_14",
        ]),
    }
    _patch_libs(monkeypatch, frames)
    result = migrate_universe_vwap(apply=True)
    assert "elapsed_seconds" in result
    assert "workers" in result
    assert result["workers"] >= 1


def test_migration_threaded_aggregates_mixed_outcomes(monkeypatch):
    """Mixed-outcome run: some symbols already canonical, some need
    migration, one read-fails, one write-fails. All should aggregate
    cleanly into the correct buckets."""
    canonical_cols = [
        "Open", "High", "Low", "Close", "Volume", "VWAP",
        "rsi_14",
    ]
    needs_migration_cols = [
        "Open", "High", "Low", "Close", "Volume", "rsi_14",
    ]
    frames = {
        "ALREADY_OK": _stock_frame(canonical_cols),
        "NEEDS_FIX_1": _stock_frame(needs_migration_cols),
        "NEEDS_FIX_2": _stock_frame(needs_migration_cols),
        "READ_BREAKS": _stock_frame(needs_migration_cols),
        "WRITE_BREAKS": _stock_frame(needs_migration_cols),
    }
    universe_lib, state = _patch_libs(monkeypatch, frames)

    original_read = universe_lib.read.side_effect

    def _selective_read(ticker):
        if ticker == "READ_BREAKS":
            raise RuntimeError("simulated arctic read failure")
        return original_read(ticker)

    universe_lib.read.side_effect = _selective_read

    def _selective_write(ticker, df, prune_previous_versions=False):
        if ticker == "WRITE_BREAKS":
            raise RuntimeError("simulated arctic write failure")
        state[ticker] = df.copy()

    universe_lib.write.side_effect = _selective_write

    result = migrate_universe_vwap(apply=True)
    assert result["migrated_count"] == 2  # NEEDS_FIX_1, NEEDS_FIX_2
    assert result["already_canonical_count"] == 1  # ALREADY_OK
    assert result["errors_count"] == 2  # READ_BREAKS + WRITE_BREAKS
    error_tickers = {e["ticker"] for e in result["errors"]}
    assert error_tickers == {"READ_BREAKS", "WRITE_BREAKS"}
    assert result["status"] == "partial"
