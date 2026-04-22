"""Regression tests for builders/backfill.py.

Two invariants locked here (ROADMAP P1, 2026-04-22):

1. **Unified short-history path.** The OHLCV-only fork for fresh-listing
   tickers was removed when PR #78 made ``compute_features`` return NaN
   for features whose warmup exceeds available history. Every ticker
   now goes through ``compute_features`` and writes the full
   OHLCV+FEATURE schema. Regressing to the fork would silently re-create
   the 2026-04-21 schema-mismatch class (daily_append's ``update()``
   fails on OHLCV-only symbols) that PR #79 had to migrate away.

2. **Macro writes gated by ``--ticker``.** A per-ticker backfill must
   not rewrite the macro library from the parquet cache — the cache's
   macro series may be stale relative to what daily_append has been
   appending, and rewriting it silently regresses SPY/VIX/XL* last_date.
   The 2026-04-22 SOLS patch knocked macro back from 4/20 to 4/17 by
   exactly this path. Operators who genuinely want to rebuild macro in
   a ticker-scoped run must pass ``--rebuild-macro`` (explicit opt-in).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


_BACKFILL = Path(__file__).parent.parent / "builders" / "backfill.py"


def _source() -> str:
    return _BACKFILL.read_text()


# ── 1. Source-text invariants ──────────────────────────────────────────────────


def test_ohlcv_only_branch_removed():
    """The ``if ticker in tickers_with_features: ... else: <OHLCV-only>``
    fork must not reappear. Unified path is a hard contract post-PR-#78.
    """
    src = _source()
    # The old fork's sentinel comment / counter / variable names:
    assert "n_ok_ohlcv_only" not in src, (
        "Found n_ok_ohlcv_only counter — the OHLCV-only branch has regressed. "
        "Every ticker must go through compute_features (PR #78 unified path)."
    )
    assert "tickers_with_features = {" not in src, (
        "Found tickers_with_features set construction — the two-tier filter "
        "has regressed. Post-PR-#78 the filter is unnecessary."
    )
    # The old comment's giveaway phrase (stripped columns for fresh listings):
    assert "write raw OHLCV" not in src, (
        "Found 'write raw OHLCV' comment — old OHLCV-only fork sentinel. "
        "Unified path writes OHLCV + FEATURE columns for every ticker."
    )


def test_compute_features_called_unconditionally():
    """Every ticker must flow through compute_features. Structural check:
    the write loop must not have a ``len(price_data[ticker]) >= MIN_ROWS_FOR_FEATURES``
    style gate that skips compute_features. Two call sites are legitimate —
    one in the write loop (unified path), one in ``validate`` — the
    assertion below rejects any additional branch-y call inside the loop.
    """
    src = _source()
    # No ``if ticker in tickers_with_features`` style gate around the call:
    # post-PR-#78 the filter is gone. See also test_ohlcv_only_branch_removed.
    # Guard against a future refactor reintroducing a different gate name.
    assert "if ticker in tickers_with_features" not in src, (
        "Found tickers_with_features gate — the unified write path has regressed."
    )
    # Belt-and-suspenders: any new gate that inspects len(price_data[ticker])
    # in the write loop (not the universe-filter up top) is forbidden.
    # The write loop is bounded by the "Compute features and write to ArcticDB"
    # comment and the macro-write section that follows.
    write_loop_start = src.find("Compute features and write to ArcticDB")
    write_loop_end = src.find("Write macro features")
    assert write_loop_start != -1 and write_loop_end != -1, "section markers moved"
    write_loop = src[write_loop_start:write_loop_end]
    assert "MIN_ROWS_FOR_FEATURES" not in write_loop, (
        "Found MIN_ROWS_FOR_FEATURES in the write loop — a per-ticker gate "
        "has regressed. PR #78 moved that check upstream (universe-filter "
        "counter only) and made compute_features handle short history."
    )


def test_macro_write_gated_by_ticker_filter():
    """The macro-library write block must be guarded by a skip_macro
    computation derived from ticker_filter + rebuild_macro.
    """
    src = _source()
    assert "skip_macro" in src, (
        "Expected skip_macro flag gating the macro library rewrite."
    )
    assert "ticker_filter is not None" in src, (
        "skip_macro must be derived from ticker_filter, not a standalone flag."
    )
    assert "rebuild_macro" in src, (
        "Expected rebuild_macro opt-in override so operators can still rebuild "
        "macro in a ticker-scoped run when they explicitly ask for it."
    )


def test_rebuild_macro_cli_flag_wired():
    """``--rebuild-macro`` must be exposed to operators."""
    src = _source()
    assert "--rebuild-macro" in src, (
        "CLI flag --rebuild-macro missing — operators have no opt-in for "
        "macro rewrite when --ticker is set."
    )


# ── 2. Functional: macro scoping ───────────────────────────────────────────────


def _minimal_ohlcv(n_rows: int = 400) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=n_rows, freq="B")
    return pd.DataFrame(
        {
            "Open": 100.0,
            "High": 101.0,
            "Low": 99.0,
            "Close": 100.0,
            "Adj_Close": 100.0,
            "Volume": 1_000_000,
        },
        index=dates,
    )


def _stub_macro(n_rows: int = 400) -> dict[str, pd.Series]:
    dates = pd.date_range("2024-01-01", periods=n_rows, freq="B")
    keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
            "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"]
    return {k: pd.Series(100.0, index=dates) for k in keys}


def _run_backfill_with_mocks(**backfill_kwargs):
    """Exercise builders.backfill.backfill with the S3/ArcticDB layer mocked.

    Returns (result, universe_lib_mock, macro_lib_mock).
    """
    from builders import backfill as _bf

    price_data = {"AAPL": _minimal_ohlcv(n_rows=400)}
    macro = _stub_macro(n_rows=400)
    sector_map = {"AAPL": "XLK"}
    fundamentals: dict = {}
    alt_data: dict = {}

    universe_lib = MagicMock()
    macro_lib = MagicMock()

    # compute_features is heavy — stub it to return the input plus a few
    # feature columns so the unified write path has something to save.
    def _fake_compute_features(df, **_):
        out = df.copy()
        # Populate a small subset of FEATURES so the write loop finds them.
        for col in ("atr_14_pct", "rsi_14", "momentum_60d"):
            out[col] = 0.5
        return out

    # _build_macro_features_df needs a non-empty frame to trigger the
    # ``macro_lib.write("features", ...)`` call we want to observe.
    fake_macro_df = pd.DataFrame(
        {"vix_level": [15.0, 16.0]},
        index=pd.date_range("2024-01-01", periods=2),
    )

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_extract_macro_series", return_value=macro), \
         patch.object(_bf, "_load_sector_map", return_value=sector_map), \
         patch.object(_bf, "_load_cached_fundamentals", return_value=fundamentals), \
         patch.object(_bf, "_load_cached_alternative", return_value=alt_data), \
         patch.object(_bf, "_build_macro_features_df", return_value=fake_macro_df), \
         patch.object(_bf, "compute_features", side_effect=_fake_compute_features), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib), \
         patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        result = _bf.backfill(**backfill_kwargs)

    return result, universe_lib, macro_lib


def test_ticker_filter_without_rebuild_macro_skips_macro_writes():
    """Regression: SOLS-class side-effect regression. A ``--ticker X``
    backfill must not touch the macro library by default.
    """
    result, universe_lib, macro_lib = _run_backfill_with_mocks(
        ticker_filter="AAPL", rebuild_macro=False
    )

    assert result["status"] == "ok"
    # Universe write happens for the requested ticker.
    universe_lib.write.assert_called_once()
    # Macro library must NOT be touched.
    macro_lib.write.assert_not_called()


def test_ticker_filter_with_rebuild_macro_rewrites_macro():
    """Opt-in override: ``--rebuild-macro`` with ``--ticker`` forces the
    macro rewrite. Confirms the flag is wired end-to-end.
    """
    _, _, macro_lib = _run_backfill_with_mocks(
        ticker_filter="AAPL", rebuild_macro=True
    )
    assert macro_lib.write.call_count >= 1, (
        "rebuild_macro=True with ticker_filter must still rewrite macro."
    )


def test_full_universe_backfill_rewrites_macro():
    """Default full-universe backfill (no ticker_filter) must rewrite
    macro — that's the original ingestion path and the ``_SKIP_TICKERS``
    sentinel keeps the parquet cache authoritative for weekly rebuilds.
    """
    _, _, macro_lib = _run_backfill_with_mocks(
        ticker_filter=None, rebuild_macro=False
    )
    assert macro_lib.write.call_count >= 1, (
        "Full-universe backfill must still rewrite macro by default."
    )


# ── 3. Functional: unified schema on short-history ticker ──────────────────────


def test_short_history_ticker_gets_feature_columns_written():
    """A ticker with < MIN_ROWS_FOR_FEATURES must still land with OHLCV
    + whatever FEATURES compute_features returned (NaN allowed). The
    removed OHLCV-only fork would have written a stripped column set
    that daily_append.update() then rejects with a schema mismatch.
    """
    from builders import backfill as _bf
    from features.feature_engineer import FEATURES

    # 50 rows — well below MIN_ROWS_FOR_FEATURES (265).
    price_data = {"NEWCO": _minimal_ohlcv(n_rows=50)}
    macro = _stub_macro(n_rows=400)
    sector_map = {"NEWCO": "XLK"}

    universe_lib = MagicMock()
    macro_lib = MagicMock()

    def _fake_compute_features(df, **_):
        out = df.copy()
        for col in ("atr_14_pct", "rsi_14"):
            out[col] = 0.5
        # Simulate a feature whose warmup exceeds the ticker's history —
        # returned as a column but NaN (compute_features post-PR-#78 contract).
        # ``dist_from_52w_high`` needs 252 bars; short-history tickers get NaN.
        out["dist_from_52w_high"] = float("nan")
        return out

    with patch.object(_bf, "_load_full_cache", return_value=price_data), \
         patch.object(_bf, "_extract_macro_series", return_value=macro), \
         patch.object(_bf, "_load_sector_map", return_value=sector_map), \
         patch.object(_bf, "_load_cached_fundamentals", return_value={}), \
         patch.object(_bf, "_load_cached_alternative", return_value={}), \
         patch.object(_bf, "_build_macro_features_df", return_value=pd.DataFrame()), \
         patch.object(_bf, "compute_features", side_effect=_fake_compute_features), \
         patch.object(_bf, "get_universe_lib", return_value=universe_lib), \
         patch.object(_bf, "get_macro_lib", return_value=macro_lib), \
         patch("builders.backfill.boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        result = _bf.backfill(ticker_filter="NEWCO")

    assert result["status"] == "ok"
    universe_lib.write.assert_called_once()
    _, written_df = universe_lib.write.call_args[0]
    # Feature columns must be present (NaN values allowed).
    assert "atr_14_pct" in written_df.columns, (
        "Short-history ticker missing feature columns — OHLCV-only fork regressed."
    )
    assert "dist_from_52w_high" in written_df.columns, (
        "Feature columns with NaN values must still be written — unified schema "
        "contract per PR #78."
    )
    # NaN value must be preserved — not dropped, not zero-filled.
    assert written_df["dist_from_52w_high"].isna().all(), (
        "Short-history NaN feature must stay NaN after the write — "
        "dropna / zero-fill would violate the PR #78 contract."
    )
