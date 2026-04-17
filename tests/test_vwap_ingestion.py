"""Regression tests for VWAP ingestion into ArcticDB universe library (2026-04-17).

VWAP is added as a first-class column in both ``builders/daily_append.py`` and
``builders/backfill.py``. daily_append reads VWAP from the daily_closes parquet
(populated upstream by either polygon's true ``vw`` or the ``(H+L+C)/3`` proxy
in ``collectors/daily_closes.py``). backfill computes the proxy inline since
the legacy ``predictor/price_cache/`` parquets pre-date VWAP.

These tests lock two contracts:
  1. ``OHLCV_COLS`` contains ``"VWAP"`` in both files (prevents a future PR
     from silently dropping VWAP from the ArcticDB write).
  2. ``_ensure_vwap_column`` produces the typical-price proxy correctly and is
     idempotent (already-populated VWAP is preserved).
"""

from __future__ import annotations

import pandas as pd


def test_daily_append_ohlcv_cols_include_vwap():
    from builders.daily_append import OHLCV_COLS
    assert "VWAP" in OHLCV_COLS, (
        "daily_append.OHLCV_COLS must include VWAP — executor's VWAP-discount "
        "entry trigger depends on this column being present in ArcticDB universe."
    )


def test_backfill_ohlcv_cols_include_vwap():
    from builders.backfill import OHLCV_COLS
    assert "VWAP" in OHLCV_COLS


def test_ensure_vwap_column_computes_typical_price_proxy():
    from builders.backfill import _ensure_vwap_column

    df = pd.DataFrame({
        "Open": [100.0],
        "High": [110.0],
        "Low": [95.0],
        "Close": [105.0],
        "Volume": [1_000_000],
    })
    out = _ensure_vwap_column(df)
    assert "VWAP" in out.columns
    # (110 + 95 + 105) / 3 = 103.3333...
    assert out["VWAP"].iloc[0] == 103.3333


def test_ensure_vwap_column_is_idempotent():
    """If VWAP is already populated (e.g. from polygon ``vw``), don't overwrite it."""
    from builders.backfill import _ensure_vwap_column

    df = pd.DataFrame({
        "Open": [100.0],
        "High": [110.0],
        "Low": [95.0],
        "Close": [105.0],
        "Volume": [1_000_000],
        "VWAP": [104.5],  # true polygon VWAP — not the (H+L+C)/3 proxy
    })
    out = _ensure_vwap_column(df)
    assert out["VWAP"].iloc[0] == 104.5, (
        "_ensure_vwap_column overwrote an existing VWAP value — proxy should only "
        "fill in when the column is absent."
    )


def test_ensure_vwap_column_noop_when_hlc_missing():
    """No-op when required columns aren't all present — don't fabricate a proxy from incomplete data."""
    from builders.backfill import _ensure_vwap_column

    df = pd.DataFrame({"Open": [100.0], "Close": [105.0]})  # missing High/Low
    out = _ensure_vwap_column(df)
    assert "VWAP" not in out.columns
