"""Regression tests for VWAP ingestion into ArcticDB universe library (2026-04-17).

Phase 7 VWAP centralization contract:

  * Polygon grouped-daily produces true volume-weighted VWAP. That value flows
    via ``collectors/daily_closes.py`` → ``predictor/daily_closes/*.parquet``
    → ``builders/daily_append.py::_load_daily_closes`` → ArcticDB universe.
  * Any other source (yfinance fallback, FRED single-close, legacy backfill
    price_cache) writes ``None`` / NaN for VWAP. No ``(H+L+C)/3`` proxy.

These tests lock two invariants:

  1. ``OHLCV_COLS`` contains ``"VWAP"`` in both ``daily_append`` and
     ``backfill`` — prevents a future PR from silently dropping VWAP from
     the ArcticDB write.
  2. ``_load_daily_closes`` passes NaN VWAP through as NaN (not coerced to
     0.0 or the typical-price proxy).
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import numpy as np
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


def test_load_daily_closes_extracts_polygon_vwap():
    """Polygon-sourced VWAP flows through _load_daily_closes unchanged."""
    from builders.daily_append import _load_daily_closes

    fake_df = pd.DataFrame(
        {
            "Open": [100.0],
            "High": [110.0],
            "Low": [95.0],
            "Close": [105.0],
            "Volume": [1_000_000],
            "VWAP": [104.5],  # true polygon vw
        },
        index=pd.Index(["AAPL"], name="ticker"),
    )

    buf = io.BytesIO()
    fake_df.to_parquet(buf, engine="pyarrow", index=True)
    buf.seek(0)

    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=buf.getvalue()))}

    records = _load_daily_closes(mock_s3, "test-bucket", "2026-04-17")
    assert records["AAPL"]["VWAP"] == 104.5


def test_load_daily_closes_passes_none_vwap_as_nan():
    """Rows with VWAP=None (yfinance / FRED fallback) become NaN, not 0.0 or proxy."""
    from builders.daily_append import _load_daily_closes

    fake_df = pd.DataFrame(
        {
            "Open": [100.0],
            "High": [110.0],
            "Low": [95.0],
            "Close": [105.0],
            "Volume": [1_000_000],
            "VWAP": [None],  # yfinance fallback semantics
        },
        index=pd.Index(["AAPL"], name="ticker"),
    )

    buf = io.BytesIO()
    fake_df.to_parquet(buf, engine="pyarrow", index=True)
    buf.seek(0)

    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=buf.getvalue()))}

    records = _load_daily_closes(mock_s3, "test-bucket", "2026-04-17")
    vwap = records["AAPL"]["VWAP"]
    assert np.isnan(vwap), f"Expected NaN VWAP, got {vwap!r}"
