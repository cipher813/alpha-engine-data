"""Regression tests for the daily_closes VWAP fallback.

Pins the behavior added after the 2026-04-10 incident: when polygon.io
fails (rate limit, auth, empty response) and the collector falls back to
yfinance, the VWAP column must be populated with a typical-price proxy
instead of None. The old code set VWAP=None on all yfinance rows, which
caused the executor to log "has no VWAP column — skipping" for up to 5
consecutive days whenever polygon was unhealthy.
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from collectors import daily_closes


def _make_yf_frame(rows):
    """Build a yfinance-shaped DataFrame from (date, open, high, low, close, volume) tuples."""
    index = pd.DatetimeIndex([r[0] for r in rows])
    df = pd.DataFrame(
        {
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Adj Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
        },
        index=index,
    )
    return df


def test_yfinance_fallback_populates_vwap_with_typical_price():
    """The yfinance fallback should compute VWAP as (H + L + C) / 3."""
    records = []
    fake_frame = _make_yf_frame([
        ("2026-04-10", 100.0, 105.0, 99.0, 103.0, 1_000_000),
    ])

    mock_yf = MagicMock()
    mock_yf.download.return_value = fake_frame

    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        count = daily_closes._fetch_yfinance_closes(
            ["AAPL"], "2026-04-10", records
        )

    assert count == 1
    assert len(records) == 1
    row = records[0]
    assert row["ticker"] == "AAPL"
    # VWAP should be typical price: (105 + 99 + 103) / 3 = 102.333...
    expected_vwap = round((105.0 + 99.0 + 103.0) / 3.0, 4)
    assert row["VWAP"] == expected_vwap
    assert row["VWAP"] is not None


def test_yfinance_fallback_vwap_not_none_multi_ticker():
    """VWAP must be non-None for every row produced by the fallback."""
    records = []
    # yfinance returns a MultiIndex-columned frame for multi-ticker downloads
    index = pd.DatetimeIndex(["2026-04-10"])
    multi = pd.DataFrame(
        {
            ("AAPL", "Open"): [100.0],
            ("AAPL", "High"): [105.0],
            ("AAPL", "Low"): [99.0],
            ("AAPL", "Close"): [103.0],
            ("AAPL", "Adj Close"): [103.0],
            ("AAPL", "Volume"): [1_000_000],
            ("MSFT", "Open"): [200.0],
            ("MSFT", "High"): [210.0],
            ("MSFT", "Low"): [198.0],
            ("MSFT", "Close"): [206.0],
            ("MSFT", "Adj Close"): [206.0],
            ("MSFT", "Volume"): [2_000_000],
        },
        index=index,
    )
    multi.columns = pd.MultiIndex.from_tuples(multi.columns)

    mock_yf = MagicMock()
    mock_yf.download.return_value = multi

    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        count = daily_closes._fetch_yfinance_closes(
            ["AAPL", "MSFT"], "2026-04-10", records
        )

    assert count == 2
    for row in records:
        assert row["VWAP"] is not None, f"VWAP is None for {row['ticker']}"
        # Typical-price invariant: Low <= VWAP <= High
        assert row["Low"] <= row["VWAP"] <= row["High"]
