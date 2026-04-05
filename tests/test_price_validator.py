"""Tests for validators/price_validator.py."""

import pandas as pd
import pytest

from validators.price_validator import validate_parquet


def _make_ohlcv(n=30, base_close=100.0):
    """Build a clean OHLCV DataFrame with n trading days."""
    dates = pd.bdate_range("2025-01-02", periods=n)
    close = [base_close + i * 0.5 for i in range(n)]
    return pd.DataFrame(
        {
            "Open": [c - 0.1 for c in close],
            "High": [c + 1.0 for c in close],
            "Low": [c - 1.0 for c in close],
            "Close": close,
            "Volume": [1_000_000 + i * 1000 for i in range(n)],
        },
        index=dates,
    )


class TestValidateParquet:
    def test_clean_data(self):
        df = _make_ohlcv()
        result = validate_parquet(df, "AAPL")
        assert result["status"] == "clean"
        assert result["anomalies"] == []

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = validate_parquet(df, "EMPTY")
        assert result["status"] == "empty"

    def test_high_less_than_low(self):
        df = _make_ohlcv()
        df.iloc[5, df.columns.get_loc("High")] = df.iloc[5]["Low"] - 1
        result = validate_parquet(df, "BAD_HL")
        assert result["status"] == "anomaly"
        assert any("High<Low" in a for a in result["anomalies"])

    def test_zero_close(self):
        df = _make_ohlcv()
        df.iloc[10, df.columns.get_loc("Close")] = 0
        result = validate_parquet(df, "ZERO")
        assert result["status"] == "anomaly"
        assert any("Close<=0" in a for a in result["anomalies"])

    def test_extreme_daily_return(self):
        df = _make_ohlcv()
        # 60% jump
        df.iloc[15, df.columns.get_loc("Close")] = df.iloc[14]["Close"] * 1.61
        result = validate_parquet(df, "SPIKE")
        assert result["status"] == "anomaly"
        assert any("50%" in a for a in result["anomalies"])

    def test_zero_volume(self):
        df = _make_ohlcv()
        df.iloc[5, df.columns.get_loc("Volume")] = 0
        result = validate_parquet(df, "NOVOL")
        assert result["status"] == "anomaly"
        assert any("zero volume" in a for a in result["anomalies"])

    def test_volume_spike(self):
        df = _make_ohlcv(n=40)
        # 15x median volume
        df.iloc[35, df.columns.get_loc("Volume")] = df.iloc[34]["Volume"] * 15
        result = validate_parquet(df, "VOLSPIKE")
        assert result["status"] == "anomaly"
        assert any("volume" in a and "median" in a for a in result["anomalies"])

    def test_trading_day_gap(self):
        df = _make_ohlcv(n=40)
        # Remove 8 calendar days worth of rows (creates a >5 day gap)
        gap_start = df.index[15]
        gap_end = gap_start + pd.Timedelta(days=8)
        df = df[(df.index < gap_start) | (df.index >= gap_end)]
        result = validate_parquet(df, "GAP")
        assert result["status"] == "anomaly"
        assert any("gap" in a.lower() for a in result["anomalies"])

    def test_multiple_anomalies(self):
        df = _make_ohlcv()
        df.iloc[5, df.columns.get_loc("Close")] = 0
        df.iloc[10, df.columns.get_loc("Volume")] = 0
        result = validate_parquet(df, "MULTI")
        assert result["status"] == "anomaly"
        assert len(result["anomalies"]) >= 2
