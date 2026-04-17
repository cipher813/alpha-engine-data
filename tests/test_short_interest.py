"""Tests for collectors/short_interest.py.

Mocks yfinance ``Ticker.info`` to exercise the collector without hitting the
network. Locks four invariants:
  1. Polygon-style well-populated info dict produces correctly-typed output.
  2. shortPercentOfFloat is converted from 0-1 ratio to percent.
  3. Per-ticker exceptions don't crash the run; row gets NaN/None fields.
  4. Below-threshold ok_ratio returns status=error rather than writing
     a partial payload to S3.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from collectors import short_interest


def _make_yf(info_by_ticker: dict[str, dict]) -> MagicMock:
    """Build a yfinance mock where Ticker(t).info returns info_by_ticker[t]."""
    yf_mock = MagicMock()

    def ticker_factory(t):
        ticker_obj = MagicMock()
        ticker_obj.info = info_by_ticker.get(t, {})
        return ticker_obj

    yf_mock.Ticker.side_effect = ticker_factory
    return yf_mock


def test_well_populated_info_produces_typed_output():
    yf_mock = _make_yf({
        "AAPL": {
            "shortPercentOfFloat": 0.025,  # 2.5%
            "shortRatio": 1.8,
            "sharesShort": 50_000_000,
        }
    })
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.short_interest.boto3.client", return_value=fake_s3):
        result = short_interest.collect(
            bucket="test-bucket",
            tickers=["AAPL"],
            run_date="2026-04-18",
            inter_request_delay=0.0,
        )

    assert result["status"] == "ok"
    assert result["ok_count"] == 1
    assert fake_s3.put_object.called
    written_body = json.loads(fake_s3.put_object.call_args.kwargs["Body"])
    aapl = written_body["data"]["AAPL"]
    assert aapl["short_pct_float"] == 2.5  # 0.025 * 100
    assert aapl["short_ratio"] == 1.8
    assert aapl["shares_short"] == 50_000_000


def test_per_ticker_exception_does_not_crash():
    yf_mock = MagicMock()

    def ticker_factory(t):
        if t == "BAD":
            raise RuntimeError("yfinance internal error")
        ticker_obj = MagicMock()
        ticker_obj.info = {"shortPercentOfFloat": 0.10, "shortRatio": 5.0, "sharesShort": 1_000_000}
        return ticker_obj

    yf_mock.Ticker.side_effect = ticker_factory
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.short_interest.boto3.client", return_value=fake_s3):
        result = short_interest.collect(
            bucket="test-bucket",
            tickers=["GOOD1", "BAD", "GOOD2"],
            run_date="2026-04-18",
            inter_request_delay=0.0,
        )

    assert result["status"] == "ok"
    assert result["ok_count"] == 2  # GOOD1 + GOOD2
    written_body = json.loads(fake_s3.put_object.call_args.kwargs["Body"])
    assert written_body["data"]["BAD"] == {
        "short_pct_float": None,
        "short_ratio": None,
        "shares_short": None,
    }


def test_below_threshold_returns_error_no_s3_write():
    """If <50% of tickers populate any field, status=error and no S3 write."""
    yf_mock = MagicMock()

    def ticker_factory(t):
        # Only AAPL gets populated info; the other 4 return empty dicts.
        ticker_obj = MagicMock()
        ticker_obj.info = (
            {"shortPercentOfFloat": 0.05, "shortRatio": 2.0, "sharesShort": 100}
            if t == "AAPL" else {}
        )
        return ticker_obj

    yf_mock.Ticker.side_effect = ticker_factory
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.short_interest.boto3.client", return_value=fake_s3):
        result = short_interest.collect(
            bucket="test-bucket",
            tickers=["AAPL", "MSFT", "GOOG", "AMZN", "META"],
            run_date="2026-04-18",
            inter_request_delay=0.0,
        )

    assert result["status"] == "error"
    assert "below 50% threshold" in result["error"].lower()
    fake_s3.put_object.assert_not_called()


def test_dry_run_samples_first_five_no_s3_write():
    yf_mock = _make_yf({
        t: {"shortPercentOfFloat": 0.03, "shortRatio": 1.5, "sharesShort": 1_000_000}
        for t in ["A", "B", "C", "D", "E", "F", "G"]
    })
    fake_s3 = MagicMock()
    with patch.dict("sys.modules", {"yfinance": yf_mock}), \
         patch("collectors.short_interest.boto3.client", return_value=fake_s3):
        result = short_interest.collect(
            bucket="test-bucket",
            tickers=["A", "B", "C", "D", "E", "F", "G"],
            run_date="2026-04-18",
            inter_request_delay=0.0,
            dry_run=True,
        )

    assert result["status"] == "ok_dry_run"
    assert result["ticker_count"] == 5  # capped to first 5
    fake_s3.put_object.assert_not_called()


def test_empty_tickers_list_errors_immediately():
    result = short_interest.collect(
        bucket="test-bucket",
        tickers=[],
        run_date="2026-04-18",
    )
    assert result["status"] == "error"
    assert "no tickers" in result["error"].lower()
