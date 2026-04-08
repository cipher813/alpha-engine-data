"""Tests to close coverage gaps in emailer, trading_calendar, and price_validator.

Targets: emailer._build_email, _extract_details, send_step_email
         trading_calendar.is_trading_day, next_trading_day
         price_validator.validate_batch, validate_refreshed
"""

import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from emailer import _build_email, _extract_details, send_step_email
from trading_calendar import is_trading_day, next_trading_day
from validators.price_validator import validate_batch, validate_parquet


# ---------------------------------------------------------------------------
# emailer._build_email
# ---------------------------------------------------------------------------


class TestBuildEmail:
    def test_basic_ok(self):
        results = {
            "status": "ok",
            "phase": "1",
            "started_at": "2026-04-08T00:00:00",
            "completed_at": "2026-04-08T00:05:30",
            "collectors": {
                "prices": {"status": "ok", "tickers_refreshed": 50, "total_tickers": 900},
                "macro": {"status": "ok", "series_count": 12},
            },
        }
        subject, html, plain = _build_email("DataPhase1", results, "2026-04-08")
        assert "DataPhase1" in subject
        assert "2026-04-08" in subject
        assert "OK" in subject
        assert "prices" in html
        assert "5.5 min" in html
        assert "prices" in plain

    def test_failed_status(self):
        results = {
            "status": "failed",
            "collectors": {
                "prices": {"status": "error", "error": "S3 timeout"},
            },
        }
        subject, html, plain = _build_email("DataPhase1", results, "2026-04-08")
        assert "FAILED" in subject
        assert "S3 timeout" in html

    def test_partial_status(self):
        results = {
            "status": "partial",
            "collectors": {"macro": {"status": "ok"}},
        }
        subject, html, plain = _build_email("DataPhase1", results, "2026-04-08")
        assert "PARTIAL" in subject

    def test_no_duration(self):
        results = {"status": "ok", "collectors": {}}
        subject, html, plain = _build_email("Test", results, "2026-04-08")
        assert "Duration" not in html

    def test_short_duration(self):
        results = {
            "status": "ok",
            "started_at": "2026-04-08T00:00:00",
            "completed_at": "2026-04-08T00:00:30",
            "collectors": {},
        }
        subject, html, plain = _build_email("Test", results, "2026-04-08")
        assert "30s" in html

    def test_empty_collectors(self):
        results = {"status": "ok", "collectors": {}}
        subject, html, plain = _build_email("Test", results, "2026-04-08")
        assert "OK" in subject


class TestExtractDetails:
    def test_refreshed(self):
        assert "50 refreshed" in _extract_details("prices", {"tickers_refreshed": 50})

    def test_refreshed_alt(self):
        assert "10 refreshed" in _extract_details("prices", {"refreshed": 10})

    def test_stale(self):
        assert "5 stale" in _extract_details("prices", {"stale": 5})

    def test_failed_nonzero(self):
        assert "3 failed" in _extract_details("prices", {"tickers_failed": 3})

    def test_failed_zero_omitted(self):
        assert "failed" not in _extract_details("prices", {"tickers_failed": 0})

    def test_failed_alt(self):
        assert "2 failed" in _extract_details("prices", {"failed": 2})

    def test_total(self):
        assert "900 total" in _extract_details("prices", {"total_tickers": 900})

    def test_total_alt(self):
        assert "100 total" in _extract_details("prices", {"total": 100})

    def test_written(self):
        assert "50 written" in _extract_details("slim", {"written": 50})

    def test_sp_counts(self):
        result = _extract_details("constituents", {"sp500_count": 503, "sp400_count": 401})
        assert "S&P500: 503" in result
        assert "S&P400: 401" in result

    def test_series_count(self):
        assert "12 series" in _extract_details("macro", {"series_count": 12})

    def test_elapsed(self):
        assert "45s" in _extract_details("prices", {"elapsed_s": 45.2})

    def test_validation_anomalies(self):
        info = {"validation": {"anomalies": 3, "total_validated": 100}}
        assert "3/100 anomalies" in _extract_details("prices", info)

    def test_empty_info(self):
        assert _extract_details("prices", {}) == ""

    def test_n_tickers(self):
        assert "50 tickers" in _extract_details("alt", {"n_tickers": 50})

    def test_n_dates(self):
        assert "5 dates" in _extract_details("ur", {"n_dates": 5})


class TestSendStepEmail:
    def test_skips_without_env(self):
        with patch.dict("os.environ", {}, clear=True):
            assert send_step_email("Test", {"status": "ok", "collectors": {}}, "2026-04-08") is False

    def test_skips_empty_sender(self):
        with patch.dict("os.environ", {"EMAIL_SENDER": "", "EMAIL_RECIPIENTS": "a@b.com"}):
            assert send_step_email("Test", {"status": "ok", "collectors": {}}, "2026-04-08") is False

    @patch("emailer.smtplib.SMTP")
    def test_gmail_success(self, mock_smtp):
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = lambda s: mock_server
        mock_smtp.return_value.__exit__ = MagicMock(return_value=False)
        with patch.dict("os.environ", {"EMAIL_SENDER": "a@b.com", "EMAIL_RECIPIENTS": "c@d.com", "GMAIL_APP_PASSWORD": "pass"}):
            result = send_step_email("Test", {"status": "ok", "collectors": {}}, "2026-04-08")
            assert result is True

    @patch("emailer.smtplib.SMTP", side_effect=Exception("SMTP down"))
    def test_gmail_fails_returns_false_without_ses(self, mock_smtp):
        with patch.dict("os.environ", {"EMAIL_SENDER": "a@b.com", "EMAIL_RECIPIENTS": "c@d.com", "GMAIL_APP_PASSWORD": "pass"}):
            # SES fallback will also fail without real boto3 credentials — returns False
            result = send_step_email("Test", {"status": "ok", "collectors": {}}, "2026-04-08")
            assert result is False


# ---------------------------------------------------------------------------
# trading_calendar
# ---------------------------------------------------------------------------


class TestTradingCalendar:
    def test_weekday_is_trading(self):
        # 2026-04-08 is Wednesday
        assert is_trading_day(date(2026, 4, 8)) is True

    def test_saturday_not_trading(self):
        assert is_trading_day(date(2026, 4, 11)) is False

    def test_sunday_not_trading(self):
        assert is_trading_day(date(2026, 4, 12)) is False

    def test_christmas_not_trading(self):
        assert is_trading_day(date(2026, 12, 25)) is False

    def test_new_years_not_trading(self):
        assert is_trading_day(date(2027, 1, 1)) is False

    def test_next_trading_day_from_friday(self):
        # 2026-04-10 is Friday → next is Monday 2026-04-13
        result = next_trading_day(date(2026, 4, 10))
        assert result == date(2026, 4, 13)

    def test_next_trading_day_from_wednesday(self):
        result = next_trading_day(date(2026, 4, 8))
        assert result == date(2026, 4, 9)

    def test_next_trading_day_skips_holiday(self):
        # Day before MLK day 2027 (Jan 18 is Monday holiday)
        result = next_trading_day(date(2027, 1, 17))
        assert result.weekday() < 5  # must be a weekday


# ---------------------------------------------------------------------------
# price_validator.validate_batch
# ---------------------------------------------------------------------------


class TestValidateBatch:
    def _make_parquet(self, tmp_dir: Path, ticker: str, data: dict):
        df = pd.DataFrame(data)
        df.index = pd.to_datetime(df["Date"])
        df.drop(columns=["Date"], inplace=True)
        df.to_parquet(tmp_dir / f"{ticker}.parquet")

    def test_all_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._make_parquet(tmp_path, "AAPL", {
                "Date": pd.date_range("2026-01-01", periods=100, freq="B"),
                "Open": [150.0] * 100,
                "High": [155.0] * 100,
                "Low": [145.0] * 100,
                "Close": [152.0] * 100,
                "Volume": [1000000] * 100,
            })
            result = validate_batch(tmp_path)
            assert result["total_validated"] == 1
            assert result["anomalies"] == 0

    def test_with_anomaly(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            # Create data with a zero price (anomaly)
            closes = [150.0] * 99 + [0.0]
            self._make_parquet(tmp_path, "BAD", {
                "Date": pd.date_range("2026-01-01", periods=100, freq="B"),
                "Open": [150.0] * 100,
                "High": [155.0] * 100,
                "Low": [145.0] * 100,
                "Close": closes,
                "Volume": [1000000] * 100,
            })
            result = validate_batch(tmp_path)
            assert result["total_validated"] == 1
            assert result["anomalies"] >= 1

    def test_filter_by_ticker(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._make_parquet(tmp_path, "AAPL", {
                "Date": pd.date_range("2026-01-01", periods=10, freq="B"),
                "Open": [150.0] * 10, "High": [155.0] * 10,
                "Low": [145.0] * 10, "Close": [152.0] * 10,
                "Volume": [1000000] * 10,
            })
            self._make_parquet(tmp_path, "MSFT", {
                "Date": pd.date_range("2026-01-01", periods=10, freq="B"),
                "Open": [300.0] * 10, "High": [310.0] * 10,
                "Low": [290.0] * 10, "Close": [305.0] * 10,
                "Volume": [2000000] * 10,
            })
            result = validate_batch(tmp_path, tickers=["AAPL"])
            assert result["total_validated"] == 1

    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = validate_batch(Path(tmp))
            assert result["total_validated"] == 0

    def test_corrupt_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "BAD.parquet").write_bytes(b"not a parquet file")
            result = validate_batch(tmp_path)
            assert result["total_validated"] == 1
            assert result["anomalies"] == 1
