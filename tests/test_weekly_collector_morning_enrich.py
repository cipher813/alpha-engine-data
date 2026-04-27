"""Tests for the --morning-enrich path in weekly_collector.

Covers:
  * _previous_trading_day finds the most recent trading day before today,
    walking back over weekends + holidays correctly.
  * _run_morning_enrich invokes daily_closes with source='polygon_only'
    (no yfinance fallback masking polygon failures) and follows up with
    daily_append on the same date.
  * Hard-fail propagation when polygon raises PolygonForbiddenError.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import weekly_collector
from polygon_client import PolygonForbiddenError


# ── _previous_trading_day ───────────────────────────────────────────────────


def test_previous_trading_day_walks_back_over_weekend():
    """Monday morning should resolve to Friday's date, not Sunday's."""
    # 2026-04-27 is a Monday. Previous trading day = 2026-04-24 (Friday).
    monday = datetime(2026, 4, 27, 13, 0, 0, tzinfo=timezone.utc)
    result = weekly_collector._previous_trading_day(reference=monday)
    assert result == "2026-04-24"


def test_previous_trading_day_skips_holiday():
    """Day after a market holiday should resolve to the trading day before it."""
    # 2026-12-25 is Christmas (NYSE closed). 2026-12-28 (Mon) → 2026-12-24 (Thu).
    day_after = datetime(2026, 12, 28, 13, 0, 0, tzinfo=timezone.utc)
    result = weekly_collector._previous_trading_day(reference=day_after)
    assert result == "2026-12-24"


def test_previous_trading_day_strict_inequality():
    """Always returns a date STRICTLY before the reference, never the same day."""
    # Even if today is a trading day, --morning-enrich is for prior session enrichment.
    # 2026-04-23 is a Thursday (trading day). Result should be 2026-04-22 (Wednesday).
    thursday = datetime(2026, 4, 23, 13, 0, 0, tzinfo=timezone.utc)
    result = weekly_collector._previous_trading_day(reference=thursday)
    assert result == "2026-04-22"


def test_previous_trading_day_raises_on_runaway():
    """Defensive: if is_trading_day returns False for 10 days straight, raise."""
    with patch("alpha_engine_lib.trading_calendar.is_trading_day", return_value=False):
        with pytest.raises(RuntimeError, match="trading_calendar.is_trading_day appears broken"):
            weekly_collector._previous_trading_day(
                reference=datetime(2026, 4, 23, 13, 0, 0, tzinfo=timezone.utc)
            )


# ── _run_morning_enrich orchestration ───────────────────────────────────────


@pytest.fixture
def enrich_args():
    return SimpleNamespace(date=None, dry_run=True, morning_enrich=True)


@pytest.fixture
def enrich_args_with_date():
    return SimpleNamespace(date="2026-04-22", dry_run=True, morning_enrich=True)


def test_morning_enrich_uses_polygon_only_source(enrich_args_with_date):
    """The morning enrichment must call daily_closes with source='polygon_only'."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}

    captured = {}
    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok_dry_run", "polygon": 100, "fred": 4, "yfinance": 0,
                "tickers_captured": 104, "source": kwargs["source"]}

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL", "MSFT", "NVDA"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, enrich_args_with_date)

    assert captured["source"] == "polygon_only"
    assert captured["run_date"] == "2026-04-22"
    assert result["status"] == "ok"
    assert result["mode"] == "morning_enrich"
    assert result["date"] == "2026-04-22"


def test_morning_enrich_hard_fails_on_polygon_forbidden(enrich_args_with_date):
    """If polygon raises PolygonForbiddenError, the enrich step must report failed."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch(
             "weekly_collector.daily_closes.collect",
             side_effect=PolygonForbiddenError("403 simulation"),
         ):
        result = weekly_collector._run_morning_enrich(config, enrich_args_with_date)

    assert result["status"] == "failed"
    assert result["collectors"]["daily_closes"]["status"] == "error"
    assert "403" in result["collectors"]["daily_closes"]["error"]
    # daily_append should NOT have run after polygon failed
    assert "arcticdb" not in result["collectors"]


def test_morning_enrich_calls_daily_append_after_polygon_succeeds(enrich_args_with_date):
    """daily_append must run after polygon-only daily_closes lands the parquet."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    daily_append_calls = []
    def fake_daily_append(**kwargs):
        daily_append_calls.append(kwargs)
        return {"status": "ok", "tickers_appended": 1}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0, "yfinance": 0,
                             "tickers_captured": 1, "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append", side_effect=fake_daily_append):
        result = weekly_collector._run_morning_enrich(config, enrich_args_with_date)

    assert result["status"] == "ok"
    assert len(daily_append_calls) == 1
    assert daily_append_calls[0]["date_str"] == "2026-04-22"


def test_morning_enrich_default_date_uses_previous_trading_day():
    """When --date is not specified, _previous_trading_day fills in."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date=None, dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    captured = {}
    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok_dry_run", "polygon": 1, "fred": 0, "yfinance": 0,
                "tickers_captured": 1, "source": "polygon_only"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._previous_trading_day", return_value="2026-04-23"):
        result = weekly_collector._run_morning_enrich(config, args)

    assert captured["run_date"] == "2026-04-23"
    assert result["date"] == "2026-04-23"


# ── daily_data health stamp refresh ─────────────────────────────────────────


def test_morning_enrich_refreshes_daily_data_stamp_on_success():
    """On success, _run_morning_enrich must call _write_module_health to refresh
    the `daily_data` stamp. Without this the executor's 26h staleness gate trips
    on Monday mornings (post-close stamp from Friday afternoon → ~65h on Monday
    open). Regression: 2026-04-27 weekday SF aborted on this exact gap."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    health_calls = []
    def fake_write_health(bucket, module_name, run_date, status, **kwargs):
        health_calls.append({
            "bucket": bucket, "module_name": module_name,
            "run_date": run_date, "status": status, **kwargs,
        })

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok", "polygon": 913, "fred": 4,
                             "yfinance": 0, "tickers_captured": 917,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health", side_effect=fake_write_health):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "ok"
    assert len(health_calls) == 1, "expected exactly one daily_data stamp write"
    stamp = health_calls[0]
    assert stamp["module_name"] == "daily_data"
    assert stamp["run_date"] == "2026-04-24"
    assert stamp["status"] == "ok"
    assert stamp["summary"]["morning_enrich"] is True
    assert stamp["summary"]["polygon"] == 913


def test_morning_enrich_does_not_stamp_on_polygon_failure():
    """If polygon fails the prior stamp must be left in place — executor's
    staleness gate then fires correctly. Writing a fresh "ok" stamp on failure
    would mask the outage."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    health_calls = []
    def fake_write_health(*args_, **kwargs):
        health_calls.append(kwargs)

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               side_effect=PolygonForbiddenError("403 simulation")), \
         patch("weekly_collector._write_module_health", side_effect=fake_write_health):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "failed"
    assert health_calls == [], (
        "morning_enrich must NOT refresh daily_data stamp on failure — would "
        "mask outages from the executor's staleness gate"
    )


def test_morning_enrich_does_not_stamp_in_dry_run():
    """Dry runs (CLI --dry-run, backfill rehearsals) must not touch S3 stamps."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    health_calls = []
    def fake_write_health(*args_, **kwargs):
        health_calls.append(kwargs)

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health", side_effect=fake_write_health):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "ok"
    assert health_calls == []


# ── --daily routes through yfinance_only ────────────────────────────────────


def test_daily_mode_calls_collect_with_yfinance_only_source():
    """--daily must invoke daily_closes with source='yfinance_only' (no polygon attempt)."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(
        date="2026-04-23", dry_run=True, morning_enrich=False,
        daily=True, only=None,
    )

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    captured = {}
    def fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok_dry_run", "polygon": 0, "fred": 4, "yfinance": 1,
                "tickers_captured": 5, "source": kwargs["source"]}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("features.compute.compute_and_write",
               return_value={"status": "ok"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}):
        weekly_collector._run_daily(config, args)

    assert captured["source"] == "yfinance_only", (
        "--daily must use source='yfinance_only' to skip polygon entirely "
        "(per the 2026-04-23 split-by-source design — polygon free-tier 403's "
        "same-day, morning enrichment fills VWAP overnight)."
    )
