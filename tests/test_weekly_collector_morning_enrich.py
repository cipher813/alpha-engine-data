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
from zoneinfo import ZoneInfo

import pytest

import weekly_collector
from polygon_client import PolygonForbiddenError

_PT = ZoneInfo("America/Los_Angeles")


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

    # Pin the skip guard to OFF so this test exercises only the
    # _previous_trading_day fill-in. Without this the test is wall-clock
    # dependent (would skip when run after 13:30 PT on a trading day).
    with patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._should_skip_morning_enrich",
               return_value=(False, None)), \
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
    # Pre-MorningEnrich preflight: refresh constituents in-process.
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL"], "date": "2026-04-24",
    }

    health_calls = []
    def fake_write_health(bucket, module_name, run_date, status, **kwargs):
        health_calls.append({
            "bucket": bucket, "module_name": module_name,
            "run_date": run_date, "status": status, **kwargs,
        })

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
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
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL"], "date": "2026-04-24",
    }

    health_calls = []
    def fake_write_health(*args_, **kwargs):
        health_calls.append(kwargs)

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
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


# ── Preflight: refresh-constituents + prune-stragglers ─────────────────────────


def test_morning_enrich_refreshes_constituents_before_collect():
    """Pre-flight architectural fix (2026-05-02 incident): MorningEnrich must
    call constituents.collect() in-process BEFORE the daily_closes call so
    polygon is asked about the freshest S&P membership, not last week's. The
    bandage scoping in PR #132/#133 then becomes a quiet no-op rather than
    the load-bearing path."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.return_value = {
        "status": "ok",
        "tickers": ["AAPL", "MSFT", "NVDA"],  # fresh, post-churn list
        "date": "2026-04-24",
    }

    captured_dc = {}
    def fake_dc_collect(**kwargs):
        captured_dc.update(kwargs)
        return {"status": "ok", "polygon": 3, "fred": 0, "yfinance": 0,
                "tickers_captured": 3, "source": "polygon_only"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               return_value={"status": "ok", "pruned_count": 0,
                             "skipped_recent_count": 0}), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_dc_collect), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health"):
        weekly_collector._run_morning_enrich(config, args)

    fake_constituents.collect.assert_called_once()
    # daily_closes must use the fresh tickers + macro additions, NOT
    # load_from_s3's stale snapshot.
    sent_tickers = captured_dc["tickers"]
    assert "AAPL" in sent_tickers and "MSFT" in sent_tickers and "NVDA" in sent_tickers
    fake_constituents.load_from_s3.assert_not_called()


def test_morning_enrich_prunes_stragglers_before_daily_append():
    """Prune must run BEFORE daily_closes/daily_append so the missing-from-
    closes + freshness checks see a coherent universe. Use the in-process
    constituents_override (not the public latest_weekly.json pointer) so
    cross-module readers don't see a half-updated pointer mid-SF."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL", "MSFT"], "date": "2026-04-24",
    }

    prune_calls = []
    def fake_prune(**kwargs):
        prune_calls.append(kwargs)
        return {"status": "ok", "pruned_count": 0, "skipped_recent_count": 0}

    da_calls = []
    def fake_daily_append(**kwargs):
        # By the time daily_append fires, prune must already have run.
        assert prune_calls, "prune must run before daily_append"
        da_calls.append(kwargs)
        return {"status": "ok"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               side_effect=fake_prune), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok", "polygon": 2, "fred": 0,
                             "yfinance": 0, "tickers_captured": 2,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append", side_effect=fake_daily_append), \
         patch("weekly_collector._write_module_health"):
        weekly_collector._run_morning_enrich(config, args)

    assert len(prune_calls) == 1
    assert prune_calls[0]["apply"] is True
    assert prune_calls[0]["absent_days"] == 5  # tighter than the 14d default
    assert prune_calls[0]["constituents_override"] == {"AAPL", "MSFT"}
    assert len(da_calls) == 1


def test_morning_enrich_aborts_if_constituents_refresh_fails():
    """Constituents refresh is the source of truth for prune + daily_closes
    request list. If it fails, we cannot proceed safely — Wikipedia outages,
    schema drift, or sector-mapping completeness failures all warrant a
    hard-fail per feedback_no_silent_fails."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.side_effect = RuntimeError("Wikipedia 503")

    dc_calls = []
    def fake_dc(**kwargs):
        dc_calls.append(kwargs)
        return {"status": "ok"}

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers"), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_dc), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}), \
         patch("weekly_collector._write_module_health"):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "failed"
    assert result["collectors"]["constituents_preflight"]["status"] == "error"
    assert "Wikipedia 503" in result["collectors"]["constituents_preflight"]["error"]
    assert dc_calls == [], "daily_closes must NOT run if constituents refresh failed"


def test_morning_enrich_continues_if_prune_fails():
    """Prune is best-effort here — daily_append's expected_tickers scoping
    (PR #132/#133) still tolerates stragglers as a fallback. A prune failure
    must surface loudly (ERROR log + result entry) but must NOT block the
    rest of the enrich pipeline tonight."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.collect.return_value = {
        "status": "ok", "tickers": ["AAPL"], "date": "2026-04-24",
    }

    da_called = []

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               side_effect=RuntimeError("ArcticDB transient")), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: (da_called.append(k), {"status": "ok"})[1]), \
         patch("weekly_collector._write_module_health"):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["prune_preflight_warning"]["status"] == "error"
    assert "ArcticDB transient" in result["prune_preflight_warning"]["error"]
    assert "prune_preflight" not in result["collectors"], (
        "prune failure must NOT land in results['collectors'] — that key feeds "
        "the status aggregator and would make the whole MorningEnrich fail"
    )
    assert len(da_called) == 1, "daily_append must still run when prune fails"
    assert result["status"] == "ok"


# ── _should_skip_morning_enrich (post-1:30pm-PT skip guard) ────────────────


def test_skip_guard_fires_after_1_30pm_pt_on_trading_day():
    """Wed 14:00 PT (a trading day, after the 13:30 cutoff) → skip=True.

    This is the failure case the guard exists to handle: a manual midweek
    Saturday-SF rerun after market close. polygon free-tier 403's the
    same-day grouped-daily, and once UTC has rolled past midnight,
    _previous_trading_day() resolves to today and the polygon call hard-fails.
    """
    # 2026-04-22 is a Wednesday (trading day).
    now = datetime(2026, 4, 22, 14, 0, 0, tzinfo=_PT)
    skip, reason = weekly_collector._should_skip_morning_enrich(now=now)
    assert skip is True
    assert reason is not None
    assert "post_close_midweek" in reason


def test_skip_guard_does_not_fire_before_1_30pm_pt():
    """Wed 13:29 PT (one minute before cutoff) → skip=False.

    The pre-13:30 window is the regular weekday-SF MorningEnrich Lambda's
    operating zone (it runs ~06:15 PT). Polygon T+1 for the prior session
    is settled by then, so the polygon path is the right one to take.
    """
    now = datetime(2026, 4, 22, 13, 29, 0, tzinfo=_PT)
    skip, reason = weekly_collector._should_skip_morning_enrich(now=now)
    assert skip is False
    assert reason is None


def test_skip_guard_does_not_fire_on_non_trading_day_after_cutoff():
    """Sat 14:00 PT → skip=False, even though the wall-clock is past 13:30.

    The Saturday SF cron itself fires at 02:00 PT, well below the cutoff,
    so this case only applies to manual reruns. On a non-trading day,
    _previous_trading_day() resolves to a settled prior session (e.g.,
    Friday) — polygon T+1 is fine, no skip needed.
    """
    # 2026-04-25 is a Saturday.
    now = datetime(2026, 4, 25, 14, 0, 0, tzinfo=_PT)
    skip, reason = weekly_collector._should_skip_morning_enrich(now=now)
    assert skip is False
    assert reason is None


def test_skip_guard_does_not_fire_on_saturday_cron_window():
    """Sat 02:00 PT (the scheduled Saturday SF cron time) → skip=False.

    Defensive: confirms the guard does not interfere with the scheduled
    Saturday SF path. This is the regression case for "we accidentally
    broke the Saturday cron path."
    """
    # 2026-04-25 is a Saturday.
    now = datetime(2026, 4, 25, 2, 0, 0, tzinfo=_PT)
    skip, reason = weekly_collector._should_skip_morning_enrich(now=now)
    assert skip is False
    assert reason is None


def test_skip_guard_does_not_fire_on_holiday_after_cutoff():
    """Christmas 14:00 PT → skip=False (NYSE closed).

    Same logic as the Saturday case: on a non-trading day the guard
    doesn't apply because polygon T+1 for the prior session is already
    settled.
    """
    # 2026-12-25 is Christmas (NYSE closed).
    now = datetime(2026, 12, 25, 14, 0, 0, tzinfo=_PT)
    skip, reason = weekly_collector._should_skip_morning_enrich(now=now)
    assert skip is False
    assert reason is None


def test_skip_guard_accepts_utc_input_via_astimezone():
    """A UTC-aware datetime that lands past 13:30 PT after conversion fires.

    Real callers pass nothing (helper grabs `datetime.now(PT)` itself), but
    callers that DO pass a `now` may pass it in any tz — the helper must
    convert. Wed 22:00 UTC = Wed 15:00 PT (during DST) = past cutoff.
    """
    # 2026-04-22 22:00 UTC = 15:00 PT (DST in effect)
    now_utc = datetime(2026, 4, 22, 22, 0, 0, tzinfo=timezone.utc)
    skip, reason = weekly_collector._should_skip_morning_enrich(now=now_utc)
    assert skip is True
    assert "post_close_midweek" in reason


# ── _run_morning_enrich integration with the skip guard ────────────────────


def test_morning_enrich_short_circuits_when_skip_guard_fires():
    """When the guard says skip, _run_morning_enrich must return
    status='skipped' WITHOUT calling polygon, daily_append, or any
    side-effecting collector. The yfinance row already in ArcticDB stays
    authoritative for this run."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date=None, dry_run=False, morning_enrich=True)

    fake_constituents = MagicMock()
    polygon_calls = []
    daily_append_calls = []
    health_calls = []

    with patch(
        "weekly_collector._should_skip_morning_enrich",
        return_value=(True, "post_close_midweek (test)"),
    ), patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect",
               side_effect=lambda **k: polygon_calls.append(k) or {"status": "ok"}), \
         patch("builders.daily_append.daily_append",
               side_effect=lambda **k: daily_append_calls.append(k) or {"status": "ok"}), \
         patch("weekly_collector._write_module_health",
               side_effect=lambda *a, **k: health_calls.append(k)):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "skipped"
    assert "post_close_midweek" in result["skip_reason"]
    assert result["would_have_targeted"]  # _previous_trading_day() filled it in
    assert polygon_calls == [], "polygon must not be called when guard skips"
    assert daily_append_calls == [], "daily_append must not be called when guard skips"
    assert health_calls == [], "health stamp must not refresh when guard skips"
    fake_constituents.collect.assert_not_called()


def test_morning_enrich_explicit_date_overrides_skip_guard():
    """Explicit --date is operator-driven backfill — must run polygon even
    after 13:30 PT. Operator knows what they're doing (e.g., backfilling a
    specific date that polygon T+1 has long since settled)."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-22", dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}

    polygon_calls = []
    def fake_collect(**kwargs):
        polygon_calls.append(kwargs)
        return {"status": "ok_dry_run", "polygon": 1, "fred": 0, "yfinance": 0,
                "tickers_captured": 1, "source": "polygon_only"}

    # Force the guard to be willing to fire — verifies that --date bypasses it.
    with patch(
        "weekly_collector._should_skip_morning_enrich",
        return_value=(True, "would-have-skipped"),
    ), patch("weekly_collector.constituents", fake_constituents), \
         patch("weekly_collector.daily_closes.collect", side_effect=fake_collect), \
         patch("builders.daily_append.daily_append", return_value={"status": "ok"}):
        result = weekly_collector._run_morning_enrich(config, args)

    assert result["status"] == "ok"
    assert result["date"] == "2026-04-22"
    assert len(polygon_calls) == 1, (
        "explicit --date must bypass the skip guard and call polygon"
    )


def test_morning_enrich_dry_run_skips_preflight_writes():
    """Dry runs must not refresh constituents.json or prune ArcticDB —
    side-effect-free is the contract."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    args = SimpleNamespace(date="2026-04-24", dry_run=True, morning_enrich=True)

    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    # collect MUST NOT be called in dry-run.

    prune_called = []

    with patch("weekly_collector.constituents", fake_constituents), \
         patch("builders.prune_delisted_tickers.prune_delisted_tickers",
               side_effect=lambda **k: (prune_called.append(k), {})[1]), \
         patch("weekly_collector.daily_closes.collect",
               return_value={"status": "ok_dry_run", "polygon": 1, "fred": 0,
                             "yfinance": 0, "tickers_captured": 1,
                             "source": "polygon_only"}), \
         patch("builders.daily_append.daily_append",
               return_value={"status": "ok"}):
        weekly_collector._run_morning_enrich(config, args)

    fake_constituents.collect.assert_not_called()
    assert prune_called == []
