"""Trading-day arithmetic in collectors/universe_returns.py.

Pre-fix `_add_business_days` skipped weekends but counted NYSE holidays
as business days, silently mis-labeling forward returns whose window
crossed a holiday. These tests pin the trading-day-aware behavior.
"""
from __future__ import annotations

from datetime import date

import pytest

from collectors.universe_returns import (
    _add_trading_days,
    _trading_days_to_process,
)


class TestAddTradingDays:
    def test_skips_weekend(self):
        assert _add_trading_days(date(2026, 4, 17), 1) == date(2026, 4, 20)

    def test_skips_good_friday_2026(self):
        # 2026-04-02 (Thu) → 5 trading days = 2026-04-10 (Fri)
        # Calendar BDs would have wrongly included 2026-04-03 (Good Friday)
        # and returned 2026-04-09.
        assert _add_trading_days(date(2026, 4, 2), 5) == date(2026, 4, 10)

    def test_skips_thanksgiving(self):
        # 2025-11-26 (Wed) → 1 trading day; 11-27 is Thanksgiving (closed),
        # 11-28 is Black Friday (open, half day but a trading day).
        assert _add_trading_days(date(2025, 11, 26), 1) == date(2025, 11, 28)

    def test_horizons_5_10_30_no_holiday_window(self):
        # 2026-04-13 (Mon): no NYSE holidays in 4/14-5/27 window;
        # trading-day counts collapse to calendar BD counts.
        assert _add_trading_days(date(2026, 4, 13), 5) == date(2026, 4, 20)
        assert _add_trading_days(date(2026, 4, 13), 10) == date(2026, 4, 27)

    def test_zero_returns_start(self):
        # Adding 0 trading days returns the input unchanged.
        assert _add_trading_days(date(2026, 4, 17), 0) == date(2026, 4, 17)


class TestTradingDaysToProcess:
    def test_only_trading_days_enumerated(self):
        # Today = Sat 2026-04-25; lookback covers 4/3 (Good Friday) and
        # 4/4-5 weekend. None should appear in the result.
        out = _trading_days_to_process(
            today=date(2026, 4, 25),
            max_lookback=20,
            existing=set(),
        )
        forbidden = {"2026-04-03", "2026-04-04", "2026-04-05"}
        assert forbidden.isdisjoint(set(out))

    def test_eligibility_requires_fwd_5d_strictly_past(self):
        # Today = 2026-04-25 (Sat). 4/20 (Mon) has fwd_5d = 4/27 (next Mon),
        # which is NOT < today → ineligible.
        out = _trading_days_to_process(
            today=date(2026, 4, 25),
            max_lookback=10,
            existing=set(),
        )
        assert "2026-04-20" not in out

    def test_existing_dates_filtered(self):
        out = _trading_days_to_process(
            today=date(2026, 4, 25),
            max_lookback=20,
            existing={"2026-04-17"},
        )
        assert "2026-04-17" not in out

    def test_4_17_eligible_today(self):
        # Sanity: on 2026-04-25, 2026-04-17 (Fri) IS eligible because
        # fwd_5d = 4/24 < 4/25 and it's a trading day.
        out = _trading_days_to_process(
            today=date(2026, 4, 25),
            max_lookback=20,
            existing=set(),
        )
        assert "2026-04-17" in out

    def test_results_sorted_chronologically(self):
        out = _trading_days_to_process(
            today=date(2026, 4, 25),
            max_lookback=30,
            existing=set(),
        )
        assert out == sorted(out)


@pytest.mark.parametrize(
    "eval_date,horizon,expected",
    [
        # Good Friday 2026 window: holiday must be skipped
        (date(2026, 4, 2), 1, date(2026, 4, 6)),  # Thu 4/2 → Mon 4/6 (skip Fri Good Friday)
        (date(2026, 4, 2), 5, date(2026, 4, 10)),
        # Thanksgiving 2025
        (date(2025, 11, 25), 5, date(2025, 12, 3)),  # Tue → 1d holiday + weekends
        # New Year 2026 — 2026-01-01 (Thu, holiday) and the long weekend
        (date(2025, 12, 31), 1, date(2026, 1, 2)),  # Wed → Fri (skip 1/1)
    ],
)
def test_horizon_table(eval_date, horizon, expected):
    assert _add_trading_days(eval_date, horizon) == expected
