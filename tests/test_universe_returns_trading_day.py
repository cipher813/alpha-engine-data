"""Trading-day eligibility filter in collectors/universe_returns.py.

The arithmetic itself (add_trading_days) is locked in alpha-engine-lib's
test_trading_calendar.py. These tests cover the data-module behavior:
that _trading_days_to_process correctly delegates to NYSE-aware arithmetic
and never enqueues weekends, holidays, or eval_dates whose 5d forward
window has not yet closed.
"""
from __future__ import annotations

from datetime import date

from collectors.universe_returns import _trading_days_to_process


class TestTradingDaysToProcess:
    def test_excludes_weekends_and_holidays(self):
        # Today = Sat 2026-04-25; lookback covers Good Friday 4/3 and the
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
        # On 2026-04-25, 2026-04-17 (Fri) IS eligible because
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

    def test_holiday_eval_date_skips_correctly(self):
        # 2026-04-02 (Thu before Good Friday): trading day. fwd_5d via
        # NYSE-aware arithmetic = 2026-04-10 (skip Good Friday). 4/10 < 4/25
        # → eligible, included.
        out = _trading_days_to_process(
            today=date(2026, 4, 25),
            max_lookback=20,
            existing=set(),
        )
        assert "2026-04-02" in out
