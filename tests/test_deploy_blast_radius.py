"""Unit tests for infrastructure/deploy_blast_radius.py (alpha-engine-config-I7800).

Covers the two candidate-trigger computations and the picker that names
whichever fires first — the deliverable is "the failure notification names
which pipeline halts next and when," so these pin the actual dates/pipeline
choice against fixed `now` instants rather than just checking the function
doesn't raise.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infrastructure"))

import deploy_blast_radius as br  # noqa: E402


class TestNextPreopenUtc:
    def test_friday_before_preopen_rolls_to_same_day(self):
        # 2026-08-21 is a Friday. 11:00 UTC = 04:00 PT, before 05:15 PT.
        now = datetime(2026, 8, 21, 11, 0, tzinfo=timezone.utc)
        nxt = br._next_preopen_utc(now)
        assert nxt.astimezone(br._PT).date().isoformat() == "2026-08-21"

    def test_friday_after_preopen_rolls_to_monday(self):
        # 2026-08-21 Friday, 15:00 UTC = 08:00 PT, after 05:15 PT preopen.
        now = datetime(2026, 8, 21, 15, 0, tzinfo=timezone.utc)
        nxt = br._next_preopen_utc(now)
        assert nxt.astimezone(br._PT).date().isoformat() == "2026-08-24"  # Monday
        assert nxt.astimezone(br._PT).weekday() == 0

    def test_never_lands_on_a_weekend(self):
        # Saturday morning.
        now = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)
        nxt = br._next_preopen_utc(now)
        assert nxt.astimezone(br._PT).weekday() == 0  # Monday


class TestNextWeeklyUtc:
    def test_before_thursday_rolls_to_thursday(self):
        # 2026-08-19 is a Wednesday.
        now = datetime(2026, 8, 19, 8, 0, tzinfo=timezone.utc)
        nxt = br._next_weekly_utc(now)
        assert nxt.date().isoformat() == "2026-08-20"  # Thursday
        assert nxt.hour == 9

    def test_thursday_after_0900_rolls_to_friday(self):
        now = datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc)  # Thu, past 09:00
        nxt = br._next_weekly_utc(now)
        assert nxt.date().isoformat() == "2026-08-21"  # Friday

    def test_saturday_after_0900_rolls_to_next_thursday(self):
        now = datetime(2026, 8, 22, 10, 0, tzinfo=timezone.utc)  # Sat, past 09:00
        nxt = br._next_weekly_utc(now)
        assert nxt.date().isoformat() == "2026-08-27"  # next Thursday
        assert nxt.weekday() == 3


class TestComputeBlastRadius:
    def test_picks_the_sooner_of_the_two_candidates(self):
        # Friday 15:00 UTC: preopen candidate is Monday 12:15 UTC (05:15 PT
        # DST); weekly candidate is Saturday 09:00 UTC. Weekly is sooner.
        now = datetime(2026, 8, 21, 15, 0, tzinfo=timezone.utc)
        result = br.compute_blast_radius(now)
        assert result["pipeline"] == "weekly"
        assert result["sm_name"] == "ne-weekly-freshness-pipeline"

    def test_preopen_wins_when_it_is_sooner(self):
        # Sunday evening: preopen Monday 05:15 PT is sooner than the
        # following Thursday 09:00 UTC weekly candidate.
        now = datetime(2026, 8, 23, 23, 0, tzinfo=timezone.utc)  # Sunday
        result = br.compute_blast_radius(now)
        assert result["pipeline"] == "preopen"
        assert result["sm_name"] == "ne-preopen-trading-pipeline"
        assert "HALTS" in result["message"]

    def test_message_names_the_pipeline_and_is_a_single_line(self):
        now = datetime(2026, 8, 21, 15, 0, tzinfo=timezone.utc)
        result = br.compute_blast_radius(now)
        assert result["sm_name"] in result["message"]
        assert "\n" not in result["message"]
        assert "alpha-engine-config-I7800" in result["message"]

    def test_defaults_to_now_when_no_arg_given(self):
        # Doesn't raise, and returns a well-formed result for whatever "now" is.
        result = br.compute_blast_radius()
        assert result["pipeline"] in ("preopen", "weekly")
