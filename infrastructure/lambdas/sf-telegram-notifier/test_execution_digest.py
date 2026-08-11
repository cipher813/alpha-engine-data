"""Unit tests for execution_digest.py (config#1672)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from execution_digest import (
    STATE_DURATION_FLOORS_SEC,
    build_execution_digest,
    build_state_durations,
    format_digest_lines,
    parse_run_date_from_execution_name,
    parse_task_state_durations,
    StateDuration,
)


def _ts(base: datetime, offset_sec: int) -> datetime:
    return base.replace(tzinfo=timezone.utc) + __import__("datetime").timedelta(seconds=offset_sec)


def test_parse_task_state_durations_computes_wall_clock():
    base = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        {
            "type": "TaskStateEntered",
            "timestamp": base,
            "taskStateEnteredEventDetails": {"name": "PredictorTraining"},
        },
        {
            "type": "TaskStateExited",
            "timestamp": _ts(base, 120),
            "taskStateExitedEventDetails": {"name": "PredictorTraining"},
        },
    ]
    assert parse_task_state_durations(events)["PredictorTraining"] == 120


def test_floor_breach_detected_when_under_minimum():
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PredictorTraining": 120},
        is_preflight=False,
        execution_start=start,
        run_date="2026-07-04",
        s3_client=None,
    )
    assert len(rows) == 1
    assert rows[0].floor_breach is True
    assert rows[0].anomaly is True


def test_preflight_suppresses_floor_breach():
    start = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    rows = build_state_durations(
        {"PredictorTraining": 30},
        is_preflight=True,
        execution_start=start,
        run_date=None,
        s3_client=None,
    )
    assert rows[0].floor_breach is False


def test_format_digest_sorts_anomalies_visually():
    rows = [
        StateDuration("Backtester", 600, 600, False, False),
        StateDuration("PredictorTraining", 120, 1200, True, False),
    ]
    lines = format_digest_lines(rows)
    assert any("PredictorTraining" in line and "⚠️" in line for line in lines)
    assert any("Backtester" in line and "✓" in line for line in lines)


def test_build_execution_digest_hollow_on_fast_predictor():
    start_ms = 1_700_000_000_000
    sf = MagicMock()
    base = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    sf.get_execution_history.return_value = {
        "events": [
            {
                "type": "TaskStateEntered",
                "timestamp": base,
                "taskStateEnteredEventDetails": {"name": "PredictorTraining"},
            },
            {
                "type": "TaskStateExited",
                "timestamp": _ts(base, 90),
                "taskStateExitedEventDetails": {"name": "PredictorTraining"},
            },
        ],
    }
    lines, hollow = build_execution_digest(
        execution_arn="arn:aws:states:us-east-1:123:execution:sm:exec",
        is_preflight=False,
        execution_start_ms=start_ms,
        run_date="2026-07-04",
        sf_client=sf,
        s3_client=None,
    )
    assert hollow is True
    assert any("PredictorTraining" in line for line in lines)
    assert STATE_DURATION_FLOORS_SEC["PredictorTraining"] == 20 * 60


def test_parse_run_date_from_execution_name_extracts_iso_date():
    # daemon.py's _trigger_eod_pipeline: name=f"eod-{run_date}-{epoch}"
    assert parse_run_date_from_execution_name("eod-2026-08-08-1754678901") == "2026-08-08"


def test_parse_run_date_from_execution_name_handles_backstop_prefix():
    # eod-backstop/index.py: name=f"eod-backstop-{trading_day}-{epoch}"
    assert (
        parse_run_date_from_execution_name("eod-backstop-2026-08-08-1700000000")
        == "2026-08-08"
    )


def test_parse_run_date_from_execution_name_returns_none_without_a_date():
    assert parse_run_date_from_execution_name("exec-001") is None


def test_parse_run_date_from_execution_name_returns_none_for_empty_input():
    assert parse_run_date_from_execution_name(None) is None
    assert parse_run_date_from_execution_name("") is None


def test_history_fetch_failure_surfaces_marker():
    sf = MagicMock()
    sf.get_execution_history.side_effect = RuntimeError("throttled")
    lines, hollow = build_execution_digest(
        execution_arn="arn:exec",
        is_preflight=False,
        execution_start_ms=1_700_000_000_000,
        run_date=None,
        sf_client=sf,
        s3_client=None,
    )
    assert hollow is False
    assert any("digest unavailable" in line for line in lines)


# ── A failure notification must name the state that broke ───────────────────
#
# Live 2026-08-10: ne-weekly-freshness-pipeline failed twice at MorningEnrich
# (watch-rerun-2026-08-10-1 and -2), whose spot bootstrap hung and was SIGKILLed
# at its SSM budget. Both Telegram alerts rendered
# "States: -(no workload states in history)-" on a 60-state pipeline, because
# no workload state had EXITED so no duration existed. The state that broke was
# in the events the whole time, as a TaskStateEntered with no matching exit.


def _entered(base, name, offset=0):
    return {
        "type": "TaskStateEntered",
        "timestamp": _ts(base, offset),
        "taskStateEnteredEventDetails": {"name": name},
    }


def test_last_workload_state_entered_finds_the_unexited_state():
    from execution_digest import last_workload_state_entered

    base = datetime(2026, 8, 10, 23, 58, 0, tzinfo=timezone.utc)
    events = [
        _entered(base, "CheckSkipMorningEnrich", 0),   # untracked gate — ignored
        _entered(base, "MorningEnrich", 10),
    ]
    assert last_workload_state_entered(events) == "MorningEnrich"


def test_last_workload_state_entered_ignores_untracked_states():
    """Naming a poll or gate state would be true and useless."""
    from execution_digest import last_workload_state_entered

    base = datetime(2026, 8, 10, 23, 58, 0, tzinfo=timezone.utc)
    events = [_entered(base, "CheckMorningEnrichStatus", 0), _entered(base, "WaitForMorningEnrich", 5)]
    assert last_workload_state_entered(events) is None


def test_digest_names_the_entered_state_instead_of_saying_nothing():
    lines = format_digest_lines([], last_entered="MorningEnrich")
    assert lines == ["MorningEnrich — entered, never completed ⚠️"]
    assert "no workload states in history" not in " ".join(lines)


def test_digest_keeps_the_empty_message_when_nothing_workload_ran():
    """A run that truly reached no workload state — the narrowed
    director-verify shape — still says so."""
    assert format_digest_lines([], last_entered=None) == [
        "_(no workload states in history)_"
    ]


def test_build_execution_digest_names_the_hung_state_end_to_end():
    """The 2026-08-10 shape, through the real entry point."""
    start_ms = 1_786_400_000_000
    base = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    sf = MagicMock()
    sf.get_execution_history.return_value = {
        "events": [
            _entered(base, "MorningEnrich", 30),  # entered, never exited
        ]
    }
    lines, hollow = build_execution_digest(
        execution_arn="arn:aws:states:us-east-1:711398986525:execution:"
        "ne-weekly-freshness-pipeline:watch-rerun-2026-08-10-2",
        is_preflight=False,
        execution_start_ms=start_ms,
        run_date="2026-08-10",
        sf_client=sf,
        s3_client=None,
    )
    assert lines == ["MorningEnrich — entered, never completed ⚠️"]
    assert hollow is False
