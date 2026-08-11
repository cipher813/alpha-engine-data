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
    """Naming a gate or wait state would be true and useless.

    The exclusion is by EVENT TYPE, not by a name whitelist
    (alpha-engine-config-I6857). Choice and Wait states emit
    ChoiceStateEntered / WaitStateEntered, which _state_name_from_event does
    not read, so they cannot reach this function whatever they are called.

    This test previously fed CheckMorningEnrichStatus in as a
    TaskStateEntered and asserted it was dropped — which passed only because
    the name was missing from DIGEST_STATE_ORDER, and asserted a shape Step
    Functions never emits. Under the whitelist that also meant every genuine
    weekday Task state was dropped with it, which is the defect I6857 fixed.
    """
    from execution_digest import last_workload_state_entered

    base = datetime(2026, 8, 10, 23, 58, 0, tzinfo=timezone.utc)
    events = [
        {
            "type": "ChoiceStateEntered",
            "timestamp": _ts(base, 0),
            "stateEnteredEventDetails": {"name": "CheckMorningEnrichStatus"},
        },
        {
            "type": "WaitStateEntered",
            "timestamp": _ts(base, 5),
            "stateEnteredEventDetails": {"name": "MorningEnrichWait"},
        },
    ]
    assert last_workload_state_entered(events) is None


def test_last_workload_state_entered_names_a_weekday_task_state():
    """The counterpart: a Task state absent from DIGEST_STATE_ORDER is named.

    Under the old whitelist this returned None for every weekday pipeline
    state, so a preopen run dying before its first exit named nothing.
    """
    from execution_digest import last_workload_state_entered

    base = datetime(2026, 8, 11, 12, 15, 0, tzinfo=timezone.utc)
    assert last_workload_state_entered([_entered(base, "SomeBrandNewState", 0)]) == "SomeBrandNewState"


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


# ── Weekday pipelines render at all (alpha-engine-config-I6857) ───────────
#
# The 2026-08-11 preopen alert rendered "_(no workload states in history)_"
# for an execution with 1403 events across 18 distinct Task states. Not a
# one-off: DIGEST_STATE_ORDER and STATE_DURATION_FLOORS_SEC listed weekly
# state names only, and build_state_durations dropped everything in neither,
# so EVERY weekday alert rendered an empty list on EVERY run.
#
# nousergon-data-PR1295 had shipped that morning and did not prevent it — it
# covered a run dying before its first TaskStateExited. This run exited many.

import execution_digest as ed
import json as _json
from pathlib import Path as _Path

_FIXTURE = _Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "sf_history_preopen_2026-08-11.json"


def _preopen_history() -> list[dict]:
    """The real 2026-08-11 preopen execution, trimmed to Task state events.

    Execution 021b85f7-4814-477e-9fe0-05c77f4296d6 — the one whose alert
    named no states. Timestamps are parsed back to datetimes because that is
    what botocore hands the digest.
    """
    from datetime import datetime

    events = _json.loads(_FIXTURE.read_text())
    for e in events:
        e["timestamp"] = datetime.fromisoformat(e["timestamp"])
    return events


def test_the_real_2026_08_11_preopen_history_renders_states():
    """The regression, against the execution that produced it."""
    events = _preopen_history()
    durations = ed.parse_task_state_durations(events)
    rows = ed.build_state_durations(
        durations,
        is_preflight=False,
        execution_start=events[0]["timestamp"],
        run_date="2026-08-11",
        s3_client=None,
    )
    lines = ed.format_digest_lines(rows, last_entered=ed.last_workload_state_entered(events))

    assert lines != ["_(no workload states in history)_"]
    assert any("Scanner" in line for line in lines), (
        "the state that degraded the run must appear in the digest of that run"
    )


def test_the_preopen_poll_loop_reports_its_elapsed_not_zero():
    """PollMorningEnrichSpot spans ~15 min across ~60 entry/exit pairs.

    Max-per-pair reported 0s, which is worse than omitting it: it asserts the
    stage was instant.
    """
    durations = ed.parse_task_state_durations(_preopen_history())
    assert durations["PollMorningEnrichSpot"] > 10 * 60
    assert durations["PollMorningArcticAppendSpot"] > 10 * 60


def test_a_state_in_neither_collection_still_renders():
    """The filter is gone; the collections annotate and order only."""
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"TotallyUnknownState": 42},
        is_preflight=False,
        execution_start=start,
        run_date="2026-08-11",
        s3_client=None,
    )
    assert [r.name for r in rows] == ["TotallyUnknownState"]
    assert rows[0].floor_sec is None
    assert not rows[0].anomaly


def test_unknown_states_sort_after_known_ones_longest_first():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"ZebraState": 10, "AardvarkState": 900, "Scanner": 700},
        is_preflight=False,
        execution_start=start,
        run_date="2026-08-11",
        s3_client=None,
    )
    rows.sort(key=ed._sort_key)
    assert [r.name for r in rows] == ["Scanner", "AardvarkState", "ZebraState"]


def test_anomalous_states_lead_so_truncation_cannot_drop_them():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"Scanner": 5, "MorningEnrich": 20 * 60},  # Scanner breaches its 60s floor
        is_preflight=False,
        execution_start=start,
        run_date="2026-08-11",
        s3_client=None,
    )
    rows.sort(key=ed._sort_key)
    assert rows[0].name == "Scanner"
    assert rows[0].floor_breach


def test_truncation_is_announced_never_silent():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    many = {f"State{i:02d}": 100 - i for i in range(ed._MAX_DIGEST_ROWS + 3)}
    rows = ed.build_state_durations(
        many, is_preflight=False, execution_start=start, run_date=None, s3_client=None
    )
    rows.sort(key=ed._sort_key)
    lines = ed.format_digest_lines(rows)

    assert len(lines) == ed._MAX_DIGEST_ROWS + 1
    assert "+3 more states" in lines[-1]


def test_no_truncation_line_when_everything_fits():
    from datetime import datetime, timezone

    start = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    rows = ed.build_state_durations(
        {"Scanner": 700}, is_preflight=False, execution_start=start, run_date=None, s3_client=None
    )
    lines = ed.format_digest_lines(rows)
    assert len(lines) == 1
    assert "elided" not in lines[0]


def test_every_weekday_state_name_in_the_order_list_exists_in_a_definition():
    """A misspelled state name is a silent no-op — it orders nothing.

    The old collections were not wrong about spelling, they were wrong about
    WHICH pipeline; this guard catches the other way of being wrong.
    """
    def _walk(states: dict) -> set[str]:
        """State names including those nested in Parallel branches / Map iterators.

        A flat scan of the top-level States map misses PredictorTraining,
        Parity and the rest, which live inside ResearchPredictorParallel's
        branches — and would then declare the digest's own weekly names
        phantom.
        """
        found = set(states)
        for body in states.values():
            for branch in body.get("Branches") or []:
                found |= _walk(branch.get("States") or {})
            iterator = body.get("Iterator") or body.get("ItemProcessor")
            if iterator:
                found |= _walk(iterator.get("States") or {})
        return found

    infra = _Path(__file__).resolve().parents[3] / "infrastructure"
    known: set[str] = set()
    for name in ("step_function.json", "step_function_daily.json", "step_function_eod.json"):
        known |= _walk(_json.loads((infra / name).read_text())["States"])

    unknown = sorted(s for s in ed.DIGEST_STATE_ORDER if s not in known)
    assert not unknown, f"DIGEST_STATE_ORDER names states no definition has: {unknown}"

    unknown_floors = sorted(s for s in ed.STATE_DURATION_FLOORS_SEC if s not in known)
    assert not unknown_floors, f"STATE_DURATION_FLOORS_SEC names states no definition has: {unknown_floors}"
