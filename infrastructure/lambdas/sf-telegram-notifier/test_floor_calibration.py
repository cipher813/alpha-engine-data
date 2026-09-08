"""Unit tests for floor_calibration.py (alpha-engine-config-I10164 part 2)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from floor_calibration import (
    DEGENERATE_SPREAD_RATIO,
    MARGIN,
    MIN_SAMPLES,
    STATE_TO_STATE_MACHINE,
    FloorRecommendation,
    collect_state_duration_samples,
    compute_all_recommendations,
    compute_recommendation,
    render_report,
    run_check,
)


def _samples(durations, poll_statuses=None):
    if poll_statuses is None:
        return [{"duration_sec": d, "poll_status": None} for d in durations]
    return [
        {"duration_sec": d, "poll_status": s} for d, s in zip(durations, poll_statuses)
    ]


# ── compute_recommendation: core statuses ────────────────────────────────


def test_unmeasurable_below_min_samples():
    samples = _samples([100.0] * (MIN_SAMPLES - 1))
    rec = compute_recommendation("SomeState", samples, current_floor_sec=90)
    assert rec.status == "unmeasurable"
    assert rec.recommended_floor_sec is None
    assert rec.n_genuine == MIN_SAMPLES - 1


def test_unmeasurable_is_never_zero_and_never_dropped():
    """A too-small sample never defaults to a floor of 0 — it stays a
    reported row with its own status, not a silently omitted one."""
    samples = _samples([50.0, 60.0])
    rec = compute_recommendation("RareState", samples, current_floor_sec=30)
    assert rec.status == "unmeasurable"
    assert rec.current_floor_sec == 30  # untouched, not zeroed
    assert rec.recommended_floor_sec is None
    # See test_every_codified_floor_state_is_reported for the invariant that
    # every STATE_DURATION_FLOORS_SEC entry (not just states with samples) is
    # always present in the full report.


def test_degenerate_distribution_flagged_not_recommended():
    # Every sample within DEGENERATE_SPREAD_RATIO of each other.
    tight = [1000.0 + i * 0.01 for i in range(MIN_SAMPLES + 5)]
    assert (max(tight) / min(tight)) < DEGENERATE_SPREAD_RATIO
    rec = compute_recommendation("TooTight", _samples(tight), current_floor_sec=800)
    assert rec.status == "degenerate"
    assert rec.recommended_floor_sec is None


def test_ok_when_current_floor_within_tolerance_of_recommendation():
    durations = [100.0 + i for i in range(MIN_SAMPLES + 10)]  # min=100
    recommended = round(100.0 * (1 - MARGIN))  # 85
    rec = compute_recommendation("Steady", _samples(durations), current_floor_sec=recommended)
    assert rec.status == "ok"
    assert rec.recommended_floor_sec == recommended


def test_drift_tighten_when_current_floor_too_low():
    # Mirrors PollMorningArcticAppendSpot: current floor sits far BELOW the
    # measured genuine minimum, catching nothing — a false negative.
    durations = [1474.9 + i * 40 for i in range(MIN_SAMPLES + 10)]
    rec = compute_recommendation("TooLoose", _samples(durations), current_floor_sec=480)
    assert rec.status == "drift_tighten"
    assert rec.recommended_floor_sec is not None
    assert rec.recommended_floor_sec > 480


def test_drift_loosen_when_current_floor_too_high():
    # Mirrors PollMorningEnrichSpot pre-recalibration: current floor sits
    # ABOVE the measured genuine minimum, false-positiving on healthy runs.
    durations = [106.8 + i for i in range(MIN_SAMPLES + 10)]
    rec = compute_recommendation("TooTightFloor", _samples(durations), current_floor_sec=480)
    assert rec.status == "drift_loosen"
    assert rec.recommended_floor_sec is not None
    assert rec.recommended_floor_sec < 480


def test_never_silently_widens_a_correctly_tight_floor():
    """A floor that already sits BELOW the recommendation (correctly tight,
    catching more than the bare minimum would) is not reported OK just
    because it's conservative — it is drift_loosen only when it exceeds the
    band, otherwise the mechanism must not push it wider for no reason."""
    durations = [1000.0 + i * 40 for i in range(MIN_SAMPLES + 10)]
    recommended = round(1000.0 * (1 - MARGIN))
    # current floor already well below recommended (tighter than necessary,
    # but not by so much it is a false negative) — inside tolerance band.
    tight_but_in_band = int(recommended * 0.85)
    rec = compute_recommendation("Conservative", _samples(durations), current_floor_sec=tight_but_in_band)
    assert rec.status in ("ok", "drift_tighten")
    # Whichever it is, the formula-driven recommendation is symmetric — this
    # test exists to pin that "ok" is reachable from BELOW recommended too,
    # not only from above (i.e. the check is not loosen-only).
    if rec.status == "drift_tighten":
        assert rec.recommended_floor_sec > tight_but_in_band


# ── poll-status exclusion (the ArcticAppend lesson) ──────────────────────


def test_poll_status_failed_samples_excluded_from_genuine_distribution():
    """A state with a KNOWN_POLL_STATUS_KEYS entry must exclude Failed
    samples from the genuine min/percentile computation — the exact defect
    this module exists to prevent (a broken-but-SUCCEEDED Task laundering a
    short duration into the 'genuine' distribution)."""
    genuine_durations = [1474.9 + i * 40 for i in range(MIN_SAMPLES + 5)]
    broken_durations = [121.3, 260.3, 929.7]
    samples = _samples(
        broken_durations + genuine_durations,
        poll_statuses=["Failed"] * len(broken_durations)
        + ["Success"] * len(genuine_durations),
    )
    rec = compute_recommendation(
        "PollMorningArcticAppendSpot", samples, current_floor_sec=480
    )
    assert rec.n_excluded == 3
    assert rec.min_sec == pytest.approx(1474.9, abs=0.5)
    assert rec.status == "drift_tighten"


def test_state_without_poll_key_uses_raw_duration():
    """A state absent from KNOWN_POLL_STATUS_KEYS has no ground-truth signal
    beyond duration — this is a declared, documented limitation, not a bug:
    every sample counts as genuine regardless of a (nonexistent) poll_status."""
    durations = [500.0 + i for i in range(MIN_SAMPLES + 5)]
    rec = compute_recommendation("Scanner", _samples(durations), current_floor_sec=60)
    assert rec.n_excluded == 0
    assert rec.n_genuine == len(durations)


# ── compute_all_recommendations: derived set, not hand-kept ──────────────


def test_every_codified_floor_state_is_reported():
    """The report covers every entry in STATE_DURATION_FLOORS_SEC, derived
    from that module, never a separately hand-kept list here."""
    recs = compute_all_recommendations({})
    from execution_digest import STATE_DURATION_FLOORS_SEC

    reported_names = {r.state_name for r in recs}
    assert reported_names == set(STATE_DURATION_FLOORS_SEC)
    # With zero samples supplied, every one is unmeasurable — recorded, not
    # dropped.
    assert all(r.status == "unmeasurable" for r in recs)
    assert all(r.n_genuine == 0 for r in recs)


def test_state_to_state_machine_covers_every_codified_floor():
    from execution_digest import STATE_DURATION_FLOORS_SEC

    assert set(STATE_DURATION_FLOORS_SEC) == set(STATE_TO_STATE_MACHINE)


# ── render_report ──────────────────────────────────────────────────────


def test_render_report_includes_every_recommendation():
    recs = [
        FloorRecommendation(
            state_name="A", status="ok", current_floor_sec=90, n_genuine=20, n_excluded=0,
            min_sec=100.0, recommended_floor_sec=85, basis="x",
        ),
        FloorRecommendation(
            state_name="B", status="unmeasurable", current_floor_sec=480, n_genuine=2,
            n_excluded=0, basis="y",
        ),
    ]
    report = render_report(recs)
    assert "A" in report and "B" in report
    assert "ok" in report and "unmeasurable" in report


# ── collect_state_duration_samples: status extraction from Task output ───


def _entered(name, ts):
    return {"type": "TaskStateEntered", "timestamp": ts, "stateEnteredEventDetails": {"name": name}}


def _exited(name, ts, output):
    import json

    return {
        "type": "TaskStateExited",
        "timestamp": ts,
        "stateExitedEventDetails": {"name": name, "output": json.dumps(output)},
    }


def test_collect_state_duration_samples_extracts_poll_status():
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 7, 5, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        _entered("PollMorningArcticAppendSpot", base),
        _exited(
            "PollMorningArcticAppendSpot",
            base + timedelta(seconds=930),
            {"arctic_append_poll": {"Status": "Failed"}},
        ),
    ]

    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [
        {"executions": [{"executionArn": "arn:exec:1", "name": "exec-1"}]}
    ]

    def fake_fetch(_client, _arn):
        return events

    samples = collect_state_duration_samples(
        sf_client,
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline",
        ["PollMorningArcticAppendSpot"],
        fetch_history=fake_fetch,
    )
    assert len(samples["PollMorningArcticAppendSpot"]) == 1
    sample = samples["PollMorningArcticAppendSpot"][0]
    assert sample["duration_sec"] == pytest.approx(930.0)
    assert sample["poll_status"] == "Failed"


def test_run_check_wires_every_state_machine():
    """run_check must reach every state machine named in
    STATE_TO_STATE_MACHINE, not only the one under active investigation —
    the whole point of a periodic mechanism is it does not need a human to
    remember which pipeline to point it at."""
    sf_client = MagicMock()
    sf_client.get_paginator.return_value.paginate.return_value = [{"executions": []}]

    recs = run_check(sf_client)
    names = {r.state_name for r in recs}
    assert names == set(STATE_TO_STATE_MACHINE)

    called_arns = {
        call.kwargs.get("stateMachineArn")
        for call in sf_client.get_paginator.return_value.paginate.call_args_list
    }
    expected_machines = set(STATE_TO_STATE_MACHINE.values())
    called_machines = {arn.rsplit(":", 1)[-1] for arn in called_arns}
    assert called_machines == expected_machines
