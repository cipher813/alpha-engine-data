"""SF-envelope completion marker wiring — preopen/weekday SF (config#2857).

Companion to tests/test_sf_completion_marker_wiring.py (the Saturday SF) and
test_sf_completion_marker_wiring_eod.py (postclose/EOD). alpha-engine-config#6692
ports EOD's Option-A degraded-terminal parity (Brian's 2026-07-28 ruling,
alpha-engine-config#2699) onto this SF: RunDaemon's success Next, its
non-fatal restart-failure Catch (via SetDaemonDegradedFlag), and the
skip-gate edge (CheckSkipRunDaemon) all now converge on CheckDegradedOutcome
rather than going straight to WriteCompletionMarker. The data-spot fail-open
path (ExtractDataSpotError) threads a $.degraded_summary flag via
SetDataSpotDegradedFlag WITHOUT changing its own fail-open continuation
(still proceeds to PublishDataSpotFailureImmediate -> CheckSkipPredictorInference).
CheckDegradedOutcome is the one place deciding the terminal: Default ->
WriteCompletionMarker (unchanged normal marker, -> PipelineComplete);
degraded -> WriteCompletionMarkerDegraded (-> DegradedRun, Type: Fail) so
status-keyed watchers (sf-watch, EventBridge, sf-telegram-notifier) engage.
A holiday skip or a real failure never reaches either marker.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"


@pytest.fixture
def daily_states():
    doc = json.loads((_INFRA / "step_function_daily.json").read_text())
    return doc["States"]


def test_marker_state_shape(daily_states):
    st = daily_states["WriteCompletionMarker"]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    assert "ne-preopen-trading-pipeline" in st["Parameters"]["Key.$"]
    assert "$$.Execution.StartTime" in st["Parameters"]["Key.$"]
    body = st["Parameters"]["Body.$"]
    assert "ne-preopen-trading-pipeline" in body
    assert "$$.Execution.Id" in body
    assert st["Next"] == "PipelineComplete"
    assert "Catch" not in st
    (retry,) = st["Retry"]
    assert retry["ErrorEquals"] == ["States.ALL"]
    assert retry["MaxAttempts"] >= 2


def test_run_daemon_success_and_catch_both_converge_on_degraded_check(daily_states):
    """alpha-engine-config#6692: neither edge writes a marker directly any
    more — both must pass through CheckDegradedOutcome so a degraded flag
    set upstream (data-spot fail-open) or by the Catch itself is honored."""
    run_daemon = daily_states["RunDaemon"]
    assert run_daemon["Next"] == "CheckDegradedOutcome"
    (catch,) = run_daemon["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "SetDaemonDegradedFlag"
    assert catch["ResultPath"] == "$.daemon_error"


def test_run_daemon_catch_degraded_flag_shape(daily_states):
    """RunDaemon restart failure is still non-fatal to the RUN (no abort) —
    SetDaemonDegradedFlag only changes what the eventual terminal says."""
    st = daily_states["SetDaemonDegradedFlag"]
    assert st["Type"] == "Pass"
    assert st["Parameters"]["degraded"] is True
    assert st["ResultPath"] == "$.degraded_summary"
    # config-I6903: PublishDaemonFailureImmediate now sits between the flag and
    # the terminal, so the invariant is that the path CONVERGES on
    # CheckDegradedOutcome, not that it hops there directly. Asserting the
    # immediate Next forbids ever adding a notification to a fail-open path —
    # which is the defect I6903 fixed. sf-pipeline-policy.md §5 names this
    # specific fail-open as one that must page immediately.
    assert st["Next"] == "PublishDaemonFailureImmediate"
    publish = daily_states["PublishDaemonFailureImmediate"]
    assert publish["Next"] == "CheckDegradedOutcome"
    assert publish["Catch"][0]["Next"] == "CheckDegradedOutcome", (
        "an SNS failure must not divert a run that deliberately continued"
    )


def test_skip_run_daemon_edge_converges_on_degraded_check(daily_states):
    (choice,) = daily_states["CheckSkipRunDaemon"]["Choices"]
    assert choice["Next"] == "CheckDegradedOutcome"


def test_data_spot_fail_open_threads_degraded_flag_without_changing_continuation(
    daily_states,
):
    """ExtractDataSpotError/PublishDataSpotFailureImmediate must keep their
    fail-open continuation to CheckSkipPredictorInference (I7811: the weekday Scanner is gone) —
    SetDataSpotDegradedFlag only adds the flag in between."""
    extract = daily_states["ExtractDataSpotError"]
    assert extract["Next"] == "SetDataSpotDegradedFlag"

    flag = daily_states["SetDataSpotDegradedFlag"]
    assert flag["Type"] == "Pass"
    assert flag["Parameters"]["degraded"] is True
    assert flag["ResultPath"] == "$.degraded_summary"
    assert flag["Next"] == "PublishDataSpotFailureImmediate"

    publish = daily_states["PublishDataSpotFailureImmediate"]
    assert publish["Next"] == "CheckSkipPredictorInference"
    (publish_catch,) = publish["Catch"]
    assert publish_catch["Next"] == "CheckSkipPredictorInference"


def test_check_degraded_outcome_routes_through_markers(daily_states):
    choice = daily_states["CheckDegradedOutcome"]
    assert choice["Default"] == "WriteCompletionMarker"
    (degraded_choice,) = choice["Choices"]
    assert degraded_choice["Next"] == "WriteCompletionMarkerDegraded"
    conditions = degraded_choice["And"]
    variables = {c["Variable"] for c in conditions}
    assert variables == {"$.degraded_summary.degraded"}


def test_marker_degraded_state_shape(daily_states):
    st = daily_states["WriteCompletionMarkerDegraded"]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::aws-sdk:s3:putObject"
    assert st["Parameters"]["Bucket"] == "alpha-engine-research"
    assert "ne-preopen-trading-pipeline" in st["Parameters"]["Key.$"]
    assert "$$.Execution.StartTime" in st["Parameters"]["Key.$"]
    body = st["Parameters"]["Body.$"]
    assert "ne-preopen-trading-pipeline" in body
    assert '"status\\":\\"DEGRADED' in body or "DEGRADED" in body
    assert "degraded_summary" in body
    assert "$$.Execution.Id" in body
    assert st["Next"] == "DegradedRun"
    assert "Catch" not in st
    (retry,) = st["Retry"]
    assert retry["ErrorEquals"] == ["States.ALL"]
    assert retry["MaxAttempts"] >= 2

    # Same S3 key format as the normal marker — parity requirement, no new
    # IAM grant needed (both live under the same _sf_completion prefix).
    normal_key = daily_states["WriteCompletionMarker"]["Parameters"]["Key.$"]
    assert st["Parameters"]["Key.$"] == normal_key


def test_degraded_run_is_a_fail_state(daily_states):
    """Brian's 2026-07-28 Option-A ruling: a degraded run must FAIL so
    status-keyed watchers engage, not a Succeed terminal."""
    st = daily_states["DegradedRun"]
    assert st["Type"] == "Fail"
    assert st["Error"] == "DegradedRun"


def test_holiday_skip_is_excluded_from_marker(daily_states):
    """A market-holiday skip must never satisfy the completion-marker SLA —
    the box is never even booted on that path."""
    holiday = daily_states["NotifyHolidaySkip"]
    assert holiday.get("Next") != "WriteCompletionMarker"
    assert "WriteCompletionMarker" not in json.dumps(holiday)
    assert "CheckDegradedOutcome" not in json.dumps(holiday)
