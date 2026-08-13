"""The sweep reports honestly: a run that could not measure is never a pass,
and it never advances the clean-run pointer (alpha-engine-config-I7249).

The obligation this file exists to discharge: **"could not measure" must be
distinguishable from "measured a pass" on every surface the sweep writes** —
the report, the metrics, the notification, and the console row. Three
detectors in this fleet have shipped unable to fail, and the specific way this
one could fail silently is by folding an unmeasured stage into green.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess

import pytest

from infrastructure import preflight_sweep as ps
from infrastructure.preflight_sweep_console import (
    ENVELOPE_ATTENTION,
    ENVELOPE_ERROR,
    ENVELOPE_OK,
    envelopes,
    load_cadence,
    rollup_envelope,
    stage_check_id,
    stage_envelope,
)
from infrastructure.preflight_sweep_stages import Stage

REPO = pathlib.Path(__file__).resolve().parent.parent
SF_PATH = REPO / "infrastructure" / "step_function.json"
MANIFEST_PATH = REPO / "infrastructure" / "preflight_sweep_manifest.json"
CADENCE_PATH = REPO / "infrastructure" / "preflight_sweep_cadence.json"


class FakeAws:
    def __init__(self):
        self.objects: dict[str, dict] = {}
        self.metrics: list[tuple[str, float]] = []
        self.notifications: list[tuple[str, str]] = []

    def put_json(self, key, payload):
        self.objects[key] = payload

    def put_text(self, key, body):
        self.objects[key] = body

    def put_metrics(self, metrics):
        self.metrics.extend(metrics)

    def publish(self, topic_arn, subject, message):
        self.notifications.append((subject, message))


def _completed(returncode: int, stderr: str = ""):
    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=["bash"], returncode=returncode, stdout="", stderr=stderr
        )

    return runner


def _stage(name="DataPhase1"):
    return Stage(
        name=name,
        classification="sweepable",
        box_dir="alpha-engine-data",
        repo="nousergon/nousergon-data",
        launcher="infrastructure/spot_data_phase1.sh",
        commands=["echo hi"],
    )


# ── Per-stage verdicts ───────────────────────────────────────────────────────


def test_a_clean_preflight_is_a_pass(tmp_path):
    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=_completed(0))
    assert result.verdict == ps.PASSED
    assert result.returncode == 0


def test_a_non_zero_preflight_is_a_failure_carrying_its_last_stderr_line(tmp_path):
    runner = _completed(1, stderr="warning: noise\nERROR: predictor.yaml not found\n")
    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=runner)
    assert result.verdict == ps.FAILED
    # No naked return codes: the operator must not have to open a log to learn
    # what broke.
    assert result.last_stderr_line == "ERROR: predictor.yaml not found"
    assert "rc=1" in result.reason


def test_an_oom_is_named_as_a_resource_kill_not_a_generic_failure(tmp_path):
    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=_completed(137))
    assert result.verdict == ps.FAILED
    assert "RESOURCE KILL" in result.reason and "OOM" in result.reason


def test_a_timeout_is_a_hard_fail_named_as_a_resource_kill(tmp_path):
    def runner(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="bash", timeout=60)

    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=runner)
    assert result.verdict == ps.FAILED
    assert "RESOURCE KILL" in result.reason
    assert "Not retried" in result.reason


def test_a_harness_fault_is_unmeasured_not_a_stage_failure(tmp_path):
    """The bug class: a detector that cannot tell its own harness fault from a
    finding reports the harness fault AS the defect, always in the alarming
    direction."""

    def runner(*_a, **_k):
        raise OSError("no such executable: bash")

    result = ps.run_stage(_stage(), str(tmp_path), 60, runner=runner)
    assert result.verdict == ps.UNMEASURED
    assert result.verdict != ps.FAILED


# ── Run-level honesty ────────────────────────────────────────────────────────


def test_a_derivation_failure_is_reported_unmeasured_and_never_ok(tmp_path):
    broken = tmp_path / "broken.json"
    broken.write_text("{not json")
    report = ps.sweep(
        definition_path=broken,
        manifest_path=MANIFEST_PATH,
        checkout_root=str(tmp_path),
        run_id="t",
    )
    assert report.measured is False
    assert report.outcome == ps.OUTCOME_FAILED
    assert report.unmeasured_reason and "NOT a clean run" in report.unmeasured_reason


def test_an_unmeasured_run_never_advances_the_clean_pointer(tmp_path):
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        outcome=ps.OUTCOME_FAILED,
        measured=False,
        unmeasured_reason="no box",
    )
    ps.emit(report, aws, "arn:sns")
    assert f"{ps.REPORT_PREFIX}/last_clean.json" not in aws.objects
    assert f"{ps.REPORT_PREFIX}/latest.json" in aws.objects


def test_a_clean_run_does_advance_the_clean_pointer():
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        outcome=ps.OUTCOME_OK,
        measured=True,
        stages_declared=1,
        stages_passed=1,
        results=[{"stage": "A", "verdict": ps.PASSED}],
    )
    ps.emit(report, aws, "arn:sns")
    assert f"{ps.REPORT_PREFIX}/last_clean.json" in aws.objects


def test_the_deadman_subject_is_emitted_even_on_an_unmeasured_run():
    """The alarm fires on the ABSENCE of this metric, so a run that reported
    'I could not measure' must still emit it — otherwise one incident produces
    two notifications, the sweep's own and the deadman's."""
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        outcome=ps.OUTCOME_FAILED,
        measured=False,
        unmeasured_reason="bootstrap failed",
    )
    ps.emit(report, aws, "arn:sns")
    assert ("PreflightSweepRunCompleted", 1) in aws.metrics


def test_one_notification_carries_every_failing_stage_by_name():
    """observability-policy §7.2a: one notification per group failure, never
    one per member — and the group notification carries the full member list."""
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID,
        run_id="t",
        started_at="now",
        outcome=ps.OUTCOME_FAILED,
        measured=True,
        stages_declared=3,
        stages_failed=2,
        results=[
            {"stage": "DataPhase1", "verdict": ps.FAILED, "reason": "rc=1",
             "last_stderr_line": "ERROR: boom"},
            {"stage": "Backtester", "verdict": ps.FAILED, "reason": "rc=137"},
            {"stage": "ParityReplay", "verdict": ps.PASSED},
        ],
    )
    subject, message = ps.render_notification(report)
    assert "DataPhase1" in message and "Backtester" in message
    assert "ERROR: boom" in message
    assert "2 failed" in subject


def test_the_unmeasured_notification_says_so_in_its_subject():
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID, run_id="t", started_at="now",
        measured=False, unmeasured_reason="the box never booted",
    )
    subject, message = ps.render_notification(report)
    assert "COULD NOT MEASURE" in subject
    assert "the box never booted" in message


def test_a_run_that_swept_nothing_is_not_reported_ok(tmp_path):
    """Zero stages passing and zero failing is not a clean sweep, it is a
    sweep that did not happen."""
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"States": {}}))
    # Derivation will fail on the absent ApplyShellRunDefaults, which is
    # itself the correct loud outcome.
    report = ps.sweep(empty, MANIFEST_PATH, str(tmp_path), "t")
    assert report.outcome != ps.OUTCOME_OK
    assert report.measured is False


def test_the_live_definition_derives_a_non_empty_stage_set(tmp_path):
    """Guards against the sweep silently grading an empty pipeline."""
    report = ps.sweep(SF_PATH, MANIFEST_PATH, "/nonexistent", "t")
    assert report.stages_declared > 0
    assert report.coverage_findings == []


# ── Console surface ──────────────────────────────────────────────────────────


def test_the_declared_cadence_is_read_never_defaulted():
    cadence = load_cadence(CADENCE_PATH)
    assert cadence["sweep_cadence"] in cadence["allowed_values"]
    assert cadence["cadence_minutes"] > 0


def test_a_cadence_manifest_that_cannot_be_read_raises(tmp_path):
    missing = tmp_path / "nope.json"
    with pytest.raises(Exception):
        load_cadence(missing)


@pytest.fixture
def cadence():
    return load_cadence(CADENCE_PATH)


def test_a_passed_stage_renders_ok_with_its_own_age(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": ps.PASSED}, "run-1", "2026-08-13T08:00:00+00:00", cadence
    )
    assert body["status"] == ENVELOPE_OK
    # ran_at + cadence_minutes are what let the console re-derive staleness
    # itself rather than trusting the status written here.
    assert body["ran_at"] == "2026-08-13T08:00:00+00:00"
    assert body["cadence_minutes"] == cadence["cadence_minutes"]


def test_an_unmeasured_stage_never_renders_green(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": ps.UNMEASURED}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ATTENTION
    assert body["status"] != ENVELOPE_OK


def test_an_unsweepable_stage_is_an_error_because_it_is_unmonitored(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": "unsweepable"}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ERROR


def test_a_verdict_with_no_declared_mapping_is_not_silently_green(cadence):
    body = stage_envelope(
        {"stage": "DataPhase1", "verdict": "something_new"}, "r", "t", cadence
    )
    assert body["status"] == ENVELOPE_ATTENTION
    assert "no declared envelope mapping" in body["verdict"]


def test_every_row_says_it_covers_preconditions_not_output_correctness(cadence):
    body = stage_envelope({"stage": "X", "verdict": ps.PASSED}, "r", "t", cadence)
    assert "PRECONDITIONS only" in body["summary"]
    assert "output is correct" in body["summary"]


def test_a_stage_the_sweep_produced_no_verdict_for_is_counted_unobserved(cadence):
    """UNOBSERVED must be visible and inside the denominator. A stage nobody
    checks is the one that breaks."""
    report = {
        "run_id": "r", "measured": True, "outcome": "ok",
        "stages_declared": 19, "stages_no_dry_path": 3,
        "stages_passed": 10, "results": [{"stage": f"S{i}", "verdict": ps.PASSED} for i in range(10)],
    }
    rollup = rollup_envelope(report, cadence, "t")
    assert rollup["stages_expected"] == 16
    assert rollup["stages_reported"] == 10
    assert rollup["stages_unobserved"] == 6
    # And it must not render green while six stages are unaccounted for.
    assert rollup["status"] == ENVELOPE_ERROR


def test_the_rollup_publishes_zero_unobserved_rather_than_omitting_it(cadence):
    report = {
        "run_id": "r", "measured": True, "outcome": "ok",
        "stages_declared": 3, "stages_no_dry_path": 0, "stages_passed": 3,
        "results": [{"stage": f"S{i}", "verdict": ps.PASSED} for i in range(3)],
    }
    rollup = rollup_envelope(report, cadence, "t")
    assert rollup["stages_unobserved"] == 0
    assert "stages_unobserved" in rollup
    assert rollup["status"] == ENVELOPE_OK


def test_an_unmeasured_run_rollup_is_an_error_row(cadence):
    rollup = rollup_envelope(
        {"run_id": "r", "measured": False, "unmeasured_reason": "no box"}, cadence, "t"
    )
    assert rollup["status"] == ENVELOPE_ERROR
    assert rollup["unmeasured_reason"] == "no box"


def test_every_run_publishes_a_row_for_every_reported_stage_plus_the_rollup(cadence):
    report = {
        "run_id": "r", "measured": True, "outcome": "failed",
        "stages_declared": 2, "stages_no_dry_path": 0, "stages_failed": 1,
        "results": [
            {"stage": "A", "verdict": ps.PASSED},
            {"stage": "B", "verdict": ps.FAILED, "reason": "rc=1"},
        ],
    }
    published = envelopes(report, cadence)
    keys = {k for k, _ in published}
    assert f"ops/checks/{stage_check_id('A')}/latest.json" in keys
    assert f"ops/checks/{stage_check_id('B')}/latest.json" in keys
    assert "ops/checks/ae-preflight-sweep/latest.json" in keys


def test_a_parallel_branch_stage_gets_a_stable_unique_console_id():
    a = stage_check_id("ResearchPredictorParallel.PredictorTraining")
    b = stage_check_id("ParityParallel.PredictorTraining")
    assert a != b
    assert "." not in a


def test_console_rows_are_published_even_when_the_run_failed(cadence):
    """A row that stops being republished is what makes the console render
    STALE. Skipping the write on a bad run turns a loud failure into a quiet
    ageing green."""
    aws = FakeAws()
    report = ps.SweepReport(
        component_id=ps.COMPONENT_ID, run_id="r", started_at="now",
        outcome=ps.OUTCOME_FAILED, measured=True, stages_declared=1, stages_failed=1,
        results=[{"stage": "A", "verdict": ps.FAILED, "reason": "rc=1"}],
    )
    ps.emit(report, aws, "arn:sns")
    assert f"ops/checks/{stage_check_id('A')}/latest.json" in aws.objects
    assert "ops/checks/ae-preflight-sweep/latest.json" in aws.objects
