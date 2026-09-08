"""The coverage sweep names the execution it is running inside.

``alpha-engine-config-I10170``. Measured on the live 2026-09-04 weekly cycle:
``_stage_coverage/_sweep/ne-weekly-freshness-pipeline/2026-09-04.json`` was
written at ``2026-09-05T21:39:18Z`` carrying ``cycle.verdict: in_flight``,
``cycle.reason: still_running`` AND ``should_alert: true`` with 13 stages
called ``absent``. ``DescribeExecution`` on ``watch-rerun-2026-09-04-4`` — the
execution the same artifact reported RUNNING — returns ``stopDate
2026-09-05T21:39:19Z``, ``status SUCCEEDED``: one second later, on the Succeed
state two hops past ``WeeklyCoverageSweep``.

The sweep IS a state of the pipeline it grades, so it can only ever observe
its own execution mid-flight. These tests pin the wiring that lets
``nousergon_lib`` know which execution is the observer, and the fourth outcome
that wiring makes reachable.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
DEFINITION = REPO / "infrastructure" / "step_function.json"
HANDLER = REPO / "infrastructure" / "lambdas" / "weekly-coverage-sweep" / "index.py"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(DEFINITION.read_text())["States"]


@pytest.fixture(scope="module")
def handler_module():
    spec = importlib.util.spec_from_file_location("weekly_coverage_sweep_index", HANDLER)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_the_sweep_state_passes_its_own_execution_id(states: dict) -> None:
    """``$$.Execution.Id``, not the state machine ARN.

    ``state_machine_arn.$`` is ``$$.StateMachine.Id`` and identifies the
    pipeline; the observer question is *which execution am I inside*, which
    only the execution context object answers.
    """
    payload = states["WeeklyCoverageSweep"]["Parameters"]["Payload"]
    assert payload["observer_execution_arn.$"] == "$$.Execution.Id"
    assert payload["state_machine_arn.$"] == "$$.StateMachine.Id"


def test_every_handler_outcome_literal_has_a_choice_branch(states: dict, handler_module) -> None:
    """A fifth outcome added without a branch lands on the Default, which is
    ``unavailable`` — honest, but it would report a sweep that RAN as one that
    did not. The literals and the branches are two declarations of one
    contract, so they are compared rather than trusted."""
    literals = {
        value
        for name, value in vars(handler_module).items()
        if name.startswith("OUTCOME_") and isinstance(value, str)
    }
    assert literals == {"clean", "findings", "deferred", "unavailable"}

    choice = states["CheckWeeklyCoverageSweepOutcome"]
    branched = {c["StringEquals"] for c in choice["Choices"]}
    # ``unavailable`` is deliberately the Default rather than a branch: an
    # outcome the Choice does not recognise is an unknown state of the
    # coverage surface, and unknown is never rendered green.
    assert branched == literals - {"unavailable"}
    assert choice["Default"] == "WeeklyCoverageSweepUnavailable"


def test_the_deferred_branch_reaches_its_own_terminal(states: dict) -> None:
    """Not shared with ``clean``. A terminal shared with the clean one makes
    "the sweep ran and could not establish coverage" unreadable in the
    execution history, which is the first place an operator looks."""
    choice = states["CheckWeeklyCoverageSweepOutcome"]
    target = next(c["Next"] for c in choice["Choices"] if c["StringEquals"] == "deferred")
    assert target == "WeeklyCoverageSweepDeferred"

    deferred = states[target]
    assert deferred["Resource"] == "arn:aws:states:::sns:publish"
    assert deferred["Next"] == "WeeklyCoverageSweepUnestablished"
    assert states["WeeklyCoverageSweepUnestablished"]["Type"] == "Succeed"
    # Observe-only tail: everything from WriteCompletionMarker down is
    # downstream of the pipeline's real success terminal and must never turn a
    # completed weekly run into a failure (sf-pipeline-policy 2.1).
    for name in ("WeeklyCoverageSweepDeferred", "WeeklyCoverageSweepUnestablished"):
        assert states[name]["Type"] in ("Task", "Succeed")


def test_the_deferred_notification_says_withheld_not_zero(states: dict) -> None:
    """The whole point of the outcome. A message that reads like a clean
    result would re-create the defect in prose."""
    message = states["WeeklyCoverageSweepDeferred"]["Parameters"]["Message.$"]
    assert "WITHHELD" in message
    assert "not zero" in message
    assert "_stage_coverage/_sweep" in message
    assert "alpha-engine-weekly-coverage-sweep" in message


def test_the_handler_declares_its_own_stage_name(handler_module) -> None:
    """``WeeklyCoverageSweep`` was 1 of the 13 absences on 2026-09-04. The
    handler must tell the library which stage it IS, or the library cannot
    know that stage's absence is guaranteed by construction."""
    assert handler_module.SWEEP_STAGE == "WeeklyCoverageSweep"


def test_the_handler_passes_both_observer_fields_to_the_reader() -> None:
    """Signature-level, because the Lambda's real import is not available in
    this test environment: the call must carry BOTH halves of the observer —
    the execution (for the cycle verdict) and the stage (for its own row)."""
    source = HANDLER.read_text()
    call = re.search(r"read_coverage_sweep\((.*?)\n        \)", source, re.S)
    assert call, "read_coverage_sweep call not found"
    body = call.group(1)
    assert "observer_execution_arn=observer_execution_arn or None" in body
    assert "observer_stage=SWEEP_STAGE" in body


def test_deferred_outranks_findings(handler_module) -> None:
    """When the cycle cannot support an absence claim, the honest headline is
    that coverage is not established — not that N stages are absent, which is
    the assertion the sweep just declined to make."""

    class _Sweep:
        deferred = True
        should_alert = True

    assert handler_module._outcome_for(_Sweep()) == "deferred"

    class _Findings:
        deferred = False
        should_alert = True

    assert handler_module._outcome_for(_Findings()) == "findings"

    class _Clean:
        deferred = False
        should_alert = False

    assert handler_module._outcome_for(_Clean()) == "clean"
