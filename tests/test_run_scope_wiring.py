"""RunScope's wiring into the weekly SF (alpha-engine-config-I7620).

The Lambda's own derivation is tested in
``infrastructure/lambdas/weekly-run-scope/test_run_scope.py`` against real
captured executions. What is pinned HERE is the half that cannot be tested from
inside the Lambda: that the state exists, that it sits where every work stage is
already behind it, that it is handed the three context values it cannot obtain
for itself, and that it can never fail the run.
"""
from __future__ import annotations

import json
import pathlib

import pytest

_SF = pathlib.Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_SF.read_text())["States"]


def test_the_state_exists_and_targets_the_run_scope_lambda(states):
    scope = states["RunScope"]
    assert scope["Type"] == "Task"
    assert (
        scope["Parameters"]["FunctionName"] == "alpha-engine-weekly-run-scope"
    )


def test_it_receives_the_context_values_it_cannot_derive_for_itself(states):
    """Execution id, state-machine id, and the ORIGINAL execution input.

    The first two are how it fetches its own history and definition — the two
    authorities the whole design rests on. The third is the run's `skip_*`
    flags, used only to EXPLAIN a NOT_REACHED row; a disposition is always
    decided by the execution record, never by what the input asked for.
    """
    payload = states["RunScope"]["Parameters"]["Payload"]
    assert payload["execution_arn.$"] == "$$.Execution.Id"
    assert payload["state_machine_arn.$"] == "$$.StateMachine.Id"
    assert payload["execution_input.$"] == "$$.Execution.Input"


def test_it_honours_the_shell_run_dry_contract(states):
    """`research_dry` is this pipeline's canonical dry signal.

    Every advisory producer on the tail runs dry rather than skipping, so the
    Friday-PM shell run exercises the real IAM grants and the real derivation
    without writing an artifact for a day that had no run.
    """
    assert states["RunScope"]["Parameters"]["Payload"]["dry_run.$"] == "$.research_dry"


def test_it_runs_after_every_work_stage_and_before_the_report_card(states):
    """Placement is the whole correctness argument for reading history.

    Both routes into the post-eval tail must pass through it, or a run that
    fail-opened its substrate health check would produce a card with no scope
    beside it.
    """
    assert states["RunScope"]["Next"] == "CheckSkipReportCard"
    predecessors = {
        name for name, body in states.items()
        if name != "RunScope" and '"RunScope"' in json.dumps(
            {k: v for k, v in body.items() if k != "Comment"}
        )
    }
    assert predecessors == {
        "CheckSubstrateHealthCheckStatus",
        "SetSubstrateHealthCheckDegradedSummary",
        # alpha-engine-config-I8167: CheckSkipSaturdayHealthCheck's bypass
        # route (skip_saturday_health_check=true) lands here directly,
        # bypassing SaturdayHealthCheck/WeeklySubstrateHealthCheck entirely
        # — a third route into the post-eval tail, alongside the two above.
        "CheckSkipSaturdayHealthCheck",
    }


def test_nothing_else_still_routes_straight_to_the_report_card_gate(states):
    """The insertion must have moved BOTH edges, not one.

    A surviving edge would let one path reach the card with no scope block —
    and an absent scope artifact is indistinguishable from a Lambda that never
    got deployed.
    """
    stragglers = {
        name for name, body in states.items()
        if name != "RunScope"
        and '"CheckSkipReportCard"' in json.dumps(
            {k: v for k, v in body.items() if k != "Comment"}
        )
    }
    assert stragglers == set()


def test_it_can_never_fail_the_run(states):
    """An advisory scope artifact must not kill a run that produced real
    trading artifacts. Its Catch rejoins the tail rather than routing to any
    failure handler."""
    catches = states["RunScope"]["Catch"]
    assert [c["ErrorEquals"] for c in catches] == [["States.ALL"]]
    assert all(c["Next"] == "CheckSkipReportCard" for c in catches)
    assert all(c["ResultPath"] == "$.run_scope_error" for c in catches)


def test_its_result_does_not_overwrite_the_execution_state(states):
    assert states["RunScope"]["ResultPath"] == "$.run_scope_result"


def test_the_state_timeout_binds_before_the_lambda_timeout(states):
    """Two API calls and a pure derivation — and the SF ceiling must be the one
    that binds.

    45s here against the Lambda's own 60s. A state whose TimeoutSeconds sits
    above the function's cannot be reasoned about from the definition, and a
    generous ceiling would let a hung advisory state hold the tail of the
    weekly run. Pinned fleet-wide by
    tests/test_sf_lambda_timeout_ordering.py.
    """
    assert states["RunScope"]["TimeoutSeconds"] == 45
