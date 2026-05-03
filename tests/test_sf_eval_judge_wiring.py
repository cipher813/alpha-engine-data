"""Pins the LLM-as-judge wiring in the Saturday Step Functions JSON.

Catches regressions like: someone re-routes CheckBacktesterStatus.Success
back to SaturdayHealthCheck and accidentally drops the eval state, or
flips the Default branch of the cadence Choice and ships every Saturday
on the (more expensive) monthly Sonnet sweep.

The corresponding alpha-engine-research Lambda
(``alpha-engine-research-eval-judge:live``) is in PR #91; this test only
asserts the SF wiring, not the handler shape.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


# ── State presence ────────────────────────────────────────────────────────


class TestStatesPresent:
    def test_all_eval_judge_states_exist(self, states):
        for name in (
            "CheckSkipEvalJudge",
            "ComputeEvalCadence",
            "CheckMonthlyCadence",
            "EvalJudgeFirstSaturday",
            "EvalJudgeWeekly",
        ):
            assert name in states, f"missing SF state: {name}"


# ── Backtester success → eval-judge skip-gate ─────────────────────────────


class TestBacktesterTransition:
    def test_success_routes_to_eval_skip_gate(self, states):
        bt = states["CheckBacktesterStatus"]
        success_choice = next(
            c for c in bt["Choices"] if c.get("StringEquals") == "Success"
        )
        assert success_choice["Next"] == "CheckSkipEvalJudge"


# ── Skip gate ─────────────────────────────────────────────────────────────


class TestSkipEvalJudge:
    def test_skip_flag_bypasses_to_health_check(self, states):
        skip = states["CheckSkipEvalJudge"]
        choice = skip["Choices"][0]
        # Both presence + boolean equality must be checked (matches
        # other skip gates like CheckSkipResearch).
        and_clauses = choice["And"]
        assert any(
            c.get("Variable") == "$.skip_eval_judge"
            and c.get("BooleanEquals") is True
            for c in and_clauses
        )
        assert choice["Next"] == "SaturdayHealthCheck"

    def test_default_runs_eval(self, states):
        assert states["CheckSkipEvalJudge"]["Default"] == "ComputeEvalCadence"


# ── Cadence computation ───────────────────────────────────────────────────


class TestComputeEvalCadence:
    def test_extracts_day_of_month_and_eval_date(self, states):
        params = states["ComputeEvalCadence"]["Parameters"]
        # Both intrinsic-function expressions must be present so the
        # downstream Choice + Payload can reference them.
        assert "day_of_month.$" in params
        assert "eval_date.$" in params
        # Reference shape — protect against accidental rename of either
        # JSONPath that would leave the Choice state matching nothing.
        assert "$$.Execution.StartTime" in params["day_of_month.$"]
        assert "$$.Execution.StartTime" in params["eval_date.$"]

    def test_writes_to_eval_cadence_path(self, states):
        assert states["ComputeEvalCadence"]["ResultPath"] == "$.eval_cadence"

    def test_routes_to_cadence_choice(self, states):
        assert states["ComputeEvalCadence"]["Next"] == "CheckMonthlyCadence"


# ── Monthly cadence Choice ────────────────────────────────────────────────


class TestCheckMonthlyCadence:
    def test_default_is_weekly(self, states):
        # Default = the COMMON path (every other Saturday). Must NOT
        # be EvalJudgeFirstSaturday — that would ship every weekly run
        # on the expensive monthly Sonnet sweep.
        assert states["CheckMonthlyCadence"]["Default"] == "EvalJudgeWeekly"

    def test_first_saturday_branch_uses_lex_compare_under_08(self, states):
        choice = states["CheckMonthlyCadence"]["Choices"][0]
        assert choice["Variable"] == "$.eval_cadence.day_of_month"
        assert choice["StringLessThan"] == "08"
        assert choice["Next"] == "EvalJudgeFirstSaturday"


# ── Lambda invocation contract ────────────────────────────────────────────


class TestEvalJudgeLambdaContract:
    @pytest.mark.parametrize(
        "state_name,expected_force_sonnet",
        [
            ("EvalJudgeFirstSaturday", True),
            ("EvalJudgeWeekly", False),
        ],
    )
    def test_payload_carries_correct_force_sonnet_flag(
        self, states, state_name, expected_force_sonnet,
    ):
        payload = states[state_name]["Parameters"]["Payload"]
        assert payload["force_sonnet_pass"] is expected_force_sonnet

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_payload_passes_eval_date(self, states, state_name):
        payload = states[state_name]["Parameters"]["Payload"]
        # SF passes the SF-execution-start-date so the Lambda evaluates
        # the same partition the captures landed in (avoids UTC-rollover
        # edge cases where the Lambda starts on day X+1).
        assert payload["date.$"] == "$.eval_cadence.eval_date"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_invokes_live_alias(self, states, state_name):
        params = states[state_name]["Parameters"]
        assert params["FunctionName"] == "alpha-engine-research-eval-judge:live"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_timeout_matches_lambda_max(self, states, state_name):
        # Lambda's hard timeout is 900s (set in alpha-engine-research
        # infrastructure/deploy.sh). SF state TimeoutSeconds must not be
        # less — otherwise SF would kill an in-progress eval prematurely.
        assert states[state_name]["TimeoutSeconds"] == 900


# ── Non-blocking failure semantics ────────────────────────────────────────


class TestEvalJudgeNonBlocking:
    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_success_continues_to_health_check(self, states, state_name):
        assert states[state_name]["Next"] == "SaturdayHealthCheck"

    @pytest.mark.parametrize(
        "state_name",
        ["EvalJudgeFirstSaturday", "EvalJudgeWeekly"],
    )
    def test_catch_routes_to_health_check_not_failure(self, states, state_name):
        # Eval is observability per ROADMAP §1635 — failures must NOT
        # halt the pipeline. Routing to HandleFailure here would be a
        # regression that shoots the whole Saturday run on a 5xx from
        # Anthropic on the eval Lambda specifically.
        catch = states[state_name]["Catch"][0]
        assert catch["ErrorEquals"] == ["States.ALL"]
        assert catch["Next"] == "SaturdayHealthCheck"
        assert catch["Next"] != "HandleFailure"
