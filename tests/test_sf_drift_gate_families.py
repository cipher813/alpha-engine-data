"""alpha-engine-config#6615 (sf-pipeline-policy §3, clause SFP-3) — pins
DeployDriftGate's halt-vs-degrade split in the weekday/preopen SF.

Before this change, `DeployDriftGate` branched on a single unioned
`has_drift` field (`sf_drift or cf_drift`, `inference/deploy_drift.py`),
so a CloudFormation stack sitting in a terminal state (cf_drift=true) with
the SF definition itself perfectly current (sf_drift=false) still routed to
HandleFailure and halted the whole pipeline — the 2026-08-05/06/07 incident
(3 lost trading days, `alpha-engine-orchestration` stuck
`UPDATE_ROLLBACK_FAILED`, `sf_drift: false` throughout).

Per the halt-vs-degrade framework (`nous-ergon-ops/policies/
sf-pipeline-policy.md` §3, Brian's 2026-08-09 ruling): a failure that
undermines trading correctness itself (a drifted SF definition — sf_drift)
still halts; orchestration-metadata failures (CloudFormation stack state —
cf_drift) degrade loudly instead — trade on the last verified frozen SHA
with a degraded flag + page.

These tests pin:
  1. sf_drift=true halts (HandleFailure), independent of cf_drift.
  2. sf_drift missing halts (fail-closed on unknown, config-I2767 convention
     preserved post-split).
  3. cf_drift missing halts (fail-closed on unknown — the gate cannot
     classify halt-vs-degrade without it).
  4. cf_drift=true (with sf_drift false/absent-from-this-branch) routes to
     the new degrade path, NOT HandleFailure.
  5. The degrade path (SetDeployDriftDegradedFlag -> PublishDeployDriftDegraded)
     sets $.degraded_summary in the same shape PR1251/config#6692 established
     for SetDaemonDegradedFlag/SetDataSpotDegradedFlag, and reaches
     TradingDayGate — the same next state a clean drift check reaches — so
     the run is NOT aborted, only its eventual terminal is marked DEGRADED
     via the pre-existing CheckDegradedOutcome chokepoint.
  6. Clean payload (both false) still reaches TradingDayGate via Default.
  7. The notify state fails open (its own Catch also leads to TradingDayGate)
     so an SNS outage cannot turn a degrade into a halt.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"


@pytest.fixture(scope="module")
def sf():
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf):
    return sf["States"]


@pytest.fixture(scope="module")
def gate(states):
    return states["DeployDriftGate"]


def _guard_pairs(choice):
    """Return (guard, comparison) for an IsPresent-guarded And clause."""
    guard, comparison = choice["And"]
    return guard, comparison


def test_gate_is_a_choice_with_four_branches_and_a_default(gate):
    assert gate["Type"] == "Choice"
    assert len(gate["Choices"]) == 4
    assert gate["Default"] == "TradingDayGate"


def test_sf_drift_true_halts_first(gate):
    """sf_drift=true must be evaluated FIRST and unconditionally halt —
    independent of whatever cf_drift says. A drifted SF definition is
    trading-correctness-critical per sf-pipeline-policy §3."""
    c = gate["Choices"][0]
    guard, comparison = _guard_pairs(c)
    assert guard == {
        "Variable": "$.drift_result.Payload.sf_drift",
        "IsPresent": True,
    }
    assert comparison["Variable"] == "$.drift_result.Payload.sf_drift"
    assert comparison["BooleanEquals"] is True
    assert c["Next"] == "HandleFailure"


def test_sf_drift_missing_halts(gate):
    """config-I2767, preserved post-split: an absent sf_drift means the
    gate cannot verify SF-definition freshness — fail CLOSED."""
    c = gate["Choices"][1]
    assert c["Not"] == {
        "Variable": "$.drift_result.Payload.sf_drift",
        "IsPresent": True,
    }
    assert c["Next"] == "HandleFailure"


def test_cf_drift_missing_halts(gate):
    """Without cf_drift the gate cannot classify a confirmed drift signal
    as halt-vs-degrade — fail CLOSED rather than silently defaulting to a
    pass or a degrade."""
    c = gate["Choices"][2]
    assert c["Not"] == {
        "Variable": "$.drift_result.Payload.cf_drift",
        "IsPresent": True,
    }
    assert c["Next"] == "HandleFailure"


def test_cf_drift_true_alone_degrades_not_halts(gate):
    """The core fix: cf_drift=true (having survived the sf_drift=true and
    both-missing branches above, i.e. sf_drift is present and false) must
    route to the degrade path, never to HandleFailure."""
    c = gate["Choices"][3]
    guard, comparison = _guard_pairs(c)
    assert guard == {
        "Variable": "$.drift_result.Payload.cf_drift",
        "IsPresent": True,
    }
    assert comparison["Variable"] == "$.drift_result.Payload.cf_drift"
    assert comparison["BooleanEquals"] is True
    assert c["Next"] == "SetDeployDriftDegradedFlag"
    assert c["Next"] != "HandleFailure"


def test_no_branch_routes_cf_drift_true_to_handle_failure(gate):
    """Belt-and-suspenders on the whole Choice: no branch may route a
    confirmed cf_drift==true (BooleanEquals true) to HandleFailure — the
    only cf_drift-conditioned HandleFailure branch permitted is the
    missing-field ('Not' / IsPresent-false) guard."""
    for c in gate["Choices"]:
        if c.get("Next") != "HandleFailure":
            continue
        for clause in c.get("And", []):
            if clause.get("Variable") == "$.drift_result.Payload.cf_drift":
                assert clause.get("BooleanEquals") is not True, (
                    "a cf_drift==true And-clause must not route to "
                    "HandleFailure"
                )
        not_clause = c.get("Not")
        if not_clause and not_clause.get("Variable") == "$.drift_result.Payload.cf_drift":
            assert not_clause.get("IsPresent") is True


def test_clean_payload_reaches_trading_day_gate_via_default(gate):
    assert gate["Default"] == "TradingDayGate"


def test_degraded_flag_state_shape(states):
    st = states["SetDeployDriftDegradedFlag"]
    assert st["Type"] == "Pass"
    assert st["ResultPath"] == "$.degraded_summary"
    assert st["Parameters"]["degraded"] is True
    assert st["Parameters"]["reason"] == "deploy_drift_metadata"
    assert st["Parameters"]["run_date.$"] == "$.run_date"
    # Mirrors SetDaemonDegradedFlag/SetDataSpotDegradedFlag's stage_error
    # convention — carries the probe's own diagnostic payload (cf_drift_reason,
    # cf_stack_status, etc.) so the eventual DEGRADED marker/notify names the
    # actual condition.
    assert st["Parameters"]["stage_error.$"] == "$.drift_result.Payload"
    assert st["Next"] == "PublishDeployDriftDegraded"


def test_degrade_path_reaches_trading_day_gate_not_a_new_terminal(states):
    """The run must NOT be aborted mid-pipeline — the degrade path reaches
    the exact same TradingDayGate a clean drift check reaches. Only the
    eventual terminal (via the pre-existing CheckDegradedOutcome, reached
    much later through RunDaemon/CheckSkipRunDaemon) is affected."""
    notify = states["PublishDeployDriftDegraded"]
    assert notify["Type"] == "Task"
    assert notify["Resource"] == "arn:aws:states:::sns:publish"
    assert notify["Next"] == "TradingDayGate"


def test_degrade_notify_fails_open_via_its_own_catch(states):
    """Mirrors PublishDataSpotFailureImmediate (config#1767): an SNS outage
    on the degrade-notify path must not itself halt the run."""
    notify = states["PublishDeployDriftDegraded"]
    (catch,) = notify["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "TradingDayGate"


def test_degrade_notify_has_timeout_and_result_path(states):
    notify = states["PublishDeployDriftDegraded"]
    assert notify["TimeoutSeconds"] == 60
    assert notify["ResultPath"] == "$.deploy_drift_degraded_notify"


def test_resultpath_threading_survives_to_check_degraded_outcome(sf, states):
    """Structural proof that $.degraded_summary, once set by
    SetDeployDriftDegradedFlag (immediately after DeployDriftGate, at the
    very top of the pipeline), is never wholesale-clobbered before it can
    be read by CheckDegradedOutcome. Walks the happy-path chain from
    TradingDayGate through to RunDaemon/CheckDegradedOutcome and asserts
    every Task/Pass state on it either has no ResultPath key that would
    default to '$' (Choice/Wait/Succeed/Fail types don't produce a Result
    the way Task/Pass do — they pass $ through untouched) or has an
    explicit ResultPath scoped to its own sub-key, never bare '$'."""
    happy_path_task_or_pass_states = [
        "TradingDayGate",
        "StartExecutorEC2",
        "InitSSMPollCounter",
        "DescribeInstanceInfo",
        "IncrementSSMPoll",
        "CodeFreshnessGate",
        "InitCodeFreshnessPoll",
        "WaitForCodeFreshness",
        "PredictorInference",
        "CheckPredictorCoverage",
        "RunMorningPlanner",
        "InitMorningPlannerPoll",
        "WaitForMorningPlanner",
        "RunDaemon",
    ]
    for name in happy_path_task_or_pass_states:
        st = states[name]
        assert st["Type"] in ("Task", "Pass"), name
        result_path = st.get("ResultPath")
        assert result_path not in (None, "$"), (
            f"{name} has no scoped ResultPath (defaults to '$', which would "
            "wholesale-replace the state input and destroy any upstream "
            "$.degraded_summary set by SetDeployDriftDegradedFlag)"
        )
        assert result_path != "$.degraded_summary", (
            f"{name} unexpectedly writes $.degraded_summary directly on the "
            "happy path"
        )
    # The only states permitted to overwrite $.degraded_summary itself are
    # the OTHER degraded-flag setters (last-write-wins is the documented,
    # intentional convention — see SetDaemonDegradedFlag's own Comment).
    # alpha-engine-config#6722 added SetMutexAcquireDegradedFlag (mutex
    # acquire infra-error fail-open) and SetScannerDegradedFlag (weekday
    # Scanner fail-open) as two more Option-A-shaped setters.
    # I7811 (Brian ruling 2026-08-20) removed SetScannerDegradedFlag with the
    # weekday Scanner — the scanner forms its cuts WEEKLY now, so there is no
    # weekday scanner failure left to fail open.
    overwriters = {
        name
        for name, st in states.items()
        if st.get("ResultPath") == "$.degraded_summary"
    }
    assert overwriters == {
        "SetDeployDriftDegradedFlag",
        "SetDaemonDegradedFlag",
        "SetDataSpotDegradedFlag",
        "SetMutexAcquireDegradedFlag",
    }


def test_check_degraded_outcome_is_still_the_single_terminal_decision_point(states):
    """CheckDegradedOutcome remains the ONE place deciding the terminal —
    this PR adds a new way to ARRIVE at a degraded $.degraded_summary, not
    a new place that reads it or a new terminal."""
    choice = states["CheckDegradedOutcome"]
    assert choice["Type"] == "Choice"
    (c,) = choice["Choices"]
    variables = {x["Variable"] for x in c["And"]}
    assert variables == {"$.degraded_summary.degraded"}
    assert c["Next"] == "WriteCompletionMarkerDegraded"
    assert choice["Default"] == "WriteCompletionMarker"
