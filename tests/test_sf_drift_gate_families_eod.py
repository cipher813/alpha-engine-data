"""alpha-engine-config-I8102 — pins the POSTCLOSE DeployDriftGate.

Sibling of ``test_sf_drift_gate_families.py``, which pins the same gate in the
preopen definition. Both files exist because the two pipelines were given the
same branch FAMILIES for different reasons, and the reasons are what a future
edit is likely to get wrong:

* **Preopen** has a market-open deadline. A false halt costs an unmanaged
  real-money session, which is why ``alpha-engine-config-I7799`` moved its
  ``sf_drift`` off the SHA stamp and onto a comparison of the deployed
  definition body.
* **Postclose** has no deadline — a lost run costs a mechanical rerun
  (``weekday_sf_rerun.py``, sf-pipeline-policy §2.5). What it has instead is
  the day's book: the position snapshot, ``EODReconcile``, the ``eod_pnl`` row
  and the NAV every downstream consumer reads. An orchestration nobody can
  vouch for producing those numbers is §2.3a rule 3's prohibited case — a run
  presenting results without saying whether the correctness check ran.

So ``sf_drift`` is a HALT family here too, and this file asserts it is routed
as one.

**It is not halting yet, and that is also asserted.** sf-pipeline-policy §7a:
a check newly added to a scheduled pipeline path whose verdict can fail a run
observes before it enforces. §1.3 is why the observe stage is not ceremony
here — every trading day must terminate with a postclose completion marker and
an ``eod_pnl`` row, *including* days nothing traded, so a false halt does not
merely lose a run, it leaves a hole in NAV continuity. §7a.2's precedent is an
EOD gap of precisely this shape.

The tests below therefore pin BOTH halves:

  1-3. The three HALT-family branches (``sf_drift`` true, ``sf_drift`` absent,
       ``cf_drift`` absent) exist, in that order, ahead of the degrade family.
  4-5. ``cf_drift=true`` and ``deploy_stamp_stale=true`` route to the
       permanently non-halting degrade path.
  6.   A clean payload reaches ``StartTradingInstance`` via ``Default`` — the
       gate costs a clean day nothing.
  7.   During observe mode the halt family routes to a Pass that still marks
       the run DEGRADED, so the verdict is loud (§7a obligation 3) rather than
       suppressed.
  8.   The probe's own failure (Catch) is an unmeasured verdict, carried on its
       own path because ``$.drift_result.Payload`` does not exist there.
  9.   The single notify state cannot raise ``States.Format`` on any path that
       reaches it, and fails open to ``StartTradingInstance``.
 10.   The promotion criterion is written down, in the states it governs
       (§7a obligation 2 — a guard parked in observe mode indefinitely is the
       failure the rule exists to prevent).
 11.   The gate is sited before the pipeline spends anything, and every route
       into ``StartTradingInstance`` from the mutex now passes through it —
       the leak this test exists to catch is a fourth caller added later that
       jumps the gate.
 12.   ``sf_name`` is explicit. Without it the probe answers about the PREOPEN
       state machine, and this gate would read a confident verdict about a
       pipeline that is not itself.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_eod.json"

_HALT_FAMILY_OBSERVE_TARGET = "SetDeployDriftObserveWouldHaltFlag"
_DEGRADE_TARGET = "SetDeployDriftDegradedFlag"
_CLEAN_NEXT = "StartTradingInstance"


@pytest.fixture(scope="module")
def states():
    return json.loads(_SF_PATH.read_text())["States"]


@pytest.fixture(scope="module")
def gate(states):
    return states["DeployDriftGate"]


def _and_pair(choice):
    guard, comparison = choice["And"]
    return guard, comparison


def test_gate_has_five_branches_and_a_default(gate):
    assert gate["Type"] == "Choice"
    assert len(gate["Choices"]) == 5
    assert gate["Default"] == _CLEAN_NEXT


def test_sf_drift_true_is_the_first_branch_and_is_halt_family(gate):
    guard, comparison = _and_pair(gate["Choices"][0])
    assert guard == {
        "Variable": "$.drift_result.Payload.sf_drift", "IsPresent": True,
    }
    assert comparison["Variable"] == "$.drift_result.Payload.sf_drift"
    assert comparison["BooleanEquals"] is True
    assert gate["Choices"][0]["Next"] == _HALT_FAMILY_OBSERVE_TARGET


def test_sf_drift_absent_is_halt_family(gate):
    """An unmeasured verdict is never a pass (§2.3a rule 2)."""
    c = gate["Choices"][1]
    assert c["Not"] == {
        "Variable": "$.drift_result.Payload.sf_drift", "IsPresent": True,
    }
    assert c["Next"] == _HALT_FAMILY_OBSERVE_TARGET


def test_cf_drift_absent_is_halt_family(gate):
    c = gate["Choices"][2]
    assert c["Not"] == {
        "Variable": "$.drift_result.Payload.cf_drift", "IsPresent": True,
    }
    assert c["Next"] == _HALT_FAMILY_OBSERVE_TARGET


def test_cf_drift_true_degrades_permanently(gate):
    guard, comparison = _and_pair(gate["Choices"][3])
    assert guard["Variable"] == "$.drift_result.Payload.cf_drift"
    assert comparison["BooleanEquals"] is True
    assert gate["Choices"][3]["Next"] == _DEGRADE_TARGET


def test_deploy_stamp_stale_degrades_after_cf_drift_and_before_default(gate):
    guard, comparison = _and_pair(gate["Choices"][4])
    assert guard["Variable"] == "$.drift_result.Payload.deploy_stamp_stale"
    assert comparison["BooleanEquals"] is True
    assert gate["Choices"][4]["Next"] == _DEGRADE_TARGET


def test_halt_family_and_degrade_family_are_disjoint(gate):
    """The three halt-family branches must all precede the degrade family.

    Ordering is the whole semantics of an ASL Choice: a degrade branch placed
    above ``sf_drift`` would silently relabel a definition mismatch as
    orchestration metadata, which is the 2026-08-05/07 miscalibration run in
    reverse.
    """
    targets = [c["Next"] for c in gate["Choices"]]
    assert targets == [_HALT_FAMILY_OBSERVE_TARGET] * 3 + [_DEGRADE_TARGET] * 2


def test_observe_mode_still_marks_the_run_degraded(states):
    """§7a obligation 3: observe mode is not silent.

    The halt-family verdict must reach BOTH surfaces — an SNS page and a
    terminal that is not a plain success — or it is a suppression wearing an
    observation's name.
    """
    flag = states[_HALT_FAMILY_OBSERVE_TARGET]
    assert flag["Type"] == "Pass"
    assert flag["Parameters"]["degraded"] is True
    assert flag["Parameters"]["reason"] == "eod_deploy_drift_observe_would_halt"
    assert flag["Parameters"]["stage_error.$"] == "$.drift_result.Payload"
    assert flag["ResultPath"] == "$.degraded_summary"
    assert flag["Next"] == "PublishDeployDriftDegraded"

    # $.degraded_summary is exactly what CheckDegradedOutcome reads, so the
    # execution terminates DegradedRun rather than a plain SUCCEEDED marker.
    outcome = states["CheckDegradedOutcome"]
    assert outcome["Choices"][0]["Next"] == "WriteCompletionMarkerDegraded"


def test_probe_failure_has_its_own_path_and_never_dereferences_a_missing_payload(states):
    """The Catch cannot route to the would-halt Pass: on that path there is no
    ``$.drift_result.Payload`` and the Pass's own Parameters would raise
    ``States.Runtime`` — turning an unmeasured verdict into a crash whose
    handler is somewhere else entirely."""
    check = states["DeployDriftCheck"]
    catch = check["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["ResultPath"] == "$.drift_error"
    assert catch["Next"] == "SetDeployDriftProbeUnreadableFlag"

    flag = states["SetDeployDriftProbeUnreadableFlag"]
    assert flag["Parameters"]["stage_error.$"] == "$.drift_error"
    assert flag["Parameters"]["reason"] == "eod_deploy_drift_observe_probe_unreadable"
    assert flag["ResultPath"] == "$.degraded_summary"
    assert flag["Next"] == "PublishDeployDriftDegraded"


def test_notify_interpolates_only_fields_every_inbound_path_writes(states):
    """Every ``States.Format`` argument must resolve on all three inbound paths.

    A ``States.Format`` on an absent path raises ``States.Runtime``, which this
    state's own Catch would swallow into ``StartTradingInstance`` — losing the
    page on exactly the runs that most need it. Restricting the arguments to
    ``$.degraded_summary`` (written by whichever Pass routed here) makes that
    structurally impossible rather than merely unlikely.
    """
    notify = states["PublishDeployDriftDegraded"]
    message = notify["Parameters"]["Message.$"]
    assert "$.degraded_summary.reason" in message
    assert "States.JsonToString($.degraded_summary)" in message
    assert "$.drift_result" not in message
    assert "$.drift_error" not in message

    # Fails open: an SNS outage may not convert a degrade into a halt.
    assert notify["Catch"][0]["Next"] == _CLEAN_NEXT
    assert notify["Next"] == _CLEAN_NEXT
    # Literal ARN, as every other publish in this definition — a malformed
    # $.sns_topic_arn must not be able to silence the alert.
    assert notify["Parameters"]["TopicArn"].startswith("arn:aws:sns:")
    assert "TopicArn.$" not in notify["Parameters"]

    inbound = [
        _HALT_FAMILY_OBSERVE_TARGET,
        "SetDeployDriftProbeUnreadableFlag",
        _DEGRADE_TARGET,
    ]
    for name in inbound:
        params = states[name]["Parameters"]
        assert params["reason"], name
        assert states[name]["ResultPath"] == "$.degraded_summary", name


def test_promotion_criterion_is_written_in_the_states_it_governs():
    """§7a obligation 2 — the criterion lives in the guard, not in a PR body.

    Asserted on the raw text rather than the parsed doc so that deleting the
    observe-mode states without also deleting this test is impossible: at
    promotion, the criterion text goes with them and this test is removed in
    the same diff.
    """
    text = _SF_PATH.read_text()
    assert "OBSERVE MODE" in text
    assert "PROMOTION CRITERION" in text
    assert "HandleFailure" in text
    assert "2026-09-08" in text


def test_gate_sits_between_the_mutex_and_the_first_spend(states):
    """Every mutex outcome must pass through the gate.

    The failure this catches is additive: a later edit adding a fourth route
    into ``StartTradingInstance`` from the mutex block, bypassing the check
    without touching it.
    """
    assert states["CheckMutexRole"]["Default"] == "DeployDriftCheck"
    assert states["AcquireMutex"]["Next"] == "DeployDriftCheck"
    assert states["SetMutexAcquireDegradedFlag"]["Next"] == "DeployDriftCheck"
    assert states["DeployDriftCheck"]["Next"] == "DeployDriftGate"

    # ...and nothing else reaches StartTradingInstance except the gate's own
    # clean/degraded routes.
    def _nexts(state):
        out = []
        for key in ("Next", "Default"):
            if state.get(key):
                out.append(state[key])
        for branch in (state.get("Choices") or []) + (state.get("Catch") or []):
            if branch.get("Next"):
                out.append(branch["Next"])
        return out

    callers = {
        name for name, state in states.items()
        if _CLEAN_NEXT in _nexts(state)
    }
    assert callers == {"DeployDriftGate", "PublishDeployDriftDegraded"}, callers


def test_probe_is_asked_about_the_postclose_state_machine(states):
    """Without ``sf_name`` the probe defaults to the PREOPEN pipeline, and this
    gate would halt-or-pass on a verdict about a state machine that is not the
    one it is guarding. ``crucible-predictor-PR545`` made the key load-bearing
    and made an undeclared name raise; this pins the caller's half."""
    payload = states["DeployDriftCheck"]["Parameters"]["Payload"]
    assert payload["action"] == "check_deploy_drift"
    assert payload["sf_name"] == "ne-postclose-trading-pipeline"
    assert states["DeployDriftCheck"]["Parameters"]["FunctionName"].endswith(":live")
    assert states["DeployDriftCheck"]["ResultPath"] == "$.drift_result"
