"""alpha-engine-config-I6025 — the parity family must DEGRADE, not FAIL, the
weekly SF; restructured for the alpha-engine-config#6030 split.

Pre-fix (the 2026-08-01 watch-rerun-2026-08-01-3 failure): the Parity state's
SSM command hit its 2h executionTimeout and the SF FAILED a run that had
already cleared Backtester, PredictorBacktest and PortfolioOptimizerBacktest.
I6025 made parity degrade-not-fail; #6030 then split the bundled stage into a
ParityParallel of three fail-open branches (PitParityLookahead /
PitParityWalkforward / ParityReplay) joined by a PitParityCompare quartet.

Shape pinned here (the I6025 invariants, distributed over the new topology):

  * every branch non-success path converges on that branch's OWN *Degraded
    Pass terminal, which ENDS THE BRANCH SUCCESS with status DEGRADED — a
    branch never throws into the Parallel, so one branch's failure never
    aborts its siblings (sf-pipeline-policy §4 blast radius);
  * any DEGRADED branch folds into the SF-controlled $.parity_degraded flag
    at the post-join ParityDegraded Pass → PublishParityDegraded pages
    distinctly → the run CONTINUES to the compare (never HandleFailure) —
    §2.3a requires the compare to run and emit verdict UNKNOWN, never to be
    skipped because a producer died;
  * the compare's own failure converges on ParityCompareDegraded (the second
    and last legal $.parity_degraded writer) → PublishParityCompareDegraded
    → CheckSkipEvaluator;
  * absence of a verdict must never render identically to a clean pass: the
    pages fire immediately, the terminal notification carries the
    parity_degraded family (test_sf_parity_gate_notify_wiring.py), and the
    ARTIFACT_REGISTRY freshness-monitor SLA alarms on missing artifacts.
"""
from __future__ import annotations

import json
import pathlib

import pytest
from tests.sf_degraded_summary_helpers import assert_degraded_continuation

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

_BRANCH_BASES = ("PitParityLookahead", "PitParityWalkforward", "ParityReplay")
_BRANCH_VARS = {
    "PitParityLookahead": "pit_parity_lookahead",
    "PitParityWalkforward": "pit_parity_walkforward",
    "ParityReplay": "parity_replay",
}

# alpha-engine-config-I8194: the key each branch terminal nests its outcome
# envelope under. It used to be the terminal's ResultPath; the terminal now
# replaces its payload instead of merging into it, so the same name is the
# sole top-level key of Parameters and $.parity_parallel_result[i].<key>
# .status resolves unchanged.
_BRANCH_ENVELOPE_KEYS = {
    "PitParityLookahead": "branch_pit_lookahead",
    "PitParityWalkforward": "branch_pit_walkforward",
    "ParityReplay": "branch_parity_replay",
}


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


@pytest.fixture(scope="module")
def branches(states) -> dict:
    out = {}
    for branch in states["ParityParallel"]["Branches"]:
        gate = branch["States"][branch["StartAt"]]
        out[gate["Default"]] = branch["States"]
    return out


def _catches(states, name) -> list:
    return states[name].get("Catch", [])


# ---------------------------------------------------------------------------
# Branch-level fail-open (the §4 blast-radius half)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("base", _BRANCH_BASES)
def test_branch_send_and_wait_catches_route_to_branch_degraded(branches, base):
    """Both Task states of every branch must Catch States.ALL → the branch's
    own *Degraded terminal — never NormalizeFailureContext (the SF-failing
    path I6025 retired for parity), and never a raw throw (which would make
    the Parallel kill the sibling branches)."""
    b = branches[base]
    var = _BRANCH_VARS[base]
    for name in (base, f"WaitFor{base}"):
        catches = b[name].get("Catch", [])
        assert catches, f"{name} must keep its fail-soft Catch"
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == f"{base}Degraded"
            assert c["ResultPath"] == f"$.{var}_error"


@pytest.mark.parametrize("base", _BRANCH_BASES)
def test_branch_degraded_terminal_ends_branch_success(branches, base):
    """The fail-open marker: a Pass that records status DEGRADED and ENDS
    the branch — the sibling-isolation guarantee lives exactly here."""
    st = branches[base][f"{base}Degraded"]
    assert st["Type"] == "Pass"
    assert st.get("End") is True
    # alpha-engine-config-I8194: envelope nested under the branch key,
    # no ResultPath — the branch returns the envelope, not its payload.
    assert st["Parameters"] == {
        _BRANCH_ENVELOPE_KEYS[base]: {"status": "DEGRADED"}
    }
    assert "ResultPath" not in st


@pytest.mark.parametrize("base", _BRANCH_BASES)
def test_branch_status_default_degrades_and_poll_is_bounded(branches, base):
    """Terminal non-Success AND poll-budget exhaustion both fall to the
    branch's Degraded terminal (a bound whose exhaustion converges on the
    happy path is not a bound, I5687) — or, for the two pit_parity passes
    (alpha-engine-config-I7267), through the RESOURCE_KILL marker check
    first, which itself falls back to the SAME Degraded terminal on
    anything but a confirmed marker hit (see
    test_sf_parity_resource_kill_halt_i7267.py for that chain's shape)."""
    check = branches[base][f"Check{base}Status"]
    if base in ("PitParityLookahead", "PitParityWalkforward"):
        assert check["Default"] == f"{base}ResourceKillCheck"
    else:
        assert check["Default"] == f"{base}Degraded"
    loop = [c for c in check["Choices"] if "And" in c]
    assert len(loop) == 1, "the poll loop must be budget-guarded"


def test_no_branch_state_reaches_failure_plane(states, branches):
    """No state inside any parity branch may name the SF failure plane."""
    blob = json.dumps(states["ParityParallel"])
    assert "NormalizeFailureContext" not in blob
    assert "HandleFailure" not in blob
    assert "FailExecution" not in blob


# ---------------------------------------------------------------------------
# The post-join fold + the flag writers
# ---------------------------------------------------------------------------


def test_any_degraded_branch_folds_into_parity_degraded(states):
    cbo = states["CheckParityBranchOutcomes"]
    # alpha-engine-config-I7267: RESOURCE_KILL is checked FIRST and routes
    # to the shared hard-fail path (never reaching the compare) — the
    # pre-existing DEGRADED fold (still fail-open through the compare) is
    # Choices[1]. See test_sf_parity_resource_kill_halt_i7267.py for the
    # dedicated coverage of the new fold.
    assert cbo["Choices"][0]["Next"] == "PitParityResourceKillDetected"
    assert {c["StringEquals"] for c in cbo["Choices"][0]["Or"]} == {"RESOURCE_KILL"}
    assert cbo["Choices"][1]["Next"] == "ParityDegraded"
    assert {c["StringEquals"] for c in cbo["Choices"][1]["Or"]} == {"DEGRADED"}
    assert cbo["Default"] == "CheckSkipPitParityCompare"


def test_exactly_two_pass_states_write_parity_degraded(states):
    """The degraded flag must be SF-controlled: exactly ParityDegraded (the
    branch fold) and ParityCompareDegraded (the compare's own failure) may
    write $.parity_degraded — nothing else, and both are Pass/Result true."""
    writers = sorted(
        name for name, st in states.items()
        if st.get("ResultPath") == "$.parity_degraded"
    )
    assert writers == ["ParityCompareDegraded", "ParityDegraded"]
    for name in writers:
        assert states[name]["Type"] == "Pass"
        assert states[name]["Result"] is True


def test_extract_parity_error_retired(states):
    """The old SF-failing normalizer stays gone — no parity path may reach
    HandleFailure."""
    assert "ExtractParityError" not in states
    for name in ("PitParityCompare", "WaitForPitParityCompare"):
        targets = [c["Next"] for c in _catches(states, name)]
        assert "NormalizeFailureContext" not in targets


# ---------------------------------------------------------------------------
# publish + continue
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pub_name,next_state",
    [
        ("PublishParityDegraded", "CheckSkipPitParityCompare"),
        ("PublishParityCompareDegraded", "CheckSkipEvaluator"),
    ],
)
def test_publish_degraded_pages_constants_and_continues(states, pub_name, next_state):
    pub = states[pub_name]
    assert pub["Type"] == "Task"
    assert pub["Resource"] == "arn:aws:states:::sns:publish"
    # config#1819: constants-only Subject/Message — the only .$ reference is
    # the topic ARN floor from InitializeInput.
    assert pub["Parameters"]["TopicArn.$"] == "$.sns_topic_arn"
    assert "Subject" in pub["Parameters"]
    assert "Subject.$" not in pub["Parameters"]
    assert "Message.$" not in pub["Parameters"]
    assert "DEGRADED" in pub["Parameters"]["Subject"]
    assert len(pub["Parameters"]["Subject"]) <= 100
    # best-effort: a publish failure must not block the non-fatal degrade
    # path this alert decorates
    catches = _catches(states, pub_name)
    assert catches and catches[0]["ErrorEquals"] == ["States.ALL"]
    assert catches[0]["Next"] == next_state
    assert pub["Next"] == next_state


def test_branch_degrade_continues_to_compare_not_around_it(states):
    """§2.3a: the branch-degraded fold must route THROUGH the compare gate —
    a degraded producer never skips the join (the compare emits the verdict
    as UNKNOWN); jumping straight to CheckSkipEvaluator would silently drop
    the verdict for runs where the OTHER branches completed fine."""
    assert_degraded_continuation(states, "ParityDegraded", "PublishParityDegraded")
    assert states["PublishParityDegraded"]["Next"] == "CheckSkipPitParityCompare"


def test_no_parity_state_catch_targets_notify_or_handle_directly(states, branches):
    """The pre-fix defect class: a Catch jumping straight to a notifier or
    the failure handler, skipping the degraded flag."""
    task_sets = [(states, "PitParityCompare"), (states, "WaitForPitParityCompare")]
    for base in _BRANCH_BASES:
        task_sets += [(branches[base], base), (branches[base], f"WaitFor{base}")]
    for scope, name in task_sets:
        targets = [c["Next"] for c in scope[name].get("Catch", [])]
        assert "NotifyComplete" not in targets
        assert "CheckShellRunNotify" not in targets
        assert "HandleFailure" not in targets


# ---------------------------------------------------------------------------
# compare poll loop resolves to terminal status
# ---------------------------------------------------------------------------


def test_compare_poll_resolves_to_terminal_status(states):
    assert states["PitParityCompare"]["Next"] == "InitPitParityComparePollCount"
    assert states["InitPitParityComparePollCount"]["Next"] == "WaitForPitParityCompare"
    assert states["WaitForPitParityCompare"]["Next"] == "CheckPitParityCompareStatus"

    choice = states["CheckPitParityCompareStatus"]
    rules = choice["Choices"]
    success = next(r for r in rules if r.get("StringEquals") == "Success")
    assert success["Variable"] == "$.pit_parity_compare_poll.Status"
    assert success["Next"] == "PitParityCompareComplete"
    assert states["PitParityCompareComplete"]["Next"] == "CheckSkipEvaluator"

    # THE pin: terminal non-Success (TimedOut / Failed / Cancelled) degrades
    # instead of failing the SF.
    assert choice["Default"] == "ParityCompareDegraded"


# ---------------------------------------------------------------------------
# completion honesty: the degrade chains set the flag then continue
# ---------------------------------------------------------------------------


def _walk_next_and_catch(states, start):
    seen: set[str] = set()
    stack = [start]
    while stack:
        name = stack.pop()
        if name in seen or name not in states:
            continue
        seen.add(name)
        nxt = states[name].get("Next")
        if nxt:
            stack.append(nxt)
        for c in states[name].get("Catch", []):
            stack.append(c["Next"])
    return seen


def test_degraded_branch_chain_reaches_compare_gate_never_failure(states):
    seen = _walk_next_and_catch(states, "ParityDegraded")
    assert "PublishParityDegraded" in seen
    assert "CheckSkipPitParityCompare" in seen
    assert "NormalizeFailureContext" not in seen
    assert "HandleFailure" not in seen


def test_degraded_compare_chain_reaches_evaluator_gate_never_failure(states):
    seen = _walk_next_and_catch(states, "ParityCompareDegraded")
    assert "PublishParityCompareDegraded" in seen
    assert "CheckSkipEvaluator" in seen
    assert "NormalizeFailureContext" not in seen
    assert "HandleFailure" not in seen
