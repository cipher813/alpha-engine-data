"""config#5688 — a bounded re-issue must never target a dead instance.

The weekly pipeline's three `*RetryGate` Choice states re-issue
`ssm:sendCommand` to `$.ec2_instance_id`. Their original rationale assumed an
always-on launcher box. config#2248 replaced that box with a per-execution
ephemeral spot (`DispatchWeeklyFreshnessSpot`), which makes *the instance is
gone* the dominant reason a poll goes non-`Success` — precisely the case where
re-issuing to the same instance id can only raise
`Ssm.InvalidInstanceIdException`.

Measured on execution ``158e7fe8-a6ca-4278-b097-c02ca4e34752`` (2026-07-30):
after `WaitForRAGIngestion` returned ``Undeliverable``, the gate re-issued and
burned **5 consecutive TaskFailed over 4m20s** on an outcome that was
deterministic from the first attempt — the pipeline's one bounded retry spent
as a guaranteed no-op.

The fix is structural: branch on *why* the poll failed before deciding to
re-issue. These tests keep it that way, and — because the stale
``always-on instance`` comment is what let the premise survive config#2248 —
also assert that text cannot come back.
"""

from __future__ import annotations

import json
import pathlib

import pytest

_DEFINITION = (
    pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"
)
_RAW = _DEFINITION.read_text(encoding="utf-8")
_DEF = json.loads(_RAW)

# StatusDetails values SSM reports when the target instance is gone. A
# re-issue in either state is a guaranteed no-op.
_SUBSTRATE_LOST_DETAILS = {"Undeliverable", "Terminated"}


def _iter_scopes(node):
    """Yield every `States` map in the definition (top level + every branch)."""
    if isinstance(node, dict):
        states = node.get("States")
        if isinstance(states, dict):
            yield states
        for value in node.values():
            yield from _iter_scopes(value)
    elif isinstance(node, list):
        for value in node:
            yield from _iter_scopes(value)


_SCOPES = list(_iter_scopes(_DEF))


def _gate_scopes():
    """(gate_name, scope) for every Choice state named `*RetryGate`."""
    for scope in _SCOPES:
        for name, state in scope.items():
            if name.endswith("RetryGate") and state.get("Type") == "Choice":
                yield name, state, scope


def _leaf_conditions(rule):
    """Flatten a Choice rule into its leaf comparison dicts."""
    for key in ("And", "Or", "Not"):
        if key in rule:
            operands = rule[key]
            if isinstance(operands, dict):
                operands = [operands]
            for operand in operands:
                yield from _leaf_conditions(operand)
            return
    yield rule


def _gate_ids():
    return [name for name, _, _ in _gate_scopes()]


def test_there_are_retry_gates_to_check():
    """Guard against the tests silently passing on an empty set."""
    assert _gate_ids(), (
        "no *RetryGate Choice states found in the weekly definition — if the "
        "bounded-re-issue pattern was retired, delete this test with the reason "
        "rather than leaving it vacuously green"
    )


@pytest.mark.parametrize("gate_name", _gate_ids())
def test_reissue_is_guarded_by_a_liveness_branch(gate_name):
    """No `*Reissue` route may be reachable without a substrate-loss branch first.

    The dead-instance branch must be evaluated BEFORE any rule that routes to
    the re-issue, since ASL evaluates Choices in order.
    """
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)

    rules = gate.get("Choices", [])
    routes = [(i, r.get("Next", "")) for i, r in enumerate(rules)]
    routes.append((len(rules), gate.get("Default", "")))
    reissue_positions = [i for i, target in routes if target.endswith("Reissue")]
    if not reissue_positions:
        pytest.skip(f"{gate_name} no longer re-issues; nothing to guard")

    liveness_positions = []
    for i, rule in enumerate(rules):
        values = {
            c.get("StringEquals")
            for c in _leaf_conditions(rule)
            if str(c.get("Variable", "")).endswith("_poll.StatusDetails")
        }
        if values & _SUBSTRATE_LOST_DETAILS:
            liveness_positions.append(i)

    assert liveness_positions, (
        f"{gate_name} routes to a re-issue without ever branching on "
        f"$.<stage>_poll.StatusDetails. After config#2248 the launcher is an "
        f"ephemeral spot, so re-issuing ssm:sendCommand to a terminated "
        f"instance id can only raise Ssm.InvalidInstanceIdException — the "
        f"pipeline's one bounded retry spent on a deterministic no-op "
        f"(config#5688)."
    )
    assert min(liveness_positions) < min(reissue_positions), (
        f"{gate_name} evaluates its re-issue rule before the substrate-loss "
        f"branch; ASL takes the first matching Choice, so the dead-instance "
        f"case would still re-issue"
    )


@pytest.mark.parametrize("gate_name", _gate_ids())
def test_substrate_lost_branch_is_distinguishable_and_reaches_the_notifier(gate_name):
    """The dead-instance path emits its own phase and joins the failure chain.

    Substrate loss and workload failure arriving as the same `phase` is what
    makes the two indistinguishable in failure telemetry. The target must also
    hand off to the same downstream state its non-substrate sibling does, so
    the alert body is produced by the identical chain.
    """
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)

    def _is_substrate_rule(rule):
        return bool(
            {
                c.get("StringEquals")
                for c in _leaf_conditions(rule)
                if str(c.get("Variable", "")).endswith("_poll.StatusDetails")
            }
            & _SUBSTRATE_LOST_DETAILS
        )

    targets = {r.get("Next") for r in gate.get("Choices", []) if _is_substrate_rule(r)}
    if not targets:
        pytest.skip(f"{gate_name} has no substrate-loss branch (covered by the guard test)")

    # The sibling is the gate's OTHER terminal route (retry-budget exhaustion),
    # found structurally rather than by name — the four gates do not share a
    # naming convention (Extract*Error vs SetDataPhase2ExhaustedError).
    siblings = [
        scope[r["Next"]]
        for r in gate.get("Choices", [])
        if not _is_substrate_rule(r) and r.get("Next") in scope
    ]
    assert siblings, (
        f"{gate_name} has no non-substrate error route to mirror; the substrate "
        f"branch cannot be checked for chain parity"
    )
    sibling = siblings[0]

    for target in targets:
        state = scope.get(target)
        assert state is not None, f"{gate_name} routes to {target}, absent from its scope"
        phase = state.get("Parameters", {}).get("phase", "")
        assert phase.endswith("/SubstrateLost"), (
            f"{target} emits phase {phase!r}; substrate loss must be separable "
            f"from workload failure in the SNS body (config#5688)"
        )
        assert state.get("ResultPath") == "$.error", (
            f"{target} must write $.error — HandleFailure's "
            f"States.JsonToString($.error) throws States.Runtime otherwise"
        )
        assert state.get("Next") == sibling.get("Next"), (
            f"{target} hands off to {state.get('Next')!r} but {gate_name}'s "
            f"non-substrate error route hands off to {sibling.get('Next')!r}; "
            f"both must join the same failure-notification chain"
        )


@pytest.mark.parametrize("gate_name", _gate_ids())
def test_substrate_lost_branch_does_not_consume_the_retry_counter(gate_name):
    """A substrate event must not spend the retry a workload failure needs."""
    _, gate, scope = next(g for g in _gate_scopes() if g[0] == gate_name)

    for rule in gate.get("Choices", []):
        leaves = list(_leaf_conditions(rule))
        is_liveness = {
            c.get("StringEquals")
            for c in leaves
            if str(c.get("Variable", "")).endswith("_poll.StatusDetails")
        } & _SUBSTRATE_LOST_DETAILS
        if not is_liveness:
            continue
        target = scope.get(rule.get("Next"), {})
        assert not str(target.get("ResultPath", "")).endswith("_attempts"), (
            f"{gate_name}'s substrate-loss branch writes an attempts counter; "
            f"a later genuine transient workload failure then has no retry left"
        )


def test_stale_always_on_instance_premise_is_gone():
    """The comment that let the invalidated premise survive config#2248."""
    assert "always-on instance" not in _RAW, (
        "a *RetryGate Comment still claims an always-on instance. There is no "
        "always-on box in this pipeline since config#2248; that text is what "
        "let the stale premise survive the change (doc-maintenance-policy: a "
        "load-bearing comment contradicted by the live system is a same-turn "
        "correction)."
    )
