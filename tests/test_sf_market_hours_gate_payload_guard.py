"""alpha-engine-config-I7165 — every raw `lambda:invoke` Payload dereference
in a Choice state must be reachable only once its key's presence is
established, either by a `ResultSelector` on the producing Task that pins
the key, or by a presence guard inside the Choice itself.

## The incident this guards

The 2026-08-13 preopen execution (`59c0ce46-d366-45dd-8cd3-acbdc06638c1`)
failed 48s in with a raw `States.Runtime` at `MarketHoursGateChoice`:
`Invalid path '$.market_hours_gate.Payload.verdict'`. No SNS alert fired —
ASL Choice states cannot carry `Catch`, so an absent-key dereference in a
Choice is UNCATCHABLE even though the producing Task's own `Catch` on
`States.ALL` is right there. Root trigger (`crucible-predictor-PR474`,
out of scope here): `alpha-engine-predictor-inference:live` was frozen on a
version with no `check_market_hours` action and fell through to its default
predict branch, returning `{"statusCode": 200, ...}` with no `verdict` key
at all.

`MarketHoursGate` was the ONLY `lambda:invoke` gate in either weekday
definition with `ResultSelector: null` — every sibling gate
(`TradingDayGate`, `DeployDriftCheck`, `CheckPredictorCoverage`, ...)
already threads its Choice comparisons through the And-with-earlier-
IsPresent guard idiom this repo has used since config#2275/I2767. This test
makes that invariant durable and generic, instead of re-litigating it one
Lambda gate at a time.

## Two ASL facts this test's model relies on, measured on throwaway state
machines in this account 2026-08-13 (not assumed):

1. A `ResultSelector` missing-key failure is NOT catchable — so "give the
   Task a ResultSelector" only relocates the uncatchable failure, it does
   not by itself make a Choice comparison downstream safe. The
   ResultSelector must actually PROJECT the key the Choice reads.
2. `Not`/`IsPresent` evaluates cleanly at any depth, on both a present-
   parent-absent-leaf and a wholly-absent-root payload. A standalone
   leading `Choices` rule of the shape
   `{"Not": {"Variable": V, "IsPresent": true}, "Next": ...}` is therefore
   sufficient, on its own, to make every LATER rule in the same `Choices`
   list safe to dereference V unconditionally — ASL evaluates `Choices` in
   order and this rule fires first whenever V is absent, so no later rule
   is ever reached in that case. This is the shape `MarketHoursGateChoice`
   was given in both weekday definitions to fix I7165, and it is a second,
   equally valid guard idiom alongside the existing And-wrap one — this
   test recognizes both.

## Why this file, not an extension of `test_sf_choice_guards.py`

That test already implements this invariant, more richly (it also drills
partial-payload routing), but scoped to `step_function.json` (the Saturday
weekly definition) alone. `step_function.json` is out of scope for I7165 —
it is being edited concurrently by another change — so this file covers
the two definitions I7165 actually touches, `step_function_daily.json`
(preopen) and `step_function_eod.json` (postclose), without depending on
or racing that concurrent edit. `test_sf_poll_resultselector.py` already
established the pattern of a `_SF_FILES`-keyed walk across multiple
definitions; this file follows it.

## Why zero false positives against the ~100 SSM poll sites is automatic

`aws-sdk:ssm:getCommandInvocation` (and the `ssm-liveness-poller` Lambda)
never produce a `Payload` wrapper — their raw shape is `{Status, Command
Id, ...}` at the ResultPath root, so a downstream Choice reading
`$.foo_poll.Status` never matches the `$.<x>.Payload.<...>` pattern this
test targets at all. Likewise the ~24 Pass-produced sites
(`$.*_retry.attempts`, `$.branch_outcomes.*`) are never nested under
`.Payload.`. The regex excludes both classes structurally, not via an
exclusion list.
"""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_SF_FILES = {
    "weekday": _INFRA / "step_function_daily.json",
    "eod": _INFRA / "step_function_eod.json",
}

_PAYLOAD_VAR_RE = re.compile(r"^\$\.([A-Za-z0-9_]+)\.Payload\.([A-Za-z0-9_]+)")


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


def _walk_states(states: dict):
    for name, st in states.items():
        if not isinstance(st, dict):
            continue
        yield name, st
        for sub in ("ItemProcessor", "Iterator"):
            inner = st.get(sub)
            if isinstance(inner, dict) and isinstance(inner.get("States"), dict):
                yield from _walk_states(inner["States"])
        for b in st.get("Branches", []) or []:
            if isinstance(b, dict) and isinstance(b.get("States"), dict):
                yield from _walk_states(b["States"])


def _result_selector_keeps(st: dict) -> set[str] | None:
    """Keys a lambda:invoke Task's ResultSelector pins, or None if the Task
    has no ResultSelector at all (raw Payload rides through unprojected)."""
    rs = st.get("ResultSelector")
    if rs is None:
        return None
    return {k[:-2] if k.endswith(".$") else k for k in rs}


def _producers_by_result_path(states: dict) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for _, st in _walk_states(states):
        if "lambda:invoke" not in str(st.get("Resource", "")).lower():
            continue
        rp = st.get("ResultPath")
        if rp:
            out.setdefault(rp, []).append(st)
    return out


def _leaf_vars(rule: dict) -> list[tuple[str, bool]]:
    """Every (Variable, is_and_guarded) leaf comparison in this rule
    subtree. is_and_guarded=True when an EARLIER operand of an enclosing
    And is exactly {Variable: same path, IsPresent: true} (the existing
    config#2275 idiom)."""
    out: list[tuple[str, bool]] = []

    def _walk(r: dict, guarded: frozenset[str]):
        if "And" in r:
            acquired = set(guarded)
            for operand in r["And"]:
                _walk(operand, frozenset(acquired))
                if operand.get("IsPresent") is True and "Variable" in operand:
                    acquired.add(operand["Variable"])
            return
        if "Or" in r:
            for operand in r["Or"]:
                _walk(operand, guarded)
            return
        if "Not" in r:
            _walk(r["Not"], guarded)
            return
        var = r.get("Variable")
        if var is None:
            return
        ops = {k for k in r if k not in ("Variable", "Next", "Comment")}
        if "IsPresent" in ops:
            return  # a presence check is always safe to evaluate
        out.append((var, var in guarded))

    _walk(rule, frozenset())
    return out


def _is_leading_not_ispresent_guard(rule: dict, var: str) -> bool:
    """True if `rule` is exactly {"Not": {"Variable": var, "IsPresent":
    true}, "Next": ...} — the standalone leading-guard idiom."""
    inner = rule.get("Not")
    if not isinstance(inner, dict):
        return False
    return inner.get("Variable") == var and inner.get("IsPresent") is True


def _violations_for_definition(sf: str, definition: dict) -> list[str]:
    states = definition["States"]
    producers = _producers_by_result_path(states)
    violations: list[str] = []

    for scope_name, scope_states in _iter_choice_scopes(states):
        for name, st in scope_states.items():
            if st.get("Type") != "Choice":
                continue
            rules = st.get("Choices", [])
            # A leading standalone Not/IsPresent guard rule establishes
            # presence for every LATER rule in this same Choices list —
            # ASL evaluates Choices in order and that rule fires first
            # whenever the variable is absent.
            established: set[str] = set()
            for rule in rules:
                for var, and_guarded in _leaf_vars(rule):
                    m = _PAYLOAD_VAR_RE.match(var)
                    if not m:
                        continue
                    root, key = m.group(1), m.group(2)
                    if and_guarded or var in established:
                        continue
                    rs_keeps = None
                    for prod in producers.get(f"$.{root}", []):
                        keeps = _result_selector_keeps(prod)
                        if keeps is not None and key in keeps:
                            rs_keeps = keeps
                            break
                    if rs_keeps is not None:
                        continue  # floored by the producing Task's ResultSelector
                    violations.append(
                        f"[{sf}] {scope_name}/{name}: rule dereferences "
                        f"{var!r} with no ResultSelector projecting "
                        f"{key!r} off $.{root} and no presence guard in "
                        f"this Choice (rule={json.dumps(rule)[:200]})"
                    )
                # Now that this rule has been evaluated, record any
                # standalone guard it represents for subsequent rules.
                inner = rule.get("Not")
                if isinstance(inner, dict) and inner.get("IsPresent") is True and "Variable" in inner:
                    established.add(inner["Variable"])
    return violations


def _iter_choice_scopes(states: dict):
    """Yield (label, states_dict) for the top scope and every nested
    Parallel/Map scope — same traversal as _walk_states but returning the
    dict itself for a fresh top-level Choices scan."""
    yield "", states
    for name, st in states.items():
        if not isinstance(st, dict):
            continue
        for sub in ("ItemProcessor", "Iterator"):
            inner = st.get(sub)
            if isinstance(inner, dict) and isinstance(inner.get("States"), dict):
                yield from _iter_choice_scopes(inner["States"])
        for i, b in enumerate(st.get("Branches", []) or []):
            if isinstance(b, dict) and isinstance(b.get("States"), dict):
                yield from ((f"{name}[{i}]/{lbl}" if lbl else f"{name}[{i}]", s)
                            for lbl, s in _iter_choice_scopes(b["States"]))


@pytest.mark.parametrize("sf", sorted(_SF_FILES))
def test_choice_payload_dereferences_are_guarded_or_projected(sf: str) -> None:
    definition = _load(_SF_FILES[sf])
    violations = _violations_for_definition(sf, definition)
    assert not violations, (
        "alpha-engine-config-I7165 — Choice states dereferencing a raw "
        "lambda:invoke Payload field with no ResultSelector projection and "
        "no presence guard (this is exactly how MarketHoursGateChoice threw "
        "an uncatchable States.Runtime on 2026-08-13):\n" + "\n".join(violations)
    )


def test_market_hours_gate_choice_guard_is_present() -> None:
    """Direct check that the specific fix landed: MarketHoursGateChoice's
    FIRST rule in both weekday definitions is the leading Not/IsPresent
    guard routing to MarketHoursGatePayloadMalformed, and that Pass state
    floors $.market_hours_gate_error before continuing."""
    for sf, path in _SF_FILES.items():
        definition = _load(path)
        states = definition["States"]
        choice = states["MarketHoursGateChoice"]
        first_rule = choice["Choices"][0]
        assert _is_leading_not_ispresent_guard(
            first_rule, "$.market_hours_gate.Payload.verdict"
        ), f"[{sf}] MarketHoursGateChoice's first rule is not the I7165 presence guard"
        assert first_rule["Next"] == "MarketHoursGatePayloadMalformed"

        malformed = states["MarketHoursGatePayloadMalformed"]
        assert malformed["Type"] == "Pass"
        assert malformed["ResultPath"] == "$.market_hours_gate_error"
        assert "Error" in malformed.get("Result", {})
        assert "Cause" in malformed.get("Result", {})


def test_regression_pre_fix_tree_is_caught() -> None:
    """Demonstrates the test actually catches the I7165 defect: with the
    leading guard rule stripped back out, MarketHoursGateChoice reverts to
    exactly the shape that produced the live 2026-08-13 States.Runtime, and
    the structural test above must flag it."""
    for sf, path in _SF_FILES.items():
        definition = _load(path)
        states = definition["States"]
        choice = states["MarketHoursGateChoice"]
        first_rule = choice["Choices"][0]
        assert _is_leading_not_ispresent_guard(
            first_rule, "$.market_hours_gate.Payload.verdict"
        )
        broken = copy.deepcopy(definition)
        broken["States"]["MarketHoursGateChoice"]["Choices"] = broken["States"][
            "MarketHoursGateChoice"
        ]["Choices"][1:]
        violations = _violations_for_definition(sf, broken)
        assert any("MarketHoursGateChoice" in v for v in violations), (
            f"[{sf}] pre-fix tree (guard rule removed) was NOT flagged — "
            "the test would not have caught the live I7165 incident"
        )
