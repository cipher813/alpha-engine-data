"""A Parallel state may never forward an unbounded branch payload
(alpha-engine-config-I8194).

2026-08-14 — the weekly SF FAILED with ``States.DataLimitExceeded``: "the
state/task 'ParityParallel' returned a result with a size exceeding the
maximum number of bytes service limit" (execution
``friday-shell-2026-08-14-validate-i7386``, event 1403). Every one of the
three parity branches SUCCEEDED. The failure was purely the shape of the
join: each branch terminal was a ``Pass`` with a ``ResultPath``, so its
output was the branch's whole *effective input* — 107,962 / 107,939 /
107,895 bytes as measured in that execution's history — with a 40-byte
status envelope merged into it. The Parallel's result array was 323,876
bytes against the 262,144-byte per-transition ceiling.

It was invisible for eight days because ``skip_parity: true`` on all three
automatic triggers routes ``CheckSkipParity`` straight past ``ParityParallel``,
so the only path that reaches the defect is one no scheduled run takes. That
is the reason this file exists rather than a fix alone: the class is not
reachable by running the pipeline, so it has to be reachable by reading the
definition.

The sibling guard for the OTHER half of this class is
``tests/test_sf_poll_resultselector.py`` — SSM stdout accumulating inside a
branch, which tripped the identical error on ``ResearchPredictorParallel`` on
2026-06-06 and again on 2026-06-19. That guard bounds what a branch *carries*;
this one bounds what a branch *returns*. Both are needed: the 2026-08-14
payload was already stdout-free.

**Derived from the definition, never from a list of state names.** A
hand-kept list of Parallel or terminal state names is exactly what drifts
when the next branch is added — the 2026-06-19 recurrence was a guard that
enumerated only the three states in the first incident's path.

The bounded shape, which is also the fleet's artifact-by-reference
convention: a branch writes its substantive result to S3 and its terminal
``Pass`` **replaces** the payload with a small outcome envelope (``Parameters``
or ``Result``, and no ``ResultPath``), so the branch's return size is a
constant. The join reads the envelope for routing and reads S3 for content.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_SF_FILES = {
    "saturday": _INFRA / "step_function.json",
    "weekday": _INFRA / "step_function_daily.json",
    "eod": _INFRA / "step_function_eod.json",
    "groom": _INFRA / "step_function_groom.json",
}

# The Step Functions per-transition payload quota, in bytes.
_PAYLOAD_LIMIT = 256 * 1024

# What a `<key>.$` field is charged when bounding a worst-case envelope: its
# value is a JSONPath into branch state, so its size is not knowable from the
# definition. 4 KB is generous for the only such fields that exist today
# (normalized `$.error` contexts and boolean degraded flags).
_JSONPATH_FIELD_ALLOWANCE = 4 * 1024

# A Parallel result must stay a rounding error against the quota, not merely
# fit under it: the result is merged into the state's input, which on the
# 2026-08-14 execution was itself 105,971 bytes.
_ENVELOPE_BUDGET = _PAYLOAD_LIMIT // 8


def _walk_states(states: dict, prefix: str = ""):
    for name, st in states.items():
        if not isinstance(st, dict):
            continue
        yield prefix + name, st
        for sub in ("Iterator", "ItemProcessor"):
            inner = st.get(sub)
            if isinstance(inner, dict) and isinstance(inner.get("States"), dict):
                yield from _walk_states(inner["States"], f"{prefix}{name}/")
        for i, b in enumerate(st.get("Branches", []) or []):
            if isinstance(b, dict) and isinstance(b.get("States"), dict):
                yield from _walk_states(b["States"], f"{prefix}{name}[{i}]/")


def _parallel_states():
    """Yield (sf, name, state, definition) for every Parallel in every SF."""
    for sf, path in _SF_FILES.items():
        if not path.exists():
            continue
        definition = json.loads(path.read_text())
        for name, st in _walk_states(definition["States"]):
            if st.get("Type") == "Parallel":
                yield sf, name, st, definition


def _branch_terminals(branch: dict):
    """Yield (name, state) for every state that ends a branch.

    A branch ends at `End: true` or at a `Succeed`/`Fail` state. `Fail`
    produces no output at all, so it is never a payload carrier.

    Only the branch's OWN states count. A `Map` iterator or a nested `Parallel`
    inside the branch has its own terminals, and those end an iteration rather
    than the branch — a nested Parallel is discovered and checked in its own
    right by `_parallel_states`.
    """
    for name, st in branch["States"].items():
        if not isinstance(st, dict):
            continue
        if st.get("Type") == "Fail":
            continue
        if st.get("End") is True or st.get("Type") == "Succeed":
            yield name, st


def _replaces_its_payload(st: dict) -> bool:
    """True when this terminal's output is its own projection, not its input.

    A `Pass` writes its projection to `ResultPath`, defaulting to `$` — the
    whole payload. Any other `ResultPath` means "merge into the input I was
    handed", which is precisely the unbounded forward.
    """
    if st.get("Type") != "Pass":
        return False
    if "Parameters" not in st and "Result" not in st:
        return False
    return st.get("ResultPath", "$") == "$"


def _projects_its_result(parallel: dict) -> bool:
    """True when the Parallel itself projects the branch array away.

    A `ResultSelector` is evaluated on the raw branch array before the state's
    output is measured, so projecting there bounds the result even when the
    branches return everything. It is the alternative satisfying shape, not
    the preferred one — the branch-terminal shape keeps the bound where the
    payload is produced.
    """
    selector = parallel.get("ResultSelector")
    if not isinstance(selector, dict) or not selector:
        return False
    return all(not isinstance(v, (dict, list)) for v in selector.values())


def _outcome_envelope(st: dict) -> dict:
    return st.get("Parameters") if "Parameters" in st else st.get("Result", {})


def _flat_keys(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from _flat_keys(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _flat_keys(v)


def _terminal_cases():
    cases = []
    for sf, name, st, _ in _parallel_states():
        if _projects_its_result(st):
            continue
        for i, branch in enumerate(st.get("Branches", [])):
            for tname, tst in _branch_terminals(branch):
                cases.append(
                    pytest.param(sf, name, i, tname, tst, id=f"{sf}-{name}[{i}]-{tname}")
                )
    return cases


def _parallel_cases():
    return [
        pytest.param(sf, name, st, definition, id=f"{sf}-{name}")
        for sf, name, st, definition in _parallel_states()
    ]


def test_the_definitions_contain_parallel_states_to_guard():
    """A guard that silently matches nothing is not a guard.

    If every Parallel is ever removed this fails loudly rather than passing
    vacuously — the failure mode the fleet keeps re-finding in detectors that
    report a true number about a smaller world than their name implies.
    """
    assert list(_parallel_states()), "no Parallel state found in any SF definition"
    assert _terminal_cases(), "no Parallel branch terminal discovered to check"


@pytest.mark.parametrize("sf,parallel,branch,tname,tst", _terminal_cases())
def test_every_parallel_branch_terminal_replaces_its_payload(
    sf, parallel, branch, tname, tst
):
    """The 2026-08-14 defect, stated as an invariant.

    `ResultPath: "$.branch_x"` on a branch terminal means the branch returns
    everything it accumulated plus a status. Nest the envelope under that same
    key inside `Parameters`/`Result` and drop `ResultPath` instead: every
    post-join JSONPath resolves identically and the branch's return size stops
    depending on how much state it walked through.
    """
    assert _replaces_its_payload(tst), (
        f"{sf}: {parallel} branch {branch} terminal {tname} forwards its branch "
        f"payload (Type={tst.get('Type')!r}, ResultPath={tst.get('ResultPath')!r}). "
        "A Parallel branch terminal must be a Pass carrying Parameters/Result with "
        "no ResultPath, so the branch returns a bounded outcome envelope and its "
        "substantive result is read from S3 by the join (alpha-engine-config-I8194)."
    )


@pytest.mark.parametrize("sf,parallel,st,definition", _parallel_cases())
def test_worst_case_parallel_result_is_a_rounding_error_against_the_quota(
    sf, parallel, st, definition
):
    """Bound the join's result from the definition, in bytes.

    The structural test above says the shape is right; this one says the shape
    actually buys the headroom. On 2026-08-14 this number was 323,876 for
    ParityParallel.
    """
    if _projects_its_result(st):
        pytest.skip(f"{parallel} projects its result with a scalar ResultSelector")
    total = 2 + 2 * len(st.get("Branches", []))  # brackets and separators
    for i, branch in enumerate(st.get("Branches", [])):
        sizes = []
        for _, tst in _branch_terminals(branch):
            envelope = _outcome_envelope(tst)
            charged = sum(
                _JSONPATH_FIELD_ALLOWANCE if k.endswith(".$") else 0
                for k in _flat_keys(envelope)
            )
            sizes.append(len(json.dumps(envelope)) + charged)
        assert sizes, f"{parallel} branch {i} has no terminal"
        total += max(sizes)
    assert total < _ENVELOPE_BUDGET, (
        f"{sf}: {parallel}'s worst-case branch array is {total} bytes, over the "
        f"{_ENVELOPE_BUDGET}-byte envelope budget ({_PAYLOAD_LIMIT}-byte SF quota, "
        "which the state's own input also has to fit inside)."
    )


@pytest.mark.parametrize("sf,parallel,st,definition", _parallel_cases())
def test_every_post_join_path_resolves_against_every_branch_terminal(
    sf, parallel, st, definition
):
    """Shrinking the payload must not drop what the join reads.

    Derived both ways: the paths come from every JSONPath in the definition
    that reads the Parallel's own ResultPath, and the fields come from the
    branch terminals themselves. A projection that omits a field some
    downstream `Parameters.$` reads throws `States.Runtime` at the join — the
    same class the branch-terminal comments already warn about, now enforced.
    """
    result_path = st.get("ResultPath")
    if not result_path or not result_path.startswith("$."):
        pytest.skip(f"{parallel} does not store its result under a named path")
    root = result_path[2:]
    text = json.dumps(definition)
    branches = st.get("Branches", [])

    pattern = re.compile(r"\$\." + re.escape(root) + r"\[(\d+)\]((?:\.[A-Za-z0-9_]+)+)")
    seen = set()
    for m in pattern.finditer(text):
        idx, rest = int(m.group(1)), m.group(2)
        seen.add((idx, rest))
        assert idx < len(branches), (
            f"{sf}: {parallel} is read at index {idx} but has {len(branches)} branches"
        )
        keys = rest.lstrip(".").split(".")
        for tname, tst in _branch_terminals(branches[idx]):
            node = _outcome_envelope(tst)
            trail = []
            for key in keys:
                trail.append(key)
                assert isinstance(node, dict), (
                    f"{sf}: {parallel}[{idx}] terminal {tname} — "
                    f"$.{root}[{idx}].{'.'.join(trail)} is read downstream but the "
                    "envelope is not an object at that level"
                )
                if key in node:
                    node = node[key]
                elif f"{key}.$" in node:
                    node = {}
                else:
                    raise AssertionError(
                        f"{sf}: {parallel}[{idx}] terminal {tname} does not provide "
                        f"$.{root}[{idx}].{'.'.join(trail)}, which the definition "
                        "reads after the join. Every terminal a branch can end at "
                        "must set every field the join extracts, or Parameters.$ "
                        "throws States.Runtime on whichever path ran."
                    )
    assert seen, (
        f"{sf}: nothing in the definition reads $.{root} — a Parallel whose result "
        "no state consumes is either dead weight or a missing join"
    )
