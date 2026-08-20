"""Legal escape sequences inside ASL intrinsic-function string literals.

AWS Step Functions intrinsic-function string literals (States.Format /
States.Array / ...) CANNOT contain or escape an embedded single quote —
shell-style ``'\\''`` doubling is NOT valid ASL and fails AWS's
SCHEMA_VALIDATION at create/update time with "must be a valid JSONPath or
a valid intrinsic function call". The repo's structural wiring tests
validate JSON shape, not AWS's ASL grammar, so this class is invisible
locally and only detonates at deploy (2026-07-01 first instance,
confirmed live via validate-state-machine-definition; 2026-07-14 second
instance took down the alpha-engine-orchestration CFN update when the
advisory + modelzoo child pipelines failed CREATE — apostrophes inside
their HandleFailure States.Format messages). The full structural fix
(validate every definition against the real AWS API before
UpdateStateMachine) is alpha-engine-config#1897; this test closes the
one grammar rule that has actually fired, cheaply and offline.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
SF_FILES = sorted(_INFRA.glob("step_function*.json"))


def _iter_payload_intrinsics(payload, path: str, key_path: str = ""):
    """Yield (key_path, intrinsic) for every intrinsic string ANYWHERE inside a
    payload template, not only at its top level.

    The top-level-only walk this replaces was itself a detection blind spot:
    the SSM ``sendCommand`` states nest their ``commands.$`` intrinsic under
    ``Parameters/Parameters``, so neither grammar rule below could ever see the
    field that has broken the deploy twice (2026-08-19, config-I7267)."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            sub = f"{key_path}/{key}" if key_path else key
            if isinstance(value, str) and value.startswith("States."):
                yield sub, value
            else:
                yield from _iter_payload_intrinsics(value, path, sub)
    elif isinstance(payload, list):
        for i, item in enumerate(payload):
            yield from _iter_payload_intrinsics(item, path, f"{key_path}[{i}]")


def _iter_intrinsic_fields(states: dict, path: str = ""):
    for name, state in states.items():
        here = f"{path}/{name}"
        for key, value in _iter_payload_intrinsics(state.get("Parameters") or {}, here):
            yield here, key, value
        if "States" in state:
            yield from _iter_intrinsic_fields(state["States"], here)
        for branch in state.get("Branches", []):
            yield from _iter_intrinsic_fields(branch.get("States", {}), here)
        for proc_key in ("ItemProcessor", "Iterator"):
            proc = state.get(proc_key)
            if proc and "States" in proc:
                yield from _iter_intrinsic_fields(proc["States"], here)


ASL_VALID_ESCAPES = set("\\'{}")


def _escape_offenders(value: str):
    """Offending offsets in one intrinsic string.

    Measured against ``validate-state-machine-definition`` 2026-08-20 — the
    ASL intrinsic grammar accepts exactly four escapes inside a ``'``-quoted
    literal (``\\\\``, ``\\'``, ``\\{``, ``\\}``) and no backslash at all
    outside one. A literal double quote is fine UNQUOTED (``'say " hi'``
    validates); it is ``\\"`` — the shell habit of escaping it — that AWS
    rejects. Likewise ``'a\\'b'`` is legal while the shell splice
    ``'a'\\''b'`` is not."""
    out, in_literal, i = [], False, 0
    while i < len(value):
        c = value[i]
        if c == "\\":
            nxt = value[i + 1] if i + 1 < len(value) else ""
            if not in_literal:
                out.append((i, f"backslash outside a quoted literal: {value[i:i+2]!r}"))
            elif nxt not in ASL_VALID_ESCAPES:
                out.append((i, f"illegal escape {value[i:i+2]!r} (legal: \\\\ \\' \\{{ \\}})"))
            i += 2
            continue
        if c == "'":
            in_literal = not in_literal
        i += 1
    return out


@pytest.mark.parametrize("sf_path", SF_FILES, ids=lambda p: p.name)
def test_intrinsic_escape_sequences_are_legal(sf_path: Path):
    """The grammar rule that has now broken the deploy three times.

    2026-07-01 and 2026-07-14: shell ``'\\''`` splices. 2026-08-19
    (alpha-engine-config-I7267, nousergon-data #1447): ``grep -qF
    "\\"timed_out\\": true"`` inside the two PitParity ``commands.$``
    intrinsics. Every in-repo test passed — the AWS validator runs only inside
    ``deploy-infrastructure.sh``, POST-merge — so its all-or-nothing preflight
    blocked the deploy of ALL FOUR definitions for three consecutive merges,
    froze the live SF stamp one SHA behind ``main``, and the next morning's
    ``ne-preopen-trading-pipeline`` halted on its ``DeployDriftGate``: a whole
    unmanaged trading session, caused by quoting in a weekly-pipeline stage."""
    doc = json.loads(sf_path.read_text())
    offenders = [
        f"{sf_path.name}{state_path} :: {key} @ char {off}: {why}"
        for state_path, key, value in _iter_intrinsic_fields(doc.get("States", {}))
        for off, why in _escape_offenders(value)
    ]
    assert not offenders, (
        "Illegal escape sequence in an ASL intrinsic — AWS rejects the whole "
        "definition at deploy time (SCHEMA_VALIDATION_FAILED), which blocks "
        "the all-or-nothing deploy of EVERY definition and freezes the live "
        "SF stamp behind main:\n  " + "\n  ".join(offenders)
    )


# --- unsupported top-level state fields ---------------------------------
#
# Second offline ASL-grammar rule, same philosophy as the single-quote check
# above: the structural wiring tests validate JSON shape, not AWS's ASL
# grammar, so an unsupported field placed at a state's TOP LEVEL (rather than
# inside its Parameters payload) is invisible locally and only detonates at
# deploy. 2026-07-15 instance: alpha-engine-config-I2702 (nousergon-data PR
# #850) added a second, precondition-probe-worded ``Message.$`` at the TOP
# LEVEL of the ``SkipEODReconcileDataGap`` sns:publish Task — a copy that was
# meant to live inside ``Parameters`` (where the state already has its real,
# test-pinned ``Message.$``). ``Message.$`` is a task-input field, only legal
# under ``Parameters``; at the state level AWS rejects the whole definition
# with ``SCHEMA_VALIDATION_FAILED: Field 'Message.$' is not supported at
# /States/SkipEODReconcileDataGap`` — which took down the Deploy Infrastructure
# workflow's UpdateStateMachine step (run 29456675231, main went RED).
#
# This test closes that grammar rule cheaply and offline. The complete
# structural fix (validate every definition against the real AWS API via
# validate-state-machine-definition before UpdateStateMachine) remains
# alpha-engine-config#1897.

# Top-level fields the Amazon States Language allows per state Type. Union of
# the fields common to all states plus each Type's own. Intentionally the full
# spec set (not just fields this repo uses today) so a future *legal* field is
# not a false positive, while an unsupported field (a payload key that leaked
# out of Parameters, a typo) is caught. ``.$``-suffixed keys are NEVER legal at
# the state level — that suffix is only for JSONPath substitution inside
# Parameters / ResultSelector / ItemSelector payload templates.
_ASL_COMMON = {"Type", "Comment", "QueryLanguage"}
_ASL_FIELDS = {
    "Task": _ASL_COMMON | {
        "Resource", "Parameters", "Arguments", "Credentials", "ResultPath",
        "ResultSelector", "Retry", "Catch", "TimeoutSeconds",
        "TimeoutSecondsPath", "HeartbeatSeconds", "HeartbeatSecondsPath",
        "Next", "End", "InputPath", "OutputPath", "Assign", "Output",
    },
    "Choice": _ASL_COMMON | {
        "Choices", "Default", "InputPath", "OutputPath", "Assign",
    },
    "Pass": _ASL_COMMON | {
        "Result", "ResultPath", "Parameters", "InputPath", "OutputPath",
        "Next", "End", "Assign", "Output",
    },
    "Wait": _ASL_COMMON | {
        "Seconds", "Timestamp", "SecondsPath", "TimestampPath", "Next", "End",
        "InputPath", "OutputPath", "Assign",
    },
    "Succeed": _ASL_COMMON | {"InputPath", "OutputPath", "Assign", "Output"},
    "Fail": _ASL_COMMON | {"Cause", "CausePath", "Error", "ErrorPath"},
    "Parallel": _ASL_COMMON | {
        "Branches", "ResultPath", "ResultSelector", "Retry", "Catch", "Next",
        "End", "InputPath", "OutputPath", "Parameters", "Arguments", "Assign",
        "Output",
    },
    "Map": _ASL_COMMON | {
        "ItemProcessor", "Iterator", "ItemsPath", "ItemReader", "ItemSelector",
        "ItemBatcher", "ResultWriter", "MaxConcurrency", "MaxConcurrencyPath",
        "ResultPath", "ResultSelector", "Retry", "Catch", "Next", "End",
        "InputPath", "OutputPath", "Parameters", "Arguments",
        "ToleratedFailurePercentage", "ToleratedFailurePercentagePath",
        "ToleratedFailureCount", "ToleratedFailureCountPath", "Assign",
        "Output",
    },
}


def _iter_states(states: dict, path: str = ""):
    """Yield (state_path, state_name, state_dict) for every state, recursing
    into nested Parallel Branches and Map ItemProcessor/Iterator sub-states."""
    for name, state in states.items():
        if not isinstance(state, dict):
            continue
        here = f"{path}/{name}"
        yield here, name, state
        for branch in state.get("Branches", []):
            yield from _iter_states(branch.get("States", {}), here)
        for proc_key in ("ItemProcessor", "Iterator"):
            proc = state.get(proc_key)
            if isinstance(proc, dict) and "States" in proc:
                yield from _iter_states(proc["States"], here)


@pytest.mark.parametrize("sf_path", SF_FILES, ids=lambda p: p.name)
def test_no_unsupported_top_level_state_fields(sf_path: Path):
    doc = json.loads(sf_path.read_text())
    offenders = []
    for state_path, _name, state in _iter_states(doc.get("States", {})):
        state_type = state.get("Type")
        allowed = _ASL_FIELDS.get(state_type)
        if allowed is None:
            offenders.append(f"{sf_path.name}{state_path} :: unknown Type={state_type!r}")
            continue
        for key in state:
            # A payload/JSONPath-substitution key (Message.$, Payload.$, ...)
            # is only legal inside Parameters — never at the state level.
            if key.endswith(".$") or key not in allowed:
                offenders.append(f"{sf_path.name}{state_path} :: {key} (Type={state_type})")
    assert not offenders, (
        "Unsupported top-level state field(s) — AWS rejects the definition at "
        "deploy time (SCHEMA_VALIDATION_FAILED: 'Field <X> is not supported'). "
        "A task-input field (Message.$, Payload, ...) belongs inside "
        "Parameters, not at the state level:\n  " + "\n  ".join(offenders)
    )
