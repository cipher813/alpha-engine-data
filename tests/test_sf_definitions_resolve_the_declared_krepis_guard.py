"""Every Step Functions command array that invokes a ``krepis.*`` module
resolves through the ops-owned guard, not a co-tenant venv.

**The defect** (`alpha-engine-config-I7364`, one frame above `I7343`). Every
state that launches a spot stage wraps its launcher in a log-capture command::

    /home/ec2-user/alpha-engine-dashboard/.venv/bin/python -m krepis.ssm_log_capture run ...

hardcoded to `crucible-dashboard`'s own venv — a co-tenant service's
dependency surface, with no declared floor and no fail-loud. `I7343` guarded
the NINE launcher scripts' own `LIB_PYTHON` default
(`tests/test_launchers_resolve_the_declared_krepis_guard.py`); that test's
surface is `spot_*.sh` / `_spot*.sh` shell scripts and cannot see a JSON state
machine definition. This test covers the layer `I7343` does not reach: the SF
definitions themselves.

**What this test holds.** Every ``krepis.*`` module invocation found anywhere
in any SF definition JSON in this repo resolves through
``/opt/nousergon/bin/lib-python`` — *unless the command executes on a host
where that guard is not provisioned*, in which case the site is carved out in
``KNOWN_UNGUARDED_SITES`` with a tracking issue. Derived from the parsed JSON
string content, not a line-number list, so a 21st site — or a site in a
definition that gains one later — is covered automatically.

**The host qualifier is load-bearing, and was added the hard way**
(`alpha-engine-config-I7382`). The guard is installed only on the dashboard
box; asserting it against commands that run on an ephemeral spot made every
weekly spot stage exit 127. See ``KNOWN_UNGUARDED_SITES`` for the measurement.

**What is deliberately OUT of scope**, same rationale as I7343's launcher
carve-out: a co-tenant venv invocation that does NOT name a ``krepis.*``
module. Two such sites exist today in ``step_function.json``:

- ``-m training.model_zoo`` — needs `crucible-predictor`'s own environment,
  not krepis.
- the wrapped payload of the health-check state,
  ``.../alpha-engine-dashboard/.venv/bin/python health_checker.py --alert``
  — `crucible-dashboard`'s own script, needing that repo's own dependencies
  (``nousergon_lib``, ``boto3``, dashboard-specific packages), not krepis.
  Its OUTER wrapper (the ``krepis.ssm_log_capture`` call around it) is in
  scope and is asserted below like every other site.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_INFRA = Path(__file__).resolve().parents[1] / "infrastructure"

#: The one interpreter every krepis module invocation in an SF definition
#: must resolve through. Same guard `I7343`'s launchers assert.
GUARD = "/opt/nousergon/bin/lib-python"

#: The pre-fix default. Its reappearance in front of a krepis module call is
#: the regression.
CO_TENANT = "/home/ec2-user/alpha-engine-dashboard/.venv/bin/python"

#: Every SF definition this repo owns, checked for the same shape. Filenames
#: only, and a DELETION or RENAME must update this set deliberately — the
#: coverage-membership test below catches a silent drop.
KNOWN_SF_DEFINITIONS = {
    "step_function.json",
    "step_function_daily.json",
    "step_function_eod.json",
    "step_function_groom.json",
}

#: Sites deliberately NOT resolving the guard, each with a tracking issue.
#: (definition filename, interpreter) -> tracking issue.
#:
#: `/opt/nousergon/bin/lib-python` is installed by
#: `nous-ergon-ops/alpha-engine-dashboard/live/infrastructure/bin/install-box-config.sh`.
#: Everything that script provisions lives under that repo's
#: `alpha-engine-dashboard/live/` tree, so the guard exists on the DASHBOARD
#: BOX and nowhere else.
#:
#: This module's original docstring claimed the guard was also provisioned on
#: "the ephemeral spot instances I7343's launchers provision". That was FALSE,
#: and the assertion built on it took the weekly pipeline down
#: (alpha-engine-config-I7382). The ephemeral weekly-freshness spot is
#: bootstrapped by
#: `infrastructure/lambdas/weekly-freshness-spot-dispatcher/index.py`, whose
#: `_bootstrap_command` clones four repos and builds
#: `/home/ec2-user/alpha-engine-dashboard/.venv`. It never creates
#: `/opt/nousergon`. Measured 2026-08-14 on execution
#: `friday-shell-2026-08-14-verify-i7376-b`, instance `i-07e65950cfeb6405b`,
#: state `MorningEnrich`::
#:
#:     /opt/nousergon/bin/lib-python: No such file or directory
#:     failed to run commands: exit status 127
#:
#: The distinction the guard turns on is therefore WHICH HOST RUNS THE
#: COMMAND, not which definition it sits in:
#:
#:   * a command executed on the dashboard box  -> the guard, always;
#:   * a command executed on an ephemeral spot  -> the interpreter that spot's
#:     own bootstrap actually builds. Nothing else exists there to resolve.
#:
#: All 18 krepis sites in `step_function.json` target `$.ec2_instance_id` —
#: the per-execution spot — so all 18 are in the second category. They are
#: carved out here rather than asserted, and the SOTA close (install the
#: ops-owned guard on the spot as part of its bootstrap, so I7364's declared
#: floor and fail-loud hold on the spot too) is tracked as
#: alpha-engine-config-I7383. Remove this entry once that ships.
#:
#: The `step_function_daily.json` entry is a different box again: the
#: `crucible-executor` module's OWN dedicated live-trading venv (local dir
#: `alpha-engine`, distinct from `alpha-engine-dashboard`). Tracked as
#: alpha-engine-config-I7365.
KNOWN_UNGUARDED_SITES: dict[tuple[str, str], str] = {
    (
        "step_function.json",
        "/home/ec2-user/alpha-engine-dashboard/.venv/bin/python",
    ): "alpha-engine-config-I7383",
    (
        "step_function_daily.json",
        "/home/ec2-user/alpha-engine/.venv/bin/python",
    ): "alpha-engine-config-I7365",
    # alpha-engine-config-I9329. EvalJudgeProcess runs on a DEDICATED
    # ephemeral spot (tag alpha-engine-eval-judge-spot) whose only interpreter
    # is the one crucible-research's own bootstrap builds -- the ops-owned
    # guard is not installed there any more than it is on the weekly launcher
    # above, and for the identical reason. Same SOTA close, same tracking
    # issue: install the guard as part of the spot bootstrap
    # (alpha-engine-config-I7383). Remove this entry with the one above.
    (
        "step_function.json",
        "/home/ec2-user/crucible-research/.venv/bin/python",
    ): "alpha-engine-config-I7383",
}

#: Definitions whose krepis commands run on an ephemeral spot rather than on
#: the dashboard box. A definition listed here MUST have every krepis site
#: targeting `$.ec2_instance_id`; `test_spot_definitions_really_target_a_spot`
#: below re-derives that from the JSON rather than trusting this list, so the
#: carve-out above cannot quietly grow to cover a dashboard-box site.
SPOT_HOSTED_DEFINITIONS = {"step_function.json"}

#: interpreter path immediately followed by `-m krepis.<module>` — matched as
#: literal substrings inside JSON string values (State.Format templates
#: included), so no ASL intrinsic-function evaluation is needed. The
#: interpreter group is restricted to path characters (starts with `/`, no
#: quotes/commas/parens) so it does not walk back across the comma-joined
#: `States.Format(...)` argument list that precedes it with no whitespace.
_KREPIS_INVOCATION = re.compile(r"(/[^\s,'\"()]*) -m (krepis\.[^\s'\"]*)")


def _existing_definitions() -> list[Path]:
    return sorted(p for p in _INFRA.glob("*.json") if p.name in KNOWN_SF_DEFINITIONS)


def _iter_strings(obj):
    """Yield every string leaf in a parsed JSON document, recursively."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _iter_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_strings(v)


def _krepis_invocations(path: Path) -> list[tuple[str, str]]:
    """[(interpreter, module), ...] for every `<interpreter> -m krepis.<mod>`
    substring found anywhere in the parsed JSON's string values."""
    data = json.loads(path.read_text())
    hits: list[tuple[str, str]] = []
    for s in _iter_strings(data):
        hits.extend(_KREPIS_INVOCATION.findall(s))
    return hits


def _krepis_command_states(data) -> list[tuple[str, dict]]:
    """[(state_name, Parameters), ...] for every state whose SSM command array
    invokes a `krepis.*` module.

    Walks `States` maps at any depth (Parallel branches, Map ItemProcessors),
    so a stage nested inside `ResearchPredictorParallel` or `ModelZooTrainMap`
    is found exactly like a top-level one.
    """
    found: list[tuple[str, dict]] = []

    def walk(node):
        if isinstance(node, dict):
            for name, state in (node.get("States") or {}).items():
                if isinstance(state, dict):
                    params = state.get("Parameters")
                    if isinstance(params, dict):
                        commands = params.get("commands.$")
                        if commands is None:
                            inner = params.get("Parameters")
                            if isinstance(inner, dict):
                                commands = inner.get("commands.$")
                        if commands and "-m krepis." in str(commands):
                            found.append((name, params))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return found


def test_every_known_sf_definition_is_present():
    """A definition file renamed or deleted must be a deliberate, reviewed
    diff to this set — not a silent shrink of what this test covers."""
    found = {p.name for p in _INFRA.glob("*.json") if p.name.startswith("step_function")}
    missing = KNOWN_SF_DEFINITIONS - found
    assert not missing, (
        f"expected SF definitions not found on disk: {sorted(missing)}. If one "
        "was renamed, update KNOWN_SF_DEFINITIONS; if deleted, this test's "
        "coverage shrank and that must be a deliberate, reviewed diff."
    )


def test_at_least_one_krepis_invocation_is_found():
    """Guards the regex itself: if the JSON shape changes such that
    `_krepis_invocations` silently stops matching anything, the two tests
    below would pass vacuously. This fails loud instead."""
    total = sum(len(_krepis_invocations(p)) for p in _existing_definitions())
    assert total > 0, (
        "no `<interpreter> -m krepis.<module>` invocation found in any SF "
        "definition — either the fleet's krepis invocations have all been "
        "removed (update this test) or `_krepis_invocations` no longer "
        "matches the JSON's shape (fix the regex/derivation)."
    )


def test_every_krepis_module_invocation_resolves_the_guard():
    """The load-bearing assertion: every krepis module invocation, in every SF
    definition, names the ops-owned guard as its interpreter — except a site
    explicitly named in KNOWN_UNGUARDED_SITES with a tracking issue."""
    for path in _existing_definitions():
        offenders = [
            (interp, mod)
            for interp, mod in _krepis_invocations(path)
            if interp != GUARD
            and (path.name, interp) not in KNOWN_UNGUARDED_SITES
        ]
        assert not offenders, (
            f"{path.name}: krepis module invocation(s) not resolving "
            f"{GUARD!r}: {offenders}. A co-tenant venv fronting a krepis "
            "module call means the version that captures these stages' logs "
            "is governed by another service's requirements.txt, with no "
            "declared floor and no fail-loud — the alpha-engine-config-I6931 "
            "failure mode, one frame above the launchers."
        )


def _ephemeral_spot_instance_fields(data: dict) -> set[str]:
    """The JSONPaths this definition PROVES hold a per-execution spot id.

    Derived, never listed: a field qualifies only if some Pass state in the
    same definition builds it from a dispatcher Task's own
    ``*.instance_id`` result. That is what makes it ephemeral -- the id was
    minted by a launch on THIS execution -- and it is exactly the property the
    carve-out below depends on.

    A hardcoded field list would have re-created the hole this test exists to
    close, one name later: alpha-engine-config-I9329 added a SECOND ephemeral
    spot ($.eval_judge_instance_id, its own dedicated box) and a list would
    have had to be edited by the same person adding the state.
    """
    fields: set[str] = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, str) and ".instance_id" in value:
                    for match in re.finditer(r'\\?"([a-z0-9_]*instance_id)\\?"', value):
                        fields.add(f"$.{match.group(1)}")
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    return fields


def test_spot_definitions_really_target_a_spot():
    """The carve-out above is only legitimate for commands that run somewhere
    the guard is not installed. Re-derive that from the JSON instead of
    trusting `SPOT_HOSTED_DEFINITIONS`: in a spot-hosted definition, every
    state carrying a krepis command array must target `$.ec2_instance_id` —
    the per-execution ephemeral instance — and never a fixed instance id or
    the dashboard box.

    Without this, `KNOWN_UNGUARDED_SITES` would be a blanket amnesty on the
    whole file, and a NEW dashboard-box state added to `step_function.json`
    later would inherit the exemption silently. That is the shape this test
    exists to prevent, one level up.
    """
    for path in _existing_definitions():
        if path.name not in SPOT_HOSTED_DEFINITIONS:
            continue
        data = json.loads(path.read_text())
        ephemeral = _ephemeral_spot_instance_fields(data)
        assert ephemeral, (
            f"{path.name}: no ephemeral-spot instance field could be derived — "
            "the derivation below would then vacuously reject every state. "
            "Either no dispatcher writes an instance id here (in which case "
            "this definition does not belong in SPOT_HOSTED_DEFINITIONS) or "
            "`_ephemeral_spot_instance_fields` no longer matches the JSON."
        )
        offenders = []
        for state_name, params in _krepis_command_states(data):
            target = params.get("InstanceIds.$")
            if target not in ephemeral:
                offenders.append((state_name, target or params.get("InstanceIds")))
        assert not offenders, (
            f"{path.name} is listed in SPOT_HOSTED_DEFINITIONS, so its krepis "
            "commands are exempted from the guard on the grounds that they run "
            "on an ephemeral spot that does not have it. These states do not "
            f"target a per-execution spot id ({sorted(ephemeral)}): {offenders}. "
            "Either they run on the "
            "dashboard box — in which case they must resolve the guard and the "
            "exemption does not apply to them — or this definition no longer "
            "belongs in SPOT_HOSTED_DEFINITIONS."
        )


def test_at_least_one_spot_hosted_krepis_state_is_found():
    """Guards the derivation in the test above: if `_krepis_command_states`
    stops matching the JSON's shape, that test would pass vacuously while the
    blanket-amnesty hole it exists to close silently reopened."""
    total = 0
    for path in _existing_definitions():
        if path.name in SPOT_HOSTED_DEFINITIONS:
            total += len(_krepis_command_states(json.loads(path.read_text())))
    assert total > 0, (
        "no krepis-bearing command state found in any spot-hosted definition — "
        "either `SPOT_HOSTED_DEFINITIONS` is stale or `_krepis_command_states` "
        "no longer matches the JSON's shape."
    )


def test_no_sf_definition_invokes_a_co_tenant_venv_for_a_krepis_module():
    """Belt-and-suspenders on the same defect: the exact conjoined substring
    `<co-tenant venv> -m krepis.` never appears, by direct substring search
    rather than the regex derivation above. Checked as one joined string
    (not per-line) because a single command line legitimately combines an
    in-scope krepis wrapper with an out-of-scope co-tenant payload script —
    e.g. the health-check state's `... -m krepis.ssm_log_capture run ... --
    <co-tenant venv> health_checker.py --alert` — and a line-level search
    would false-positive on that shape."""
    needle = f"{CO_TENANT} -m krepis."
    for path in _existing_definitions():
        if (path.name, CO_TENANT) in KNOWN_UNGUARDED_SITES:
            # Spot-hosted: the guard is not installed on the ephemeral
            # instance, so this venv — the one that spot's own bootstrap
            # builds — is the only interpreter that exists there. Scoped by
            # the same carve-out the assertion above uses, and bounded by
            # test_spot_definitions_really_target_a_spot.
            continue
        text = path.read_text()
        assert needle not in text, (
            f"{path.name}: contains {needle!r} — a krepis module invoked "
            "through the co-tenant venv."
        )
