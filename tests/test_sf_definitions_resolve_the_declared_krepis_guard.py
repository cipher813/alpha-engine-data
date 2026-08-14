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
``/opt/nousergon/bin/lib-python``. Derived from the parsed JSON string
content, not a line-number list, so a 21st site — or a site in a definition
that gains one later — is covered automatically.

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

#: Sites deliberately NOT yet resolving the guard, pending a separate fix.
#: (definition filename, interpreter) -> tracking issue. This is NOT the
#: dashboard co-tenant venv `CO_TENANT` I7364 targets — it is the
#: `crucible-executor` module's OWN dedicated live-trading box's venv
#: (local dir `alpha-engine`, distinct from `alpha-engine-dashboard`).
#: `/opt/nousergon/bin/lib-python` is installed by
#: `nous-ergon-ops/alpha-engine-dashboard/live/infrastructure/bin/install-box-config.sh`,
#: scoped to the dashboard box and the ephemeral spot instances I7343's
#: launchers provision — there is no evidence it is provisioned on the
#: executor's own persistent box, and repointing this blind risks breaking
#: `krepis.ssm_log_capture` on the LIVE weekday preopen trading pipeline.
#: Tracked: alpha-engine-config-I7365. Remove this entry once resolved there.
KNOWN_UNGUARDED_SITES: dict[tuple[str, str], str] = {
    (
        "step_function_daily.json",
        "/home/ec2-user/alpha-engine/.venv/bin/python",
    ): "alpha-engine-config-I7365",
}

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
        text = path.read_text()
        assert needle not in text, (
            f"{path.name}: contains {needle!r} — a krepis module invoked "
            "through the co-tenant venv."
        )
