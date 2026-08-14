"""Every spot launcher in this repo resolves through the ops-owned krepis guard.

**The defect** (`alpha-engine-config-I6931`, second half tracked as
`alpha-engine-config-I7343`). Until 2026-08-14 every spot launcher in the fleet
defaulted its interpreter to a co-tenant's checkout::

    LIB_PYTHON="${LIB_PYTHON:-/home/ec2-user/alpha-engine-dashboard/.venv/bin/python}"

— NINE such lines across FIVE repos. So the version of ``krepis.ec2_spot``,
``krepis.ssm_dispatcher`` and ``krepis.spot_bootstrap`` that launches every spot
workload of all three Step Functions pipelines was governed by
``crucible-dashboard/requirements.txt``, which no merge in this repo can see, and
an absent or too-old venv produced a ``ModuleNotFoundError`` at launch rather
than a statement of what was wrong.

**The fix** is one ops-owned wrapper, ``/opt/nousergon/bin/lib-python``
(`nous-ergon-ops-PR676`), which execs the box's DECLARED krepis venv and aborts
with ``EX_CONFIG`` (78) naming the version it found when that venv is absent or
below the launcher floor. It never falls back. Each launcher's diff is the one
line naming it — writing the guard per launcher would be nine copies of one
contract across five repos, which is the `alpha-engine-config-I6922` defect one
layer down.

**What this test holds.** That every launcher in THIS repo resolves through the
interpreter *the host that executes it actually has*, and that no launcher
re-acquires a private fallback. Every assertion is derived from the scripts on
disk — no line numbers, because these files move.

**The host qualifier was added the hard way** (`alpha-engine-config-I7386`).
`I7343` pointed these defaults at the guard on the premise that a spot launcher
runs "on the dispatcher box". These two do not: they are sourced by
``spot_morning_enrich.sh`` / ``spot_data_phase1.sh`` / ``spot_rag_ingestion.sh``,
which the weekly SF delivers as ``ssm:sendCommand`` payloads to
``$.ec2_instance_id`` — the ephemeral weekly-freshness spot, whose bootstrap
(``infrastructure/lambdas/weekly-freshness-spot-dispatcher/index.py``,
``_bootstrap_command``) builds ``/home/ec2-user/alpha-engine-dashboard/.venv``
and never creates ``/opt/nousergon``. Measured on execution
``friday-shell-2026-08-14-validate-i7382``, state ``MorningEnrich``::

    _spot_common.sh: line 128: /opt/nousergon/bin/lib-python:
      No such file or directory
    failed to run commands: exit status 127

So the rule is per host, not per repo:

* a launcher executed on the dashboard box -> the guard, always;
* a launcher executed on an ephemeral spot -> the interpreter that spot's own
  bootstrap builds. Nothing else exists there to resolve.

This is the exact twin of the SF-definition layer's carve-out in
``tests/test_sf_definitions_resolve_the_declared_krepis_guard.py``
(`alpha-engine-config-I7382`). The SOTA close for both — install the guard on
the spot, then restore both defaults — is `alpha-engine-config-I7383`.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_INFRA = Path(__file__).resolve().parents[1] / "infrastructure"

#: The one path every launcher repo's LIB_PYTHON default must name. Declared and
#: installed by nous-ergon-ops (``bin/lib-python`` +
#: ``bin/install-box-config.sh``); asserted identically in all five repos.
GUARD = "/opt/nousergon/bin/lib-python"

#: The pre-I7343 default. Its reappearance anywhere executable is the regression.
CO_TENANT = "/home/ec2-user/alpha-engine-dashboard/.venv/bin/python"

#: Launcher scripts this repo is known to own. A rename must update this list;
#: a DELETION must not silently drop coverage, which is what the membership
#: assertion below catches. Filenames only — never line numbers.
KNOWN_LAUNCHERS = {'_spot_common.sh', 'spot_data_weekly.sh'}

#: Launchers that execute ON THE EPHEMERAL SPOT rather than on the dashboard
#: box, and therefore cannot resolve `GUARD` — see the module docstring for the
#: measurement. Each must default to `CO_TENANT`, the venv the spot's own
#: bootstrap builds, until `alpha-engine-config-I7383` puts the guard there too.
#:
#: Not a blanket amnesty: `test_spot_hosted_launchers_are_really_spot_hosted`
#: below re-derives this membership from `infrastructure/step_function.json` —
#: a script listed here must actually be reachable from an SSM command array
#: that targets `$.ec2_instance_id` — so a dashboard-box launcher cannot be
#: added to this set to make a failing assertion go away.
SPOT_HOSTED_LAUNCHERS = {'_spot_common.sh', 'spot_data_weekly.sh'}


def _expected_default(name: str) -> str:
    """The interpreter this launcher's host actually provides."""
    return CO_TENANT if name in SPOT_HOSTED_LAUNCHERS else GUARD

_ASSIGN = re.compile(r'^([ \t]*)LIB_PYTHON=(.*)$', re.M)
_DEFAULTED = re.compile(r'^[ \t]*LIB_PYTHON="\$\{LIB_PYTHON:-([^}]*)\}"[ \t]*$')


def _shell_scripts() -> list[Path]:
    """The SPOT-LAUNCHER surface, derived from the naming convention rather than
    listed: ``spot_*.sh`` plus the ``_spot_common.sh`` they source.

    Scoped this way on purpose. Other scripts under ``infrastructure/`` — the
    dashboard box's own health, alert and deploy units — legitimately name that
    box's venv for their own service; they are not launching a spot and I7343
    does not touch them. A filename denylist would need editing every time a box
    service is added; this derivation does not.
    """
    return sorted(
        p
        for p in _INFRA.rglob("*.sh")
        if p.is_file() and (p.name.startswith("spot_") or p.name.startswith("_spot"))
    )


def _assignment_sites() -> dict[str, list[str]]:
    """{script name: [each raw LIB_PYTHON assignment line]} — derived, not listed."""
    out: dict[str, list[str]] = {}
    for path in _shell_scripts():
        hits = [m.group(0) for m in _ASSIGN.finditer(path.read_text())]
        if hits:
            out[path.name] = hits
    return out


def _executable_lines(path: Path) -> list[str]:
    """Non-comment, non-blank lines. Comments legitimately quote the old default
    (the whole rationale is about it), so the sweeps run over code only."""
    return [
        line
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def test_every_known_launcher_still_declares_its_interpreter():
    """A launcher that stops assigning LIB_PYTHON has not become safe — it has
    become one that inherits whatever the caller's environment happens to hold,
    which is the pre-guard behaviour with no line to grep for."""
    found = _assignment_sites()
    missing = KNOWN_LAUNCHERS - set(found)
    assert not missing, (
        f"launchers with no LIB_PYTHON assignment: {sorted(missing)}. If one was "
        "renamed, update KNOWN_LAUNCHERS; if one was deleted, this test's coverage "
        "shrank and that must be a deliberate, reviewed diff."
    )


def test_every_launcher_defaults_to_the_interpreter_its_host_has():
    """The load-bearing assertion: the default names the guard on the dashboard
    box, and the spot's own venv on a spot-hosted launcher — in every script
    that assigns it, including any launcher added after this test was written.

    A launcher NOT listed in SPOT_HOSTED_LAUNCHERS gets the strict I7343 rule
    with no softening: it runs where the guard exists, so nothing else is
    acceptable there.
    """
    for name, lines in sorted(_assignment_sites().items()):
        expected = _expected_default(name)
        for line in lines:
            match = _DEFAULTED.match(line)
            assert match, (
                f"{name}: LIB_PYTHON assignment is not the "
                'LIB_PYTHON="${LIB_PYTHON:-<path>}" form: {line.strip()!r}. The '
                "override idiom is what lets a rehearsal point at another "
                "interpreter without editing the script."
            )
            if expected is GUARD:
                assert match.group(1) == GUARD, (
                    f"{name}: LIB_PYTHON defaults to {match.group(1)!r}, not the "
                    f"ops-owned guard {GUARD!r}. This launcher runs on the "
                    "dashboard box, where the guard exists; pointing it at a "
                    "repo-local or co-tenant venv restores the "
                    "alpha-engine-config-I6931 defect — the krepis version that "
                    "launches every spot stage becomes whatever that checkout "
                    "happens to hold, with no declared floor."
                )
            else:
                assert match.group(1) == CO_TENANT, (
                    f"{name}: LIB_PYTHON defaults to {match.group(1)!r}. This "
                    "launcher is delivered by ssm:sendCommand to the ephemeral "
                    "weekly-freshness spot, which has no /opt/nousergon — "
                    f"defaulting to {GUARD!r} there makes every stage exit 127 "
                    "(alpha-engine-config-I7386). It must name "
                    f"{CO_TENANT!r}, the venv that spot's own bootstrap builds, "
                    "until alpha-engine-config-I7383 installs the guard there."
                )


def test_spot_hosted_launchers_are_really_spot_hosted():
    """SPOT_HOSTED_LAUNCHERS is an exemption from the strict guard rule, so its
    membership is re-derived rather than trusted: a script named there must be
    reachable from an SSM command array in `infrastructure/step_function.json`
    that targets `$.ec2_instance_id`.

    Without this, a dashboard-box launcher could be added to the set to silence
    a failing assertion, which is the shape this test exists to prevent one
    level up.
    """
    sf_path = _INFRA / "step_function.json"
    data = json.loads(sf_path.read_text())

    spot_targeted_text: list[str] = []

    def walk(node):
        if isinstance(node, dict):
            for state in (node.get("States") or {}).values():
                if not isinstance(state, dict):
                    continue
                params = state.get("Parameters")
                if isinstance(params, dict) and params.get("InstanceIds.$") == "$.ec2_instance_id":
                    spot_targeted_text.append(json.dumps(params))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    assert spot_targeted_text, (
        "no state in step_function.json targets $.ec2_instance_id — either the "
        "definition's shape moved or the spot dispatch was removed; this "
        "derivation must be fixed before its exemption can be trusted."
    )
    joined = "\n".join(spot_targeted_text)

    # `_spot_common.sh` is never named in a command array — it is SOURCED by the
    # spot_*.sh scripts that are. Treat it as spot-hosted iff at least one
    # script that sources it is.
    sourcing_common = {
        p.name
        for p in _shell_scripts()
        if "_spot_common.sh" in p.read_text() and p.name != "_spot_common.sh"
    }

    for name in sorted(SPOT_HOSTED_LAUNCHERS):
        if name == "_spot_common.sh":
            reached = any(n in joined for n in sourcing_common)
            detail = f"no script sourcing it appears in a spot-targeted command (checked {sorted(sourcing_common)})"
        else:
            reached = name in joined
            detail = "it appears in no spot-targeted command array"
        assert reached, (
            f"{name} is exempted from the {GUARD!r} rule on the grounds that it "
            f"runs on the ephemeral spot, but {detail}. Either it runs on the "
            "dashboard box — in which case it must resolve the guard and the "
            "exemption does not apply — or this derivation no longer matches "
            "the definition's shape."
        )


def test_the_env_var_override_is_preserved():
    """``LIB_PYTHON=... script.sh`` must still win. The guard is the DEFAULT, not
    a hardcode — a rehearsal or a second box needs the override."""
    for name, lines in sorted(_assignment_sites().items()):
        for line in lines:
            assert "${LIB_PYTHON:-" in line, (
                f"{name}: LIB_PYTHON is hardcoded, losing the env override: "
                f"{line.strip()!r}"
            )


def test_no_launcher_falls_back_to_a_co_tenant_checkout():
    """The whole defect was a silent fallback to whichever checkout was newest.

    A launcher that keeps a second candidate path — as a fallback branch, a
    ``||``, or a bare reference — recreates it invisibly, and does so while a
    declaration exists to point at, which is worse than the original.
    """
    for path in _shell_scripts():
        offenders = [line for line in _executable_lines(path) if CO_TENANT in line]
        if path.name in SPOT_HOSTED_LAUNCHERS:
            # The ONE declaration line is the host's real interpreter, not a
            # fallback — see the module docstring. Any OTHER executable mention
            # is still the defect this test exists for, so only the single
            # LIB_PYTHON default is forgiven, and only if there is exactly one.
            declarations = [line for line in offenders if _DEFAULTED.match(line)]
            assert len(declarations) == 1, (
                f"{path.name}: expected exactly one LIB_PYTHON default naming "
                f"{CO_TENANT!r}, found {len(declarations)}: {declarations}"
            )
            offenders = [line for line in offenders if line not in declarations]
        assert not offenders, (
            f"{path.name}: executable line(s) name the co-tenant venv "
            f"{CO_TENANT!r} outside the single LIB_PYTHON default: {offenders}. "
            "The launcher must resolve ONE interpreter and nothing else — a "
            "second candidate path silently restores the fallback the declared "
            "floor exists to remove."
        )


def test_no_launcher_reimplements_the_guard_locally():
    """Nine copies of one fail-loud contract across five repos is exactly the
    `alpha-engine-config-I6922` defect. The version check lives in
    ``bin/lib-python``, in the repo that owns the box's provisioning — a
    launcher-side ``krepis.__version__`` comparison is that defect returning."""
    for path in _shell_scripts():
        text = path.read_text()
        for line in _executable_lines(path):
            assert "krepis.__version__" not in line, (
                f"{path.name}: a launcher-local krepis version check "
                f"({line.strip()!r}) duplicates the contract "
                f"{GUARD!r} already enforces for all five repos."
            )
        del text
