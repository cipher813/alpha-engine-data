"""No install path in this repo may start a scheduled workload service.

WHY
---
2026-08-28 03:00:42 UTC, a `crucible-dashboard` deploy ran an installer whose
only scheduling action was `systemctl enable --now <x>.timer`. Within four
seconds three timer-driven services started — none due, one of them a weekly
OSS bakeoff that spends real LLM tokens. All failed, and box-health paged
critical (alpha-engine-config-I9000).

THE MECHANISM, measured, not assumed
------------------------------------
`Requires=` in a TIMER's `[Unit]` section is a START dependency **of the
timer**, not a declaration of what the timer triggers. `systemctl enable --now
<x>.timer` therefore enqueues a start job for `<x>.service` as well — and does
so even when the timer is already active and no calendar point has elapsed,
because the transaction still carries the dependency jobs. `Wants=` does the
same weakly.

`Persistent=` catch-up was the standing hypothesis and it is WRONG: a
`Persistent=` replay WRITES the timer's stamp under `/var/lib/systemd/timers/`,
and the stamps showed no elapse at the time of the incident.

THIS REPO'S TWO INSTANCES
-------------------------
Measured live on i-09b539c844515d549 on 2026-08-28, after `crucible-dashboard`
PR792 had cleaned its own ten timers:

    daily-news.timer                requires=[sysinit.target daily-news.service -.mount]
    systemd-unit-drift-check.timer  requires=[systemd-unit-drift-check.service -.mount sysinit.target]

`install-daily-news.sh` arms both, and `install-metron-intraday.sh` arms the
drift-check timer on a second box, so every run of either installer restarted
the news collector and the drift check off-schedule. `daily-news.service` is the
04:00 PT feeder that `morning-signal.service` declares `Wants=`/`After=` on via
its `10-after-news.conf` drop-in, and the digest it writes is read by a
freshness guard a mistimed run trips — this repo owns the most expensive unit in
the class.

WHAT THIS TEST PINS
-------------------
The causal chain has two links, and breaking either one is a valid fix:

    installer says `enable --now <timer>` / `start <unit>`
        -> the unit's [Unit] dependencies pull in <service>
            -> <service> is a scheduled workload and runs off-schedule

So this is deliberately NOT a grep for `--now`. It models the chain: it reads
the start-shaped `systemctl` invocations out of every `infrastructure/install-*.sh`,
expands each started unit through the `Requires=` / `Requisite=` / `BindsTo=` /
`Wants=` edges declared in this repo's own unit files and drop-ins, and fails if
the resulting start set contains a scheduled workload. Restoring
`Requires=daily-news.service` to `daily-news.timer` fails it again; so would
adding `systemctl start daily-news.service` to an installer while the timers
stay clean. Either removal passes, which is correct — either alone breaks the
chain.

A "scheduled workload" is DERIVED, not listed: a `Type=oneshot` service that
some timer in this repo names as its `Unit=` (or triggers by the same-basename
default). That is exactly the class whose whole contract is "runs when the clock
says so".

THE ESCAPE HATCH LIVES IN THE UNIT, NOT HERE
--------------------------------------------
A service that an install path is MEANT to start declares `X-InstallMayStart=yes`
under its own `[Unit]`, with the reason written beside it — the convention
`crucible-dashboard` PR792 established, and the same shape as `X-DeadManStaleness=`,
so the justification sits next to the thing it justifies and travels with a unit
that moves repos. `X-` keys are ignored by systemd, so it costs nothing on the
box, and `crucible-dashboard`'s live `install_start_dependency_scan` in
`infrastructure/box_health.sh` reads it via `systemctl cat` (`systemctl show`
does not surface `X-` keys). No unit in THIS repo declares it today — both of
ours were fixed by dropping the edge — and a declaration no install path
exercises is failed below, so the set cannot rot into a stale allowlist.

SCOPE, stated because it bounds the guarantee
---------------------------------------------
Source-text analysis. The installers run as root on an EC2 box against
`/home/ec2-user` paths, so executing them in CI is not meaningful; what is
pinned is the CONTRACT between the scripts and the unit files in this repo.

Two gaps this cannot see, named rather than left implicit:
  * a `systemctl <verb> "$unit"` whose unit name is a shell variable — tokens
    containing `$` are skipped;
  * dependency edges declared by units this repo does not own, so a chain that
    leaves and re-enters this repo's unit set is invisible here. The live
    backstop for that is `crucible-dashboard`'s box-side
    `install_start_dependency_scan`, which reads merged systemd state on the box
    and is blind to no repo.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parents[1] / "infrastructure"
SYSTEMD = INFRA / "systemd"

# systemctl verbs that cause a unit to be ACTIVATED. `enable` alone and
# `daemon-reload` do not, which is why they are absent.
_START_RE = re.compile(
    r"systemctl\s+(?P<flags>(?:--\S+\s+)*)"
    r"(?P<verb>start|restart|enable|reenable|reload-or-restart|try-restart)\s+"
    r"(?P<rest>[^\n;|&)]*)"
)
_DEP_KEYS = ("Requires", "Requisite", "BindsTo", "Wants")

# `reenable` rewrites symlinks and does NOT activate; it is matched above only so
# that a following `start` on the same unit is not the sole signal, and dropped
# here.
_INERT_VERBS = {"reenable"}


def _sections(text: str) -> dict[str, list[str]]:
    """Split a unit file into {section: [lines]}, ignoring comments."""
    out: dict[str, list[str]] = {}
    current = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1]
            out.setdefault(current, [])
            continue
        out.setdefault(current, []).append(line)
    return out


def _directive(lines: list[str], key: str) -> list[str]:
    vals: list[str] = []
    for line in lines:
        k, _, v = line.partition("=")
        if k.strip() == key:
            vals.extend(v.split())
    return vals


def _load_units() -> dict[str, dict[str, list[str]]]:
    """Every unit this repo installs, with its drop-ins merged in."""
    units: dict[str, dict[str, list[str]]] = {}
    for path in sorted(SYSTEMD.glob("*")):
        if path.is_dir() or path.suffix not in (".service", ".timer"):
            continue
        units[path.name] = _sections(path.read_text())
    for dropin_dir in sorted(SYSTEMD.glob("*.service.d")):
        target = units.setdefault(dropin_dir.name[: -len(".d")], {})
        for conf in sorted(dropin_dir.glob("*.conf")):
            for section, lines in _sections(conf.read_text()).items():
                target.setdefault(section, []).extend(lines)
    return units


UNITS = _load_units()


def _triggered_service(timer: str, sections: dict[str, list[str]]) -> str:
    explicit = _directive(sections.get("Timer", []), "Unit")
    if explicit:
        return explicit[-1]
    return timer[: -len(".timer")] + ".service"


def _scheduled_workloads() -> set[str]:
    """Type=oneshot services that a timer in this repo exists to trigger."""
    out = set()
    for name, sections in UNITS.items():
        if not name.endswith(".timer"):
            continue
        target = _triggered_service(name, sections)
        target_sections = UNITS.get(target)
        if target_sections is None:
            continue
        if "oneshot" in _directive(target_sections.get("Service", []), "Type"):
            out.add(target)
    return out


# Shell noise that is not a unit name. `>/dev/null` and the `2>` left behind when
# `_START_RE` stops at the `&` of `2>&1` both survive a naive split, and
# `crucible-dashboard` PR792's copy of this parser reports them as units named
# `>/dev/null.service` and `2>.service`. They can never match a real unit, so
# they change no verdict — but a guard whose output contains obvious garbage is
# a guard nobody trusts the day it fires for real.
_NOT_A_UNIT_NAME = set("<>&(){}[]!\\`")


def _normalise(token: str) -> str | None:
    token = token.strip().strip("\"'")
    if not token or token.startswith("-") or "$" in token or "*" in token:
        return None
    if _NOT_A_UNIT_NAME & set(token):
        return None
    if "." not in token.rsplit("/", 1)[-1]:
        return token + ".service"
    return token


# A `systemctl` inside an `echo`/`printf`/`log` is operator guidance printed by
# an installer ("Run now: sudo systemctl start x.service"), not something the
# script does. Reading those as executions is how a guard cries wolf until it is
# deleted — the same class as the 2026-08-27 scan that matched `ne-admin` inside
# a YAML comment. Full-line comments are stripped in `_started_units`; only
# FULL-line, because truncating at any `#` turns a false positive into a false
# negative (`systemctl start x  # why` would stop being seen).
_QUOTED_RE = re.compile(r"\b(echo|printf|log|say|cat)\b")


def _started_units(script: Path) -> set[str]:
    """Units the script activates, by source text."""
    started: set[str] = set()
    for line in script.read_text().splitlines():
        if line.lstrip().startswith("#"):
            continue
        for match in _START_RE.finditer(line):
            prefix = line[: match.start()]
            if _QUOTED_RE.search(prefix):
                continue
            verb = match.group("verb")
            if verb in _INERT_VERBS:
                continue
            flags = match.group("flags") or ""
            rest = match.group("rest")
            if verb == "enable" and "--now" not in flags and "--now" not in rest:
                continue
            for token in rest.split():
                unit = _normalise(token)
                if unit:
                    started.add(unit)
    return started


def _start_closure(unit: str) -> set[str]:
    """`unit` plus everything starting it pulls in, per this repo's units."""
    seen: set[str] = set()
    stack = [unit]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        sections = UNITS.get(current)
        if sections is None:
            continue
        for key in _DEP_KEYS:
            for dep in _directive(sections.get("Unit", []), key):
                if dep.endswith((".service", ".timer")) and dep not in seen:
                    stack.append(dep)
    return seen


def _install_paths() -> list[Path]:
    return sorted(INFRA.glob("install-*.sh"))


def _may_start(unit: str) -> bool:
    sections = UNITS.get(unit, {})
    return "yes" in _directive(sections.get("Unit", []), "X-InstallMayStart")


def test_the_repo_still_has_scheduled_workloads_to_protect():
    """Guard against the guard silently covering nothing."""
    workloads = _scheduled_workloads()
    assert "daily-news.service" in workloads, workloads
    assert "systemd-unit-drift-check.service" in workloads, workloads
    assert len(workloads) >= 3, workloads


def test_install_paths_are_parsed_at_all():
    """Guard against a regex that matches nothing reporting a clean sweep."""
    paths = _install_paths()
    assert len(paths) >= 2, paths
    installer = INFRA / "install-daily-news.sh"
    assert "daily-news.timer" in _started_units(installer)
    assert "systemd-unit-drift-check.timer" in _started_units(installer)


def test_a_commented_out_systemctl_is_not_read_as_an_execution():
    """The false-positive class that gets guards deleted.

    `install-metron-intraday.sh` carries the line "This script used to copy units
    and `systemctl enable --now` without ever ..." in a full-line comment. A
    scanner that reads it as an execution path reports a unit named `without`.
    """
    started = _started_units(INFRA / "install-metron-intraday.sh")
    assert "without.service" not in started, started
    assert started == {
        "metron-intraday.timer",
        "systemd-unit-drift-check.timer",
    }, started


@pytest.mark.parametrize("script", _install_paths(), ids=lambda p: p.name)
def test_no_install_path_starts_a_scheduled_workload(script: Path):
    workloads = _scheduled_workloads()
    offenders: dict[str, set[str]] = {}
    for unit in _started_units(script):
        pulled = _start_closure(unit) & workloads
        pulled = {u for u in pulled if not _may_start(u)}
        if pulled:
            offenders[unit] = pulled
    assert not offenders, (
        f"{script.name} activates a scheduled workload off-schedule: "
        + "; ".join(
            f"`systemctl start {unit}` pulls in {sorted(pulled)}"
            for unit, pulled in sorted(offenders.items())
        )
        + ". A deploy must never run a timer-driven job. Either drop the start "
        "(`enable` without `--now` still arms the timer) or drop the [Unit] "
        "dependency edge that pulls the service in — a timer binds its service "
        "with `Unit=`, never with `Requires=`. If the start is deliberate and "
        "free, declare `X-InstallMayStart=yes` in that service's [Unit] with "
        "the reason beside it (alpha-engine-config-I9000)."
    )


def test_every_install_may_start_declaration_is_still_exercised():
    """A declaration nobody uses is a stale allowlist entry, not a rationale.

    Vacuous today by design: no unit in this repo declares the key, because both
    of this repo's instances were fixed by dropping the dependency edge rather
    than by allowlisting the start. The assertion exists so the FIRST
    declaration is held to the same bar `crucible-dashboard`'s three are.
    """
    declared = {u for u in UNITS if _may_start(u)}
    started: set[str] = set()
    for script in _install_paths():
        for unit in _started_units(script):
            started |= _start_closure(unit)
    unused = declared - started
    assert not unused, (
        f"X-InstallMayStart=yes declared on {sorted(unused)} but no install path "
        "starts it. Remove the declaration."
    )
