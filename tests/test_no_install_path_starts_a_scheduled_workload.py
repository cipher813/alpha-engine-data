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

WHERE THE PARSER LIVES
----------------------
`nousergon_lib.systemd_install_guard`, since 2026-08-28. This file was the
SECOND of three copies cut the same day, and the second-adoption trigger had
already fired when it was written; `alpha-engine-config-I9099` is the lift. The
shell-noise and `reenable` fixes this copy carried are in the lifted version,
along with one more of the same class none of the three had: a trailing
`# comment` after a `systemctl` invocation had its words harvested as unit
names.

What stays HERE is what must not become a library allowlist: the paths, the
presence assertions, this narrative, and the failure message that names this
repo's fix.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from nousergon_lib.systemd_install_guard import (
    load_units,
    may_start,
    scheduled_workloads,
    start_closure,
    started_units,
    violations,
)

INFRA = Path(__file__).resolve().parents[1] / "infrastructure"
SYSTEMD = INFRA / "systemd"

UNITS = load_units(SYSTEMD)


def _install_paths() -> list[Path]:
    return sorted(INFRA.glob("install-*.sh"))


def test_the_repo_still_has_scheduled_workloads_to_protect():
    """Guard against the guard silently covering nothing."""
    workloads = scheduled_workloads(UNITS)
    assert "daily-news.service" in workloads, workloads
    assert "systemd-unit-drift-check.service" in workloads, workloads
    assert len(workloads) >= 3, workloads


def test_install_paths_are_parsed_at_all():
    """Guard against a regex that matches nothing reporting a clean sweep."""
    paths = _install_paths()
    assert len(paths) >= 2, paths
    installer = INFRA / "install-daily-news.sh"
    assert "daily-news.timer" in started_units(installer)
    assert "systemd-unit-drift-check.timer" in started_units(installer)


def test_a_commented_out_systemctl_is_not_read_as_an_execution():
    """The false-positive class that gets guards deleted.

    `install-metron-intraday.sh` carries the line "This script used to copy units
    and `systemctl enable --now` without ever ..." in a full-line comment. A
    scanner that reads it as an execution path reports a unit named `without`.
    """
    started = started_units(INFRA / "install-metron-intraday.sh")
    assert "without.service" not in started, started
    assert started == {
        "metron-intraday.timer",
        "systemd-unit-drift-check.timer",
    }, started


@pytest.mark.parametrize("script", _install_paths(), ids=lambda p: p.name)
def test_no_install_path_starts_a_scheduled_workload(script: Path):
    offenders = violations(script, UNITS)
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
    declared = {u for u in UNITS if may_start(u, UNITS)}
    started: set[str] = set()
    for script in _install_paths():
        for unit in started_units(script):
            started |= start_closure(unit, UNITS)
    unused = declared - started
    assert not unused, (
        f"X-InstallMayStart=yes declared on {sorted(unused)} but no install path "
        "starts it. Remove the declaration."
    )
