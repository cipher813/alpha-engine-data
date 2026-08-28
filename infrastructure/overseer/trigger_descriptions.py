#!/usr/bin/env python3
"""trigger_descriptions.py — the AWS-surface half of a playbook's wake declaration.

**The defect this closes (alpha-engine-config-I9045).** Measured 2026-08-28:
``aws scheduler list-schedules`` showed all four
``alpha-engine-alert-drain-{0400,1000,1600,2200}utc`` schedules ``DISABLED``, and
the drain had nonetheless run every day that week (``drain_ledger`` objects for
08-24 through 08-28, each with an EC2 spot run log). The real wake leg is an
EventBridge **Rule**, ``alpha-engine-freshness-monitor-cron``, whose Lambda
invokes the Overseer router directly with ``{"playbook":"alert-drain"}`` on a
freshness CRITICAL (``alpha-engine-config-I3282``). That linkage was written
down — in ``playbooks.yaml``'s ``wake:`` prose, in one YAML file, in one private
repo. **Nothing on the AWS resources said it.** An operator, a cost audit, a
conformance sweep or an incident responder reading the AWS surface alone reached
the wrong answer in whichever direction they happened to be looking.

``principles.md`` §7: a component is not healthy because a list says DISABLED.
Here the inverse bit — the list said DISABLED and the thing was running.

**What this module does.** It derives, from ``playbooks.yaml`` alone, a compact
machine-readable marker for each live AWS trigger resource, which the owning
``deploy.sh`` appends to that resource's ``Description`` on every reconcile. One
derivation, two consumers:

  * ``infrastructure/lambdas/*/deploy.sh --reconcile-triggers`` WRITES it.
  * ``infrastructure/overseer/trigger_surface_drift.py`` ASSERTS it.

Because both call this function, a hand-edited description or a stale deploy is
drift rather than a second opinion, and there is no second copy of the grammar
to keep in sync.

**The grammar**, stable and greppable::

    [wakes: alert-drain] [sibling-legs: events:alpha-engine-freshness-monitor-cron,...]

  ``wakes``         — every playbook this AWS resource can start, whether it
                      targets the router itself (a ``scheduler``/``events``
                      trigger declared on the playbook) or starts a Lambda that
                      dispatches the router in-process (an ``event-time``
                      trigger's ``depends_on``). The two are indistinguishable
                      to whoever is asking "what runs alert-drain".
  ``sibling-legs``  — every OTHER live AWS resource that can start any of those
                      same playbooks. This is the field that answers I9045: read
                      off a DISABLED alert-drain schedule, it names the enabled
                      freshness rule that is doing the work.

**Prose is NOT part of the marker.** Each ``deploy.sh`` keeps its own
human-readable sentence and this module appends the marker to it. The drift
check compares the MARKER only, so an operator improving the prose is not
graded as drift, while the machine-readable half cannot be edited away.

Usage:
  ./trigger_descriptions.py --trigger scheduler:alpha-engine-alert-drain-0400utc \\
      --prose "Overseer alert-drain 0400 UTC daily via the overseer-dispatcher router"
  ./trigger_descriptions.py --trigger events:alpha-engine-freshness-monitor-cron --marker-only
  ./trigger_descriptions.py --list           # every resource playbooks.yaml implies
  ./trigger_descriptions.py --list --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).parent.resolve()
REGISTRY_PATH = HERE / "playbooks.yaml"

#: Surfaces that name a real, probeable AWS trigger resource. Same vocabulary as
#: ``arming.py``'s ``JOINABLE_SURFACES`` and ``automation_pause.py``'s manifest
#: blocks — ``events`` (EventBridge Rule) and ``scheduler`` (EventBridge
#: Scheduler schedule) are DIFFERENT APIs, not aliases.
AWS_SURFACES = ("events", "scheduler")

#: AWS caps both ``events:PutRule --description`` and
#: ``scheduler:{Create,Update}Schedule --description`` at 512 characters, and
#: both TRUNCATE rather than reject on some paths. A truncated marker is a
#: marker that parses to the wrong answer, so the generator refuses instead.
MAX_DESCRIPTION_CHARS = 512


class DescriptionTooLong(ValueError):
    """The prose + marker would exceed what AWS will store intact."""


def load_registry(path: Path = REGISTRY_PATH) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def declared_units(registry: dict) -> list[tuple[str, list[dict]]]:
    """``(unit name, triggers)`` for every playbook and T1 automation.

    Mirrors ``arming.py::declared_units`` in what it walks — both halves of
    ``playbooks.yaml`` declare triggers and both owe an AWS-surface marker.
    """
    units: list[tuple[str, list[dict]]] = []
    for name, spec in sorted((registry.get("playbooks") or {}).items()):
        units.append((name, list((spec or {}).get("triggers") or [])))
    for entry in registry.get("t1_automations") or []:
        units.append((entry.get("name"), list(entry.get("triggers") or [])))
    return units


def _key(surface: str, name: str) -> str:
    return f"{surface}:{name}"


def resource_index(registry: dict) -> dict[str, dict]:
    """``{"scheduler:name": {"wakes": {playbook, ...}, "kinds": {...}}}``.

    An AWS resource earns an entry two ways, and the distinction is recorded but
    deliberately does NOT change ``wakes``:

      ``direct``    — the playbook declares it as its own ``events``/``scheduler``
                      trigger. The resource targets the Overseer router.
      ``event-time``— the playbook declares an ``event-time`` leg whose
                      ``depends_on`` names it. The resource starts some other
                      Lambda, which then invokes the router in-process. This is
                      the alert-drain / freshness-monitor shape, and it is
                      precisely the leg that was invisible.
    """
    index: dict[str, dict] = {}

    def _add(surface: str, name: str, unit: str, kind: str) -> None:
        if surface not in AWS_SURFACES or not name or not unit:
            return
        entry = index.setdefault(_key(surface, name), {"wakes": set(), "kinds": set()})
        entry["wakes"].add(unit)
        entry["kinds"].add(kind)

    for unit, triggers in declared_units(registry):
        for trig in triggers:
            surface = trig.get("surface")
            if surface in AWS_SURFACES:
                _add(surface, trig.get("name"), unit, "direct")
            elif surface == "event-time":
                for dep in trig.get("depends_on") or []:
                    _add(dep.get("surface"), dep.get("name"), unit, "event-time")
    return index


def sibling_legs(index: dict[str, dict], key: str) -> list[str]:
    """Other AWS resources that can start any playbook ``key`` starts."""
    wakes = index[key]["wakes"]
    return sorted(k for k, v in index.items() if k != key and v["wakes"] & wakes)


def marker_for(registry: dict, surface: str, name: str) -> str:
    """The machine-readable suffix for one AWS trigger resource.

    Raises ``KeyError`` when ``playbooks.yaml`` does not imply this resource at
    all. That is not defensiveness: a deploy.sh asking for a marker it cannot
    get is a resource being created with no declaration behind it, which is the
    undeclared-trigger half of the same defect.
    """
    index = resource_index(registry)
    key = _key(surface, name)
    if key not in index:
        raise KeyError(
            f"{key} is not declared in playbooks.yaml — no playbook lists it as a "
            "trigger and no event-time leg names it in depends_on. Declare it "
            "before creating it, or this resource is a wake path nothing records."
        )
    parts = [f"[wakes: {','.join(sorted(index[key]['wakes']))}]"]
    siblings = sibling_legs(index, key)
    if siblings:
        parts.append(f"[sibling-legs: {','.join(siblings)}]")
    return " ".join(parts)


def description_for(registry: dict, surface: str, name: str, prose: str) -> str:
    """``prose`` + the marker, refusing anything AWS would silently truncate."""
    marker = marker_for(registry, surface, name)
    prose = (prose or "").strip()
    full = f"{prose} {marker}".strip() if prose else marker
    if len(full) > MAX_DESCRIPTION_CHARS:
        raise DescriptionTooLong(
            f"{_key(surface, name)}: description is {len(full)} chars, over AWS's "
            f"{MAX_DESCRIPTION_CHARS}-char ceiling (marker alone is {len(marker)}). "
            "Shorten the prose — the marker is not optional and must not be cut."
        )
    return full


def _split_trigger(spec: str) -> tuple[str, str]:
    surface, _, name = spec.partition(":")
    if surface not in AWS_SURFACES or not name:
        raise SystemExit(
            f"--trigger must be '<{'|'.join(AWS_SURFACES)}>:<resource-name>', got {spec!r}"
        )
    return surface, name


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--playbooks", type=Path, default=REGISTRY_PATH)
    p.add_argument("--trigger", help="<surface>:<name> of one AWS trigger resource")
    p.add_argument("--prose", default="", help="the human-readable half")
    p.add_argument("--marker-only", action="store_true")
    p.add_argument("--list", action="store_true", help="every resource + marker")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    registry = load_registry(args.playbooks)

    if args.list:
        index = resource_index(registry)
        rows = [
            {
                "resource": key,
                "wakes": sorted(index[key]["wakes"]),
                "kinds": sorted(index[key]["kinds"]),
                "marker": marker_for(registry, *key.split(":", 1)),
            }
            for key in sorted(index)
        ]
        print(json.dumps(rows, indent=2) if args.json
              else "\n".join(f"{r['resource']}\n    {r['marker']}" for r in rows))
        return 0

    if not args.trigger:
        p.error("one of --trigger or --list is required")

    surface, name = _split_trigger(args.trigger)
    try:
        out = (marker_for(registry, surface, name) if args.marker_only
               else description_for(registry, surface, name, args.prose))
    except (KeyError, DescriptionTooLong) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
