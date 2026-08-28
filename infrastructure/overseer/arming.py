#!/usr/bin/env python3
"""arming.py — is each Overseer playbook ARMED, and if not, did someone DECIDE that?

**The question this answers, and why nothing answered it before.**

``observability-policy.md`` §8.3 requires every component to resolve to exactly
one state, and singles out the collapse that must never happen: ``DISABLED``
(a decision) reported as ``MISSED`` (a defect), or the reverse. For the nine
Overseer components that distinction was, until this module, underivable —
a paused playbook and a broken playbook both produce **exactly nothing**, and
"nothing" was the only evidence available.

The run record (``overseer/run_telemetry/``) cannot fix that on its own. It
answers "did it run and how did it end", which is silent in both cases. So the
disambiguation needs a second channel answering a different question, and
§9.2 names it: *a monitor whose subject is correctly idle most of the time is
watched on a statement about the FUTURE — enabled, scheduled, next-run-at — not
inferred from the absence of a complaint.*

This module is that statement. It emits **every tick, for every playbook,
whether or not anything ran**, so a component is never silent even when it is
correctly doing nothing.

**The join, and why it did not exist.**

Three sources have to agree, and no code read all three together:

  1. ``infrastructure/overseer/playbooks.yaml`` — the SSoT for what wakes each
     playbook. Until this change it declared its triggers in ``wake:`` PROSE
     ("scheduler:alpha-engine-alert-drain-0400utc/1000utc/..."), which no
     reader could join on. The machine-readable ``triggers:`` list added
     alongside it is what makes this module possible.
  2. ``infrastructure/automation_pause.json`` — Brian's ruling of 2026-08-07
     (``alpha-engine-config-I6617``) naming what is deliberately off. This is
     the HUMAN declaration, and it is the only thing entitled to turn silence
     into ``DISABLED``. ``observability-policy.md`` §8.3 is explicit that
     ``DISABLED`` is declared, never inferred; this module reads that
     declaration and never writes one.
  3. Live AWS — the actual ``State`` of each trigger, via
     ``automation_pause._live_state``.

**The verdict that matters most is ``undeclared-dark``.** A trigger that is
DISABLED live with no entry in the pause manifest is a component that is off
with nobody's decision behind it — indistinguishable, from telemetry alone,
from one that was paused on purpose. That is the exact failure §8.3 forbids,
and before this module nothing computed it.

**Arming is a property of a TRIGGER, not of a playbook**, which is why
``partially-armed`` is a first-class verdict rather than a rounding error.
Measured 2026-08-12: all four ``alert-drain`` Scheduler schedules are DISABLED,
and alert-drain nonetheless dispatched twice that day — its event-time
freshness-CRITICAL path runs off the freshness monitors, which Brian
deliberately kept enabled. A playbook-level boolean would have read "paused"
and been simply false.

**This module never acts.** It reads three sources and prints a verdict. It
cannot enable, disable, dispatch or remediate anything — ``overseer-policy.md``
§2 keeps detection non-agentic and deterministic, and re-enabling scheduled
work unattended is the one thing the ruling it reads exists to prevent.

Usage:
  ./infrastructure/overseer/arming.py --report          # human-readable
  ./infrastructure/overseer/arming.py --report --json   # the arming record
  ./infrastructure/overseer/arming.py --check           # exit 1 on undeclared drift
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).parent.resolve()
REGISTRY_PATH = HERE / "playbooks.yaml"

sys.path.insert(0, str(HERE.parent))
import automation_pause as ap  # noqa: E402

SCHEMA_VERSION = 1

# Surfaces whose live state this module can actually read. Everything else is
# declared with a reason and reported as `unjoinable` — named, never silently
# treated as armed. A surface we cannot probe is not a surface we may assume.
JOINABLE_SURFACES = ("events", "scheduler")

# The component_id each playbook carries in the fleet observability registry
# (`nous-ergon-ops/governance/observability.d/`). Declared here rather than
# derived from the playbook key, because the registry's ids are the join key
# for every downstream consumer and a convention that "usually matches" is the
# kind of implicit contract §2.2 exists to remove.
COMPONENT_ID_PREFIX = "overseer-"
T1_COMPONENT_ID_PREFIX = "t1-"


def load_registry(path: Path = REGISTRY_PATH) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def declared_units(registry: dict | None = None) -> list[dict]:
    """Every playbook and T1 automation, with its component id and triggers.

    One list, because the observability registry does not distinguish them —
    all nine carry a row and all nine owe the same answer.
    """
    reg = registry if registry is not None else load_registry()
    units: list[dict] = []
    for name, spec in sorted((reg.get("playbooks") or {}).items()):
        units.append({
            "component_id": f"{COMPONENT_ID_PREFIX}{name}",
            "playbook": name,
            "kind": "playbook",
            "triggers": list(spec.get("triggers") or []),
        })
    for entry in reg.get("t1_automations") or []:
        name = entry.get("name")
        units.append({
            "component_id": f"{T1_COMPONENT_ID_PREFIX}{name}",
            "playbook": name,
            "kind": "t1_automation",
            "triggers": list(entry.get("triggers") or []),
        })
    return units


def _classify_trigger(trig: dict, paused: set[str], kept: set[str], live_state) -> dict:
    """One trigger's arming verdict.

    `live_state` is injected so tests exercise this without AWS and so a
    caller can batch. It takes (surface, name) and returns the live State or
    None; it RAISES on anything that is not a genuine not-found, because a
    permissions error read as absence would let this grade itself green by
    losing its own access (the posture automation_pause._live_state already
    takes, and the reason this defers to it rather than re-querying).
    """
    surface = trig.get("surface")
    out = {"surface": surface, "name": trig.get("name") or trig.get("ref")}

    if surface not in JOINABLE_SURFACES:
        # alpha-engine-config-I9045. A leg with no AWS resource of its own may
        # still declare `depends_on`: the resources whose live state IS its
        # arming. Resolving it is not a nicety — measured 2026-08-28,
        # alert-drain's event-time leg was the ONLY thing running the drain
        # (daily, all week) while its four schedules sat DISABLED, and this
        # branch reported the one live leg as `unjoinable`. A blank standing
        # where the answer was derivable is what §8.3 forbids.
        deps = trig.get("depends_on") or []
        if deps:
            resolved = [
                _classify_trigger(dict(d), paused, kept, live_state) for d in deps
            ]
            out["declared"] = trig.get("declared_by") or "derived-from-dependencies"
            out["depends_on"] = resolved
            dep_verdicts = [r["verdict"] for r in resolved]
            out["live"] = ",".join(str(r.get("live", "-")) for r in resolved)
            if "armed" in dep_verdicts:
                # ANY armed dependency wakes this leg. Not `all` — the leg fires
                # whenever any one of the rules it rides fires, and reading it
                # as dark because a sibling is off is the false-negative that
                # would hide a running playbook all over again.
                out["verdict"] = "armed-via-dependency"
            elif "trigger-absent" in dep_verdicts:
                out["verdict"] = "trigger-absent"
                out["detail"] = (
                    f"event-time leg '{out['name']}' depends on a trigger that does not "
                    "exist live, so this wake path is broken: "
                    + "; ".join(
                        r.get("detail", "") for r in resolved
                        if r["verdict"] == "trigger-absent"
                    )
                )
            elif all(v == "paused-declared" for v in dep_verdicts):
                out["verdict"] = "paused-declared"
            else:
                out["verdict"] = "undeclared-dark"
                out["detail"] = (
                    f"event-time leg '{out['name']}' is dark: every trigger it depends on "
                    "is DISABLED and at least one of them is in neither the paused nor "
                    "the kept block of automation_pause.json."
                )
            return out

        out["verdict"] = "unjoinable"
        out["declared"] = trig.get("declared_by") or "out-of-band"
        out["detail"] = trig.get("reason") or (
            f"surface '{surface}' has no live AWS state this module can read, and the "
            "registry states no reason — which is a blank, not a value"
        )
        return out

    name = trig["name"]
    declared = "paused" if name in paused else ("kept" if name in kept else "undeclared")
    out["declared"] = declared

    state = live_state(surface, name)
    out["live"] = state if state is not None else "ABSENT"

    if state is None:
        out["verdict"] = "trigger-absent"
        out["detail"] = (
            f"{surface}:{name} is declared in playbooks.yaml but does not exist live. "
            "Nothing can wake this playbook through it."
        )
    elif state == "ENABLED":
        out["verdict"] = "armed"
    elif declared in ("paused",):
        out["verdict"] = "paused-declared"
    else:
        # DISABLED, and no human said so. This is the finding the module exists
        # for: from telemetry alone it is indistinguishable from a deliberate
        # pause, and §8.3 forbids reporting one as the other.
        out["verdict"] = "undeclared-dark"
        out["detail"] = (
            f"{surface}:{name} is live DISABLED but appears in NEITHER the paused nor "
            "the kept block of automation_pause.json. Nobody's decision is recorded "
            "behind it, so it cannot be rendered DISABLED and must not be rendered "
            "healthy. Declare it, or re-enable it."
        )
    return out


def _unit_verdict(triggers: list[dict]) -> str:
    """Roll a unit's trigger verdicts into one arming state.

    Deliberately NOT a majority or a default. The order below is a precedence,
    and `undeclared-dark` outranks everything because an undeclared dark
    trigger is the one condition that makes every other reading unsafe.
    """
    # `armed-via-dependency` is an ARMED reading, not a weaker one: the leg has
    # a live enabled AWS resource behind it, reached one hop away
    # (alpha-engine-config-I9045). Folding it here is what makes a playbook
    # running solely off its event-time leg read `partially-armed` rather than
    # `paused-declared` — the false reading measured on 2026-08-12.
    v = ["armed" if t["verdict"] == "armed-via-dependency" else t["verdict"]
         for t in triggers]
    if not v:
        return "undeclared-no-triggers"
    if "undeclared-dark" in v:
        return "undeclared-dark"
    if "trigger-absent" in v:
        return "trigger-absent"
    joinable = [x for x in v if x != "unjoinable"]
    if not joinable:
        return "unjoinable"
    if all(x == "armed" for x in joinable):
        return "armed"
    if all(x == "paused-declared" for x in joinable):
        return "paused-declared"
    return "partially-armed"


def build_record(registry: dict | None = None, manifest: dict | None = None,
                 live_state=None) -> dict:
    """The arming record — emitted every tick, for every unit, always.

    A unit with nothing to say still appears, with its arming verdict and the
    ruling behind it. That is the whole point: silence is never the output,
    because silence is what this exists to disambiguate.
    """
    live_state = live_state or ap._live_state
    m = manifest if manifest is not None else ap.load_manifest()
    paused = ap.paused_names(m)
    kept = ap.kept_names(m)
    ruling = m.get("ruling") or {}

    units = []
    for unit in declared_units(registry):
        trigs = [_classify_trigger(t, paused, kept, live_state) for t in unit["triggers"]]
        units.append({
            "component_id": unit["component_id"],
            "playbook": unit["playbook"],
            "kind": unit["kind"],
            "arming": _unit_verdict(trigs),
            "triggers": trigs,
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0).isoformat(),
        "pause_ruling": {
            "by": ruling.get("by"),
            "date": ruling.get("date"),
            "statement": ruling.get("statement"),
        },
        "units": units,
    }


def findings(record: dict) -> list[dict]:
    """The subset a CI step should go red on.

    `paused-declared` is NOT a finding — it is the ruling working. Reporting a
    deliberate operator disable as a defect is the collapse in the other
    direction, and `overseer-policy.md` §7 is explicit that a kill switch is
    state, surfaced everywhere, never alerted.
    """
    out = []
    for u in record["units"]:
        for t in u["triggers"]:
            if t["verdict"] in ("undeclared-dark", "trigger-absent"):
                out.append({
                    "component_id": u["component_id"],
                    "trigger": f"{t['surface']}:{t.get('name')}",
                    "kind": t["verdict"],
                    "detail": t.get("detail", ""),
                })
        if u["arming"] == "undeclared-no-triggers":
            out.append({
                "component_id": u["component_id"],
                "trigger": "-",
                "kind": "undeclared-no-triggers",
                "detail": (
                    "declares no `triggers:` at all, so whether it is armed cannot be "
                    "computed and its silence cannot be classified. Declare its "
                    "triggers in playbooks.yaml, using surface 'none' with a reason "
                    "if it genuinely has none."
                ),
            })
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Overseer playbook arming report")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--report", action="store_true", help="print the arming record")
    mode.add_argument("--check", action="store_true", help="exit 1 on undeclared drift")
    p.add_argument("--json", action="store_true", help="machine-readable output")
    args = p.parse_args(argv)

    try:
        record = build_record()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    found = findings(record)

    if args.json:
        print(json.dumps(record if args.report else
                         {"generated_at": record["generated_at"], "findings": found}, indent=2))
    else:
        print(f"overseer arming — {len(record['units'])} unit(s) @ {record['generated_at']}")
        for u in record["units"]:
            print(f"  {u['arming']:<24} {u['component_id']}")
            for t in u["triggers"]:
                nm = t.get("name") or t["surface"]
                print(f"      {t['verdict']:<20} {t['surface']}:{nm}"
                      f" (declared={t.get('declared')}, live={t.get('live', '-')})")
        if found:
            print(f"\n  {len(found)} finding(s):")
            for f in found:
                print(f"  ✗ [{f['kind']}] {f['component_id']} {f['trigger']}")
                print(f"      {f['detail']}")
        else:
            print("\n  ✓ every declared trigger is either armed or declared paused")

    if args.check:
        return 1 if found else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
