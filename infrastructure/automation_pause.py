#!/usr/bin/env python3
"""automation_pause.py — enforce and verify the scheduled-automation pause.

**Background (Brian ruling, 2026-08-07).** Every scheduled AWS trigger in this
account was disabled except the weekly Step Function (Saturday), the daily
preopen and postclose Step Functions (Mon-Fri), the 24/7 dashboard box, and the
two cost-safety backstops that protect them. ``infrastructure/automation_pause.json``
is the record of that ruling and the source of truth for which triggers are
intentionally off.

**Why this file exists at all.** Disabling a rule in the console is not a
decision anyone can find later, and it does not survive the machinery that
re-asserts ``ENABLED``:

  * ``deploy-infrastructure.yml`` re-applies the CloudFormation stack on EVERY
    push to main. MEASURED 2026-08-07: a successful run 49 seconds after the
    live disable did NOT revert the four CFN-owned rules — an unchanged
    template yields an empty changeset, and CloudFormation touches nothing. The
    revert risk is the NEXT template edit to those resources, which would
    rewrite them from a template still claiming ``State: ENABLED``. So the
    template edit is not urgency, it is truth: those four are pinned
    ``State: DISABLED`` in
    ``infrastructure/cloudformation/alpha-engine-orchestration.yaml`` so the
    source stops asserting something the operator has decided against.
  * ``infrastructure/lambdas/*/deploy.sh --reconcile-schedules`` recreates its
    Scheduler rules with the AWS default state (``ENABLED``) whenever that
    Lambda is redeployed. Those call sites are NOT yet pause-aware (see the
    delta note in this repo's PR): the pause survives them by detection, not
    prevention — ``--check`` runs in the daily drift sweep and goes red the next
    morning naming the exact ``--enforce`` command.

The check is deliberately two-directional. A pause that silently lifted and a
manifest entry for a rule that no longer exists are both findings: an entry that
can never fail is not a record, it is a comment.

**Alarm-action ownership (alpha-engine-config-I7174).** A paused component's
absence-alarm (``treat_missing_data: breaching``) cannot tell "gated off by
declaration" from "upstream died" — it has no input saying which case it is
in, because the pause manifest and the alarm configuration were not connected.
The ``paused_alarms`` block in ``automation_pause.json`` closes that: each
entry names a watch-plane/liveness CloudWatch alarm and the trigger name(s) it
``watches``. The alarm is JUSTIFIED — i.e. its actions should be silenced — for
exactly as long as every name in ``watches`` is itself in ``paused_names()``.
That is computed live on every read, never cached in a second field, so lifting
a pause (deleting/moving the watched trigger's entry out of ``paused``) makes
the alarm's justification lapse on the very next ``--check``/``--enforce`` —
the SAME manifest edit that restores the trigger's schedule, no separate AWS
CLI command. ``enforce()`` disables actions on a justified-but-armed alarm and
RE-ENABLES actions on an alarm whose justification has lapsed; alarms are
silenced with ``disable-alarm-actions``, never deleted, so their history and
configuration survive for later reconstruction. Re-enabling an alarm is NOT
the trigger-reenable asymmetry below — it only resumes paging, it starts no
scheduled work, so it is safe for ``enforce()`` to do unattended.

Usage:
  ./infrastructure/automation_pause.py --check     # verify; exit 1 on any finding
  ./infrastructure/automation_pause.py --enforce   # re-disable anything that came back,
                                                    # and reconcile alarm-action state
  ./infrastructure/automation_pause.py --enforce --alarms-only  # touch ONLY alarm actions;
                                                    # never disables/enables a trigger
  ./infrastructure/automation_pause.py --check --json

``--check`` needs ``events:DescribeRule`` + ``scheduler:GetSchedule`` +
``cloudwatch:DescribeAlarms``; ``--enforce`` additionally needs
``events:DisableRule`` + ``scheduler:UpdateSchedule`` +
``cloudwatch:EnableAlarmActions`` + ``cloudwatch:DisableAlarmActions``. CI runs
``--check`` (read-only) and ``--enforce --alarms-only`` (alarm actions only)
under the ``github-actions-iam-drift-check`` role.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

MANIFEST = Path(__file__).parent.resolve() / "automation_pause.json"
REGION = "us-east-1"


def load_manifest(path: Path = MANIFEST) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def paused_entries(manifest: dict | None = None) -> list[tuple[str, str, str]]:
    """Return [(surface, name, reason)] for every paused trigger.

    ``surface`` is ``"events"`` (an EventBridge rule) or ``"scheduler"`` (an
    EventBridge Scheduler schedule) — they are different APIs, not aliases.
    """
    m = manifest if manifest is not None else load_manifest()
    out: list[tuple[str, str, str]] = []
    for name, reason in sorted(m["paused"]["events_rules"].items()):
        out.append(("events", name, reason))
    for name, reason in sorted(m["paused"]["scheduler_schedules"].items()):
        out.append(("scheduler", name, reason))
    return out


def pending_names(manifest: dict | None = None) -> set[str]:
    """Triggers that must be BORN disabled but do not exist live yet.

    Separate from ``paused`` because ``check()`` requires every paused entry to
    exist live — a not-yet-created name there would be a permanent
    ``missing-in-aws`` finding. Keys prefixed ``_`` are prose, not triggers.
    """
    m = manifest if manifest is not None else load_manifest()
    return {k for k in m.get("pending", {}) if not k.startswith("_")}


def kept_names(manifest: dict | None = None) -> set[str]:
    """Triggers for which ENABLED is the INTENDED live state.

    The ``not_paused`` block holds two kinds of key: real trigger names, and
    prose labels grouping things the pause does not reach (``_non-eventbridge``
    covers a DLM policy and two SSM associations; ``_reactive-notifier-rules``
    names an EventPattern family with no schedule to remove). Prose keys carry
    the leading-underscore marker ``pending`` already uses, so the two are
    distinguishable without a second allowlist that could drift from this one.
    """
    m = manifest if manifest is not None else load_manifest()
    return {k for k in m.get("not_paused", {}) if not k.startswith("_")}


def paused_names(manifest: dict | None = None) -> set[str]:
    """Every name for which DISABLED is the INTENDED live state.

    Includes ``pending``. This is the question the drift checkers ask — "is a
    DISABLED trigger drift, or deliberate?" — and the answer is the same for
    both blocks. It is NOT the question ``check()`` asks, which is "does this
    exist live and is it off"; that one iterates ``paused_entries()`` and so
    still ignores ``pending``.

    Getting this wrong cost a red deploy on 2026-08-07: the pending block was
    wired into the bash helper but not here, so expense-collector correctly
    created its schedule DISABLED and its own post-deploy assertion then failed
    the deploy for the state it had just been told to write.
    """
    return {name for _, name, _ in paused_entries(manifest)} | pending_names(manifest)


def alarm_entries(manifest: dict | None = None) -> list[dict]:
    """Every declared ``paused_alarms`` entry: ``{name, reason, watches}``.

    ``_``-prefixed keys (``_why``) are prose, exactly the convention
    ``pending``/``not_paused`` already use, and are skipped here for the same
    reason: a prose value is invisible to every checker that iterates keys, so
    nothing that must be checked may live inside one.
    """
    m = manifest if manifest is not None else load_manifest()
    out: list[dict] = []
    for name, entry in sorted(m.get("paused_alarms", {}).items()):
        if name.startswith("_"):
            continue
        out.append({
            "name": name,
            "reason": entry.get("reason", ""),
            "watches": list(entry.get("watches", [])),
        })
    return out


def alarm_justified(entry: dict, manifest: dict | None = None) -> bool:
    """Is silencing ``entry`` still justified by the manifest, right now?

    True iff EVERY trigger it ``watches`` is currently in ``paused_names()``.
    An entry with an empty ``watches`` list is never justified — a declaration
    that names nothing to watch cannot be graded, and grading it as justified
    would silence an alarm on a claim nobody can verify.
    """
    watches = entry.get("watches") or []
    if not watches:
        return False
    names = paused_names(manifest)
    return all(w in names for w in watches)


def _aws(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        ["aws"] + args + ["--region", REGION], capture_output=True, text=True, check=False
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _live_state(surface: str, name: str) -> str | None:
    """Return the live State, or None if the trigger does not exist.

    Any failure that is NOT a genuine not-found is raised. A permissions error
    read as absence would let this check grade itself green by losing its own
    access — the same failure mode check-schedule-drift.py guards against.
    """
    if surface == "events":
        rc, out, err = _aws(
            ["events", "describe-rule", "--name", name, "--query", "State", "--output", "text"]
        )
        not_found = "ResourceNotFoundException" in err
    else:
        rc, out, err = _aws(
            ["scheduler", "get-schedule", "--name", name, "--query", "State", "--output", "text"]
        )
        not_found = "ResourceNotFoundException" in err
    if rc != 0:
        if not_found:
            return None
        raise RuntimeError(f"aws {surface} describe/get {name}: {err}")
    return out


def _disable(surface: str, name: str) -> None:
    if surface == "events":
        rc, _, err = _aws(["events", "disable-rule", "--name", name])
        if rc != 0:
            raise RuntimeError(f"aws events disable-rule --name {name}: {err}")
        return
    # Scheduler has no disable verb: update-schedule is a full replace, so the
    # live spec must be round-tripped or every unspecified attribute is lost.
    rc, out, err = _aws(["scheduler", "get-schedule", "--name", name, "--output", "json"])
    if rc != 0:
        raise RuntimeError(f"aws scheduler get-schedule --name {name}: {err}")
    spec = json.loads(out)
    for derived in ("Arn", "CreationDate", "LastModificationDate", "ResponseMetadata"):
        spec.pop(derived, None)
    spec["State"] = "DISABLED"
    rc, _, err = _aws(["scheduler", "update-schedule", "--cli-input-json", json.dumps(spec)])
    if rc != 0:
        raise RuntimeError(f"aws scheduler update-schedule --name {name}: {err}")


def _alarm_actions_enabled(name: str) -> bool | None:
    """Live ``ActionsEnabled`` for a CloudWatch alarm, or None if it does not exist.

    Same not-found-vs-raise posture as ``_live_state``: an access error read as
    "alarm does not exist" would let this check grade itself green by losing
    its own permission, rather than reporting the AccessDenied.
    """
    rc, out, err = _aws([
        "cloudwatch", "describe-alarms", "--alarm-names", name,
        "--query", "MetricAlarms[0].ActionsEnabled", "--output", "text",
    ])
    if rc != 0:
        raise RuntimeError(f"aws cloudwatch describe-alarms --alarm-names {name}: {err}")
    if out in ("None", ""):
        return None
    return out == "True"


def _set_alarm_actions(name: str, enabled: bool) -> None:
    verb = "enable-alarm-actions" if enabled else "disable-alarm-actions"
    rc, _, err = _aws(["cloudwatch", verb, "--alarm-names", name])
    if rc != 0:
        raise RuntimeError(f"aws cloudwatch {verb} --alarm-names {name}: {err}")


def check() -> list[dict]:
    findings: list[dict] = []
    for surface, name, reason in paused_entries():
        state = _live_state(surface, name)
        if state is None:
            findings.append({
                "trigger": name, "surface": surface, "kind": "missing-in-aws",
                "detail": (
                    "listed as paused but does not exist live — it was deleted, or "
                    "renamed. Remove the entry from automation_pause.json."
                ),
            })
        elif state != "DISABLED":
            findings.append({
                "trigger": name, "surface": surface, "kind": "unexpectedly-enabled",
                "detail": (
                    f"paused on 2026-08-07 but live state is {state} — a deploy or a "
                    f"console edit lifted the pause. Reason it is paused: {reason}. "
                    f"Fix: ./infrastructure/automation_pause.py --enforce"
                ),
            })

    # ── the other direction: a KEPT trigger that stopped running ─────────────
    #
    # Until 2026-08-11 `not_paused` asserted nothing. It was a list of reasons,
    # and by this file's own standard — "an entry that can never fail is not a
    # record, it is a comment" — it was a comment. The paused half was already
    # two-directional; the kept half was not directional at all.
    #
    # What that costs is specific and was the reason this was written. Every
    # entry here is kept because something breaks without it: the two SF
    # triggers, the cost-safety backstops that stop a t3.large running all
    # night, the freshness monitors, and now the expense collector — the sole
    # guard against the provider-credit exhaustion that left every autonomous
    # lane dark for three days (config#6613). A `deploy.sh --reconcile-schedules`
    # rewrite, a console edit, or a CFN template edit can flip any of them to
    # DISABLED, and nothing here would have said so. A guard that silently
    # stopped running looks exactly like a guard with nothing to report.
    #
    # NOT a mirror of `enforce()`: this reports only. Re-ENABLING a trigger
    # unattended would let this script start scheduled work, which is the one
    # thing the ruling it implements exists to prevent.
    for name in sorted(kept_names()):
        surfaces = {s: _live_state(s, name) for s in ("events", "scheduler")}
        live = {s: v for s, v in surfaces.items() if v is not None}
        if not live:
            findings.append({
                "trigger": name, "surface": "unknown", "kind": "kept-but-missing",
                "detail": (
                    "listed as deliberately KEPT but exists on neither the events nor "
                    "the scheduler surface — it was deleted or renamed, and whatever it "
                    "protected is now unprotected. If it is prose rather than a trigger "
                    "name, prefix the key with '_' as the pending block does."
                ),
            })
            continue
        for surface, state in live.items():
            if state != "ENABLED":
                findings.append({
                    "trigger": name, "surface": surface, "kind": "kept-but-disabled",
                    "detail": (
                        f"deliberately kept ENABLED, but live state is {state}. A deploy, "
                        f"a --reconcile-schedules run or a console edit turned off a "
                        f"trigger the pause explicitly spared. Re-enable it deliberately "
                        f"— this script will not do it for you, because re-enabling "
                        f"scheduled work unattended is what the pause forbids."
                    ),
                })

    # ── alarm-action state, both directions (alpha-engine-config-I7174) ─────
    #
    # Unlike the trigger halves above, BOTH directions here are actionable by
    # `enforce()` — re-enabling an alarm's actions only resumes paging, it
    # starts no scheduled work, so it carries none of the risk that keeps
    # `enforce()` from ever re-enabling a kept-but-disabled TRIGGER.
    findings.extend(alarm_findings())
    return findings


def alarm_findings() -> list[dict]:
    """Every disagreement between ``paused_alarms`` and live CloudWatch state."""
    out: list[dict] = []
    for entry in alarm_entries():
        name = entry["name"]
        justified = alarm_justified(entry)
        live = _alarm_actions_enabled(name)
        if live is None:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "alarm-missing-in-aws",
                "detail": (
                    "declared in paused_alarms but no such CloudWatch alarm exists live — "
                    "it was deleted or renamed. Remove or correct the entry."
                ),
            })
        elif justified and live:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "alarm-unexpectedly-enabled",
                "detail": (
                    f"every trigger it watches ({', '.join(entry['watches'])}) is still "
                    f"paused, so this alarm should be silenced, but ActionsEnabled=true — "
                    f"the pause-caused page this entry exists to stop is live. "
                    f"Fix: ./infrastructure/automation_pause.py --enforce --alarms-only"
                ),
            })
        elif not justified and not live:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "alarm-stale-disabled",
                "detail": (
                    f"the trigger(s) it watches ({', '.join(entry['watches']) or 'none'}) "
                    f"are no longer all paused, so silencing is no longer justified, but "
                    f"ActionsEnabled=false — a pause was lifted and this alarm was not "
                    f"re-armed. Fix: ./infrastructure/automation_pause.py --enforce "
                    f"--alarms-only (re-enables it), then remove the stale paused_alarms "
                    f"entry."
                ),
            })
    return out


def enforce(alarms_only: bool = False) -> list[str]:
    """Drive live AWS to match the manifest.

    ``alarms_only`` restricts this to CloudWatch alarm-action state — never
    disables or enables a trigger. That is the mode safe to run unattended and
    on a schedule (see the CI wiring in sf-arn-drift-check.yml): touching only
    alarm actions cannot start scheduled work, the one thing full ``enforce()``
    deliberately never does automatically for a KEPT trigger.
    """
    acted: list[str] = []
    if not alarms_only:
        for surface, name, _ in paused_entries():
            state = _live_state(surface, name)
            if state is not None and state != "DISABLED":
                _disable(surface, name)
                acted.append(f"{surface}:{name}")

    for entry in alarm_entries():
        name = entry["name"]
        justified = alarm_justified(entry)
        live = _alarm_actions_enabled(name)
        if live is None:
            continue  # reported by alarm_findings(); nothing to act on
        if justified and live:
            _set_alarm_actions(name, enabled=False)
            acted.append(f"cloudwatch:{name}:disabled")
        elif not justified and not live:
            _set_alarm_actions(name, enabled=True)
            acted.append(f"cloudwatch:{name}:enabled")

    return acted


def main() -> int:
    ap = argparse.ArgumentParser(description="scheduled-automation pause check / enforce")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="verify the pause holds")
    mode.add_argument("--enforce", action="store_true", help="re-disable anything enabled")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--alarms-only", action="store_true",
                     help="with --enforce, touch ONLY CloudWatch alarm-action state; "
                          "never disable or enable a trigger")
    args = ap.parse_args()
    if args.alarms_only and not args.enforce:
        ap.error("--alarms-only only makes sense with --enforce")

    entries = paused_entries()

    try:
        if args.enforce:
            acted = enforce(alarms_only=args.alarms_only)
            if args.json:
                print(json.dumps({"re_disabled": acted}, indent=2))
            else:
                print(f"automation pause — {len(entries)} paused trigger(s)")
                for a in acted:
                    print(f"  re-disabled {a}")
                if not acted:
                    print("  ✓ nothing had come back; no action taken")
            return 0

        findings = check()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps({"checked": len(entries),
                          "kept_checked": len(kept_names()),
                          "alarms_checked": len(alarm_entries()),
                          "findings": findings}, indent=2))
    else:
        kept = kept_names()
        alarms = alarm_entries()
        print(f"automation pause — {len(entries)} paused / {len(kept)} kept trigger(s), "
              f"{len(alarms)} declared alarm(s) checked")
        if not findings:
            print("  ✓ every paused trigger exists live and is DISABLED")
            print("  ✓ every kept trigger exists live and is ENABLED")
            print("  ✓ every declared alarm's ActionsEnabled matches its current justification")
        for f in findings:
            print(f"  ✗ [{f['kind']}] {f['surface']}:{f['trigger']}")
            print(f"      {f['detail']}")

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
