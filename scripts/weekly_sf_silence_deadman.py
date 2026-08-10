#!/usr/bin/env python3
"""weekly_sf_silence_deadman.py — silence deadman for the weekly pipeline's
two trigger roles (alpha-engine-config#6689 deliverable 3, resolving #5599).

**Background.** The weekly pipeline (``ne-weekly-freshness-pipeline``) is
launched two ways: the Saturday EventBridge cron (``pipeline_role=weekly``,
self-gated by ``step_function.json``'s ``WeeklyRunDayGateChoice`` to the one
correct calendar day per week) and, when ``infrastructure/weekly_cadence.json``
declares ``exercise_cadence=daily``, a chained launch from every trading day's
postclose (``pipeline_role=exercise``, ``step_function_eod.json``'s
``LaunchWeeklyExerciseRun``). Measured over 2026-07-26 -> 08-09: 4 of 10
trading days got no exercise run with ZERO signal — postclose FAILED before
the launch state on 07-27/07-28, and postclose never fired AT ALL on 08-05/
08-06. Nothing paged on the silent days because the only prior deadman
candidate, the CloudWatch ``AWS/States`` ``ExecutionsStarted`` metric, carries
no ``pipeline_role`` dimension — it cannot tell a gated-off day from a dead
one. **This script supersedes that metric-dimension approach** (alpha-engine-
config#5599's proposed resolution): it reads the declared cadence and the
NYSE trading calendar directly, derives the run-slots that SHOULD exist, and
checks the pipeline's own execution history for each one — no new metric
dimension needed.

**Classification, per expected slot:**

  * ``GATED_OFF`` (OK, exit 0) — the declared cadence says this slot should
    NOT fire (e.g. an exercise slot on a trading day when
    ``exercise_cadence`` is ``weekly-only`` or ``off``).
  * ``OK`` (exit 0) — expected, and a matching execution exists.
  * ``CRITICAL`` (pages, exit 1) — expected, and none does. This is the
    silence this script exists to catch: a day the pipeline should have run
    and produced nothing, indistinguishable from a holiday until now.

**Gotchas this script is built around (measured, not theoretical):**

  * A SUCCEEDED ``pipeline_role=weekly`` execution that completes in under
    ``GATE_NOOP_MAX_SECONDS`` is ``WeeklyRunDayGateChoice``'s designed 2-second
    no-op skip on a non-run day (2 of every 3 THU-SAT cron firings), NOT a
    real weekly run. Counting it as "ran" would hide a genuinely silent week.
  * Neither launch path passes an explicit Step Functions ``Name`` — every
    execution gets an AWS-generated UUID name. ``pipeline_role`` is read from
    each execution's OWN INPUT via ``describe_execution``, never inferred
    from the name.
  * The weekly cron fires THU-SAT and self-gates true on the day AFTER the
    week's last trading session (Saturday on a normal week; Friday/Thursday
    on a holiday-shortened one — real precedent Fri 2026-07-03). This script
    therefore expects the REAL weekly run on ``last_session + 1 calendar
    day``, not on the last session itself — a deliberate refinement of the
    filing issue's "weekly for the week's last session" wording, recorded as
    a documented deviation in this change's PR body.

  * A slot on TODAY is usually not-yet-due rather than missing — every
    launch path fires late in the UTC day. See ``last_due_day``; the CLI
    stops at yesterday by default and ``--through today`` opts back in.

**Scheduling (alpha-engine-config#6738).** This module is BOTH an operator
CLI and the slot-derivation library for the scheduled check. The
``alpha-engine-pipeline-watchdog`` Lambda imports ``compute_expected_slots``
/ ``evaluate`` / ``fetch_execution_records`` / ``load_cadence_from_ssm`` /
``last_due_day`` from here (its ``deploy.sh`` packages this file flat into
the zip) and runs them on the already-un-paused
``alpha-engine-pipeline-watchdog-daily`` 14:00-UTC trigger, paging the
independent ``alpha-engine-watchdog-alerts`` topic. One implementation, two
entry points — a manual rerun and the scheduled check cannot disagree about
which slots the declaration expects.

Run manually, this script still best-effort publishes a CRITICAL finding to
the fleet's ``alpha-engine-alerts`` SNS topic and always exits non-zero
regardless of whether the publish succeeds — the exit code is the durable
signal, and the scheduled path uses the watchdog topic instead.

Usage:
  ./scripts/weekly_sf_silence_deadman.py                     # manifest cadence, 5-day window, through yesterday
  ./scripts/weekly_sf_silence_deadman.py --live               # cadence read from live SSM instead
  ./scripts/weekly_sf_silence_deadman.py --window-days 10
  ./scripts/weekly_sf_silence_deadman.py --through today      # include today (only after its postclose)
  ./scripts/weekly_sf_silence_deadman.py --json --no-notify   # for scripted/CI consumption
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "infrastructure" / "weekly_cadence.json"
SSM_PARAM_NAME = "/alpha-engine/weekly-sf/exercise-cadence"
STATE_MACHINE_NAME = "ne-weekly-freshness-pipeline"
REGION = "us-east-1"
ACCOUNT_ID = "711398986525"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"

ALLOWED_CADENCE = {"daily", "weekly-only", "off"}
DEFAULT_WINDOW_DAYS = 5
GATE_NOOP_MAX_SECONDS = 90


# ---------------------------------------------------------------------------
# Cadence source
# ---------------------------------------------------------------------------

def _validate_cadence(value: object, source: str) -> str:
    if value not in ALLOWED_CADENCE:
        raise ValueError(f"{source} declared exercise_cadence={value!r}, not one of {sorted(ALLOWED_CADENCE)}")
    return value  # type: ignore[return-value]


def load_cadence_from_manifest(path: Path = MANIFEST_PATH) -> str:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    return _validate_cadence(manifest.get("exercise_cadence"), str(path))


def load_cadence_from_ssm(ssm_client) -> str:
    resp = ssm_client.get_parameter(Name=SSM_PARAM_NAME)
    return _validate_cadence(resp["Parameter"]["Value"], SSM_PARAM_NAME)


# ---------------------------------------------------------------------------
# Expected run-slot derivation (pure — no AWS)
# ---------------------------------------------------------------------------

def last_due_day(today: date) -> date:
    """The most recent day whose expected run-slots are unambiguously DUE.

    Every launch path for this state machine fires LATE in the UTC day: the
    exercise leg is chained off that trading day's postclose (~20:00-20:30
    UTC) and the Saturday cron fires 09:00 UTC on the day after the week's
    last session. So "today" is, for most of any given UTC day, a slot that
    has not happened YET rather than a slot that is missing — and a detector
    that cannot tell those apart reports CRITICAL on a healthy pipeline.

    Measured 2026-08-09 (alpha-engine-config#6738): a live run of this script
    at 21:00 PT — 2026-08-10 in UTC — classified the 2026-08-10 exercise slot
    CRITICAL alongside the two genuinely silent days, because Monday's
    postclose was ~23h in the future. Three CRITICALs, one of them false.

    Anchoring evaluation one day back removes the whole ambiguity class
    rather than racing it with a cutoff hour: yesterday's exercise slot is
    18h+ old by the time any consumer reads it, and Saturday's 09:00-UTC
    weekly slot is 29h+ old when evaluated on Sunday. Both are overdue, not
    early. Shared by BOTH entry points — the CLI's default anchor and the
    pipeline-watchdog Lambda's — so neither can drift back into the trap.
    """
    return today - timedelta(days=1)


@dataclass(frozen=True)
class ExpectedSlot:
    day: date
    role: str  # "exercise" | "weekly"
    gated_off: bool = False
    note: str = ""


def _is_last_session_of_week(
    d: date,
    is_trading_day: Callable[[date], bool],
    next_trading_day: Callable[[date], date],
) -> bool:
    """True iff d is a trading day and the NEXT trading day falls in a
    different ISO week — i.e. d is the last trading session of its week."""
    if not is_trading_day(d):
        return False
    nxt = next_trading_day(d)
    return nxt.isocalendar()[:2] != d.isocalendar()[:2]


def compute_expected_slots(
    today: date,
    cadence: str,
    window_days: int,
    is_trading_day: Callable[[date], bool],
    next_trading_day: Callable[[date], date],
) -> list[ExpectedSlot]:
    """Derive every run-slot expected in [today - window_days, today].

    Exercise slots: one per trading day in the window, gated_off when
    ``cadence != "daily"`` — LaunchWeeklyExerciseRun's CheckExerciseCadence
    only fires the launch on 'daily' (step_function_eod.json).

    Weekly slots: one per week whose last-trading-session-plus-one-day run
    date falls within the window. NEVER gated by ``cadence`` — the Saturday
    cron is a separate trigger this knob deliberately does not touch.
    """
    slots: list[ExpectedSlot] = []
    d = today - timedelta(days=window_days)
    while d <= today:
        if is_trading_day(d):
            gated = cadence != "daily"
            slots.append(ExpectedSlot(
                day=d,
                role="exercise",
                gated_off=gated,
                note=(
                    f"cadence={cadence} — LaunchWeeklyExerciseRun is skipped on trading days"
                    if gated else
                    f"cadence={cadence} — LaunchWeeklyExerciseRun fires after this trading day's postclose"
                ),
            ))
        if _is_last_session_of_week(d, is_trading_day, next_trading_day):
            run_day = d + timedelta(days=1)
            if today - timedelta(days=window_days) <= run_day <= today:
                slots.append(ExpectedSlot(
                    day=run_day,
                    role="weekly",
                    gated_off=False,
                    note=(
                        f"day after the week's last trading session ({d.isoformat()}) — "
                        "Saturday-cron WeeklyRunDayGateChoice self-gates true on this day"
                    ),
                ))
        d += timedelta(days=1)
    return slots


# ---------------------------------------------------------------------------
# Execution matching (pure — takes already-fetched records)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExecutionRecord:
    name: str
    role: str | None
    status: str
    start: datetime  # tz-aware UTC
    stop: datetime | None


def normalize_role(raw: object) -> str | None:
    return raw if raw in ("exercise", "weekly") else None


@dataclass(frozen=True)
class SlotResult:
    slot: ExpectedSlot
    classification: str  # "OK" | "GATED_OFF" | "CRITICAL"
    matched_execution: str | None
    detail: str


def _is_gate_noop(e: ExecutionRecord) -> bool:
    if e.status != "SUCCEEDED" or e.stop is None:
        return False
    return (e.stop - e.start).total_seconds() < GATE_NOOP_MAX_SECONDS


def evaluate_slot(slot: ExpectedSlot, executions: list[ExecutionRecord]) -> SlotResult:
    if slot.gated_off:
        return SlotResult(slot, "GATED_OFF", None, slot.note)

    candidates = [e for e in executions if e.role == slot.role and e.start.date() == slot.day]

    if slot.role == "weekly":
        real = [e for e in candidates if not _is_gate_noop(e)]
        if real:
            e = real[0]
            return SlotResult(slot, "OK", e.name, f"found {e.name} status={e.status}")
        if candidates:
            return SlotResult(
                slot, "CRITICAL", None,
                f"only run-day-gate no-op(s) found on {slot.day.isoformat()} "
                f"({len(candidates)}) — WeeklyRunDayGateChoice's skip is not a real run",
            )
        return SlotResult(
            slot, "CRITICAL", None,
            f"no weekly-role execution found on {slot.day.isoformat()} — expected the Saturday-cron run",
        )

    # exercise: not gated by the run-day gate (bypasses it by design), so any
    # matching execution is a real run.
    if candidates:
        e = candidates[0]
        return SlotResult(slot, "OK", e.name, f"found {e.name} status={e.status}")
    return SlotResult(
        slot, "CRITICAL", None,
        f"no exercise-role execution found on {slot.day.isoformat()} — postclose should have chained it (cadence=daily)",
    )


def evaluate(slots: list[ExpectedSlot], executions: list[ExecutionRecord]) -> list[SlotResult]:
    return [evaluate_slot(s, executions) for s in slots]


# ---------------------------------------------------------------------------
# AWS plumbing (thin, injectable — mirrors weekly_sf_rerun.py's shape)
# ---------------------------------------------------------------------------

def _list_all_executions(sf_client, sm_arn: str, cap: int = 500) -> list[dict]:
    out: list[dict] = []
    token = None
    while len(out) < cap:
        kwargs = {"stateMachineArn": sm_arn, "maxResults": 200}
        if token:
            kwargs["nextToken"] = token
        resp = sf_client.list_executions(**kwargs)
        out.extend(resp["executions"])
        token = resp.get("nextToken")
        if not token:
            break
    return out[:cap]


def fetch_execution_records(
    sf_client, window_days: int, today: date | None = None
) -> list[ExecutionRecord]:
    """Live AWS fetch: list recent executions, then describe each one for its
    pipeline_role (never present on the list-executions summary itself —
    every launch path here omits an explicit Name, so role must come from
    input, not the name).

    ``today`` — the anchor the lookback is measured back from. Defaults to
    ``_today()`` (the CLI's behaviour, unchanged). The pipeline-watchdog
    Lambda passes its own ``now_utc.date()`` instead (alpha-engine-config
    #6738) so the fetch window and the slot window are anchored to the SAME
    instant: a handler that pins ``now`` while this function read the wall
    clock would silently evaluate slots the fetch never covered.
    """
    sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"
    # +2 day pad: a weekly slot's run_day can land one calendar day after the
    # window's nominal start when the window boundary splits a week. Derived
    # from the anchor (not a fresh datetime.now() call) so a test that pins
    # the anchor gets a fully deterministic window.
    anchor = today if today is not None else _today()
    since = datetime.combine(anchor, datetime.min.time(), tzinfo=timezone.utc) - timedelta(days=window_days + 2)

    records: list[ExecutionRecord] = []
    for ex in _list_all_executions(sf_client, sm_arn):
        start = ex["startDate"]
        if start < since:
            continue
        role = None
        try:
            desc = sf_client.describe_execution(executionArn=ex["executionArn"])
            inp = json.loads(desc.get("input") or "{}")
            role = normalize_role(inp.get("pipeline_role"))
        except Exception as exc:  # noqa: BLE001 — best-effort per-execution; unrecognized role is a safe fallback, not a hidden failure (surfaced below)
            print(f"WARN: could not read input of {ex['executionArn']}: {exc}", file=sys.stderr)
        records.append(ExecutionRecord(
            name=ex["name"],
            role=role,
            status=ex["status"],
            start=start,
            stop=ex.get("stopDate"),
        ))
    return records


# ---------------------------------------------------------------------------
# Reporting + CLI
# ---------------------------------------------------------------------------

def _today() -> date:
    """Isolated seam so CLI-level tests can pin 'today' deterministically
    without depending on the real wall clock."""
    return datetime.now(timezone.utc).date()


def format_report(results: list[SlotResult]) -> str:
    marks = {"OK": "✓", "GATED_OFF": "·", "CRITICAL": "✗"}
    lines = []
    for r in sorted(results, key=lambda r: (r.slot.day, r.slot.role)):
        lines.append(
            f"  {marks[r.classification]} {r.slot.day.isoformat()} {r.slot.role:8s} "
            f"[{r.classification}] {r.detail}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS, help=f"trailing days to check (default {DEFAULT_WINDOW_DAYS})")
    ap.add_argument("--live", action="store_true", help="read the declared cadence from live SSM instead of the local manifest")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--no-notify", action="store_true", help="never publish to SNS, even on CRITICAL (dry runs / CI)")
    ap.add_argument(
        "--through",
        choices=("last-due", "today"),
        default="last-due",
        help=(
            "newest day to evaluate. 'last-due' (default) stops at yesterday, "
            "because today's exercise slot is chained off today's ~20:00 UTC "
            "postclose and is therefore not-yet-due, not missing. 'today' "
            "includes it — only correct when invoked AFTER today's postclose "
            "has had time to chain."
        ),
    )
    args = ap.parse_args(argv)

    import boto3  # local import: keeps the pure functions above importable/testable with zero AWS deps
    from nousergon_lib.trading_calendar import is_trading_day, next_trading_day

    sf = boto3.client("stepfunctions", region_name=REGION)

    if args.live:
        ssm = boto3.client("ssm", region_name=REGION)
        cadence = load_cadence_from_ssm(ssm)
    else:
        cadence = load_cadence_from_manifest()

    today = _today()
    through = today if args.through == "today" else last_due_day(today)
    executions = fetch_execution_records(sf, args.window_days, today=today)
    slots = compute_expected_slots(through, cadence, args.window_days, is_trading_day, next_trading_day)
    results = evaluate(slots, executions)
    critical = [r for r in results if r.classification == "CRITICAL"]

    if args.json:
        print(json.dumps({
            "cadence": cadence,
            "evaluated_through": through.isoformat(),
            "window_days": args.window_days,
            "checked": len(results),
            "critical": len(critical),
            "results": [
                {
                    "day": r.slot.day.isoformat(),
                    "role": r.slot.role,
                    "classification": r.classification,
                    "matched_execution": r.matched_execution,
                    "detail": r.detail,
                }
                for r in results
            ],
        }, indent=2))
    else:
        print(
            f"weekly-sf silence deadman — cadence={cadence} "
            f"window={args.window_days}d through={through.isoformat()}"
            + ("" if args.through == "today" else " (today excluded: not yet due — --through today to include)")
        )
        print(format_report(results))
        print(f"\n✗ {len(critical)} CRITICAL slot(s) — expected and absent" if critical else "\n✓ no silent slots")

    if critical and not args.no_notify:
        try:
            sns = boto3.client("sns", region_name=REGION)
            detail = "\n".join(f"- {r.slot.day.isoformat()} {r.slot.role}: {r.detail}" for r in critical)
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="Alpha Engine — weekly-SF silence deadman: CRITICAL",
                Message=(
                    "weekly_sf_silence_deadman.py found expected run-slot(s) with no matching "
                    f"execution on {STATE_MACHINE_NAME} (cadence={cadence}):\n\n{detail}\n\n"
                    "This means a day the pipeline should have run produced NO execution at "
                    "all — not a failure, silence. See alpha-engine-config#6689."
                ),
            )
        except Exception as exc:  # noqa: BLE001 — best-effort page; exit code below is the durable signal regardless
            print(f"WARN: SNS publish failed: {exc}", file=sys.stderr)

    return 1 if critical else 0


if __name__ == "__main__":
    sys.exit(main())
