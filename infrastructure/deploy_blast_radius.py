"""deploy_blast_radius — name which scheduled SF run halts next on a stale
deploy stamp (alpha-engine-config-I7800).

2026-08-19: `Deploy Infrastructure` failed 3x on `main` (runs `32300162711`,
`32302092674`, `32305068379`). `notify-main-failure` fired each time as an
ordinary red-CI Telegram alert. Nothing connected those failures to the fact
that they had ALREADY DECIDED the outcome of the 2026-08-20 05:15 PT
preopen: the live SF stamp was frozen behind `main`, so `DeployDriftGate`
would halt — and it did. A full unmanaged trading session was determined
~8 hours in advance by a signal that read as routine CI noise.

This module computes, from `now` (UTC) alone, which SCHEDULED trigger is
next to invoke a Step Function whose `DeployDriftCheck` stage will read the
now-stale stamp and (for the weekday/EOD pipelines) halt, or (all three,
per sf-pipeline-policy §3) at minimum register drift. Two candidate
triggers, per the alpha-engine skill's pipeline map:

  - preopen: `ne-preopen-trading-pipeline`, weekday-only, EventBridge
    Scheduler `cron(15 5 ...)` `America/Los_Angeles` (05:15 PT), the ONLY
    schedule that actually HALTS the run on `sf_drift=true`
    (`DeployDriftGate`, config#6615).
  - weekly: `ne-weekly-freshness-pipeline`, EventBridge rule
    `alpha-engine-saturday`, `cron(0 9 ? * THU-SAT *)` (09:00 UTC,
    Thu/Fri/Sat) — `WeeklyRunDayGate` self-gates to the ONE real run of the
    week, but this module deliberately does NOT reimplement that gate's
    holiday logic; it reports the EARLIEST Thu/Sat 09:00 UTC firing as the
    candidate. That is a conservative (over-, never under-) approximation:
    the real single run is somewhere in [that candidate, +2 days], so
    naming the earliest instant never UNDERSTATES the blast radius.

Whichever candidate is sooner is "the next scheduled run this will halt."
Both pipelines' definitions carry a `DeployDriftCheck` stage (verified
2026-08-20: 15 occurrences in step_function.json, 7 in
step_function_daily.json) so naming either is never speculative.

Deliberately excluded: the EOD/postclose pipeline. It fires off the daemon's
shutdown hook (not a fixed clock schedule) and can only run AFTER a preopen
that already halted or completed, so it is never the FIRST scheduled
casualty of a stale stamp — but it does read the same stamp, and the
`Comment` line below says so.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_PT = ZoneInfo("America/Los_Angeles")
_UTC = timezone.utc

PREOPEN_HOUR_PT = 5
PREOPEN_MINUTE_PT = 15
WEEKLY_HOUR_UTC = 9


def _next_preopen_utc(now_utc: datetime) -> datetime:
    """Next weekday 05:15 America/Los_Angeles, expressed in UTC.

    Weekday-only by clock (Mon-Fri), not NYSE-holiday-aware — the preopen
    trigger itself fires on a plain weekday cron; `TradingDayGate` is what
    turns a holiday firing into a fast no-op, and DeployDriftCheck runs
    BEFORE that gate (see infrastructure/step_function_daily.json), so a
    holiday firing is still a real invocation that reads the stale stamp.
    """
    now_pt = now_utc.astimezone(_PT)
    candidate_pt = now_pt.replace(
        hour=PREOPEN_HOUR_PT, minute=PREOPEN_MINUTE_PT, second=0, microsecond=0
    )
    if candidate_pt <= now_pt:
        candidate_pt += timedelta(days=1)
    while candidate_pt.weekday() >= 5:  # Sat=5, Sun=6
        candidate_pt += timedelta(days=1)
    return candidate_pt.astimezone(_UTC)


def _next_weekly_utc(now_utc: datetime) -> datetime:
    """Earliest Thu/Fri/Sat 09:00 UTC firing (`alpha-engine-saturday` rule,
    `cron(0 9 ? * THU-SAT *)`) — see module docstring for why this is
    deliberately a conservative earliest-candidate, not a holiday-aware
    reimplementation of `WeeklyRunDayGate`."""
    candidate = now_utc.replace(hour=WEEKLY_HOUR_UTC, minute=0, second=0, microsecond=0)
    if candidate <= now_utc:
        candidate += timedelta(days=1)
    # THU=3, FRI=4, SAT=5 (Monday=0)
    while candidate.weekday() not in (3, 4, 5):
        candidate += timedelta(days=1)
    return candidate


def compute_blast_radius(now_utc: datetime | None = None) -> dict:
    """Returns the next scheduled trigger a stale deploy stamp will reach.

    ``{"pipeline": str, "sm_name": str, "next_run_utc": iso str,
       "next_run_local": human string, "message": str}``
    """
    now_utc = (now_utc or datetime.now(_UTC)).astimezone(_UTC)
    preopen_at = _next_preopen_utc(now_utc)
    weekly_at = _next_weekly_utc(now_utc)

    if preopen_at <= weekly_at:
        pipeline, sm_name, next_run = "preopen", "ne-preopen-trading-pipeline", preopen_at
        local = next_run.astimezone(_PT).strftime("%Y-%m-%d %H:%M %Z")
        consequence = "HALTS at DeployDriftGate (sf_drift=true routes to HandleFailure — no trading that day)"
    else:
        pipeline, sm_name, next_run = "weekly", "ne-weekly-freshness-pipeline", weekly_at
        local = next_run.astimezone(_UTC).strftime("%Y-%m-%d %H:%M UTC")
        consequence = "registers drift at its own DeployDriftCheck stage (sf-pipeline-policy §3)"

    message = (
        f"BLAST RADIUS (alpha-engine-config-I7800): the deployed SF stamp is now "
        f"behind main. Next scheduled run this reaches is {sm_name} "
        f"at {local} ({next_run.isoformat()}), which {consequence} unless "
        f"deploy-infrastructure.sh succeeds before then."
    )
    return {
        "pipeline": pipeline,
        "sm_name": sm_name,
        "next_run_utc": next_run.isoformat(),
        "next_run_local": local,
        "message": message,
    }


if __name__ == "__main__":
    import json
    import sys

    result = compute_blast_radius()
    # --message-only for direct use in a GHA `run:` step (GITHUB_OUTPUT-safe,
    # single line — the f-string above never embeds a newline).
    if "--message-only" in sys.argv:
        print(result["message"])
    else:
        print(json.dumps(result, indent=2))
