"""alpha-engine-eod-success-friday-shell-trigger — start the weekly SF after EOD.

NAME IS HISTORICAL. This Lambda no longer fires a Friday-only shell run; it
starts a FULL weekly-freshness execution after EVERY successful post-close SF.
The physical function/rule names are kept because the CloudFormation template
references them and a rename is a separate, reversible change (tracked
separately) — not because the name is right.

WHY DAILY (Brian ruling 2026-07-29): the weekly pipeline was failing, and every
failure waited a week to resurface. Defects were deferred to "next Saturday",
Saturday failed, and the cycle restarted — 15+ hand-driven reruns across
2026-07-26/27 to nurse one weekly through. Running the full pipeline after
every trading day converts a weekly guessing game into a daily signal: it
breaks today, it gets fixed today. This is the same reasoning weekly-sf-policy
already states about rehearsal paths ("a change that would make Saturday robust
waits for a Saturday to validate, while every Saturday runs on the un-hardened
path") — applied to the pipeline itself rather than to changes against it.

Mechanism: subscribes to EventBridge ``Step Functions Execution Status Change``
filtered to ``ne-postclose-trading-pipeline`` + ``SUCCEEDED``, derives the
trading_day the execution closed against, and starts
``ne-weekly-freshness-pipeline``. Three properties carried over from the
Friday-shell design, all still load-bearing:

  1. **No fire on EOD failure.** If EOD never reaches SUCCEEDED this never
     invokes, so the weekly does not chase a broken upstream.
  2. **Late re-runs work for free.** A fixed-and-rerun EOD still produces a
     SUCCEEDED transition with the same trading_day.
  3. **trading_day-bound, not wall-clock.** Derived via
     ``last_closed_trading_day`` from ``detail.stopDate``, so an EOD that
     succeeds at 02:00 UTC (= previous evening ET) stamps the right day.

Run shape — three deliberate input choices:

  * ``shell_run`` is NOT set. The 2026-07-26/27 failures were WORKLOAD
    failures (skip-parity, skip-backtester, bt-eval), which a shell run
    short-circuits past and would never have caught. A dry pass that cannot
    see the breakage is not a signal.
  * ``skip_weekly_run_day_gate: true`` — the sanctioned operator bypass
    (config#1617 skip-flag charter). Without it ``CheckWeeklyRunDayGate``
    routes to ``WeeklyRunDayGate``, which is true only when yesterday was the
    week's LAST trading session, so Mon-Thu firings would be rejected at
    state one and this whole change would be a no-op.
  * ``pipeline_role: "weekly"`` — keeps the existing mutex semantics. The
    mutex key is ``{state-machine}#{pipeline_role}#{run_date}``, so two
    firings for the same trading day (an EOD rerun, an EventBridge retry)
    collapse to one execution instead of racing.

Fail-loud (this is a producer of weekly-SF starts):

  * Missing ``detail.stopDate`` → raises (upstream contract violation).
  * trading_calendar lookup failure → raises.
  * ``states:StartExecution`` failure → raises, so the EventBridge retry
    policy engages and the Lambda Errors alarm pages. A silent failure here
    means no weekly ran and nothing said so.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3

from nousergon_lib.trading_calendar import last_closed_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

SATURDAY_SF_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline"
)
EOD_SF_NAME = "ne-postclose-trading-pipeline"

SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN",
    f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts",
)

def _derive_trading_day_utc_ms(stop_date_ms: int):
    """trading_day = NYSE last-closed session at the EventBridge stopDate moment.

    Accepts epoch milliseconds (UTC) from ``event.detail.stopDate`` and hands
    a tz-aware UTC datetime to the lib helper. The helper itself converts to
    NYSE local time before walking back to the most recent closed session,
    so callers do not have to reason about UTC ↔ ET rollover.
    """
    dt_utc = datetime.fromtimestamp(int(stop_date_ms) / 1000, tz=timezone.utc)
    return last_closed_trading_day(dt_utc)


def _build_run_input() -> str:
    """Full weekly-freshness run input.

    NO ec2_instance_id (config#2248): the SF's own CheckSpotDispatchNeeded/
    DispatchWeeklyFreshnessSpot states populate $.ec2_instance_id from a fresh
    ephemeral spot per execution. Hardcoding it here reintroduced a SPOF.

    NO shell_run: this is a real run. See the module docstring — the failures
    this exists to surface are workload failures a shell run skips past.
    """
    return json.dumps(
        {
            "sns_topic_arn": SNS_TOPIC_ARN,
            "pipeline_role": "weekly",
            "skip_weekly_run_day_gate": True,
        }
    )


def _start_weekly_run(execution_name: str) -> str:
    client = boto3.client("stepfunctions", region_name=REGION)
    resp = client.start_execution(
        stateMachineArn=SATURDAY_SF_ARN,
        name=execution_name,
        input=_build_run_input(),
    )
    return resp["executionArn"]


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    detail = event.get("detail") or {}

    sm_arn = detail.get("stateMachineArn", "")
    sm_name = sm_arn.rsplit(":", 1)[-1]
    status = detail.get("status", "")
    if sm_name != EOD_SF_NAME or status != "SUCCEEDED":
        # EventBridge rule is filtered, but defend against accidental
        # invocations (manual test fires, rule drift). Not a fail-loud case
        # — this is a "wrong audience" log, not a contract violation.
        logger.info(
            "ignored event: sm_name=%s status=%s (expected %s/SUCCEEDED)",
            sm_name,
            status,
            EOD_SF_NAME,
        )
        return {"fired": False, "reason": "wrong_event"}

    stop_date_ms = detail.get("stopDate")
    if stop_date_ms is None:
        raise RuntimeError(
            "EOD SUCCEEDED event missing detail.stopDate — upstream contract violation"
        )

    trading_day = _derive_trading_day_utc_ms(stop_date_ms)

    # No weekday filter. Every successful post-close starts a full weekly run
    # (Brian ruling 2026-07-29) — see the module docstring for why the old
    # Friday-only shell-run shape was the problem rather than the design.
    eod_name = detail.get("name", "unknown")
    run_name = f"eod-daily-{trading_day.isoformat()}-{eod_name}"[:80]
    run_arn = _start_weekly_run(run_name)
    logger.info(
        "EOD SUCCEEDED (trading_day=%s) -> started weekly-freshness run: %s",
        trading_day.isoformat(),
        run_arn,
    )
    return {
        "fired": True,
        "trading_day": trading_day.isoformat(),
        "weekly_execution_arn": run_arn,
        "weekly_execution_name": run_name,
    }
