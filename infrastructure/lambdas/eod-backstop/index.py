"""alpha-engine-eod-backstop — same-day EOD-pipeline trigger of last resort.

The EOD Step Function (``ne-postclose-trading-pipeline``) is normally started by
the trading daemon's shutdown hook (``daemon.py`` finally block). That is the
SOLE trigger — a deliberate "no-backstop design". If the daemon dies before
its shutdown hook, the SSM ``RunDaemon`` step never reaches the finally block,
or the daemon never starts, the EOD SF never fires: no PostMarketData, no
CaptureSnapshot, and — the load-bearing failure — NO ``eod_pnl`` ROW for the
day. The next day's EOD reconcile then has no adjacent prior-day NAV baseline
and the headline daily return/alpha span multiple sessions (the 2026-06-24
gap → RGEN +14.92% class of bug; config#1229).

This Lambda is the missing backstop. Triggered by EventBridge ~22:30 UTC on
weekdays (well after the daemon's nominal ~20:15 UTC EOD), it starts the EOD
SF IFF:

  1. it is a NYSE trading day (an expected-EOD day at all), AND
  2. no EOD execution has STARTED today — so we never double-run after a
     daemon-triggered EOD that already completed (or is mid-flight).

WIDENED 2026-08-09 (alpha-engine-config-I6690): the original design also
required the trading box to still be RUNNING before dispatching, on the
theory that a stopped box meant "EOD already ran" or "box never booted, so
nothing to reconcile". That second premise was wrong: on 2026-08-05/06 the
PREOPEN pipeline itself failed pre-boot (config-I6615), the trading box never
started AT ALL, the daemon never ran, and this backstop's box-running gate
made it a silent no-op too — no EOD, no ``eod_pnl`` row, and (with
``alpha-engine-pipeline-watchdog-daily`` paused under I6617) no alert of any
kind. A box-never-started day is exactly the case this backstop most needs to
cover, so the box-running condition is dropped entirely: the EOD SF's own
first real state, ``StartTradingInstance`` (``step_function_eod.json:96``),
boots the box unconditionally and is idempotent (``ec2:startInstances`` on an
already-running box is a no-op) — so this Lambda needs no boot logic of its
own, whether the box was up, down, or never started. ``_trading_box_running``
is retained purely to TAG the dispatch (``triggered_by``) for observability,
never to gate it.

If the box is already stopped, EOD either ran (success or failure — both end
in stopping the box, caught by the ``eod_ran_today`` guard) or the box never
booted (caught by the widened dispatch above). The late-discovery case (box
long gone, gap found days later) is NOT this Lambda's job — that is the IBKR
Flex Query ``eod_pnl`` backfill (config#1229).

The EOD SF's own DynamoDB mutex (``AcquireMutex``) is the concurrency
backstop: if a daemon-triggered EOD is mid-flight when this fires, our
StartExecution would only hit ``MutexConflict`` and fail cleanly — but the
``eod_ran_today`` guard means we don't even attempt it.

CaptureSnapshot on a freshly-booted box (config-I6690 evidence, verified
against crucible-executor + step_function_eod.json, not assumed): IB Gateway
comes up via the ``ibgateway.service`` systemd unit, which is a hard
``Requires=``/``After=`` dependency of both ``alpha-engine-daemon.service``
and ``alpha-engine-morning.service`` (both ``WantedBy=multi-user.target``,
i.e. boot-enabled) — so any boot of the trading box, cold or warm, pulls
ibgateway up as a systemd dependency with no separate enablement needed.
Between ``StartTradingInstance`` and ``CaptureSnapshot`` the EOD SF spends
several minutes on ``WaitForInstanceReady``/the SSM-readiness poll (up to
~3 min), the conditional executor-checkout refresh, and — load-bearing —
``LaunchPostMarketDataSpot``'s post-market-data phase on a wholly separate
ephemeral spot box (``TimeoutSeconds: 420`` on the launch alone, plus its own
poll-to-completion), none of which touch or wait on the trading box's IB
session. That multi-minute buffer comfortably exceeds
``wait-for-ibgateway.sh``'s own 120s max-wait budget and the ~30s TOTP-auth
window ``alpha-engine-morning.service`` budgets for. ``executor/
snapshot_capturer.py``'s ``IBKRClient.connect`` additionally retries 3x with
exponential backoff (``executor/retry.py``) on its own, an extra ~60-90s of
tolerance. No explicit ``wait-for-ibgateway`` state exists inside
``CaptureSnapshot``'s own SSM command, so this is evidence of comfortable
timing margin, not a guarantee — if a cold-boot CaptureSnapshot failure is
ever observed in practice, the fix is to add an explicit gateway-readiness
poll to ``CaptureSnapshot``'s SSM command, not to skip the snapshot (the
I2700 skip shape assumes a snapshot ALREADY exists from an earlier attempt
in the same execution — that is never true on a box that never booted, so
skipping here would just hand ``EODReconcile`` no snapshot to read and it
would hard-fail with no fallback, by design).

Fail-loud (``feedback_no_silent_fails``): any AWS call failure raises so the
EventBridge retry + Lambda-error CloudWatch alarm page the operator. We must
never silently skip the check on the one day it matters.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

from nousergon_lib.trading_calendar import is_trading_day, last_closed_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

EOD_SF_ARN = os.environ.get(
    "EOD_SF_ARN",
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline",
)
# The trading box (CaptureSnapshot / EODReconcile / StopTradingInstance target)
# and the dashboard box. ec2_instance_id (dashboard box) no longer targets an
# SSM InstanceIds param directly since DailySubstrateHealthCheck was spun out
# to a standalone dashboard-box systemd timer (alpha-engine-config-I2722,
# 2026-07-16) — it is still carried through the SF's top-level input because
# HealDispatchReplay passes it verbatim into its own replay execution's Input
# (schema fidelity for the closed self-heal loop, config-I2702). Mirror the
# daemon's _trigger_eod_pipeline input shape so the SF runs identically to a
# normal EOD.
TRADING_INSTANCE_ID = os.environ.get("TRADING_INSTANCE_ID", "i-018eb3307a21329bf")
DASHBOARD_INSTANCE_ID = os.environ.get("DASHBOARD_INSTANCE_ID", "i-09b539c844515d549")
SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN", f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"
)

# Count an EOD as "already fired today" regardless of terminal status — a
# started-then-failed EOD still ran HandleFailure → ForceStopInstance (box
# stopped again either way, config-I6690: no longer load-bearing here since
# dispatch isn't gated on box state); this guard's job is preventing a
# double-start, including racing a mid-flight (RUNNING) EOD.
_STARTED_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")


def _trading_box_running(ec2_client: Optional[object] = None) -> bool:
    """True iff the trading EC2 instance is in the ``running`` state.

    OBSERVABILITY-ONLY (config-I6690): no longer gates dispatch — a stopped
    box is exactly the box-never-started case this backstop must still cover.
    Used solely to tag ``_start_eod``'s ``triggered_by`` value so a dashboard
    reader can tell "daemon was up but didn't fire EOD" apart from "box was
    never running at all" without re-deriving it from EC2 state. Raises on an
    EC2 API failure (fail-loud)."""
    if ec2_client is None:  # pragma: no cover — production path
        ec2_client = boto3.client("ec2", region_name=REGION)
    resp = ec2_client.describe_instances(InstanceIds=[TRADING_INSTANCE_ID])
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            state = (inst.get("State") or {}).get("Name")
            logger.info("Trading box %s state=%s", TRADING_INSTANCE_ID, state)
            return state == "running"
    logger.info("Trading box %s not found in describe_instances", TRADING_INSTANCE_ID)
    return False


def _eod_ran_today(now_utc: datetime, sf_client: Optional[object] = None) -> bool:
    """True iff at least one EOD SF execution STARTED since 00:00 UTC today.

    At the ~22:30 UTC firing time, today's expected EOD (~20:00–21:30 UTC) is
    within the since-midnight window, while the prior trading day's EOD is not
    — so this is trading-day-correct without a per-day marker. Raises on a
    ListExecutions failure (fail-loud)."""
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    for status_filter in _STARTED_STATUSES:
        next_token: Optional[str] = None
        while True:
            kwargs = {
                "stateMachineArn": EOD_SF_ARN,
                "statusFilter": status_filter,
                "maxResults": 100,
            }
            if next_token:
                kwargs["nextToken"] = next_token
            resp = sf_client.list_executions(**kwargs)
            for row in resp.get("executions") or []:
                start = row.get("startDate")
                if not hasattr(start, "astimezone"):
                    continue
                start_utc = (
                    start.astimezone(timezone.utc)
                    if start.tzinfo
                    else start.replace(tzinfo=timezone.utc)
                )
                if start_utc >= midnight:
                    logger.info(
                        "EOD execution %s already started today (%s, %s)",
                        row.get("name"), start_utc.isoformat(), status_filter,
                    )
                    return True
            next_token = resp.get("nextToken")
            if not next_token:
                break
    return False


def _start_eod(trading_day: str, triggered_by: str, sf_client: Optional[object] = None) -> str:
    """Start the EOD SF with the same input shape the daemon uses (config-I6690:
    identical regardless of whether the box was running — see the module
    docstring's CaptureSnapshot-on-a-cold-box evidence), tagged with the given
    ``triggered_by``. Returns the execution ARN."""
    if sf_client is None:  # pragma: no cover — production path
        sf_client = boto3.client("stepfunctions", region_name=REGION)
    resp = sf_client.start_execution(
        stateMachineArn=EOD_SF_ARN,
        name=f"eod-backstop-{trading_day}-{int(time.time())}",
        input=json.dumps(
            {
                "trading_instance_id": [TRADING_INSTANCE_ID],
                "ec2_instance_id": [DASHBOARD_INSTANCE_ID],
                "sns_topic_arn": SNS_TOPIC_ARN,
                "run_date": trading_day,
                "triggered_by": triggered_by,
                "pipeline_role": "eod",
            }
        ),
    )
    arn = resp.get("executionArn", "")
    logger.warning(
        "EOD-BACKSTOP fired (triggered_by=%s): no EOD ran today for "
        "trading_day=%s — started EOD SF %s",
        triggered_by, trading_day, arn,
    )
    return arn


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    now_utc = datetime.now(timezone.utc)

    # Only trading days have an expected EOD. The EventBridge rule is MON-FRI,
    # so this skips NYSE holidays that fall on weekdays.
    if not is_trading_day(now_utc.date()):
        logger.info("Not a NYSE trading day (%s) — no EOD expected; no-op.", now_utc.date())
        return {"action": "noop", "reason": "not_a_trading_day", "date": str(now_utc.date())}

    trading_day = last_closed_trading_day(now_utc).isoformat()

    if _eod_ran_today(now_utc):
        logger.info("An EOD execution already started today — no-op.")
        return {"action": "noop", "reason": "eod_already_ran_today", "trading_day": trading_day}

    # config-I6690: box state no longer gates dispatch — it is checked only
    # to tag the run (StartTradingInstance boots the box unconditionally and
    # idempotently either way; see module docstring).
    box_was_running = _trading_box_running()
    triggered_by = "backstop" if box_was_running else "backstop-box-stopped"
    execution_arn = _start_eod(trading_day, triggered_by)
    return {
        "action": "started_eod",
        "trading_day": trading_day,
        "execution_arn": execution_arn,
        "box_was_running": box_was_running,
    }
