"""alpha-engine-dlq-redrive-monitor — the Overseer intake DLQ's replay path
(alpha-engine-config-I8111).

WHY THIS EXISTS. On 2026-08-21 `nousergon-overseer-intake-dlq` held 93
visible messages, oldest 218h24m (9.1 days) against 14-day retention — ~4.9
days from silent expiry. Nothing replayed them and nothing paged: the queue
drained only because Brian happened to re-enable the four
`alpha-engine-alert-drain-*utc` schedules, whose next run (22:00 UTC)
incidentally cleared the backlog. `grep -rn "StartMessageMoveTask"` across
the fleet returns zero hits — SQS's native DLQ redrive was never wired up.
`alpha-engine-config/scripts/dlq_archive_purge.py` is the only existing DLQ
tool, and it is manual-only and archive-then-DELETE, never replay.

WHAT THIS LAMBDA DOES, every invocation (bounded + idempotent, safe to run
as often as scheduled):

  1. REDRIVE (deliverable 1). If the DLQ holds any messages and no
     `StartMessageMoveTask` is already RUNNING against it, start one —
     `nousergon-overseer-intake-dlq` -> `nousergon-overseer-intake`. If one
     IS already running, this call is a no-op poll: it logs the task's
     current `ApproximateNumberOfMessagesMoved`/`...ToMove` and does not
     start a second (SQS itself only permits one active move task per
     source queue, so this doubles as the idempotency guard, not just an
     optimization). Deliberately DECOUPLED from
     `infrastructure/automation_pause.py`'s pause/reactivation edge rather
     than hooked to it: this Lambda's own schedule runs independently of
     — and is never listed in — the pause manifest (see deploy.sh), so it
     keeps redriving on its own cadence whether the alert-drain lane is
     paused, just reactivated, or was always on. That satisfies "before or
     with the first run" after ANY reactivation, including a live console
     edit that bypasses `automation_pause.py` entirely, without editing a
     file four other in-flight PRs are touching concurrently
     (alpha-engine-config-I8111 dispatch constraint).

  2. AGE ALARM (deliverable 2). Reads `ApproximateNumberOfMessages` +
     `ApproximateAgeOfOldestMessage` on the DLQ. Past `AGE_THRESHOLD_SECONDS`
     (default 10 of the 14-day retention, i.e. 864000s) with the queue
     non-empty, pages via `krepis.alerts.publish(severity="error", ...)` —
     the severity floor `alpha-engine-config-I7857` requires for visibility
     on the current transport — naming the count and oldest age, deduped to
     once per `AGE_ALARM_DEDUP_WINDOW_MIN` (default 1440 = daily) so a
     standing breach re-surfaces without paging every 5 minutes. This runs
     on ITS OWN schedule (see deploy.sh), independent of whether the
     alert-drain lane is paused — the drain being off is exactly the
     condition under which the DLQ grows unwatched, so the age check must
     not depend on the drain running.

  3. POISON VS BACKLOG (deliverable 3) is not separate code here — it falls
     out of (1) plus the queue's own `RedrivePolicy maxReceiveCount=5`
     (`infrastructure/setup_overseer_intake.sh`). A message already in the
     DLQ when this runs failed 5 times under UNKNOWN conditions (possibly
     "the consumer was paused," which is backlog, not poison) — the redrive
     step above gives it back to a LIVE consumer with no prejudgment. Only
     a message that fails 5 MORE times against that live consumer is
     unambiguous poison, and SQS's own redrive policy returns exactly that
     message to the DLQ a second time; is a candidate for
     `alpha-engine-config/scripts/dlq_archive_purge.py`'s archive-then-purge
     path, which already exists and does not need duplicating here.

FAIL LOUD (fleet no-silent-swallows rule). Any unexpected `StartMessageMoveTask`
/ `ListMessageMoveTasks` / `GetQueueAttributes` failure raises
`DlqRedriveError` and is NOT caught — the handler's own failure is Lambda's
correctly-alarmed failure surface (this Lambda IS the DLQ's replay guarantee;
swallowing its own errors would recreate exactly the invisible failure mode
this issue exists to close). The one exception SQS defines for "a move task
is already running" is matched by CODE, not swallowed by a bare except.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

from krepis.alerts import publish

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

QUEUE_NAME = os.environ.get("DLQ_REDRIVE_QUEUE_NAME", "nousergon-overseer-intake")
DLQ_NAME = os.environ.get("DLQ_REDRIVE_DLQ_NAME", "nousergon-overseer-intake-dlq")

# 10 of the 14-day (1,209,600s) SQS retention (issue's suggested threshold) —
# leaves 4 days of headroom between the alarm firing and silent expiry.
AGE_THRESHOLD_SECONDS = int(
    os.environ.get("DLQ_REDRIVE_AGE_THRESHOLD_SECONDS", str(10 * 86400))
)
AGE_ALARM_DEDUP_WINDOW_MIN = int(
    os.environ.get("DLQ_REDRIVE_AGE_ALARM_DEDUP_WINDOW_MIN", "1440")
)
# Throttle the move so a large backlog does not itself hammer the intake
# queue / its downstream consumer faster than it can keep up. None = SQS
# default (unthrottled) is also a legal value; the env override allows
# tightening without a redeploy.
MAX_MOVE_MESSAGES_PER_SECOND = os.environ.get("DLQ_REDRIVE_MAX_PER_SECOND")

_RUNNING_STATUSES = {"RUNNING"}


class DlqRedriveError(RuntimeError):
    """Raised on any unexpected SQS condition. Fail-loud by default — this
    Lambda IS the DLQ's replay guarantee; a swallowed error here reproduces
    the exact invisible-failure class alpha-engine-config-I8111 closes."""


def _queue_url(sqs, name: str) -> str:
    try:
        return sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except ClientError as exc:
        raise DlqRedriveError(f"get_queue_url({name}) failed: {exc}") from exc


def _queue_arn(sqs, queue_url: str) -> str:
    try:
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        return attrs["Attributes"]["QueueArn"]
    except (ClientError, KeyError) as exc:
        raise DlqRedriveError(f"get_queue_attributes(QueueArn) for {queue_url} failed: {exc}") from exc


def _dlq_depth_and_age(sqs, dlq_url: str) -> tuple[int, int]:
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateAgeOfOldestMessage"],
        )["Attributes"]
    except ClientError as exc:
        raise DlqRedriveError(f"get_queue_attributes(depth/age) for {dlq_url} failed: {exc}") from exc
    count = int(attrs.get("ApproximateNumberOfMessages", "0"))
    # SQS omits ApproximateAgeOfOldestMessage entirely when the queue has no
    # messages in flight for that stat yet — treat absent as 0, never as a
    # breach (an absent number is not a large number).
    age = int(attrs.get("ApproximateAgeOfOldestMessage", "0"))
    return count, age


def _active_move_task(sqs, dlq_arn: str) -> dict[str, Any] | None:
    try:
        tasks = sqs.list_message_move_tasks(SourceArn=dlq_arn).get("Results", [])
    except ClientError as exc:
        raise DlqRedriveError(f"list_message_move_tasks({dlq_arn}) failed: {exc}") from exc
    for task in tasks:
        if task.get("Status") in _RUNNING_STATUSES:
            return task
    return None


def _start_redrive(sqs, dlq_arn: str, dest_arn: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"SourceArn": dlq_arn, "DestinationArn": dest_arn}
    if MAX_MOVE_MESSAGES_PER_SECOND:
        kwargs["MaxNumberOfMessagesPerSecond"] = int(MAX_MOVE_MESSAGES_PER_SECOND)
    try:
        return sqs.start_message_move_task(**kwargs)
    except ClientError as exc:
        # A task another concurrent invocation started between our check and
        # this call races only against ourselves (idempotent by construction,
        # not by luck): SQS rejects the second start with a message naming
        # an existing move task, which is exactly the "already redriving"
        # state _active_move_task would otherwise have reported. Not a bare
        # except — matched by the specific message text; everything else
        # re-raises as a DlqRedriveError.
        if "already" in str(exc).lower() and "move task" in str(exc).lower():
            logger.info("redrive: a move task started concurrently — treating as running")
            return {"TaskHandle": None}
        raise DlqRedriveError(
            f"start_message_move_task({dlq_arn} -> {dest_arn}) failed: {exc}"
        ) from exc


def run_redrive(sqs) -> dict[str, Any]:
    """Deliverable 1 + 3. Returns a summary dict; never raises on the
    expected "nothing to do" / "already running" paths."""
    dlq_url = _queue_url(sqs, DLQ_NAME)
    dlq_arn = _queue_arn(sqs, dlq_url)
    dest_url = _queue_url(sqs, QUEUE_NAME)
    dest_arn = _queue_arn(sqs, dest_url)

    active = _active_move_task(sqs, dlq_arn)
    if active is not None:
        moved = active.get("ApproximateNumberOfMessagesMoved", 0)
        to_move = active.get("ApproximateNumberOfMessagesToMove", 0)
        logger.info(
            "redrive: move task already RUNNING for %s -> %s (moved=%s to_move=%s)",
            dlq_arn, dest_arn, moved, to_move,
        )
        return {
            "action": "poll",
            "status": "RUNNING",
            "messages_moved": moved,
            "messages_to_move": to_move,
        }

    depth, _age = _dlq_depth_and_age(sqs, dlq_url)
    if depth == 0:
        logger.info("redrive: %s empty — nothing to redrive", DLQ_NAME)
        return {"action": "skip", "reason": "dlq-empty", "dlq_depth": 0}

    result = _start_redrive(sqs, dlq_arn, dest_arn)
    logger.info(
        "redrive: started move task %s (%s -> %s), dlq_depth_at_start=%d",
        result.get("TaskHandle"), dlq_arn, dest_arn, depth,
    )
    return {
        "action": "started",
        "task_handle": result.get("TaskHandle"),
        "dlq_depth_at_start": depth,
    }


def run_age_check(sqs) -> dict[str, Any]:
    """Deliverable 2. Returns a summary dict; pages iff the threshold is
    breached with the queue non-empty."""
    dlq_url = _queue_url(sqs, DLQ_NAME)
    count, age_seconds = _dlq_depth_and_age(sqs, dlq_url)

    breach = count > 0 and age_seconds >= AGE_THRESHOLD_SECONDS
    if breach:
        age_days = age_seconds / 86400
        message = (
            f"{DLQ_NAME}: {count} message(s) on the DLQ, oldest {age_days:.1f}d "
            f"(threshold {AGE_THRESHOLD_SECONDS / 86400:.0f}d of 14d retention). "
            f"Redrive is running on its own schedule (alpha-engine-config-I8111) "
            f"but has not cleared this — investigate whether the intake consumer "
            f"is actually processing what gets redriven."
        )
        logger.error("age-check: BREACH — %s", message)
        publish(
            message,
            severity="error",
            source="dlq-redrive-monitor",
            dedup_key=f"dlq-age-breach-{DLQ_NAME}",
            dedup_window_min=AGE_ALARM_DEDUP_WINDOW_MIN,
        )
    else:
        logger.info(
            "age-check: %s count=%d oldest_age_s=%d (threshold=%ds) — OK",
            DLQ_NAME, count, age_seconds, AGE_THRESHOLD_SECONDS,
        )
    return {
        "breach": breach,
        "dlq_count": count,
        "oldest_age_seconds": age_seconds,
        "threshold_seconds": AGE_THRESHOLD_SECONDS,
    }


def handler(event: dict, context: Any) -> dict[str, Any]:  # noqa: ARG001
    sqs = boto3.client("sqs", region_name=REGION)
    redrive_result = run_redrive(sqs)
    age_result = run_age_check(sqs)
    return {"redrive": redrive_result, "age_check": age_result}
