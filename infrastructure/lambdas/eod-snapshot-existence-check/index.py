"""alpha-engine-eod-snapshot-existence-check — pre-midnight positive
existence check for CaptureSnapshot's output (alpha-engine-config-I5569
deliverable #3, tracked as alpha-engine-config-I6705).

**Why this Lambda exists, separate from the EOD SF.** I5569 deliverables
#1-2 (bounded same-day retry + irreversible-deadline paging, shipped in
nousergon-data-PR1260) live INSIDE `infrastructure/step_function_eod.json` —
they only fire if the EOD SF actually reaches (or retries) the
`CaptureSnapshot` state. If the EOD SF never starts at all (daemon crash
before the shutdown hook, SSM `RunDaemon` step killed, the daemon never
boots), or dies at an earlier state, PR1260's machinery never runs and the
day's snapshot silently never gets captured. This Lambda is independent
scheduled infrastructure — it fires on its own EventBridge Scheduler
cron and asserts the artifact exists REGARDLESS of what the EOD SF did
or didn't do.

**What "irreversible" means here.** `executor/snapshot_capturer.py`
(crucible-executor) captures LIVE Interactive Brokers account/position
state — it is explicitly NOT a historical read (see that module's
`run()` docstring: a non-today `run_date` is refused). Once NYSE local
midnight passes without a snapshot for `run_date`, that day's IB state is
gone — there is no way to reconstruct it after the fact. The cost of a
missed snapshot was measured in alpha-engine-config-I5325.

**Behavior:**

  1. Not a NYSE trading day (`nousergon_lib.trading_calendar.is_trading_day`
     on today's America/New_York calendar date) → no-op, log only. No
     snapshot is expected on a non-trading day.
  2. Trading day + snapshot present (`s3 head_object` on
     `trades/snapshots/{trading_day}.json` succeeds) → silent success,
     log only. This is the expected happy path on every normal trading
     day and must NOT page.
  3. Trading day + snapshot ABSENT (`head_object` 404 / NoSuchKey) → page
     `alpha-engine-watchdog-alerts` (the existing SNS topic pipeline-watchdog
     already uses for exactly this "structural pipeline health" channel —
     no new topic). The message names the irreversibility and the manual
     recovery command.
  4. Any other AWS error (IAM drift, throttling, a malformed bucket name)
     → RAISE. Per `feedback_no_silent_fails`: a probe that treats "I
     couldn't check" the same as "verified absent" would non-deterministically
     skip paging on the one evening it matters. The EventBridge retry +
     Lambda-error CloudWatch alarm are the backstop for a probe-infra
     failure.

**Timing.** Scheduled for 20:30 America/Los_Angeles (Scheduler-native
timezone, DST-correct without manual cron edits) MON-FRI — after the
13:00 PT EOD daemon shutdown window and the 22:30 UTC eod-backstop
firing, comfortably before NYSE-local midnight ET (America/New_York is
always UTC-4/UTC-5, i.e. 3 hours ahead of Pacific — 20:30 PT is 23:30 ET,
leaving a 30-minute-plus margin before the day's snapshot genuinely
becomes unrecoverable).

Why paging uses `krepis.alerts.publish` (via the `nousergon_lib.alerts`
back-compat shim) rather than a raw `boto3 sns.publish` call: this is the
established fleet primitive for the operator-surveillance fan-out
(`alpha-engine-pipeline-watchdog` is the sibling consumer of the SAME
`alpha-engine-watchdog-alerts` topic) — mirroring rather than
reinventing a third paging call site.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError
from nousergon_lib import alerts
from nousergon_lib.trading_calendar import is_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")
BUCKET = os.environ.get("SNAPSHOT_CHECK_BUCKET", "alpha-engine-research")
SNS_TOPIC_ARN = os.environ.get(
    "WATCHDOG_SNS_TOPIC_ARN",
    f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-watchdog-alerts",
)

# NYSE-local calendar date is what `is_trading_day` + the snapshot's
# `run_date` key are both keyed on — never UTC, which can disagree with
# the ET trading date near either midnight boundary.
_ET = ZoneInfo("America/New_York")


def _snapshot_key(trading_day: str) -> str:
    """Mirrors `executor/snapshot_capturer.py::_snapshot_key` exactly —
    duplicated rather than imported because this Lambda deploys standalone
    (no crucible-executor package in its zip); keep both in sync by hand."""
    return f"trades/snapshots/{trading_day}.json"


def _snapshot_exists(s3client, bucket: str, key: str) -> bool:
    """True iff `key` exists in `bucket`. False only on a genuine
    NoSuchKey/404 — the legitimate "not captured yet" state. ANY OTHER S3
    failure RAISES (fail-loud) — see module docstring point 4."""
    try:
        s3client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return False
        raise
    return True


def _page_absent_snapshot(trading_day: str, key: str) -> None:
    """Publish the irreversibility alert to `alpha-engine-watchdog-alerts`.

    No `dedup_key` — this Lambda fires at most once per trading day (one
    EventBridge Scheduler cron tick), so within-run duplicate suppression
    buys nothing and would only add S3 dedup-marker IAM surface for no
    benefit. `alerts.publish` never raises (best-effort by lib design,
    see krepis.alerts module docstring) — a paging-channel failure must
    not mask or crash this probe.
    """
    message = (
        f"No EOD snapshot found at s3://{BUCKET}/{key} as of the pre-midnight "
        f"check. CaptureSnapshot (executor/snapshot_capturer.py) is "
        f"LIVE-CAPTURE-ONLY — it reads now-as-of Interactive Brokers account "
        f"and position state, not a historical read. After NYSE-local "
        f"midnight tonight, {trading_day}'s snapshot becomes PERMANENTLY "
        f"UNRECOVERABLE (cost of a missed day measured in "
        f"alpha-engine-config-I5325). Manual capture while there is still "
        f"time: run `python executor/snapshot_capturer.py --date {trading_day}` "
        f"on ae-trading while IB Gateway is up. The EOD SF's own same-day "
        f"bounded retry + irreversible-deadline paging (nousergon-data-PR1260) "
        f"already covers a CaptureSnapshot step that ran and failed — this "
        f"alert means the EOD SF likely never reached that step at all "
        f"today (or never started)."
    )
    result = alerts.publish(
        message=message,
        severity="critical",
        source="alpha-engine-eod-snapshot-existence-check",
        sns=True,
        telegram=False,
        sns_topic_arn=SNS_TOPIC_ARN,
    )
    logger.warning(
        "EOD-SNAPSHOT-EXISTENCE-CHECK ALERT: trading_day=%s key=%s sns_ok=%s",
        trading_day, key, result.sns.ok,
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    now_et = datetime.now(_ET)
    today = now_et.date()

    if not is_trading_day(today):
        logger.info("Not a NYSE trading day (%s, ET) — no snapshot expected; no-op.", today)
        return {"action": "noop", "reason": "not_a_trading_day", "date": str(today)}

    trading_day = today.isoformat()
    key = _snapshot_key(trading_day)

    s3client = boto3.client("s3", region_name=REGION)
    if _snapshot_exists(s3client, BUCKET, key):
        logger.info("Snapshot present: s3://%s/%s — silent success.", BUCKET, key)
        return {"action": "noop", "reason": "snapshot_present", "trading_day": trading_day}

    _page_absent_snapshot(trading_day, key)
    return {"action": "paged", "reason": "snapshot_absent", "trading_day": trading_day, "key": key}
