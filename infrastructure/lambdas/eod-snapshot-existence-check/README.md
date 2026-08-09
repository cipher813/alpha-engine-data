# alpha-engine-eod-snapshot-existence-check

Pre-midnight **positive existence check** for the EOD pipeline's CaptureSnapshot output (**alpha-engine-config-I6705**, I5569 deliverable #3).

## Why

`executor/snapshot_capturer.py` (crucible-executor) is **live-capture-only** — it reads now-as-of Interactive Brokers account/position state, never a historical replay. If the day's `trades/snapshots/{run_date}.json` is never written before NYSE-local midnight, that day's IB state is gone permanently (cost measured in alpha-engine-config-I5325).

I5569 deliverables #1-2 (same-day bounded retry + irreversible-deadline paging, nousergon-data-PR1260) live **inside** the EOD Step Function's `CaptureSnapshot` state — they only help if the SF actually reaches that state. This Lambda is deliberately **separate scheduled infrastructure**: it fires on its own cron and checks the artifact directly, so it still catches the case where the EOD SF never started at all (daemon crash before the shutdown hook, a killed `RunDaemon` SSM step, or any earlier-state failure).

## What it does

Fires **20:30 America/Los_Angeles MON-FRI** (23:30 ET — after the 13:00 PT EOD window and the 22:30 UTC eod-backstop firing, >30min before NYSE-local midnight):

1. **Not a NYSE trading day** → no-op, log only.
2. **Trading day, snapshot present** (`head_object` succeeds) → silent success, log only.
3. **Trading day, snapshot absent** (`head_object` 404/NoSuchKey) → pages `alpha-engine-watchdog-alerts` (existing SNS topic — the same one `alpha-engine-pipeline-watchdog` uses) with the irreversibility framing + the manual recovery command.
4. **Any other AWS error** → raises (fail-loud). A probe that resolves "I couldn't check" to "verified absent" would non-deterministically skip paging on the one evening it matters.

## Fail-loud

Per `feedback_no_silent_fails`: only a genuine NoSuchKey/404 counts as "absent". `alerts.publish` itself is best-effort by lib design (never raises) — a paging-channel failure must not crash or mask this probe's own S3 check.

## Deploy / safe rollout

```bash
# first-time create (Scheduler schedule created per the automation-pause manifest's state)
bash infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh --bootstrap
# code update only
bash infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh
```

IAM role/policy creation requires `iam:CreateRole`/`iam:PutRolePolicy`, which the CI OIDC role (`github-actions-lambda-deploy`) deliberately does not hold (single-writer rule, `infrastructure/iam/README.md`) — `--bootstrap` must be run by an operator with `AWS_PROFILE=ne-admin`.

## Config

| env var | default |
|---|---|
| `SNAPSHOT_CHECK_BUCKET` | `alpha-engine-research` |
| `WATCHDOG_SNS_TOPIC_ARN` | `…:alpha-engine-watchdog-alerts` |

Schedule: `cron(30 20 ? * MON-FRI *)` in `America/Los_Angeles` (Scheduler-native timezone — DST-correct with no seasonal cron edit).
