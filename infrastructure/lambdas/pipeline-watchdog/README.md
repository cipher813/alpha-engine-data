# alpha-engine-pipeline-watchdog

Phase 4 of the pipeline-reporting-revamp arc (ROADMAP L3050). Daily
NYSE-trading-day-aware watchdog for the 3 Alpha Engine Step Functions, plus
(alpha-engine-config#2412) a preopen schedule-buffer canary.

## What it does

Cron-fires daily at 14:00 UTC (≈ 07:00 PT, well after every SF's expected
start time). For each of the 3 Step Functions, checks whether at least one
execution started in the expected window. If a check fails, publishes an
alert via `nousergon_lib.alerts.publish` to a DISTINCT SNS topic
(`alpha-engine-watchdog-alerts`, NOT `alpha-engine-alerts`) AND to Telegram
in parallel — channel independence preserved per plan doc §3.5.

| SF | Window | Watch-day condition |
|---|---|---|
| Weekday SF | 24h | TODAY is a NYSE trading day (via `nousergon_lib.trading_calendar`) |
| EOD SF     | 24h | TODAY is a NYSE trading day |
| Saturday SF | 7d  | TODAY is Sunday (Saturday SF fires Sat 09:00 UTC; by Sun 14:00 UTC any missed firing is 24+h overdue) |

## Preopen schedule-buffer canary (alpha-engine-config#2412)

A 4th check, run alongside the 3 above whenever today is a NYSE trading day.
`WeekdayPipelineSchedule`'s trigger has been moved earlier twice after
finishing after the 06:30 AM PT market open — 06:00→05:45 PT (2026-05-19),
then 05:45→05:15 PT (2026-07-13) — both times the buffer erosion was
noticed anecdotally, days after it started. This check reads the finish
(`stopDate`) of the most recently CLOSED trading day's SUCCEEDED Weekday-SF
execution and alerts BEFORE the buffer is consumed again:

- **Hard floor** (severity=error): finish at/after **06:15 PT** (15-min
  buffer floor). A finish at/after the actual **06:30 PT** open gets a
  distinct "MISSED THE OPEN" message.
- **Early warning** (severity=warning): finish at/after **06:10 PT** but
  before the hard floor.
- **5-day trend** (severity=warning): even when today's own reading is
  quiet, a **median** over the last 5 trading days' SUCCEEDED finishes at/
  past the 06:10 PT floor fires a distinct trend alert — catches a creep
  that never individually crosses either threshold on a single day.
- **No SUCCEEDED execution** for the target day → deferred silently to the
  existing Weekday-SF liveness check (0-executions case) or the SF's own
  failure alert — never double-paged.

All thresholds are evaluated in `America/Los_Angeles` via `zoneinfo`
(DST-correct year-round; market open is a fixed 06:30 PT local-clock time).
Reuses the same `WATCHDOG_SNS_TOPIC_ARN` + Telegram fan-out as the 3 checks
above — no new channel. Deliberately does NOT filter by `pipeline_role`
(would need `states:DescribeExecution`, which this Lambda's IAM role does
not currently grant — see the PR body for the live AccessDeniedException
this already causes on the Saturday-SF role-filtered path); instead uses
the earliest-started SUCCEEDED execution per PT calendar day as the proxy
for "the scheduled run".

## Why this exists (vs. a dumb CW alarm)

Per Phase 0 Q2 SOTA-lock, a naive `AWS/States ExecutionsStarted` alarm with
a 24h window would false-positive every weekend for Weekday + EOD. Alert
hygiene is load-bearing: a watchdog that false-positives twice every weekend
trains the operator to silence it, defeating its purpose. The
`nousergon_lib.trading_calendar` chokepoint encodes NYSE
holiday + weekend awareness so the Lambda fires cleanly only on genuine
missed executions on expected trading days.

## Channel-independence design (plan doc §3.5)

The watchdog publishes to a NEW SNS topic (`alpha-engine-watchdog-alerts`),
NOT the existing `alpha-engine-alerts` topic. Rationale: if the
operator's regular `alpha-engine-alerts` → email path silently breaks,
this watchdog's separate publish path still reaches the operator.
Telegram (delivered via the lib's dual fan-out) is the non-overlapping
second channel.

Subscribers to the watchdog topic are operator choice — email, pagerduty,
slack — without polluting the trade-decision alert channel.

## Dedup

Each per-(SF, date) alert carries a deterministic `dedup_key` and a
12-hour window so a persistent outage doesn't re-page the operator
every cron firing. Once the underlying issue is fixed and the SF runs,
the next cron firing clears the check and stops alerting.

## Deploy

```bash
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --bootstrap   # first-time
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh                # code update
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --dry-run     # preview
bash infrastructure/lambdas/pipeline-watchdog/deploy.sh --smoke       # invoke once
```

Bootstrap is idempotent — re-running creates only missing resources.

## Subscribe email to the watchdog topic (manual, post-deploy)

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:711398986525:alpha-engine-watchdog-alerts \
  --protocol email \
  --notification-endpoint cipher813@gmail.com \
  --region us-east-1
# confirm subscription via the email link AWS sends
```

## Operational

- Cron: `cron(0 14 * * ? *)` — daily 14:00 UTC, MUTABLE via
  `aws events put-rule --schedule-expression ...` if the firing window
  needs to shift.
- Lambda timeout: 60s. Three `ListExecutions` paginated walks per
  invocation; in practice completes in < 5s.
- Logs: `/aws/lambda/alpha-engine-pipeline-watchdog` in us-east-1
  CloudWatch Logs.
- IAM: minimum-privilege per `iam-policy.json` — list SF executions,
  publish to ONE SNS topic, read 2 SSM parameters (Telegram creds),
  read/write 1 S3 prefix (dedup markers). NO `states:StartExecution`,
  NO `alpha-engine-alerts` publish.

## Composes with

- `nousergon_lib.alerts` v0.24.0+ (dual-channel publish + dedup)
- `nousergon_lib.trading_calendar` v0.27.0+ (NYSE-holiday-aware gate)
- `sf-telegram-notifier` (data #275) — sibling Lambda using same
  EventBridge → Telegram delivery pattern (this Lambda uses the
  lib chokepoint instead of duplicating the Telegram code)
- `eod-success-friday-shell-trigger` (data #282) — sibling Lambda
  using identical deploy.sh + IAM + cron-Lambda structure (this is
  the mirror pattern per Q2 lock)
- Plan doc §3.5 (channel-independence design)
- ROADMAP L3050 (Phase 4 tracking line)
