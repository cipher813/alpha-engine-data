#!/usr/bin/env bash
# deploy_eod_eventbridge.sh — Wire the alpha-engine-eod-pipeline SF to
# its EventBridge trigger.
#
# Background: the EOD SF was provisioned 2026-04-07 and orphaned
# 2026-04-22 (PR #94 in alpha-engine removed the daemon-side trigger
# without replacing it). For 6 days, EOD ran via systemd timers on
# ae-trading instead of the SF — silent regression that hosted the
# 4/27 historical-rerun corruption class. Phase 1 of the EOD-SF
# cutover (this script + retired systemd timers in alpha-engine PR
# feat/eod-pipeline-sf-canonical) restores the SF as the single
# authoritative EOD path.
#
# Mirrors deploy_step_function_daily.sh's pattern: put-rule with
# weekday cron + put-targets pointing at the SF + put-role-policy
# extending alpha-engine-eventbridge-sfn-role to start this SF.
#
# Idempotent. Re-run after any cron / instance-id / SNS change.
#
# Usage:
#   ./infrastructure/deploy_eod_eventbridge.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

STATE_MACHINE_NAME="alpha-engine-eod-pipeline"
SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
EVENTBRIDGE_RULE="alpha-engine-eod"
EB_ROLE_NAME="alpha-engine-eventbridge-sfn-role"
EB_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${EB_ROLE_NAME}"

TRADING_INSTANCE="${AE_TRADING_INSTANCE_ID:-i-018eb3307a21329bf}"

# 20:05 UTC = 13:05 PDT (matches the prior systemd daily-data timer
# cadence). In PST winter, this fires at 12:05 PT — slight misalignment
# accepted; system can be re-cronned at DST boundaries.
SCHEDULE_EXPRESSION="cron(5 20 ? * MON-FRI *)"

echo "=== Alpha Engine EOD EventBridge Deployment ==="
echo "  Region:          $REGION"
echo "  Account:         $ACCOUNT_ID"
echo "  State machine:   $STATE_MACHINE_NAME"
echo "  Trading EC2:     $TRADING_INSTANCE"
echo "  Schedule:        $SCHEDULE_EXPRESSION (13:05 PDT / 12:05 PST)"
echo ""

# ── Verify SF exists ────────────────────────────────────────────────────────

if ! aws stepfunctions describe-state-machine \
        --state-machine-arn "$SM_ARN" \
        --region "$REGION" > /dev/null 2>&1; then
    echo "ERROR: state machine $STATE_MACHINE_NAME not found at $SM_ARN" >&2
    echo "       The SF must be provisioned before this script can wire it" >&2
    echo "       to EventBridge. See infrastructure/cloudformation/ for" >&2
    echo "       the SF definition." >&2
    exit 1
fi
echo "  SF exists:       OK"

# ── EventBridge Rule ────────────────────────────────────────────────────────

echo "Creating EventBridge rule: $EVENTBRIDGE_RULE..."

aws events put-rule \
    --name "$EVENTBRIDGE_RULE" \
    --schedule-expression "$SCHEDULE_EXPRESSION" \
    --state ENABLED \
    --description "Weekday 20:05 UTC (13:05 PDT) — post-market data + EOD reconcile + stop trading instance" \
    --region "$REGION" > /dev/null

# ── Update EventBridge → SFN role policy to include EOD pipeline ────────────
#
# The role policy must list every SF the role is allowed to start.
# Dropping one silently breaks that day's run (caught 2026-04-21 when
# the policy lost weekday-pipeline). All three pipelines listed here.

echo "Updating $EB_ROLE_NAME policy (must include all 3 pipeline ARNs)..."
EB_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": [
        "arn:aws:states:'"$REGION"':'"$ACCOUNT_ID"':stateMachine:alpha-engine-saturday-pipeline",
        "arn:aws:states:'"$REGION"':'"$ACCOUNT_ID"':stateMachine:alpha-engine-weekday-pipeline",
        "'"$SM_ARN"'"
      ]
    }
  ]
}'

aws iam put-role-policy \
    --role-name "$EB_ROLE_NAME" \
    --policy-name "${EB_ROLE_NAME}-policy" \
    --policy-document "$EB_POLICY" \
    --region "$REGION"

# ── Target ──────────────────────────────────────────────────────────────────

INPUT_JSON=$(cat <<EOF
{
  "trading_instance_id": ["$TRADING_INSTANCE"],
  "sns_topic_arn": "$SNS_TOPIC_ARN"
}
EOF
)

aws events put-targets \
    --rule "$EVENTBRIDGE_RULE" \
    --targets '[{
        "Id": "1",
        "Arn": "'"$SM_ARN"'",
        "RoleArn": "'"$EB_ROLE_ARN"'",
        "Input": '"$(echo "$INPUT_JSON" | python3 -c "import sys,json; print(json.dumps(json.dumps(json.load(sys.stdin))))")"'
    }]' \
    --region "$REGION" > /dev/null

echo "  Rule:            $EVENTBRIDGE_RULE → $STATE_MACHINE_NAME"

# ── Disable redundant alpha-engine-stop-trading scheduler ───────────────────
#
# The SF's StopTradingInstance step replaces this Scheduler. Per
# the "no redundant paths around load-bearing scheduled infra"
# preference, retire it. Idempotent — `update-schedule` succeeds
# whether the schedule is currently ENABLED or DISABLED.

echo "Disabling redundant alpha-engine-stop-trading scheduler..."
if aws scheduler get-schedule --name "alpha-engine-stop-trading" \
        --region "$REGION" > /dev/null 2>&1; then
    # Re-pull current schedule to preserve all fields, then update state.
    SCHED=$(aws scheduler get-schedule --name "alpha-engine-stop-trading" --region "$REGION")
    SCHED_EXPR=$(echo "$SCHED" | python3 -c "import sys,json; print(json.load(sys.stdin)['ScheduleExpression'])")
    SCHED_TZ=$(echo "$SCHED" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ScheduleExpressionTimezone', 'UTC'))")
    SCHED_TARGET=$(echo "$SCHED" | python3 -c "import sys,json; t=json.load(sys.stdin)['Target']; print(json.dumps({k:v for k,v in t.items() if k in ('Arn','RoleArn','Input')}))")

    aws scheduler update-schedule \
        --name "alpha-engine-stop-trading" \
        --schedule-expression "$SCHED_EXPR" \
        --schedule-expression-timezone "$SCHED_TZ" \
        --flexible-time-window '{"Mode":"OFF"}' \
        --target "$SCHED_TARGET" \
        --state DISABLED \
        --region "$REGION" > /dev/null
    echo "  Scheduler:       alpha-engine-stop-trading → DISABLED (SF StopTradingInstance step replaces it)"
else
    echo "  Scheduler:       alpha-engine-stop-trading not found — skipping (already removed?)"
fi

echo ""
echo "=== EOD EventBridge Deployment Complete ==="
echo ""
echo "Verify:"
echo "  aws events describe-rule --name $EVENTBRIDGE_RULE --region $REGION"
echo "  aws events list-targets-by-rule --rule $EVENTBRIDGE_RULE --region $REGION"
echo ""
echo "First firing: tomorrow weekday at 13:05 PDT (20:05 UTC)."
