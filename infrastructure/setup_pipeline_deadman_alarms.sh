#!/usr/bin/env bash
# setup_pipeline_deadman_alarms.sh — provisions the INDEPENDENT backstop SNS
# topic + subscriptions for the pipeline deadman alarms. No longer creates
# the alarms themselves.
#
# alpha-engine-config-I7359, executing the I7359 ownership ruling: an alarm is
# an account resource (infrastructure-ownership-policy.md §2), and this repo
# is PUBLIC while the alarm definitions and applier now live in the PRIVATE
# nous-ergon-ops repo, codified as
# infrastructure/cloudwatch/alarms/alpha-engine-pipeline-deadman-{preopen,postclose}-trading.json.
# Edit those files there; do not add `put-metric-alarm` back here —
# nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the build
# if it reappears.
#
#   nous-ergon-ops: infrastructure/cloudwatch/apply.py --prefix alpha-engine-pipeline-deadman-
#
# What THIS script still does, and why it is not also retired: the
# alpha-engine-alarm-backstop SNS topic and its email + Telegram-forwarder
# subscriptions are not CloudWatch alarms — they are the independent alert
# channel those alarms route to, and this script is their sole provisioner
# (mirrors update_eod_pipeline_sf.sh being the EOD state machine's sole
# manager). Retiring alarm creation must not retire that.
#
# Why the topic/subscription setup stays here rather than moving to
# nous-ergon-ops: SNS topic + Lambda-permission + subscription provisioning is
# ordinary application infrastructure for a Lambda that lives in this repo
# (backstop-telegram-notifier), not an "infrastructure surface" in
# infrastructure-ownership-policy.md §3's sense — no second consumer exists to
# trigger that policy's corollary. Only the alarm DEFINITIONS moved.
#
# Usage:
#   ./infrastructure/setup_pipeline_deadman_alarms.sh [--dry-run]
#   BACKSTOP_ALERT_EMAIL=you@example.com ./infrastructure/setup_pipeline_deadman_alarms.sh

set -euo pipefail

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

# Deliberately NOT alpha-engine-alerts — see header comment.
BACKSTOP_TOPIC_NAME="alpha-engine-alarm-backstop"
BACKSTOP_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${BACKSTOP_TOPIC_NAME}"
BACKSTOP_ALERT_EMAIL="${BACKSTOP_ALERT_EMAIL:-cipher813@gmail.com}"

# The backstop Telegram forwarder Lambda — deploys together with the forwarder
# at infrastructure/lambdas/backstop-telegram-notifier/deploy.sh. MUST be
# bootstrapped before this script's Telegram subscription can succeed.
FORWARDER_FUNCTION_NAME="alpha-engine-backstop-telegram-notifier"
FORWARDER_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FORWARDER_FUNCTION_NAME}"

echo "Provisioning pipeline-deadman backstop channel"
echo "  Region:         $REGION"
echo "  Backstop topic: $BACKSTOP_TOPIC_ARN"
echo "  Backstop email: $BACKSTOP_ALERT_EMAIL"
echo "  NOTE: the alarms themselves are no longer created by this script"
echo "  (alpha-engine-config-I7359) — see nous-ergon-ops/infrastructure/cloudwatch/alarms/"
echo "  alpha-engine-pipeline-deadman-*.json"

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# --- 1. Provision the independent backstop SNS topic + subscription --------

echo ""
echo "==> Ensuring backstop SNS topic exists..."
if $DRY_RUN; then
  echo "DRY: aws sns create-topic --name $BACKSTOP_TOPIC_NAME"
else
  aws sns create-topic --name "$BACKSTOP_TOPIC_NAME" --region "$REGION" --query "TopicArn" --output text >/dev/null
fi

EXISTING_SUBS=""
if ! $DRY_RUN; then
  EXISTING_SUBS=$(aws sns list-subscriptions-by-topic \
    --topic-arn "$BACKSTOP_TOPIC_ARN" \
    --query "Subscriptions[?Protocol=='email' && Endpoint=='${BACKSTOP_ALERT_EMAIL}'].Endpoint" \
    --output text --region "$REGION" 2>/dev/null || echo "")
fi
if [[ -z "$EXISTING_SUBS" ]]; then
  echo "  Subscribing $BACKSTOP_ALERT_EMAIL (requires manual email confirmation)..."
  run aws sns subscribe \
    --region "$REGION" \
    --topic-arn "$BACKSTOP_TOPIC_ARN" \
    --protocol email \
    --notification-endpoint "$BACKSTOP_ALERT_EMAIL" >/dev/null
else
  echo "  Subscription for $BACKSTOP_ALERT_EMAIL already present."
fi

# --- 1b. Backstop Telegram forwarder subscription (I2899) -------------------

if aws lambda get-function --function-name "${FORWARDER_FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  if ! $DRY_RUN; then
    aws lambda add-permission \
      --function-name "${FORWARDER_FUNCTION_NAME}" \
      --statement-id "sns-${BACKSTOP_TOPIC_NAME}" \
      --action lambda:InvokeFunction \
      --principal sns.amazonaws.com \
      --source-arn "${BACKSTOP_TOPIC_ARN}" \
      --region "${REGION}" 2>/dev/null || true
  fi

  EXISTING_LAMBDA_SUB=""
  if ! $DRY_RUN; then
    EXISTING_LAMBDA_SUB=$(aws sns list-subscriptions-by-topic \
      --topic-arn "${BACKSTOP_TOPIC_ARN}" \
      --query "Subscriptions[?Protocol=='lambda' && Endpoint=='${FORWARDER_ARN}'].SubscriptionArn" \
      --output text --region "${REGION}" 2>/dev/null || echo "")
  fi
  if [[ -z "$EXISTING_LAMBDA_SUB" || "$EXISTING_LAMBDA_SUB" == "None" ]]; then
    echo "  Subscribing ${FORWARDER_FUNCTION_NAME} to ${BACKSTOP_TOPIC_NAME}..."
    run aws sns subscribe \
      --region "${REGION}" \
      --topic-arn "${BACKSTOP_TOPIC_ARN}" \
      --protocol lambda \
      --notification-endpoint "${FORWARDER_ARN}" \
      --query 'SubscriptionArn' --output text
  else
    echo "  Telegram forwarder subscription already exists: ${EXISTING_LAMBDA_SUB}"
  fi
else
  echo "  WARNING: ${FORWARDER_FUNCTION_NAME} Lambda does not exist — skipping"
  echo "  Telegram subscription. Run the following to deploy it:"
  echo "    bash infrastructure/lambdas/backstop-telegram-notifier/deploy.sh --bootstrap"
  echo "  Then re-run this script to wire the subscription."
fi

echo ""
echo "Done — backstop topic/subscriptions ensured."
echo "The alarms themselves are applied from nous-ergon-ops:"
echo "  infrastructure/cloudwatch/apply.py --prefix alpha-engine-pipeline-deadman-"
