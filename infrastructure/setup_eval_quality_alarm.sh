#!/usr/bin/env bash
# setup_eval_quality_alarm.sh — One-shot CloudWatch alarm setup for the
# LLM-as-judge rolling-4-week-mean signal (PR 4c, ROADMAP §1634).
#
# Idempotent: safe to re-run after metric or threshold tweaks. The
# alarm fires when the MIN across all (judged_agent_id, criterion,
# judge_model) combos of agent_quality_score_4w_mean drops below 3.0.
#
# Why a SEARCH expression rather than one alarm per combo: with
# ~6 sector teams × 3 sub-agents × 4-5 criteria × 2 judge tiers = 150+
# distinct streams the per-combo alarm count is unwieldy. SEARCH
# dynamically discovers every matching stream at evaluation time so
# new agents/criteria added later are auto-monitored without
# re-running this script.
#
# Notification target: reuses the existing alpha-engine-alerts SNS
# topic (created by deploy_step_function.sh) so eval regressions
# land in the same operator inbox as pipeline failures.
#
# Usage: ./infrastructure/setup_eval_quality_alarm.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
ALARM_NAME="alpha-engine-eval-quality-regression"
NAMESPACE="AlphaEngine/Eval"
DERIVED_METRIC="agent_quality_score_4w_mean"
THRESHOLD="3.0"

echo "Configuring CloudWatch alarm: $ALARM_NAME"
echo "  Region:       $REGION"
echo "  SNS topic:    $SNS_TOPIC_ARN"
echo "  Threshold:    < $THRESHOLD (rolling 4-week mean)"
echo "  Source:       $NAMESPACE / $DERIVED_METRIC"

# Verify the SNS topic exists — fail fast rather than create an alarm
# with a broken target.
if ! aws sns get-topic-attributes \
    --topic-arn "$SNS_TOPIC_ARN" \
    --region "$REGION" > /dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run deploy_step_function.sh first." >&2
  exit 1
fi

# CloudWatch put-metric-alarm with metric math: SEARCH expression
# scans all streams under the namespace + metric name, then MIN()
# reduces them to a single time series the threshold compares against.
aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "$ALARM_NAME" \
  --alarm-description "Fires when the MIN rolling-4-week-mean of any agent/criterion/judge combo of $NAMESPACE/$DERIVED_METRIC drops below $THRESHOLD. Surfaces silent regressions in LLM agent output quality before they show up in alpha. Eval is observability per ROADMAP §1635 — alarm names a regression but does not gate any deploy." \
  --comparison-operator "LessThanThreshold" \
  --evaluation-periods 1 \
  --threshold "$THRESHOLD" \
  --treat-missing-data "ignore" \
  --metrics "[
    {
      \"Id\": \"q1\",
      \"Expression\": \"SEARCH('Namespace=\\\"$NAMESPACE\\\" MetricName=\\\"$DERIVED_METRIC\\\"', 'Average', 86400)\",
      \"ReturnData\": false,
      \"Period\": 86400
    },
    {
      \"Id\": \"min_across_combos\",
      \"Expression\": \"MIN(q1)\",
      \"Label\": \"min rolling-4w mean across all (agent, criterion, judge) combos\",
      \"ReturnData\": true,
      \"Period\": 86400
    }
  ]" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN"

echo ""
echo "Alarm $ALARM_NAME configured."
echo ""
echo "Validation: aws cloudwatch describe-alarms --alarm-names $ALARM_NAME --region $REGION --query 'MetricAlarms[0].StateValue' --output text"
echo ""
echo "First firing eligibility: 4 weeks after the eval-judge Lambda starts emitting raw scores. Until then, treat-missing-data=ignore keeps the alarm in INSUFFICIENT_DATA without paging."
