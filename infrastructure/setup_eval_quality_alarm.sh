#!/usr/bin/env bash
# setup_eval_quality_alarm.sh — One-shot CloudWatch alarm setup for the
# LLM-as-judge rolling-4-week-mean signal (PR 4c, ROADMAP §1634).
#
# Idempotent: safe to re-run after threshold tweaks. The alarm fires
# when the dimensionless floor metric agent_quality_score_4w_mean_min
# (= MIN across every (judged_agent_id, criterion, judge_model)
# combo's 4-week rolling mean) drops below 3.0.
#
# The floor is pre-computed by the rolling-mean Lambda
# (alpha-engine-research evals/rolling_mean.py) and emitted as a
# single dimensionless datapoint per weekly run. We must pre-compute
# rather than reduce in the alarm itself: CloudWatch alarms reject
# SEARCH expressions ("SEARCH is not supported on Metric Alarms"),
# so dynamic cross-combo reduction at alarm-evaluation time isn't
# expressible. This is fine — operator workflow on alarm fire is the
# same either way (click dashboard to identify the combo).
#
# Notification target: reuses the alpha-engine-alerts SNS topic
# created by deploy_step_function.sh so eval regressions land in the
# same operator inbox as pipeline failures.
#
# Usage: ./infrastructure/setup_eval_quality_alarm.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
ALARM_NAME="alpha-engine-eval-quality-regression"
NAMESPACE="AlphaEngine/Eval"
FLOOR_METRIC="agent_quality_score_4w_mean_min"
THRESHOLD="3.0"

echo "Configuring CloudWatch alarm: $ALARM_NAME"
echo "  Region:       $REGION"
echo "  SNS topic:    $SNS_TOPIC_ARN"
echo "  Threshold:    < $THRESHOLD (rolling 4-week-mean floor)"
echo "  Source:       $NAMESPACE / $FLOOR_METRIC"

# Verify the SNS topic exists — fail fast rather than create an alarm
# with a broken target.
if ! aws sns get-topic-attributes \
    --topic-arn "$SNS_TOPIC_ARN" \
    --region "$REGION" > /dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run deploy_step_function.sh first." >&2
  exit 1
fi

# Simple metric alarm against the dimensionless floor — no SEARCH,
# no metric math. Period 86400 (24h) with EvaluationPeriods=1 means
# the alarm checks the most recent 24h window; the rolling-mean
# Lambda emits weekly so any single emission's value is evaluated.
aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "$ALARM_NAME" \
  --alarm-description "Fires when the floor (MIN across all combos) of $NAMESPACE/$FLOOR_METRIC drops below $THRESHOLD. Surfaces silent regressions in LLM agent output quality before they show up in alpha. The floor is pre-computed by the rolling-mean Lambda — operator clicks the dashboard quality-trend page to identify which (agent, criterion, judge) combo triggered. Eval is observability per ROADMAP §1635 — alarm names a regression but does not gate any deploy." \
  --comparison-operator "LessThanThreshold" \
  --evaluation-periods 1 \
  --period 86400 \
  --statistic Minimum \
  --threshold "$THRESHOLD" \
  --treat-missing-data "ignore" \
  --namespace "$NAMESPACE" \
  --metric-name "$FLOOR_METRIC" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN"

echo ""
echo "Alarm $ALARM_NAME configured."
echo ""
echo "Validation: aws cloudwatch describe-alarms --alarm-names $ALARM_NAME --region $REGION --query 'MetricAlarms[0].StateValue' --output text"
echo ""
echo "First firing eligibility: 4 weeks after the eval-judge Lambda starts emitting raw scores (the rolling-mean Lambda needs 4 weeks of source data before the floor metric reflects a real 4-week mean). Until then, treat-missing-data=ignore keeps the alarm in INSUFFICIENT_DATA without paging."
