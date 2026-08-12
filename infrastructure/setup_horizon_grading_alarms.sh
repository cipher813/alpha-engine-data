#!/usr/bin/env bash
# setup_horizon_grading_alarms.sh — CloudWatch alarm setup for the
# horizon-grading freshness gate (config#2972).
#
# Background: a prior groom pass queried research.db directly, found
# predictor_outcomes.horizon_days / universe_returns.log_return_21d NULL for
# every date >= 2026-06-17, and mistook this for a silently-broken write
# path. Root-cause re-investigation found NO break — a 21-trading-day-
# forward metric is *expected* to lag "today" by up to 21 trading days
# before it can be populated at all (add_trading_days(2026-06-17, 21) ==
# 2026-07-20, which simply hadn't arrived yet). The apparent "cutoff" was
# the natural lag boundary, not a stall — but nothing distinguished the two
# cases, so a real investigation pass burned cycles on a false alarm.
#
# collectors/signal_returns.py::_emit_horizon_grading_lag_metric emits two
# gauges (AlphaEngine/Data namespace) every data-weekly run:
#   - universe_returns_horizon_grading_lag_trading_days
#   - predictor_outcomes_grading_lag_trading_days
# Both are 0 on a healthy pipeline (every date whose forward window has
# closed gets graded by the next run) and grow without bound on a genuine
# stall (the JOIN/backfill breaking). This script wires alarms on sustained
# non-zero lag — NOT on the raw NULL count, which is expected to be nonzero
# for the trailing `forward_days` window at all times.
#
# Idempotent: safe to re-run after threshold tweaks. Points at the existing
# alpha-engine-alerts SNS topic (same target as setup_substrate_alarms.sh).
#
# Cadence: universe_returns/predictor_outcomes are populated by the
# data-weekly collector (Saturday SF, ~weekly cadence — see
# collectors/universe_returns.py / collectors/signal_returns.py). One
# datapoint per week.
#
# BOTH ALARMS BELOW WERE REJECTED BY CLOUDWATCH AND HAVE NEVER EXISTED.
# Measured live 2026-08-12 (`describe-alarms`): neither name is present in the
# account. The declaration was Period=604800 + EvaluationPeriods=3, and
# CloudWatch enforces `EvaluationPeriods * Period <= 604800` for any alarm with
# `period >= 3600` — 1,814,400 is a ValidationError, so `put-metric-alarm`
# failed, `set -e` stopped the script, and every run of it since has died on
# the first alarm. The horizon-grading gate this file exists to provide has
# been unwatched the whole time, with nothing reporting that.
#
# Now: Period=86400 (1d) + EvaluationPeriods=7 + DatapointsToAlarm=1. Legal
# (7 * 86400 == 604800), and it reads the same weekly datapoint: exactly one
# daily bucket in any 7-day window carries data, so Maximum > 0 in that bucket
# alarms. Daily granularity rather than one 604800s period is deliberate — a
# single period exactly equal to the producer's interval sits on the boundary
# and flaps immediately before each scheduled run.
#
# DELTA, STATED RATHER THAN SILENT: this fires on ONE weekly run with lag > 0.
# The original intent was 2 of the last 3 runs, so that a single week's lag
# right after a new forward window closes (the expected transient) would not
# page. Three weekly periods is 21 days of alarm window and is NOT expressible
# under the cap at any period >= 3600, so the suppression cannot live in the
# alarm. It belongs in the producer: emit a consecutive-weeks-non-zero counter
# from _emit_horizon_grading_lag_metric and alarm on that at threshold >= 2,
# which has no window at all. Tracked as alpha-engine-config-I7044. Until it
# lands, a detector that may page one week early beats two that do not exist.
#
# treat-missing-data=notBreaching: a week where the collector didn't run for
# an unrelated reason (already covered by weekly_collector_manifest /
# research_db_backup freshness checks in alpha-engine-config's
# ARTIFACT_REGISTRY.yaml) doesn't independently page this alarm too.
#
# Usage:
#   pip install nousergon-lib  # (or activate a venv with it — not required
#                               # by this script itself, kept for parity with
#                               # setup_substrate_alarms.sh's doc convention)
#   ./infrastructure/setup_horizon_grading_alarms.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
NAMESPACE="AlphaEngine/Data"

echo "Configuring CloudWatch alarms for horizon-grading freshness (config#2972)"
echo "  Region:    $REGION"
echo "  SNS topic: $SNS_TOPIC_ARN"
echo "  Namespace: $NAMESPACE"

# Verify the SNS topic exists — fail fast rather than create alarms with
# broken targets.
if ! aws sns get-topic-attributes \
    --topic-arn "$SNS_TOPIC_ARN" \
    --region "$REGION" > /dev/null 2>&1; then
  echo "ERROR: SNS topic $SNS_TOPIC_ARN not found. Run infrastructure/deploy_step_function.sh first." >&2
  exit 1
fi

# --- universe_returns 21d-horizon grading lag --------------------------------

echo ""
echo "==> alpha-engine-universe-returns-horizon-lag"

aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "alpha-engine-universe-returns-horizon-lag" \
  --alarm-description "Fires when universe_returns_horizon_grading_lag_trading_days (AlphaEngine/Data, HorizonDays=21 dimension) is > 0 on the most recent weekly data-weekly run (any of the last 7 daily periods). Lag=0 on a healthy pipeline: every eval_date whose 21-trading-day forward window has closed gets return_21d/log_return_21d populated by the next run. Sustained lag means the collectors/universe_returns.py backfill (_get_existing_dates / _trading_days_to_process) has genuinely stalled — NOT that recent dates are still waiting on their forward window to close (that's the expected, non-alarming transient). config#2972." \
  --comparison-operator "GreaterThanThreshold" \
  --evaluation-periods 7 \
  --datapoints-to-alarm 1 \
  --period 86400 \
  --statistic "Maximum" \
  --threshold 0 \
  --treat-missing-data "notBreaching" \
  --namespace "$NAMESPACE" \
  --metric-name "universe_returns_horizon_grading_lag_trading_days" \
  --dimensions "Name=HorizonDays,Value=21" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN" > /dev/null

# --- predictor_outcomes grading lag -------------------------------------------

echo "==> alpha-engine-predictor-outcomes-grading-lag"

aws cloudwatch put-metric-alarm \
  --region "$REGION" \
  --alarm-name "alpha-engine-predictor-outcomes-grading-lag" \
  --alarm-description "Fires when predictor_outcomes_grading_lag_trading_days (AlphaEngine/Data, HorizonDays=21 dimension) is > 0 on the most recent weekly data-weekly run (any of the last 7 daily periods). Lag=0 on a healthy pipeline: every prediction_date whose forward_days window has closed gets horizon_days/correct/actual_log_alpha populated by collectors/signal_returns.py::_backfill_predictor_returns on the next run. Sustained lag means the universe_returns JOIN this backfill depends on has stopped finding matches for closed-window predictions — NOT that recent predictions are still waiting on grading (that's the expected, non-alarming transient). config#2972." \
  --comparison-operator "GreaterThanThreshold" \
  --evaluation-periods 7 \
  --datapoints-to-alarm 1 \
  --period 86400 \
  --statistic "Maximum" \
  --threshold 0 \
  --treat-missing-data "notBreaching" \
  --namespace "$NAMESPACE" \
  --metric-name "predictor_outcomes_grading_lag_trading_days" \
  --dimensions "Name=HorizonDays,Value=21" \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN" > /dev/null

echo ""
echo "Horizon-grading alarms configured."
echo ""
echo "Validation:"
echo "  aws cloudwatch describe-alarms --region $REGION \\"
echo "    --alarm-names alpha-engine-universe-returns-horizon-lag alpha-engine-predictor-outcomes-grading-lag \\"
echo "    --query 'MetricAlarms[].[AlarmName,StateValue]' --output table"
echo ""
echo "First firing eligibility: both alarms remain INSUFFICIENT_DATA until one weekly data-weekly run has emitted the metric (~1 week). treat-missing-data=notBreaching means a skipped run doesn't independently page — the existing weekly_collector_manifest / research_db_backup freshness checks (alpha-engine-config ARTIFACT_REGISTRY.yaml) already cover run-absence. The 2-of-3-weeks suppression these alarms were meant to carry is not expressible under CloudWatch's window cap and moves to the producer: alpha-engine-config-I7044."
