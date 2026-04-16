#!/usr/bin/env bash
# deploy_step_function_daily.sh — Create/update the weekday pipeline.
#
# Orchestrates: Daily Data → Predictor Inference → EC2 Start (executor)
# Triggered Mon-Fri at 13:05 UTC (6:05 AM PT).
#
# Prerequisites:
#   - Saturday pipeline already deployed (IAM roles, SNS topic exist)
#   - Predictor Lambda (alpha-engine-predictor-inference) deployed
#   - SSM agent on micro instance
#
# Usage:
#   ./infrastructure/deploy_step_function_daily.sh
#   ./infrastructure/deploy_step_function_daily.sh --disable-old-rules

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STATE_MACHINE_NAME="alpha-engine-weekday-pipeline"
ROLE_NAME="alpha-engine-step-functions-role"  # reuse from Saturday pipeline
SNS_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:alpha-engine-alerts"
EVENTBRIDGE_RULE="alpha-engine-weekday"

# EC2 instances
MICRO_INSTANCE="${AE_EC2_INSTANCE_ID:-i-09b539c844515d549}"
TRADING_INSTANCE="${AE_TRADING_INSTANCE_ID:-i-018eb3307a21329bf}"

echo "=== Alpha Engine Weekday Pipeline Deployment ==="
echo "  Region:          $REGION"
echo "  Account:         $ACCOUNT_ID"
echo "  Micro EC2:       $MICRO_INSTANCE"
echo "  Trading EC2:     $TRADING_INSTANCE"
echo ""

# ── Update Step Functions role with EC2 start permission ─────────────────────

echo "Updating IAM role with EC2 start + predictor Lambda permissions..."
POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LambdaInvoke",
      "Effect": "Allow",
      "Action": ["lambda:InvokeFunction"],
      "Resource": [
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:alpha-engine-research-runner*",
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:alpha-engine-data-collector*",
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:alpha-engine-predictor-inference*",
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:alpha-engine-predictor-health-check*"
      ]
    },
    {
      "Sid": "SSMSendCommand",
      "Effect": "Allow",
      "Action": ["ssm:SendCommand"],
      "Resource": [
        "arn:aws:ssm:'"$REGION"'::document/AWS-RunShellScript",
        "arn:aws:ec2:'"$REGION"':'"$ACCOUNT_ID"':instance/'"$MICRO_INSTANCE"'",
        "arn:aws:ec2:'"$REGION"':'"$ACCOUNT_ID"':instance/'"$TRADING_INSTANCE"'"
      ]
    },
    {
      "Sid": "SSMGetCommandInvocation",
      "Effect": "Allow",
      "Action": ["ssm:GetCommandInvocation"],
      "Resource": "*"
    },
    {
      "Sid": "EC2StartStop",
      "Effect": "Allow",
      "Action": ["ec2:StartInstances", "ec2:StopInstances"],
      "Resource": "arn:aws:ec2:'"$REGION"':'"$ACCOUNT_ID"':instance/'"$TRADING_INSTANCE"'"
    },
    {
      "Sid": "SNSPublish",
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "'"$SNS_TOPIC_ARN"'"
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
        "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery", "logs:ListLogDeliveries",
        "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
      ],
      "Resource": "*"
    }
  ]
}'

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "${ROLE_NAME}-policy" \
  --policy-document "$POLICY" \
  --region "$REGION"
echo "  Role updated"

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# ── Create log group ────────────────────────────────────────────────────────

aws logs create-log-group \
  --log-group-name "/aws/stepfunctions/${STATE_MACHINE_NAME}" \
  --region "$REGION" 2>/dev/null || true

# ── State Machine ───────────────────────────────────────────────────────────

echo "Creating/updating state machine: $STATE_MACHINE_NAME..."

ASL_FILE="$SCRIPT_DIR/step_function_daily.json"
DEFINITION=$(cat "$ASL_FILE")

SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" --region "$REGION" &>/dev/null; then
  echo "  Updating existing state machine..."
  aws stepfunctions update-state-machine \
    --state-machine-arn "$SM_ARN" \
    --definition "$DEFINITION" \
    --role-arn "$ROLE_ARN" \
    --region "$REGION" > /dev/null
else
  echo "  Creating new state machine..."
  aws stepfunctions create-state-machine \
    --name "$STATE_MACHINE_NAME" \
    --definition "$DEFINITION" \
    --role-arn "$ROLE_ARN" \
    --type STANDARD \
    --logging-configuration '{
      "level": "ERROR",
      "includeExecutionData": true,
      "destinations": [
        {
          "cloudWatchLogsLogGroup": {
            "logGroupArn": "arn:aws:logs:'"$REGION"':'"$ACCOUNT_ID"':log-group:/aws/stepfunctions/'"$STATE_MACHINE_NAME"':*"
          }
        }
      ]
    }' \
    --region "$REGION" > /dev/null
fi
echo "  State machine ARN: $SM_ARN"

# ── EventBridge Rule ────────────────────────────────────────────────────────

echo "Creating EventBridge rule: $EVENTBRIDGE_RULE..."

# 13:05 UTC = 6:05 AM PT (Mon-Fri)
aws events put-rule \
  --name "$EVENTBRIDGE_RULE" \
  --schedule-expression "cron(5 13 ? * MON-FRI *)" \
  --state ENABLED \
  --description "Weekday 13:05 UTC (6:05 AM PT) — daily data + predictor + executor start" \
  --region "$REGION"

# Reuse EventBridge role from Saturday pipeline
EB_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/alpha-engine-eventbridge-sfn-role"

# Update EventBridge role to also allow starting this state machine
EB_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": [
        "arn:aws:states:'"$REGION"':'"$ACCOUNT_ID"':stateMachine:alpha-engine-saturday-pipeline",
        "'"$SM_ARN"'"
      ]
    }
  ]
}'
aws iam put-role-policy \
  --role-name "alpha-engine-eventbridge-sfn-role" \
  --policy-name "alpha-engine-eventbridge-sfn-role-policy" \
  --policy-document "$EB_POLICY" \
  --region "$REGION"

INPUT_JSON=$(cat <<EOF
{
  "ec2_instance_id": ["$MICRO_INSTANCE"],
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
  --region "$REGION"

echo "  EventBridge rule: cron(5 13 ? * MON-FRI *) -> $STATE_MACHINE_NAME"

# ── Disable old rules (optional) ───────────────────────────────────────────

if [ "${1:-}" = "--disable-old-rules" ]; then
  echo ""
  echo "Disabling old weekday rules..."
  aws events disable-rule --name "ae-predictor-run" --region "$REGION" 2>/dev/null && \
    echo "  Disabled: ae-predictor-run" || echo "  Not found: ae-predictor-run"
  echo "  Old rules disabled. Delete after 2 successful weeks."
fi

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "  State machine:  $SM_ARN"
echo "  EventBridge:    $EVENTBRIDGE_RULE (Mon-Fri 13:05 UTC / 6:05 AM PT)"
echo "  SNS topic:      $SNS_TOPIC_ARN"
echo ""
echo "To test manually:"
echo "  aws stepfunctions start-execution \\"
echo "    --state-machine-arn $SM_ARN \\"
echo "    --input '{\"ec2_instance_id\": [\"$MICRO_INSTANCE\"], \"trading_instance_id\": [\"$TRADING_INSTANCE\"], \"sns_topic_arn\": \"$SNS_TOPIC_ARN\"}' \\"
echo "    --region $REGION"
echo ""
