#!/usr/bin/env bash
# deploy_step_function.sh — Create/update the Saturday pipeline Step Functions
# state machine, IAM role, SNS topic, and EventBridge trigger.
#
# Prerequisites:
#   1. AWS CLI configured with admin credentials
#   2. SSM agent installed on the always-on EC2 instance
#   3. Research Lambda (alpha-engine-research-runner) deployed
#   4. Data Phase 2 Lambda (alpha-engine-data-collector) deployed
#   5. Repos cloned on always-on EC2: alpha-engine-data, alpha-engine-predictor,
#      alpha-engine-backtester
#
# Usage:
#   ./infrastructure/deploy_step_function.sh
#   ./infrastructure/deploy_step_function.sh --disable-old-crons
#
# After deployment:
#   1. Run a test execution from the Step Functions console
#   2. Monitor first automated Saturday run (00:00 UTC)
#   3. After 2 successful weeks, run with --disable-old-crons

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STATE_MACHINE_NAME="alpha-engine-saturday-pipeline"
ROLE_NAME="alpha-engine-step-functions-role"
SNS_TOPIC_NAME="alpha-engine-alerts"
EVENTBRIDGE_RULE="alpha-engine-saturday"

# Always-on EC2 instance ID (micro instance that runs data collection + launches spot)
EC2_INSTANCE_ID="${AE_EC2_INSTANCE_ID:-}"
if [ -z "$EC2_INSTANCE_ID" ]; then
  echo "ERROR: Set AE_EC2_INSTANCE_ID env var to the always-on EC2 instance ID"
  echo "       (e.g., export AE_EC2_INSTANCE_ID=i-0abc123def456)"
  exit 1
fi

echo "=== Alpha Engine Step Functions Deployment ==="
echo "  Region:     $REGION"
echo "  Account:    $ACCOUNT_ID"
echo "  EC2:        $EC2_INSTANCE_ID"
echo ""

# ── 1. SNS Topic ────────────────────────────────────────────────────────────

echo "Creating SNS topic: $SNS_TOPIC_NAME..."
SNS_TOPIC_ARN=$(aws sns create-topic \
  --name "$SNS_TOPIC_NAME" \
  --query "TopicArn" --output text \
  --region "$REGION")
echo "  Topic ARN: $SNS_TOPIC_ARN"

# Check if email subscription exists
EXISTING_SUBS=$(aws sns list-subscriptions-by-topic \
  --topic-arn "$SNS_TOPIC_ARN" \
  --query "Subscriptions[?Protocol=='email'].Endpoint" --output text \
  --region "$REGION" 2>/dev/null || echo "")
if [ -z "$EXISTING_SUBS" ]; then
  echo "  WARNING: No email subscriptions on $SNS_TOPIC_NAME."
  echo "  Add one: aws sns subscribe --topic-arn $SNS_TOPIC_ARN --protocol email --notification-endpoint your@email.com"
fi

# ── 2. IAM Role for Step Functions ──────────────────────────────────────────

echo "Creating IAM role: $ROLE_NAME..."

# Trust policy
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "states.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}'

aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document "$TRUST_POLICY" \
  --region "$REGION" 2>/dev/null || echo "  Role already exists"

# Inline policy: Lambda invoke, SSM, SNS, CloudWatch
POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LambdaInvoke",
      "Effect": "Allow",
      "Action": ["lambda:InvokeFunction"],
      "Resource": [
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:alpha-engine-research-runner*",
        "arn:aws:lambda:'"$REGION"':'"$ACCOUNT_ID"':function:alpha-engine-data-collector*"
      ]
    },
    {
      "Sid": "SSMRunCommand",
      "Effect": "Allow",
      "Action": [
        "ssm:SendCommand",
        "ssm:GetCommandInvocation"
      ],
      "Resource": [
        "arn:aws:ssm:'"$REGION"'::document/AWS-RunShellScript",
        "arn:aws:ec2:'"$REGION"':'"$ACCOUNT_ID"':instance/'"$EC2_INSTANCE_ID"'"
      ]
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
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:CreateLogDelivery",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies",
        "logs:DescribeLogGroups"
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

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo "  Role ARN: $ROLE_ARN"

# Wait for IAM propagation
echo "  Waiting for IAM propagation..."
sleep 10

# ── 3. State Machine ───────────────────────────────────────────────────────

echo "Creating/updating state machine: $STATE_MACHINE_NAME..."

ASL_FILE="$SCRIPT_DIR/step_function.json"
if [ ! -f "$ASL_FILE" ]; then
  echo "ERROR: $ASL_FILE not found"
  exit 1
fi

# Read the ASL definition
DEFINITION=$(cat "$ASL_FILE")

# Check if state machine exists
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
  SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
fi
echo "  State machine ARN: $SM_ARN"

# ── 4. EventBridge Rule ────────────────────────────────────────────────────

echo "Creating EventBridge rule: $EVENTBRIDGE_RULE..."

aws events put-rule \
  --name "$EVENTBRIDGE_RULE" \
  --schedule-expression "cron(0 0 ? * SAT *)" \
  --state ENABLED \
  --description "Saturday 00:00 UTC (Friday 5/8pm PT) — triggers full Alpha Engine pipeline" \
  --region "$REGION"

# EventBridge needs a role to start Step Functions executions
EB_ROLE_NAME="alpha-engine-eventbridge-sfn-role"
EB_TRUST='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "events.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}'

aws iam create-role \
  --role-name "$EB_ROLE_NAME" \
  --assume-role-policy-document "$EB_TRUST" \
  --region "$REGION" 2>/dev/null || true

EB_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "states:StartExecution",
      "Resource": "'"$SM_ARN"'"
    }
  ]
}'

aws iam put-role-policy \
  --role-name "$EB_ROLE_NAME" \
  --policy-name "${EB_ROLE_NAME}-policy" \
  --policy-document "$EB_POLICY" \
  --region "$REGION"

EB_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${EB_ROLE_NAME}"

# The EventBridge target passes the execution input with EC2 instance ID and SNS topic
INPUT_JSON=$(cat <<EOF
{
  "ec2_instance_id": ["$EC2_INSTANCE_ID"],
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

echo "  EventBridge rule: cron(0 0 ? * SAT *) -> $STATE_MACHINE_NAME"

# ── 5. Disable old crons (optional) ────────────────────────────────────────

if [ "${1:-}" = "--disable-old-crons" ]; then
  echo ""
  echo "Disabling old scheduling rules (keeping as fallback)..."

  # Research weekly EventBridge
  aws events disable-rule --name "alpha-research-weekly" --region "$REGION" 2>/dev/null && \
    echo "  Disabled: alpha-research-weekly" || echo "  Not found: alpha-research-weekly"

  # Backtester weekly EventBridge
  aws events disable-rule --name "alpha-engine-backtester-weekly" --region "$REGION" 2>/dev/null && \
    echo "  Disabled: alpha-engine-backtester-weekly" || echo "  Not found: alpha-engine-backtester-weekly"

  echo ""
  echo "  Old rules DISABLED (not deleted). Delete after 2 successful weeks:"
  echo "    aws events delete-rule --name alpha-research-weekly --region $REGION"
  echo "    aws events delete-rule --name alpha-engine-backtester-weekly --region $REGION"
  echo ""
  echo "  EC2 crons (predictor training, backtester, data collection) must be"
  echo "  disabled manually on the EC2 instance:"
  echo "    ae-trading 'crontab -l'   # review"
  echo "    ae-trading 'crontab -e'   # comment out old entries"
fi

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "  State machine:  $SM_ARN"
echo "  EventBridge:    $EVENTBRIDGE_RULE (Saturday 00:00 UTC)"
echo "  SNS topic:      $SNS_TOPIC_ARN"
echo ""
echo "To test manually:"
echo "  aws stepfunctions start-execution \\"
echo "    --state-machine-arn $SM_ARN \\"
echo "    --input '{\"ec2_instance_id\": [\"$EC2_INSTANCE_ID\"], \"sns_topic_arn\": \"$SNS_TOPIC_ARN\"}' \\"
echo "    --region $REGION"
echo ""
echo "To monitor:"
echo "  aws stepfunctions list-executions --state-machine-arn $SM_ARN --region $REGION"
echo ""
