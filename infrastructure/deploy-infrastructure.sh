#!/usr/bin/env bash
# infrastructure/deploy-infrastructure.sh — Deploy Alpha Engine orchestration infrastructure.
#
# Uploads Step Function definitions to S3, then deploys/updates the CloudFormation
# stack. Also updates the state machines directly (CloudFormation can't update
# Step Function definitions from S3 on stack update — it only reads on create).
#
# Usage:
#   bash infrastructure/deploy-infrastructure.sh              # deploy/update
#   bash infrastructure/deploy-infrastructure.sh --dry-run    # validate only
#
# Prerequisites:
#   - AWS CLI configured with appropriate permissions
#   - Step Function JSON files in infrastructure/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUCKET="alpha-engine-research"
STACK_NAME="alpha-engine-orchestration"
TEMPLATE="$SCRIPT_DIR/cloudformation/alpha-engine-orchestration.yaml"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

DRY_RUN=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════"
echo "  Alpha Engine Infrastructure Deploy"
echo "═══════════════════════════════════════════════════════════════"
echo "  Stack:    $STACK_NAME"
echo "  Region:   $REGION"
echo "  Account:  $ACCOUNT_ID"
echo "  Dry run:  $DRY_RUN"
echo ""

# ── 1. Validate CloudFormation template ──────────────────────────────────────
echo "==> Validating CloudFormation template..."
aws cloudformation validate-template --template-body "file://$TEMPLATE" --query "Description" --output text
echo "  Template valid."

if $DRY_RUN; then
    echo ""
    echo "Dry run complete. No changes made."
    exit 0
fi

# ── 2. Upload Step Function definitions to S3 ────────────────────────────────
echo ""
echo "==> Uploading Step Function definitions to S3..."
aws s3 cp "$SCRIPT_DIR/step_function.json" "s3://$BUCKET/infrastructure/step_function.json" --quiet
aws s3 cp "$SCRIPT_DIR/step_function_daily.json" "s3://$BUCKET/infrastructure/step_function_daily.json" --quiet
echo "  Uploaded to s3://$BUCKET/infrastructure/"

# ── 3. Update Step Functions directly ────────────────────────────────────────
echo ""
echo "==> Updating Step Function definitions..."

SAT_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-saturday-pipeline"
DAILY_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-weekday-pipeline"

aws stepfunctions update-state-machine --state-machine-arn "$SAT_ARN" --definition "$(cat "$SCRIPT_DIR/step_function.json")" --query "updateDate" --output text
echo "  Saturday pipeline updated."

aws stepfunctions update-state-machine --state-machine-arn "$DAILY_ARN" --definition "$(cat "$SCRIPT_DIR/step_function_daily.json")" --query "updateDate" --output text
echo "  Weekday pipeline updated."

# ── 4. Deploy/update CloudFormation stack ────────────────────────────────────
echo ""
echo "==> Deploying CloudFormation stack..."

# Check if stack exists
STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [ "$STACK_STATUS" = "DOES_NOT_EXIST" ]; then
    echo "  Creating new stack..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://$TEMPLATE" \
        --capabilities CAPABILITY_NAMED_IAM \
        --query "StackId" --output text
    echo "  Waiting for stack creation..."
    aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
else
    echo "  Updating existing stack (current status: $STACK_STATUS)..."
    aws cloudformation update-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://$TEMPLATE" \
        --capabilities CAPABILITY_NAMED_IAM \
        --query "StackId" --output text 2>/dev/null || {
        echo "  No updates needed (stack is current)."
    }
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Infrastructure deploy complete."
echo "═══════════════════════════════════════════════════════════════"
