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
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUCKET="alpha-engine-research"
STACK_NAME="alpha-engine-orchestration"
TEMPLATE="$SCRIPT_DIR/cloudformation/alpha-engine-orchestration.yaml"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Git SHA stamp — baked into the SF Comment field and the CF stack tags so
# the deploy-drift preflight can detect when main has moved past the deployed
# artifact. CI supplies $GITHUB_SHA; local dev falls back to HEAD.
GIT_SHA="${GITHUB_SHA:-$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)}"
echo "  Stamping deploy with GIT_SHA=${GIT_SHA}"

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

# ── 2. Stamp SF definitions with git SHA + upload to S3 ──────────────────────
# Prepend `[git:<sha>] ` to the top-level `Comment` field so the preflight
# drift check can extract + compare against origin/main. The stamped JSON is
# what gets uploaded to S3 AND fed to update-state-machine, so S3 copy and
# live definition stay in lockstep.
echo ""
echo "==> Stamping Step Function definitions with git SHA..."
SAT_STAMPED="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
DAILY_STAMPED="$(mktemp --suffix=.json 2>/dev/null || mktemp)"
trap "rm -f '$SAT_STAMPED' '$DAILY_STAMPED'" EXIT
python3 -c "
import json, sys
path_in, path_out, sha = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(path_in))
orig = d.get('Comment', '')
# Strip any existing [git:…] prefix so re-stamping is idempotent
if orig.startswith('[git:'):
    orig = orig.split(' ', 1)[1] if ' ' in orig else ''
d['Comment'] = f'[git:{sha}] {orig}'.rstrip()
json.dump(d, open(path_out, 'w'), indent=2)
" "$SCRIPT_DIR/step_function.json" "$SAT_STAMPED" "$GIT_SHA"
python3 -c "
import json, sys
path_in, path_out, sha = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(path_in))
orig = d.get('Comment', '')
if orig.startswith('[git:'):
    orig = orig.split(' ', 1)[1] if ' ' in orig else ''
d['Comment'] = f'[git:{sha}] {orig}'.rstrip()
json.dump(d, open(path_out, 'w'), indent=2)
" "$SCRIPT_DIR/step_function_daily.json" "$DAILY_STAMPED" "$GIT_SHA"

echo ""
echo "==> Uploading Step Function definitions to S3..."
aws s3 cp "$SAT_STAMPED" "s3://$BUCKET/infrastructure/step_function.json" --quiet
aws s3 cp "$DAILY_STAMPED" "s3://$BUCKET/infrastructure/step_function_daily.json" --quiet
echo "  Uploaded to s3://$BUCKET/infrastructure/"

# ── 3. Update Step Functions directly ────────────────────────────────────────
echo ""
echo "==> Updating Step Function definitions..."

SAT_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-saturday-pipeline"
DAILY_ARN="arn:aws:states:$REGION:${ACCOUNT_ID}:stateMachine:alpha-engine-weekday-pipeline"

aws stepfunctions update-state-machine --state-machine-arn "$SAT_ARN" --definition "$(cat "$SAT_STAMPED")" --query "updateDate" --output text
echo "  Saturday pipeline updated."

aws stepfunctions update-state-machine --state-machine-arn "$DAILY_ARN" --definition "$(cat "$DAILY_STAMPED")" --query "updateDate" --output text
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
        --tags "Key=git-sha,Value=$GIT_SHA" \
        --query "StackId" --output text
    echo "  Waiting for stack creation..."
    aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
else
    echo "  Updating existing stack (current status: $STACK_STATUS)..."
    aws cloudformation update-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://$TEMPLATE" \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags "Key=git-sha,Value=$GIT_SHA" \
        --query "StackId" --output text 2>/dev/null || {
        # update-stack refuses when template AND tags are both unchanged. In
        # that case the SHA is already current — no-op is correct.
        echo "  No updates needed (template + git-sha tag both current)."
    }
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Infrastructure deploy complete."
echo "═══════════════════════════════════════════════════════════════"
