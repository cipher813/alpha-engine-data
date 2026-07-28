#!/usr/bin/env bash
# deploy.sh — create or update alpha-engine-overseer-backstop-responder and
# subscribe it to the dumb backstop SNS topic (alpha-engine-config-I4480).
#
# WHY (overseer-policy.md §1, §4 inv. 1/3/16, §5 layer D): the backstop topic's
# only subscriber was an email address, so degradation of the Overseer plane
# itself terminated in a notification. This Lambda makes that class' response
# bounded and automatic: report plane state, attempt ONE allowlisted recovery
# per cooldown window, escalate on the second firing.
#
# DEPENDENCIES: boto3 (in the runtime) and the standard library. Nothing else,
# deliberately — §4 inv. 3 makes the backstop's dumbness permanent, and a
# third-party dependency is one more thing that can fail non-obviously in the
# one component that must survive everything else failing. There is no
# requirements.txt to install and no vendored wheel: the zip is index.py.
#
# IAM (iam-policy.json): logs + ssm:GetParameter on the two Telegram params +
# read-only lambda:GetFunctionConfiguration / sqs attributes /
# cloudwatch:GetMetricStatistics + lambda:InvokeFunction scoped to EXACTLY the
# router and the liveness probe + s3 Get/Put confined to the cooldown prefix.
# No IAM, no deletes, no config writes, no EC2, no queue consumption. The
# InvokeFunction grant naming two ARNs is the enforcement layer under the
# in-code action allowlist — an added allowlist row that named a third function
# would AccessDenied rather than act.
#
# DEPLOY ATOMICITY (§4 inv. 5 — the rule this component exists to honour):
# the sibling dispatchers/probes are operator-deployed, so merging their PRs has
# ZERO live effect. That pattern is precisely the defect that disarmed the
# liveness probe for four days (alpha-engine-config-I4472/G1). This component
# was therefore applied LIVE in-session BEFORE its PR was opened; the PR
# documents what is already running. Re-running this script is idempotent.
#
# Usage:
#   bash .../overseer-backstop-responder/deploy.sh             # update code only
#   bash .../overseer-backstop-responder/deploy.sh --bootstrap # create + wire SNS
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="711398986525"
FUNCTION="alpha-engine-overseer-backstop-responder"
ROLE="alpha-engine-overseer-backstop-responder-role"
TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT}:alpha-engine-alarm-backstop"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP="false"
[ "${1:-}" = "--bootstrap" ] && BOOTSTRAP="true"

log() { echo "[backstop-responder-deploy] $*"; }

BUILD="$(mktemp -d)"
trap 'rm -rf "$BUILD"' EXIT
cp "${HERE}/index.py" "${BUILD}/index.py"
( cd "$BUILD" && zip -qr function.zip index.py )
log "package built ($(du -h "${BUILD}/function.zip" | cut -f1))"

if [ "$BOOTSTRAP" = "true" ]; then
  log "bootstrap: role ${ROLE}"
  aws iam create-role --role-name "$ROLE" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    >/dev/null 2>&1 || log "role exists"
  # Idempotent: put-role-policy overwrites in place, so re-running picks up any
  # grant change in iam-policy.json without a delete/recreate cycle.
  aws iam put-role-policy --role-name "$ROLE" \
    --policy-name "${ROLE}-inline" \
    --policy-document "file://${HERE}/iam-policy.json" >/dev/null
  log "inline policy applied"
  sleep 10  # IAM propagation before the first CreateFunction

  aws lambda create-function --function-name "$FUNCTION" \
    --runtime python3.12 --handler index.handler \
    --role "arn:aws:iam::${ACCOUNT}:role/${ROLE}" \
    --zip-file "fileb://${BUILD}/function.zip" \
    --timeout 120 --memory-size 256 \
    --description "Bounded non-agentic recovery + decision-shaped page for Overseer plane alarms (I4480)" \
    --region "$REGION" >/dev/null 2>&1 \
    || { log "function exists — updating code instead"
         aws lambda update-function-code --function-name "$FUNCTION" \
           --zip-file "fileb://${BUILD}/function.zip" --region "$REGION" >/dev/null; }

  log "subscribing to ${TOPIC_ARN}"
  aws lambda add-permission --function-name "$FUNCTION" \
    --statement-id sns-backstop-invoke --action lambda:InvokeFunction \
    --principal sns.amazonaws.com --source-arn "$TOPIC_ARN" \
    --region "$REGION" >/dev/null 2>&1 || log "invoke permission exists"
  aws sns subscribe --topic-arn "$TOPIC_ARN" --protocol lambda \
    --notification-endpoint "arn:aws:lambda:${REGION}:${ACCOUNT}:function:${FUNCTION}" \
    --region "$REGION" >/dev/null
  log "subscribed (the existing email subscription is untouched — two"
  log "independent paths sharing no component, per §5 layer D)"
else
  aws lambda update-function-code --function-name "$FUNCTION" \
    --zip-file "fileb://${BUILD}/function.zip" --region "$REGION" >/dev/null
  log "code updated"
fi

aws lambda wait function-updated --function-name "$FUNCTION" --region "$REGION"
log "done — ${FUNCTION} is live"
