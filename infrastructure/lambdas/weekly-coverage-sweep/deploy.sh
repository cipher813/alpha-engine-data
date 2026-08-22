#!/usr/bin/env bash
# deploy.sh — create or update the alpha-engine-weekly-coverage-sweep Lambda.
#
# alpha-engine-config-I8214 (carrying I8154 d4 and I8186's state-machine half).
# Runs nousergon_lib's stage-coverage sweep over one weekly CYCLE, publishes the
# sweep artifact + metric, and augments the SF completion marker with the
# cycle's real shape. See index.py's module docstring for why this is a Lambda
# rather than an SSM command on the run's own box.
#
# INVOKED BY the weekly SF's `WeeklyCoverageSweep` state — there is no schedule
# and no EventBridge wiring here, same as its sibling weekly-run-scope.
#
# Managed OUTSIDE CloudFormation — operator-deployed, narrow OIDC blast radius,
# same rationale as weekly-run-scope / eod-backstop / crypto-balances.
#
# Usage:
#   bash infrastructure/lambdas/weekly-coverage-sweep/deploy.sh             # update code only
#   bash infrastructure/lambdas/weekly-coverage-sweep/deploy.sh --bootstrap # first-time create
#   bash infrastructure/lambdas/weekly-coverage-sweep/deploy.sh --apply-iam # re-apply iam-policy.json only (config#2825)
#   bash infrastructure/lambdas/weekly-coverage-sweep/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"

FUNCTION_NAME="alpha-engine-weekly-coverage-sweep"
ROLE_NAME="alpha-engine-weekly-coverage-sweep-role"
POLICY_NAME="alpha-engine-weekly-coverage-sweep-policy"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="711398986525"

BOOTSTRAP=false; DRY_RUN=false; APPLY_IAM=false
for arg in "$@"; do
  case "$arg" in
    --bootstrap) BOOTSTRAP=true ;;
    --dry-run) DRY_RUN=true ;;
    --apply-iam) APPLY_IAM=true ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Tests gate the deploy -------------------------------------------
# The handler's outcome routing is exercised against stubs and needs no AWS,
# so there is no reason for a deploy to be the first thing that runs it.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3

# ----- 1. Package: pip install deps + zip handler ---------------------------
# Unlike weekly-run-scope, this handler has a real dependency: the sweep itself
# lives in nousergon-lib. Docker-based install (never bare `pip install -t` on
# macOS) — see lambda_pip_install.sh for the arch/ownership rationale.
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${SCRIPT_DIR}/../lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched."
  exit 0
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" \
        --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 120 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,RESEARCH_BUCKET=alpha-engine-research,PIPELINE=ne-weekly-freshness-pipeline}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
    verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"
    exit 0
  fi
  echo "  Lambda exists, code will be updated below"
fi

echo "Updating ${FUNCTION_NAME} code..."
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastModified' --output text

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

