#!/usr/bin/env bash
# deploy.sh — create or update the alpha-engine-weekly-run-scope Lambda.
#
# alpha-engine-config-I7620. Derives the weekly pipeline's own scope from its
# definition + its execution history and writes backtest/{run_date}/run_scope.json.
# See index.py's module docstring for why this is derived rather than kept in a
# registry.
#
# INVOKED BY the weekly SF's `RunScope` state — there is no schedule and no
# EventBridge wiring here, which is why this script is shorter than its
# siblings.
#
# Managed OUTSIDE CloudFormation — operator-deployed, narrow OIDC blast radius,
# same rationale as eod-backstop / eod-precondition-probe / crypto-balances.
#
# Usage:
#   bash infrastructure/lambdas/weekly-run-scope/deploy.sh             # update code only
#   bash infrastructure/lambdas/weekly-run-scope/deploy.sh --bootstrap # first-time create
#   bash infrastructure/lambdas/weekly-run-scope/deploy.sh --apply-iam # re-apply iam-policy.json only (config#2825)
#   bash infrastructure/lambdas/weekly-run-scope/deploy.sh --dry-run   # show actions, do not apply

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"

FUNCTION_NAME="alpha-engine-weekly-run-scope"
ROLE_NAME="alpha-engine-weekly-run-scope-role"
POLICY_NAME="alpha-engine-weekly-run-scope-policy"
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
# The derivation runs against captured real executions and needs no AWS, so
# there is no reason for a deploy to be the first thing that exercises it.
# alpha-engine-config-I8373: index.py now does a REAL `import krepis.dates`,
# so the gate needs the lambda's own requirements.txt installed alongside
# pytest — the same `-r requirements.txt` shape freshness-monitor/deploy.sh
# uses, not the old bare `boto3` list (which never covered a real
# `import krepis`).
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" -r "${SCRIPT_DIR}/requirements.txt"

# ----- 1. Package: pip install runtime deps into $PKG -----------------------
# alpha-engine-config-I8373: requirements.txt now carries krepis (for
# krepis.dates.resolve_trading_day), so the zip is built the same
# Lambda-safe-Docker-pip way every other dependency-carrying lambda in this
# tree is (see freshness-monitor/deploy.sh) — bare `cp` of the two source
# files was correct only while requirements.txt was empty.
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT
echo "Installing runtime deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${SCRIPT_DIR}/run_scope.py" "${PKG}/"
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
      --timeout 60 \
      --memory-size 256 \
      --environment 'Variables={LOG_LEVEL=INFO,RESEARCH_BUCKET=alpha-engine-research}' \
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

