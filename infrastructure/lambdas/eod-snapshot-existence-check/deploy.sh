#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-eod-snapshot-existence-check
# Lambda + its EventBridge Scheduler schedule.
#
# alpha-engine-config-I6705 (I5569 deliverable #3): pre-midnight POSITIVE
# existence check for CaptureSnapshot's output. Independent of the EOD SF's
# own same-day retry + paging (nousergon-data-PR1260, deliverables #1-2) —
# this fires even if the EOD SF never started or died before reaching
# CaptureSnapshot. See index.py's module docstring for the full rationale.
#
# Managed OUTSIDE CloudFormation — operator-deployed, narrow OIDC blast
# radius, same rationale as eod-backstop / eod-precondition-probe /
# crypto-balances. Merging the PR has ZERO live effect until an operator
# runs this with --bootstrap.
#
# Usage:
#   bash infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh             # update code only
#   bash infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh --bootstrap # first-time create + wire EventBridge Scheduler
#   bash infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh --apply-iam # re-apply iam-policy.json only (no bootstrap side effects, config#2825)
#   bash infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh --dry-run   # show actions, do not apply
#   bash infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh --smoke     # invoke once (real head_object; pages for real if the snapshot is absent)

set -euo pipefail

# alpha-engine-config-I6619: --state must come from the automation-pause
# manifest, not from the API default (ENABLED). See infrastructure/lambdas/_shared/pause.sh.
# shellcheck source=infrastructure/lambdas/_shared/pause.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
FUNCTION_NAME="alpha-engine-eod-snapshot-existence-check"
ROLE_NAME="alpha-engine-eod-snapshot-existence-check-role"
POLICY_NAME="alpha-engine-eod-snapshot-existence-check-policy"
SCHED_ROLE_NAME="alpha-engine-eod-snapshot-existence-check-scheduler-role"
SCHED_POLICY_NAME="invoke-eod-snapshot-existence-check"
SCHED_NAME="alpha-engine-eod-snapshot-existence-check"
# 20:30 America/Los_Angeles = 23:30 America/New_York (ET is always UTC-4/5,
# 3h ahead of PT) MON-FRI — after the 13:00 PT EOD daemon window and the
# 22:30 UTC eod-backstop firing, >30min before NYSE-local midnight. Using
# Scheduler's native timezone field (not a hand-computed UTC cron) so DST
# transitions never need a seasonal cron edit.
SCHED_CRON="cron(30 20 ? * MON-FRI *)"
SCHED_TZ="America/Los_Angeles"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"

FN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
SCHED_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}"

# DRY_RUN honors an ambient env var (true/1/yes) as well as the --dry-run
# flag below, so DRY_RUN=1/true from a caller's shell actually no-ops
# instead of silently running the real deploy path (alpha-engine-config-
# I2752 incident, 2026-07-16: an operator assumed DRY_RUN=<env var> worked
# here, matching other tools' convention, and triggered a real deploy).
case "${DRY_RUN:-false}" in
  true|1|yes|TRUE|YES) DRY_RUN=true ;;
  *) DRY_RUN=false ;;
esac
BOOTSTRAP=false
APPLY_IAM=false
SMOKE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --bootstrap) BOOTSTRAP=true ;;
    --apply-iam) APPLY_IAM=true ;;
    --smoke) SMOKE=true ;;
    -h|--help) sed -n '2,/^$/p' "$0"; exit 0 ;;
  esac
done

# shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
source "${SCRIPT_DIR}/../_shared/deploy_run.sh"

# ----- 0. Validate handler + run unit tests ----------------------------------

python3 -c "
import ast
src = open('${SCRIPT_DIR}/index.py').read()
ast.parse(src)
print('index.py syntax OK')
"

# ----- Preflight handler unit tests (shared gate — config#2381) -------------
# Delegates to the one _shared/run_handler_tests.sh so this gate can never
# re-drift into the naive no-install `python3 -m pytest` form (config#2295).
# Passes -r requirements.txt (like eod-backstop) so is_trading_day runs
# against the real nousergon-lib in tests — trading-day math is pure/
# deterministic and safe to exercise for real; only alerts.publish and the
# S3 client are mocked per-test.
source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
run_handler_tests "${SCRIPT_DIR}" boto3 -r "${SCRIPT_DIR}/requirements.txt"

# ----- 1. Package: pip install deps + zip handler ---------------------------

PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${SCRIPT_DIR}/../lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"

cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- 2. Bootstrap (first-time only) ---------------------------------------

# ----- Apply IAM only (config#2825, no bootstrap side effects) -------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  ✓ IAM applied. Nothing else was touched — no code, no env, no alarms."
  exit 0
fi

if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."

  # --- 2a. Lambda execution role + inline least-privilege policy ---
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating IAM role: ${ROLE_NAME}"
    run aws iam create-role \
      --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi

  echo "  Applying inline policy: ${POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for IAM role propagation..."
    sleep 10
  fi

  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --query 'Configuration.FunctionName' --output text >/dev/null 2>&1; then
    echo "  Creating Lambda: ${FUNCTION_NAME}"
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --role "${ROLE_ARN}" \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --timeout 30 \
      --memory-size 128 \
      --environment 'Variables={LOG_LEVEL=INFO}' \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda exists, code will be updated in step 3"
  fi

  # --- 2b. EventBridge Scheduler execution role (invoke this Lambda only) ---
  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    echo "  Creating Scheduler execution role: ${SCHED_ROLE_NAME}"
    run aws iam create-role \
      --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --description "EventBridge Scheduler role: invoke ${FUNCTION_NAME} MON-FRI 20:30 PT" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler execution role exists: ${SCHED_ROLE_NAME}"
  fi
  SCHED_INVOKE_POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"lambda:InvokeFunction\"],\"Resource\":\"${FN_ARN}\"}]}"
  echo "  Applying Scheduler invoke policy: ${SCHED_POLICY_NAME}"
  run aws iam put-role-policy \
    --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}" --policy-document "${SCHED_INVOKE_POLICY}"

  if ! $DRY_RUN; then
    echo "  Waiting 10s for Scheduler role propagation..."
    sleep 10
  fi

  # --- 2c. The EventBridge Scheduler schedule (cron, PT-native timezone) ---
  TARGET="{\"Arn\":\"${FN_ARN}\",\"RoleArn\":\"${SCHED_ROLE_ARN}\",\"Input\":\"{}\"}"
  if aws scheduler get-schedule --name "${SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null 2>&1; then
    echo "  Updating Scheduler schedule: ${SCHED_NAME} → ${SCHED_CRON} (${SCHED_TZ})"
    run aws scheduler update-schedule --name "${SCHED_NAME}" --state "$(pause_state "${SCHED_NAME}")" \
      --schedule-expression "${SCHED_CRON}" --schedule-expression-timezone "${SCHED_TZ}" \
      --flexible-time-window '{"Mode":"OFF"}' \
      --target "${TARGET}" --region "${REGION}" --query 'ScheduleArn' --output text
  else
    echo "  Creating Scheduler schedule: ${SCHED_NAME} → ${SCHED_CRON} (${SCHED_TZ})"
    run aws scheduler create-schedule --name "${SCHED_NAME}" --state "$(pause_state "${SCHED_NAME}")" \
      --schedule-expression "${SCHED_CRON}" --schedule-expression-timezone "${SCHED_TZ}" \
      --flexible-time-window '{"Mode":"OFF"}' \
      --target "${TARGET}" --region "${REGION}" --query 'ScheduleArn' --output text
  fi
  if ! $DRY_RUN; then
    aws scheduler get-schedule --name "${SCHED_NAME}" --region "${REGION}" --query 'Name' --output text >/dev/null \
      || { echo "ERROR: Scheduler schedule ${SCHED_NAME} not found after create/update" >&2; exit 1; }
  fi
fi

# ----- 3. Update function code (always after bootstrap, idempotent) ---------

echo "Updating Lambda function code: ${FUNCTION_NAME}"
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"
fi

verify_code_deployed "${FUNCTION_NAME}" "${REGION}" "${ZIP}"

echo "✓ Code deployed."

# ----- 4. Smoke (real invoke; PAGES FOR REAL if today's snapshot is absent) --

# shellcheck source=infrastructure/lambdas/_shared/smoke.sh
source "${SCRIPT_DIR}/../_shared/smoke.sh"
if $SMOKE; then
  echo ""
  echo "WARNING: --smoke will publish a REAL page to alpha-engine-watchdog-alerts"
  echo "         if today is a trading day AND today's snapshot is genuinely absent."
  RESP=$(mktemp)
  INVOKE_STDOUT=$(aws lambda invoke \
    --function-name "${FUNCTION_NAME}" \
    --cli-binary-format raw-in-base64-out \
    --payload '{}' \
    --region "${REGION}" \
    "${RESP}")
  cat "${RESP}"
  echo ""
  assert_no_function_error "${INVOKE_STDOUT}" "${RESP}"
  rm -f "${RESP}"
fi
