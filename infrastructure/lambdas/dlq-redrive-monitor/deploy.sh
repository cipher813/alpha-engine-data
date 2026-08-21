#!/usr/bin/env bash
# deploy.sh — Create or update the alpha-engine-dlq-redrive-monitor Lambda +
# its own EventBridge Scheduler schedule (alpha-engine-config-I8111).
#
# Gives the Overseer intake DLQ (nousergon-overseer-intake-dlq) a replay
# path. Two responsibilities, run every invocation:
#   1. Redrive: SQS StartMessageMoveTask, DLQ -> nousergon-overseer-intake,
#      bounded + idempotent (see index.py docstring).
#   2. Age alarm: page (krepis.alerts, severity=error) once the oldest DLQ
#      message crosses 10 of the 14-day retention.
#
# DELIBERATELY its own schedule, independent of the four
# alpha-engine-alert-drain-*utc schedules and of automation_pause.py: this
# Lambda must keep running when the drain lane is PAUSED — that is exactly
# the condition under which the DLQ grows unwatched (measured 2026-08-21:
# 93 messages, 9.1 days old, during a pause). --bootstrap creates a
# 30-minute schedule that self-invokes this Lambda directly (no router
# hop — there is no playbook here, just a bounded idempotent housekeeping
# task).
#
# Managed OUTSIDE CloudFormation, same as every sibling dispatcher/monitor.
# Flagless run is code-only (CI auto-deploy path); --bootstrap is
# operator-only.
#
# Usage:
#   bash .../dlq-redrive-monitor/deploy.sh             # update code only
#   bash .../dlq-redrive-monitor/deploy.sh --bootstrap # operator-only: role + Lambda + schedule
#   bash .../dlq-redrive-monitor/deploy.sh --apply-iam # re-apply iam-policy.json only
#   bash .../dlq-redrive-monitor/deploy.sh --dry-run
#   bash .../dlq-redrive-monitor/deploy.sh --smoke     # invoke once after deploy

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"
# shellcheck source=infrastructure/lambdas/_shared/smoke.sh
source "${SCRIPT_DIR}/../_shared/smoke.sh"
FUNCTION_NAME="alpha-engine-dlq-redrive-monitor"
ROLE_NAME="alpha-engine-dlq-redrive-monitor-role"
POLICY_NAME="alpha-engine-dlq-redrive-monitor-policy"
SCHED_NAME="alpha-engine-dlq-redrive-monitor-30min"
SCHED_ROLE_NAME="alpha-engine-dlq-redrive-monitor-scheduler-role"
SCHED_POLICY_NAME="invoke-dlq-redrive-monitor"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"
FUNCTION_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
LAMBDA_ENV_BOOTSTRAP='Variables={LOG_LEVEL=INFO}'

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

run() { if $DRY_RUN; then echo "DRY: $*"; else "$@"; fi; }

# ----- 0. Validate handler syntax + preflight unit tests --------------------
PKG=$(mktemp -d)
trap "rm -rf '$PKG'" EXIT

python3 -c "
import ast
ast.parse(open('${SCRIPT_DIR}/index.py').read())
print('index.py syntax OK')
"

source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"
KREPIS_REQ=$(grep -E '^krepis' "${SCRIPT_DIR}/requirements.txt" | head -1)
run_handler_tests "${SCRIPT_DIR}" boto3 "${KREPIS_REQ}"

# ----- 1. Package ------------------------------------------------------------
LAMBDAS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
echo "Installing deps into ${PKG} (Lambda-safe Docker pip)..."
bash "${LAMBDAS_DIR}/lambda_pip_install.sh" "${PKG}" "${SCRIPT_DIR}/requirements.txt"
cp "${SCRIPT_DIR}/index.py" "${PKG}/index.py"
ZIP="${PKG}/function.zip"
(cd "${PKG}" && zip -qr "function.zip" . -x "function.zip")
echo "Packaged ${ZIP} ($(wc -c < "${ZIP}") bytes)"

# ----- Apply IAM only, then EXIT --------------------------------------------
if $APPLY_IAM; then
  echo "Applying IAM (role=${ROLE_NAME}, policy=${POLICY_NAME})..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"
  echo "  IAM applied. Nothing else was touched — no code, no env, no schedule."
  exit 0
fi

# ----- 2. Bootstrap ----------------------------------------------------------
if $BOOTSTRAP; then
  echo "Bootstrapping ${FUNCTION_NAME}..."
  TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    run aws iam create-role --role-name "${ROLE_NAME}" \
      --assume-role-policy-document "${TRUST_POLICY}" \
      --query 'Role.RoleName' --output text
  else
    echo "  IAM role exists: ${ROLE_NAME}"
  fi
  run aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name "${POLICY_NAME}" \
    --policy-document "file://${SCRIPT_DIR}/iam-policy.json"
  if ! $DRY_RUN; then echo "  Waiting 10s for IAM propagation..."; sleep 10; fi

  if ! aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    run aws lambda create-function \
      --function-name "${FUNCTION_NAME}" \
      --runtime python3.12 \
      --architectures x86_64 \
      --handler index.handler \
      --zip-file "fileb://${ZIP}" \
      --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
      --timeout 60 \
      --memory-size 256 \
      --environment "${LAMBDA_ENV_BOOTSTRAP}" \
      --region "${REGION}" \
      --query 'FunctionArn' --output text
  else
    echo "  Lambda function exists: ${FUNCTION_NAME}"
  fi

  # --- Scheduler role (invoke THIS Lambda only) -----------------------------
  SCHED_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  if ! aws iam get-role --role-name "${SCHED_ROLE_NAME}" --query 'Role.RoleName' --output text >/dev/null 2>&1; then
    run aws iam create-role --role-name "${SCHED_ROLE_NAME}" \
      --assume-role-policy-document "${SCHED_TRUST}" \
      --query 'Role.RoleName' --output text
  else
    echo "  Scheduler role exists: ${SCHED_ROLE_NAME}"
  fi
  run aws iam put-role-policy --role-name "${SCHED_ROLE_NAME}" \
    --policy-name "${SCHED_POLICY_NAME}" \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"lambda:InvokeFunction\",\"Resource\":\"${FUNCTION_ARN}\"}]}"
  if ! $DRY_RUN; then sleep 10; fi

  # --- Every-30-minutes schedule -> THIS Lambda directly --------------------
  # Deliberately NOT gated by automation_pause.py: this monitor's whole job
  # is to keep working while the drain lane it watches over is paused.
  if aws scheduler get-schedule --name "${SCHED_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    echo "  Schedule exists: ${SCHED_NAME} (updating)"
    VERB=update-schedule
  else
    VERB=create-schedule
  fi
  # Zero-retry: a transient failure pages loudly on the NEXT run (30 min
  # later) rather than AWS Scheduler silently retrying this idempotent,
  # frequently-scheduled task for up to a day (mirrors alert-drain-
  # dispatcher's config#2902 rationale).
  run aws scheduler "${VERB}" \
    --name "${SCHED_NAME}" \
    --schedule-expression "rate(30 minutes)" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --description "Overseer intake DLQ redrive + age alarm, every 30min, independent of the alert-drain pause (alpha-engine-config-I8111)" \
    --target "{\"Arn\":\"${FUNCTION_ARN}\",\"RoleArn\":\"arn:aws:iam::${ACCOUNT_ID}:role/${SCHED_ROLE_NAME}\",\"Input\":\"{}\",\"RetryPolicy\":{\"MaximumRetryAttempts\":0,\"MaximumEventAgeInSeconds\":300}}" \
    --region "${REGION}" > /dev/null || echo "  WARN: ${VERB} ${SCHED_NAME} failed"
fi

# ----- 3. Update code (always) -----------------------------------------------
echo "Updating ${FUNCTION_NAME} code..."
run aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --zip-file "fileb://${ZIP}" \
  --region "${REGION}" \
  --query 'LastUpdateStatus' --output text

if ! $DRY_RUN; then
  aws lambda wait function-updated --function-name "${FUNCTION_NAME}" --region "${REGION}"
fi

# ----- 4. Auto-apply IAM policy (idempotent — #4472 pattern) ---------------
TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
apply_iam_policy_on_deploy "${ROLE_NAME}" "${POLICY_NAME}" "${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"

# ----- 5. Smoke test (optional) ---------------------------------------------
if $SMOKE && ! $DRY_RUN; then
  echo "Invoking ${FUNCTION_NAME} for a smoke test..."
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
  echo "  Smoke: handler did not crash."
fi

echo "Done."
