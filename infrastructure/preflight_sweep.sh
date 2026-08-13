#!/usr/bin/env bash
# preflight_sweep.sh — on-box entrypoint for the nightly all-stage
# `--preflight-only` sweep (alpha-engine-config-I7249).
#
# Runs on the SHARED LAUNCHER BOX that
# `alpha-engine-weekly-freshness-spot-dispatcher` builds — the same box, the
# same bootstrap, the same repo paths and the same venv the real weekly
# pipeline runs its stages from. That is deliberate: the bootstrap itself has
# been a failure source (`No module named 'nousergon_lib'`, a missing
# backtester config.yaml symlink), so the sweep must exercise it rather than
# construct a cleaner environment of its own and grade the wrong substrate.
#
# Sequence:
#   1. Arm a short orphan watchdog (the dispatcher's own is sized for a 12h SF
#      run; this job is ~1h and must not hold a box for the difference).
#   2. Wait for the dispatcher's bootstrap SSM command to reach a terminal
#      status. This command is sent async and races the clone+venv build.
#   3. Run infrastructure/preflight_sweep.py.
#   4. Ship the log and SHUT DOWN — on EVERY path, including every failure
#      path. The instance is launched with InstanceInitiatedShutdownBehavior=
#      terminate, so `shutdown -h now` terminates it.
#
# Usage (the dispatcher's SSM command):
#   BOOTSTRAP_COMMAND_ID=<id> bash infrastructure/preflight_sweep.sh
#
# Zero-spend rehearsal (anywhere, no AWS):
#   python3 infrastructure/preflight_sweep.py --derive-only --checkout-root <root>

set -uo pipefail

REGION="${AWS_REGION:-us-east-1}"
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"
export HOME="${HOME:-/home/ec2-user}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-/home/ec2-user/.cache}"

RUN_ID="${PREFLIGHT_SWEEP_RUN_ID:-preflight-sweep-$(date -u +%Y%m%dT%H%M%SZ)}"
CHECKOUT_ROOT="${PREFLIGHT_SWEEP_CHECKOUT_ROOT:-/home/ec2-user}"
DATA_DIR="${CHECKOUT_ROOT}/alpha-engine-data"
PYTHON_BIN="${PREFLIGHT_SWEEP_PYTHON:-${CHECKOUT_ROOT}/alpha-engine-dashboard/.venv/bin/python}"
BUCKET="${PREFLIGHT_SWEEP_BUCKET:-alpha-engine-research}"
SNS_TOPIC="${PREFLIGHT_SWEEP_SNS_TOPIC:-arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts}"
BOOTSTRAP_COMMAND_ID="${BOOTSTRAP_COMMAND_ID:-}"
BOOTSTRAP_WAIT_SECONDS="${PREFLIGHT_SWEEP_BOOTSTRAP_WAIT_SECONDS:-1500}"
WATCHDOG_SECONDS="${PREFLIGHT_SWEEP_WATCHDOG_SECONDS:-7200}"
SKIP_SHUTDOWN="${PREFLIGHT_SWEEP_SKIP_SHUTDOWN:-0}"

LOG="/var/log/preflight-sweep-${RUN_ID}.log"
S3_LOG="s3://${BUCKET}/_ssm_logs/preflight-sweep/$(date -u +%Y-%m-%d)/${RUN_ID}.log"

mkdir -p "$(dirname "$LOG")" 2>/dev/null || true
exec > >(tee -a "$LOG") 2>&1

# ── Terminal path: ship the log, then stop the box. Runs on EVERY exit,
# including the failure paths — an orphaned launcher box is a spend leak, and
# the sweep's own failure must not become one. ───────────────────────────────
on_exit() {
  local rc=$?
  echo "[preflight-sweep] exiting rc=${rc} at $(date -u +%FT%TZ)"
  aws s3 cp "$LOG" "$S3_LOG" --region "$REGION" --quiet || \
    echo "[preflight-sweep] WARNING: could not ship log to ${S3_LOG} (the run's own report in s3://${BUCKET}/_preflight_sweep/${RUN_ID}/ is the durable record)"
  if [ "$SKIP_SHUTDOWN" = "1" ]; then
    echo "[preflight-sweep] PREFLIGHT_SWEEP_SKIP_SHUTDOWN=1 — leaving the box up (operator debugging only)."
  else
    echo "[preflight-sweep] shutting down (InstanceInitiatedShutdownBehavior=terminate)"
    shutdown -h now || /sbin/shutdown -h now || true
  fi
  exit "$rc"
}
trap on_exit EXIT

echo "[preflight-sweep] run_id=${RUN_ID} checkout_root=${CHECKOUT_ROOT}"

# ── 1. Short orphan watchdog ────────────────────────────────────────────────
# Backstop only. The trap above is the happy-path stop; this covers the case
# where this script is killed by something that does not run traps.
systemd-run --on-active="${WATCHDOG_SECONDS}" --unit=alpha-engine-preflight-sweep-watchdog \
  --description='alpha-engine preflight-sweep orphan-prevention watchdog' \
  /sbin/shutdown -h now >/dev/null 2>&1 \
  || echo "[preflight-sweep] WARNING: could not arm the ${WATCHDOG_SECONDS}s watchdog; the dispatcher's watchdog-deadline tag and the spot-orphan-reaper remain the backstop"

# ── 2. Wait for the dispatcher's bootstrap to finish ────────────────────────
# The sweep command is dispatched async and races the clone+venv build. Every
# exit from this block is loud: a bootstrap that failed is reported as a run
# that COULD NOT MEASURE, never as a clean sweep.
report_unmeasured() {
  local reason="$1"
  echo "[preflight-sweep] COULD NOT MEASURE: ${reason}"
  local payload
  payload=$(printf '{"component_id":"ae-preflight-sweep","run_id":"%s","outcome":"failed","measured":false,"unmeasured_reason":"%s","stages_declared":0,"stages_swept":0,"stages_passed":0,"stages_failed":0,"stages_unmeasured":0,"stages_unsweepable":0,"coverage_findings":[],"results":[]}' \
    "$RUN_ID" "$(printf '%s' "$reason" | sed 's/"/\\"/g')")
  printf '%s' "$payload" > /tmp/preflight-sweep-unmeasured.json
  aws s3 cp /tmp/preflight-sweep-unmeasured.json \
    "s3://${BUCKET}/_preflight_sweep/${RUN_ID}/report.json" --region "$REGION" --quiet || true
  aws s3 cp /tmp/preflight-sweep-unmeasured.json \
    "s3://${BUCKET}/_preflight_sweep/latest.json" --region "$REGION" --quiet || true
  # The deadman's subject fires on every terminal path INCLUDING this one:
  # the sweep did report, it reported that it could not measure. The alarm
  # exists for the case where nothing reports at all.
  aws cloudwatch put-metric-data --namespace AlphaEngine \
    --metric-name PreflightSweepRunCompleted \
    --dimensions Component=ae-preflight-sweep --value 1 --unit Count \
    --region "$REGION" || true
  aws sns publish --topic-arn "$SNS_TOPIC" \
    --subject "[preflight-sweep] COULD NOT MEASURE — ${RUN_ID}" \
    --message "$(printf 'component: ae-preflight-sweep\nrun_id:    %s\noutcome:   failed   measured: false\nreason:    %s\n\nNo stage preflight was exercised. This is NOT a clean run.\nlog: %s\n' "$RUN_ID" "$reason" "$S3_LOG")" \
    --region "$REGION" >/dev/null || true
}

if [ -n "$BOOTSTRAP_COMMAND_ID" ]; then
  INSTANCE_ID=$(curl -s --max-time 5 -H "X-aws-ec2-metadata-token: $(curl -s --max-time 5 -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')" http://169.254.169.254/latest/meta-data/instance-id)
  if [ -z "$INSTANCE_ID" ]; then
    report_unmeasured "could not read this instance's own id from IMDS — cannot poll the bootstrap command"
    exit 1
  fi
  echo "[preflight-sweep] waiting for bootstrap command ${BOOTSTRAP_COMMAND_ID} on ${INSTANCE_ID}..."
  DEADLINE=$(( $(date +%s) + BOOTSTRAP_WAIT_SECONDS ))
  BOOTSTRAP_STATUS=""
  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    BOOTSTRAP_STATUS=$(aws ssm get-command-invocation \
      --command-id "$BOOTSTRAP_COMMAND_ID" --instance-id "$INSTANCE_ID" \
      --query 'Status' --output text --region "$REGION" 2>/dev/null || echo "")
    case "$BOOTSTRAP_STATUS" in
      Success) echo "[preflight-sweep] bootstrap Success."; break ;;
      Failed|Cancelled|TimedOut|Undeliverable|Terminated)
        report_unmeasured "launcher-box bootstrap reached ${BOOTSTRAP_STATUS} — the repos and venv the stages run from were never built"
        exit 1 ;;
      *) sleep 10 ;;
    esac
  done
  if [ "$BOOTSTRAP_STATUS" != "Success" ]; then
    report_unmeasured "launcher-box bootstrap did not reach a terminal status within ${BOOTSTRAP_WAIT_SECONDS}s (last status: ${BOOTSTRAP_STATUS:-unknown})"
    exit 1
  fi
else
  echo "[preflight-sweep] no BOOTSTRAP_COMMAND_ID supplied — assuming the checkout is already in place."
fi

# ── 3. Run the sweep ────────────────────────────────────────────────────────
if [ ! -x "$PYTHON_BIN" ]; then
  report_unmeasured "interpreter ${PYTHON_BIN} is absent or not executable — the launcher box's venv did not build"
  exit 1
fi
if [ ! -f "${DATA_DIR}/infrastructure/preflight_sweep.py" ]; then
  report_unmeasured "${DATA_DIR}/infrastructure/preflight_sweep.py is absent — the data repo clone is missing or predates this sweep"
  exit 1
fi

cd "$DATA_DIR" || { report_unmeasured "could not cd to ${DATA_DIR}"; exit 1; }

echo "[preflight-sweep] running the sweep..."
"$PYTHON_BIN" -u infrastructure/preflight_sweep.py \
  --run-id "$RUN_ID" \
  --checkout-root "$CHECKOUT_ROOT" \
  --bucket "$BUCKET" \
  --sns-topic "$SNS_TOPIC" \
  --region "$REGION"
SWEEP_RC=$?

echo "[preflight-sweep] sweep exited rc=${SWEEP_RC}"
exit "$SWEEP_RC"
