#!/usr/bin/env bash
# infrastructure/_spot_common.sh — Shared spot-instance infrastructure for
# nousergon-data per-stage launcher scripts.
#
# Source this file from per-stage scripts:
#   SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SCRIPT_DIR/_spot_common.sh"
#
# Provides shared defaults, spot launch, SSM dispatch, cleanup with
# spot-interruption retry, bootstrap, and dependency install.
#
# Each per-stage script MUST set the following BEFORE sourcing this file:
#   _SPOT_NAME    — spot instance Name tag suffix (e.g. "morning-enrich")
#   _SSM_SLUG     — log-capture slug for krepis.ssm_log_capture
#   _PROCESS_NAME — CloudWatch dimension Process name
#   MAX_RUNTIME_SECONDS — SSM command timeout for the workload
#   ORIG_ARGS     — array copy of "$@" captured before flag parsing

set -euo pipefail

# ── Global defaults ──────────────────────────────────────────────────────────

AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-alpha-engine-research}"
BRANCH="${BRANCH:-main}"

INSTANCE_TYPES="${INSTANCE_TYPES:-c5.large,m5.large,c6i.large,c5a.large}"
INSTANCE_TYPE=""
AMI_ID="ami-0c421724a94bba6d6"  # Amazon Linux 2023 x86_64
KEY_NAME="alpha-engine-key"
SECURITY_GROUP="sg-03cd3c4bd91e610b0"
SUBNETS="${SUBNETS:-subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,subnet-c670118d,subnet-7cff7c43,subnet-e07166ec}"
IAM_PROFILE="alpha-engine-executor-profile"
# Lib CLI path: every spot launcher on the dispatcher box resolves its
# interpreter through the ops-owned guard /opt/nousergon/bin/lib-python
# (nous-ergon-ops: alpha-engine-dashboard/live/infrastructure/bin/lib-python).
# That guard execs the box's DECLARED krepis venv and aborts with EX_CONFIG
# (78), naming the version it found, when the venv is absent or below the
# launcher floor. It never falls back to a co-tenant checkout — the silent
# fallback is exactly the defect alpha-engine-config-I6931/I7343 removes.
# Do NOT add a guard block here: the contract lives ONCE, in the repo that
# owns this box's provisioning (nine copies across five repos is I6922).
LIB_PYTHON="${LIB_PYTHON:-/opt/nousergon/bin/lib-python}"

# Spot-reclaim relaunch (#883)
MAX_SPOT_ATTEMPTS="${MAX_SPOT_ATTEMPTS:-2}"
SPOT_ATTEMPT="${SPOT_ATTEMPT:-1}"
SF_EXECUTION_TIMEOUT="${SF_EXECUTION_TIMEOUT:-}"
SPOT_RETRY_BACKOFF_SECONDS="${SPOT_RETRY_BACKOFF_SECONDS:-20}"

# Per-stage identity — DECLARED HERE, NEVER DEFAULTED HERE.
#
# This block used to carry `data-weekly` / `spot-data` defaults, and all three
# per-stage scripts then set their own with the SAME `${VAR:-...}` form AFTER
# sourcing this file — where the parameter is already non-empty, so the
# expansion is a NO-OP and the stage identity silently stayed whatever this
# file said. spot_morning_enrich.sh asks for `morning-enrich`, spot_data_
# phase1.sh for `data-phase1`, spot_rag_ingestion.sh for `rag-ingestion`, and
# every one of them ran as `data-weekly`: the instance Name tag, the
# `Process` dimension on the CloudWatch `Heartbeat` metric, the `Process`
# dimension on `SpotInterruptionRetry`, and the `run_ssm` command description
# all carried the wrong stage. Three stages, one identity — so a heartbeat
# gap could not be attributed and a retry could not be counted per stage.
#
# crucible-predictor hit and fixed the identical defect in its twin of this
# file (measured on `watch-rerun-2026-08-10-9`, 2026-08-11: the heartbeat
# emitted under slug `spot-spot-training` although the launcher asked for
# `spot-full-training`). This is the mirror, per alpha-engine-config-I6922 —
# the same no-op class, unported until now.
#
# Declared EMPTY so `set -u` is satisfied at source time and the per-stage
# `${VAR:-<stage default>}` lines are load-bearing again (an explicit env
# override still wins, because it is set before this file runs). `spot_launch`
# asserts all four are non-empty before any instance exists, so a stage that
# forgets one fails loud and free rather than inheriting a sibling's identity.
_SPOT_NAME="${_SPOT_NAME:-}"
_SSM_SLUG="${_SSM_SLUG:-}"
_PROCESS_NAME="${_PROCESS_NAME:-}"
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-}"

# Stage-coverage window (alpha-engine-config-I7214): the instant this launcher
# started. An artifact whose LastModified predates it is a leftover from a
# previous cycle, not this run's output — an existence-only probe cannot tell
# those apart, which is how a stage that STOPPED writing keeps reading green.
# Captured here rather than at assertion time because the workload runs in
# between: a window taken after the write would be trivially satisfied by it.
_STAGE_WINDOW_START="${_STAGE_WINDOW_START:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

# Derived at launch time
_INSTANCE_ID=""
_S3_STAGING_PREFIX=""
_S3_STAGING=""

# krepis RUN_TOKEN forwarding
if [ -n "${RUN_TOKEN:-}" ]; then
  _RUN_TOKEN_EXPORT="export RUN_TOKEN=${RUN_TOKEN}"$'\n'
else
  _RUN_TOKEN_EXPORT="export RUN_TOKEN=spot-data-weekly-$(date -u +%Y%m%d)"$'\n'
fi

# ── Spot launch (capacity-resilient) ─────────────────────────────────────────

spot_launch() {
  # Stage identity must be real BEFORE anything is billable. See the
  # "Per-stage identity" block above for why these can no longer be defaulted
  # in this file: a default here silently swallows the per-stage assignment.
  local _unset=""
  [ -n "$_SPOT_NAME" ] || _unset="${_unset} _SPOT_NAME"
  [ -n "$_SSM_SLUG" ] || _unset="${_unset} _SSM_SLUG"
  [ -n "$_PROCESS_NAME" ] || _unset="${_unset} _PROCESS_NAME"
  [ -n "$MAX_RUNTIME_SECONDS" ] || _unset="${_unset} MAX_RUNTIME_SECONDS"
  if [ -n "$_unset" ]; then
    echo "ERROR: per-stage variable(s) unset before spot_launch:${_unset}" >&2
    echo "       Set them AFTER sourcing _spot_common.sh — see its header." >&2
    exit 2
  fi

  echo "==> Requesting spot instance (lib CLI rotation: types=[$INSTANCE_TYPES], subnets=[$SUBNETS])..."

  _INSTANCE_ID=$("$LIB_PYTHON" -m krepis.ec2_spot launch \
    --types "$INSTANCE_TYPES" \
    --subnets "$SUBNETS" \
    --image-id "$AMI_ID" \
    --key-name "$KEY_NAME" \
    --security-group "$SECURITY_GROUP" \
    --iam-profile "$IAM_PROFILE" \
    --name "alpha-engine-data-${_SPOT_NAME}-$(date +%Y%m%d)" \
    --region "$AWS_REGION")
  local ec2_spot_rc=$?

  if [ "$ec2_spot_rc" -ne 0 ] || [ -z "$_INSTANCE_ID" ]; then
    if [ "$ec2_spot_rc" -eq 64 ]; then
      echo "ERROR: capacity exhausted across all instance_type x subnet combinations" >&2
    fi
    if [ "$ec2_spot_rc" -eq 0 ]; then
      echo "ERROR: ec2_spot launch exited 0 without an instance id — failing loud (config#1646)" >&2
      ec2_spot_rc=1
    fi
    exit "$ec2_spot_rc"
  fi

  echo "  Instance ID: $_INSTANCE_ID"

  local _RUN_ID
  _RUN_ID="$(date +%Y%m%dT%H%M%SZ)-${_INSTANCE_ID}"
  _S3_STAGING_PREFIX="tmp/spot_data_weekly/${_RUN_ID}"
  _S3_STAGING="s3://${S3_BUCKET}/${_S3_STAGING_PREFIX}"

  echo "  S3 staging: ${_S3_STAGING}/"
}

# ── Cleanup (instance + S3 staging) ──────────────────────────────────────────

cleanup() {
  local _keep="${KEEP_INSTANCE:-0}"
  if [ "$_keep" = "1" ]; then
    [ -n "$_S3_STAGING" ] && aws s3 rm "$_S3_STAGING" --recursive --quiet 2>/dev/null || true
    echo "  launch-only: instance $_INSTANCE_ID left running (SF-owned); staging cleaned."
    return 0
  fi
  if [ -n "$_INSTANCE_ID" ]; then
    echo ""
    echo "==> Terminating spot instance $_INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids "$_INSTANCE_ID" --region "$AWS_REGION" --output text > /dev/null 2>&1 || true
  fi
  [ -n "$_S3_STAGING" ] && aws s3 rm "$_S3_STAGING" --recursive --quiet 2>/dev/null || true
  [ -n "$_INSTANCE_ID" ] && echo "  Instance terminated; S3 staging cleaned."
  return 0
}

# ── Spot failure classification ──────────────────────────────────────────────

_spot_failure_reason() {
  local rc="$1"
  if [ "$rc" -eq 64 ]; then echo "launch-capacity-exhausted"; return 0; fi
  [ -z "$_INSTANCE_ID" ] && return 1
  # See alpha-engine-config-I7009 — migrated off the exit-code contract to --json.
  local _decide_json="" _decide_rc=0
  _decide_json="$("$LIB_PYTHON" -m krepis.ec2_spot relaunch-decision \
    --instance-id "$_INSTANCE_ID" \
    --region "$AWS_REGION" \
    --attempt "$SPOT_ATTEMPT" \
    --max-attempts "$MAX_SPOT_ATTEMPTS" \
    ${SF_EXECUTION_TIMEOUT:+--sf-execution-timeout "$SF_EXECUTION_TIMEOUT" --per-attempt-seconds "$MAX_RUNTIME_SECONDS"} \
    --json \
    2>/dev/null)" || _decide_rc=$?
  [ "$_decide_rc" -eq 0 ] || return 1   # CLI failed to answer -> treat as hold (do not relaunch)
  local _relaunch=""
  _relaunch="$(printf '%s' "$_decide_json" | "$LIB_PYTHON" -c 'import json,sys; print("1" if json.load(sys.stdin).get("relaunch") else "0")')"
  echo "  spot relaunch-decision (attempt $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS): $_decide_json" >&2
  [ "$_relaunch" = "1" ] || return 1
  echo "confirmed-reclaim${_decide_json:+ ($_decide_json)}"
}

# ── EXIT handler (classification + cleanup + optional relaunch) ──────────────

on_exit() {
  local rc=$?
  local reason=""
  if [ "$rc" -ne 0 ]; then
    reason="$(_spot_failure_reason "$rc")" || reason=""
  fi
  cleanup
  if [ "$rc" -ne 0 ] && [ -n "$reason" ] && [ "$SPOT_ATTEMPT" -lt "$MAX_SPOT_ATTEMPTS" ]; then
    aws cloudwatch put-metric-data \
      --namespace "AlphaEngine" \
      --metric-name "SpotInterruptionRetry" \
      --dimensions "Process=${_PROCESS_NAME}" \
      --value 1 --unit "Count" \
      --region "$AWS_REGION" 2>/dev/null || true
    echo "" >&2
    echo "==> Spot interruption (reason=$reason) on attempt $SPOT_ATTEMPT/$MAX_SPOT_ATTEMPTS — relaunching in ${SPOT_RETRY_BACKOFF_SECONDS}s..." >&2
    sleep "$SPOT_RETRY_BACKOFF_SECONDS"
    trap - EXIT
    SPOT_ATTEMPT=$((SPOT_ATTEMPT + 1)) exec bash "$0" ${ORIG_ARGS[@]+"${ORIG_ARGS[@]}"}
  fi
  if [ "$rc" -ne 0 ] && [ -n "$reason" ]; then
    echo "ERROR: spot interruption (reason=$reason) persisted across all $MAX_SPOT_ATTEMPTS attempt(s) — giving up." >&2
  fi
  exit "$rc"
}

# ── SSM agent wait ───────────────────────────────────────────────────────────

wait_ssm_agent() {
  echo "==> Waiting for SSM agent to come Online..."
  for i in $(seq 1 36); do
    local ping
    ping=$(aws ssm describe-instance-information \
      --filters "Key=InstanceIds,Values=$_INSTANCE_ID" \
      --query 'InstanceInformationList[0].PingStatus' \
      --output text --region "$AWS_REGION" 2>/dev/null || true)
    if [ "$ping" = "Online" ]; then
      echo "  SSM agent Online."
      return 0
    fi
    if [ "$i" -eq 36 ]; then
      echo "ERROR: SSM agent not Online after 180s (instance $_INSTANCE_ID)" >&2
      exit 1
    fi
    sleep 5
  done
}

# ── SSM dispatch ─────────────────────────────────────────────────────────────

run_ssm() {
  local description="$1" script="$2" timeout_s="${3:-3600}"
  printf '%s' "$script" | "$LIB_PYTHON" -m krepis.ssm_dispatcher run \
    --instance-id "$_INSTANCE_ID" \
    --description "${_PROCESS_NAME}: $description" \
    --timeout "$timeout_s" \
    --output-bucket "$S3_BUCKET" \
    --output-key-prefix "${_S3_STAGING_PREFIX}/ssm-output" \
    --region "$AWS_REGION" \
    --diagnostics-bucket "$S3_BUCKET" \
    --diagnostics-prefix "_spot_diagnostics/ae-data" \
    --script-stdin
}

# ── Config staging ───────────────────────────────────────────────────────────

stage_config() {
  local src="$1" dest_key="${2:-config.yaml}"
  echo "==> Staging ${src} → ${_S3_STAGING}/${dest_key}"
  aws s3 cp "$src" "${_S3_STAGING}/${dest_key}" --region "$AWS_REGION" --quiet
}

# ── Bootstrap (watchdog + python + clone + config) ───────────────────────────

bootstrap_spot() {
  echo "==> Bootstrapping spot (watchdog, python, clone, config)..."
  # Rendered by krepis.spot_bootstrap (alpha-engine-config-I6922) rather than
  # built as an inline heredoc. That module is the fleet's single canonical
  # source for the watchdog unit, the interpreter install and the clone/config
  # shape this heredoc used to hand-carry (nousergon-data#1294, #1296;
  # crucible-predictor#461, #462, #463 were the same defects reaching the
  # sibling copy hours to days later). Region and branch are passed as
  # LAUNCHER-SIDE LITERALS rather than left for the remote shell to expand —
  # the predictor's REPO_URL/BRANCH class of bug (crucible-predictor#463: a
  # value interpolated into the heredoc but never actually exported, so the
  # remote expansion silently resolved to an empty string) cannot happen when
  # the value is baked into the rendered script instead of read from the
  # remote environment. `--region us-east-1` mirrors this heredoc's prior
  # hardcode exactly (it never read the outer $AWS_REGION); tests/
  # test_spot_bootstrap_invariants.py and its siblings assert the rendered
  # output stays byte-for-byte equivalent on the parts that must not drift.
  local _script
  _script="$("$LIB_PYTHON" -m krepis.spot_bootstrap render \
    --repo-url https://github.com/nousergon/nousergon-data.git \
    --checkout /home/ec2-user/data \
    --branch "${BRANCH:-main}" \
    --region us-east-1 \
    --export "S3_STAGING=${_S3_STAGING}" \
    --config-copy config.yaml:/home/ec2-user/alpha-engine-config/data/config.yaml:/home/ec2-user/alpha-engine-config)"
  run_ssm "bootstrap" "$_script" 300
  echo "  Bootstrap complete."
}

# ── Dependency installation ──────────────────────────────────────────────────

install_deps() {
  # config-I6949 / config-I6963, ported from crucible-predictor per
  # alpha-engine-config-I6922: this step used to pipe pip through `tail -1`,
  # so a run that exited 0 having silently skipped an extra was
  # indistinguishable from a clean one — and pip reports a dropped extra as a
  # WARNING on a SUCCESSFUL exit, which is precisely the line `tail -1` could
  # not keep. The fleet copy is krepis.spot_bootstrap.render_install_deps;
  # keep the three in step (tests/test_spot_bootstrap_invariants.py).
  echo "==> Installing python deps..."
  run_ssm "deps" "$(cat <<'DEPS'
set -eo pipefail
export HOME=/home/ec2-user XDG_CACHE_HOME=/tmp AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
cd /home/ec2-user/data
command -v python3.12 >/dev/null && PY=python3.12 || PY=python3
_pip_log=/tmp/pip-install-deps.log
if ! $PY -m pip install --no-warn-script-location -r requirements.txt > "$_pip_log" 2>&1; then
  echo "ERROR: pip install -r requirements.txt failed" >&2
  tail -80 "$_pip_log" >&2
  exit 1
fi
grep -E "^Successfully installed" "$_pip_log" || true
# A dropped extra is a BROKEN ENVIRONMENT, not a note. pip reports it as a
# WARNING on a SUCCESSFUL exit, so nothing downstream fails until an import
# does — in another process, minutes later, with the install log long gone.
# That is exactly how config#6963 reached production in the predictor: the
# training smoke died on `ModuleNotFoundError: No module named 'flow_doctor'`
# out of krepis.logging.setup_logging, from an install pip had called a
# success. This repo's requirements.txt requests four extras on one line
# (nousergon-lib[arcticdb,flow-doctor,rag,contracts]), and AL2023 ships pip
# 23.2.1, which predates PEP 685 extras normalisation (measured boundary:
# 23.2.1 drops, 23.3.2 honours) — so this bootstrap sits on the broken side
# by default and cannot rely on the resolver to be strict. Fail here, where
# the log is still in hand and the cause is one line.
if grep -E "^WARNING: .*does not provide the extra" "$_pip_log" >&2; then
  echo "ERROR: pip dropped a requested extra (above) — the environment is incomplete." >&2
  echo "       Extras must be HYPHENATED; pip <23.3 does not normalise '_' to '-'." >&2
  exit 1
fi
# Non-fatal on purpose: an inconsistent environment is reported, not raised.
# (a) The failure mode left unraised is a pre-existing AMI-baked conflict
# unrelated to this checkout, which would otherwise fail every stage on every
# run. (b) It is recorded on stdout of this SSM step, captured with the rest
# of the deps output.
$PY -m pip check || echo "WARNING: pip check reports an inconsistent environment (above)"
DEPS
)" 600
  echo "  Deps installed."
}

# ── Utilities ────────────────────────────────────────────────────────────────

print_banner() {
  local title="$1"
  echo ""
  echo "═══════════════════════════════════════════════════════════════"
  echo "  ${title}"
  echo "═══════════════════════════════════════════════════════════════"
}

emit_heartbeat() {
  aws cloudwatch put-metric-data \
    --namespace "AlphaEngine" \
    --metric-name "Heartbeat" \
    --dimensions "Process=${_PROCESS_NAME}" \
    --value 1 --unit "Count" \
    --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
    && echo "Heartbeat emitted: ${_PROCESS_NAME}" \
    || echo "WARNING: Failed to emit heartbeat (non-fatal)"
}
