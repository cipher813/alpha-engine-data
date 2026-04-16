#!/usr/bin/env bash
# infrastructure/spot_data_phase1.sh — Run weekly DataPhase1 on a spot EC2 instance.
#
# Launches a c5.large spot instance, clones alpha-engine-data, runs
# `python weekly_collector.py --phase 1`, emits a heartbeat on success, and
# self-terminates.
#
# Origin: moved off ae-dashboard (t3.micro, 1 GB RAM) after the 2026-04-16
# OOM incident in which features/compute.py in the DAILY code path exhausted
# micro memory. Saturday's Phase 1 uses a different code path and hasn't
# OOM'd historically, but running heavy data-refresh workloads on a 1 GB
# instance is fragile-by-design. This spot pattern mirrors the Backtester +
# PredictorTraining spot launchers so all heavy weekly compute lives on
# fresh, self-terminating instances instead of the always-on micro.
#
# Usage:
#   ./infrastructure/spot_data_phase1.sh                    # full Phase 1
#   ./infrastructure/spot_data_phase1.sh --smoke-only       # quick validation, then terminate
#   ./infrastructure/spot_data_phase1.sh --instance-type c5.xlarge   # override size
#   ./infrastructure/spot_data_phase1.sh --branch my-branch          # override branch
#
# Prerequisites on the launching host (ae-dashboard when invoked by the
# Saturday Step Function):
#   - AWS CLI with perms to RunInstances / TerminateInstances / DescribeInstances
#   - SSH key at ~/.ssh/alpha-engine-key.pem
#   - .env at /home/ec2-user/.alpha-engine.env (for API keys, ALPHA_ENGINE_LIB_TOKEN)
#   - alpha-engine-data checked out at the script's parent dir

set -euo pipefail

# SSM RunCommand does not set HOME; default it for the .env/SSH lookups below.
export HOME="${HOME:-/home/ec2-user}"

# ── Locate .env ──────────────────────────────────────────────────────────────
# Step Function path first; fall back to repo-local .env for manual runs.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ENV_FILE="$HOME/.alpha-engine.env"
if [ ! -f "$ENV_FILE" ]; then
    ENV_FILE="$REPO_ROOT/.env"
fi
if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
    echo "Loaded .env from $ENV_FILE"
else
    echo "ERROR: No .env file found (checked ~/.alpha-engine.env, repo/.env)"
    exit 1
fi

# ── Spot configuration ──────────────────────────────────────────────────────
# Values mirror alpha-engine-backtester/infrastructure/spot_backtest.sh so
# new IAM/security-group/subnet resources aren't introduced. If any of these
# change in the backtester launcher, this file should change in lockstep.
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-alpha-engine-research}"
BRANCH="${BRANCH:-main}"
INSTANCE_TYPE="c5.large"            # 2 vCPU, 4 GB RAM — DataPhase1 is memory-bound, not CPU-bound
AMI_ID="ami-0c421724a94bba6d6"      # Amazon Linux 2023 x86_64
KEY_NAME="alpha-engine-key"
KEY_FILE="$HOME/.ssh/alpha-engine-key.pem"
SECURITY_GROUP="sg-03cd3c4bd91e610b0"
SUBNET_ID="subnet-e07166ec"
IAM_PROFILE="alpha-engine-executor-profile"

# ── Parse flags ──────────────────────────────────────────────────────────────
RUN_MODE="full"  # full | smoke-only
while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke-only) RUN_MODE="smoke-only"; shift ;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
        --branch) BRANCH="$2"; shift 2 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════"
echo "  DataPhase1 Spot Run — $(date +%Y-%m-%d)"
echo "═══════════════════════════════════════════════════════════════"
echo "  Instance type : $INSTANCE_TYPE"
echo "  AMI           : $AMI_ID"
echo "  Region        : $AWS_REGION"
echo "  Branch        : $BRANCH"
echo "  Run mode      : $RUN_MODE"
echo "  S3 bucket     : $S3_BUCKET"
echo ""

# ── Preflight ───────────────────────────────────────────────────────────────
if [ ! -f "$KEY_FILE" ]; then
    echo "ERROR: SSH key not found at $KEY_FILE"
    exit 1
fi

# ALPHA_ENGINE_LIB_TOKEN required for pip install of the private lib.
if [ -z "${ALPHA_ENGINE_LIB_TOKEN:-}" ]; then
    echo "ERROR: ALPHA_ENGINE_LIB_TOKEN not set in .env — required for alpha-engine-lib pip install on spot"
    exit 1
fi

# ── Launch spot ──────────────────────────────────────────────────────────────
echo "==> Requesting spot instance ($INSTANCE_TYPE)..."

INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP" \
    --subnet-id "$SUBNET_ID" \
    --iam-instance-profile Name="$IAM_PROFILE" \
    --instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}' \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=alpha-engine-data-phase1-$(date +%Y%m%d)}]" \
    --region "$AWS_REGION" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo "  Instance ID: $INSTANCE_ID"

# Always terminate, even on error.
cleanup() {
    echo ""
    echo "==> Terminating spot instance $INSTANCE_ID..."
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" --output text > /dev/null 2>&1 || true
    echo "  Instance terminated."
}
trap cleanup EXIT

echo "==> Waiting for instance to enter running state..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"

PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text \
    --region "$AWS_REGION")

if [ "$PUBLIC_IP" = "None" ] || [ -z "$PUBLIC_IP" ]; then
    echo "ERROR: Instance has no public IP. Check subnet/VPC configuration."
    exit 1
fi

echo "  Public IP: $PUBLIC_IP"

# ── Wait for SSH ─────────────────────────────────────────────────────────────
echo "==> Waiting for SSH to become available..."
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"

for i in $(seq 1 30); do
    if ssh $SSH_OPTS -i "$KEY_FILE" ec2-user@"$PUBLIC_IP" "echo ok" 2>/dev/null; then
        echo "  SSH ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: SSH not available after 150s"
        exit 1
    fi
    sleep 5
done

run_remote() {
    ssh $SSH_OPTS -i "$KEY_FILE" ec2-user@"$PUBLIC_IP" "$@"
}

# ── Bootstrap spot: python + git ─────────────────────────────────────────────
echo "==> Bootstrapping spot environment..."
run_remote bash -s <<'BOOTSTRAP'
set -euo pipefail
sudo dnf install -y -q python3.12 python3.12-pip python3.12-devel git gcc 2>/dev/null || \
    sudo dnf install -y -q python3 python3-pip python3-devel git gcc
if command -v python3.12 &>/dev/null; then
    echo "Using: $(python3.12 --version)"
else
    echo "Using: $(python3 --version)"
fi
mkdir -p ~/.ssh
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null
BOOTSTRAP

# ── Clone alpha-engine-data on spot ──────────────────────────────────────────
echo "==> Cloning alpha-engine-data (branch: $BRANCH)..."
# HTTPS clone with PAT — matches the lib-install pattern below.
run_remote "git clone --depth 1 --branch $BRANCH https://github.com/cipher813/alpha-engine-data.git /home/ec2-user/alpha-engine-data"

# ── Upload .env BEFORE pip install (for ALPHA_ENGINE_LIB_TOKEN) ──────────────
echo "==> Uploading .env to spot..."
scp $SSH_OPTS -i "$KEY_FILE" \
    "$ENV_FILE" \
    ec2-user@"$PUBLIC_IP":/home/ec2-user/alpha-engine-data/.env

# ── Install python deps ──────────────────────────────────────────────────────
echo "==> Installing Python dependencies..."
run_remote bash -s <<'DEPS'
set -euo pipefail
cd /home/ec2-user/alpha-engine-data

set -a
# shellcheck disable=SC1091
source /home/ec2-user/alpha-engine-data/.env
set +a
if [ -z "${ALPHA_ENGINE_LIB_TOKEN:-}" ]; then
    echo "ERROR: ALPHA_ENGINE_LIB_TOKEN not set in .env"
    exit 1
fi
git config --global url."https://x-access-token:${ALPHA_ENGINE_LIB_TOKEN}@github.com/cipher813/alpha-engine-lib".insteadOf "https://github.com/cipher813/alpha-engine-lib"

if command -v python3.12 &>/dev/null; then
    PIP="python3.12 -m pip"
else
    PIP="python3 -m pip"
fi

$PIP install --upgrade pip -q
$PIP install -q -r requirements.txt

# numpy<2 pin to match other spot workloads (pyarrow compiled against 1.x).
$PIP install -q 'numpy<2'

echo "Dependencies installed."
DEPS

REMOTE_PYTHON=$(run_remote "command -v python3.12 || command -v python3")
ENV_SOURCE='set -a; [ -f /home/ec2-user/alpha-engine-data/.env ] && source /home/ec2-user/alpha-engine-data/.env; set +a; export XDG_CACHE_HOME=/tmp;'

# ── Smoke-only: imports + --phase 1 --dry-run ────────────────────────────────
if [ "$RUN_MODE" = "smoke-only" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  SMOKE TEST"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<SMOKE
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "==> Smoke: python import weekly_collector"
$REMOTE_PYTHON -c "import weekly_collector; print('import OK')"

echo ""
echo "==> Smoke: weekly_collector.py --phase 1 --dry-run"
$REMOTE_PYTHON weekly_collector.py --phase 1 --dry-run 2>&1 | tail -30
SMOKE

    echo "==> Smoke complete — instance will be terminated."
    exit 0
fi

# ── Full DataPhase1 ──────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  FULL DATA PHASE 1"
echo "═══════════════════════════════════════════════════════════════"

run_remote bash -s <<PHASE1
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "Starting weekly_collector.py --phase 1 at \$(date)"
if ! $REMOTE_PYTHON weekly_collector.py --phase 1 2>&1; then
    echo "ERROR: weekly_collector.py --phase 1 failed." >&2
    exit 1
fi
echo "DataPhase1 complete at \$(date)"
PHASE1

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  DataPhase1 complete. Instance will be terminated."
echo "═══════════════════════════════════════════════════════════════"

# Heartbeat on successful completion — mirrors backtester pattern so
# CloudWatch alarms can detect missed runs.
aws cloudwatch put-metric-data \
  --namespace "AlphaEngine" \
  --metric-name "Heartbeat" \
  --dimensions "Process=data-phase1" \
  --value 1 --unit "Count" \
  --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
  && echo "Heartbeat emitted: data-phase1" \
  || echo "WARNING: Failed to emit heartbeat (non-fatal)"
