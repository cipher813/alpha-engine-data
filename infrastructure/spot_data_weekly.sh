#!/usr/bin/env bash
# infrastructure/spot_data_weekly.sh — Run weekly data workloads on a spot EC2.
#
# Bundles DataPhase1 + RAGIngestion on a single spot: launches c5.large,
# clones alpha-engine-data, runs `python weekly_collector.py --phase 1`
# followed by `bash rag/pipelines/run_weekly_ingestion.sh`, emits a
# heartbeat on success, and self-terminates.
#
# Origin: moved off ae-dashboard (t3.micro, 1 GB RAM) after the 2026-04-16
# OOM incident (features/compute.py in the DAILY code path exhausted micro
# memory). Saturday's Phase 1 uses a different code path and hasn't OOM'd
# historically, but running heavy data-refresh workloads on 1 GB RAM is
# fragile-by-design. This spot pattern mirrors the Backtester +
# PredictorTraining launchers so all heavy weekly compute lives on
# fresh, self-terminating instances instead of the always-on micro.
#
# Bundling rationale: Phase 1 and RAG ingestion are sequential SF steps
# that share the same repo + venv. One spot per bundle saves ~7 min of
# bootstrap overhead and one spot request. Trade-off: any failure fails
# both — acceptable since partial Saturday failures typically require a
# full-pipeline rerun anyway.
#
# Usage:
#   ./infrastructure/spot_data_weekly.sh                   # phase1 + rag
#   ./infrastructure/spot_data_weekly.sh --smoke-only      # quick validation, then terminate
#   ./infrastructure/spot_data_weekly.sh --instance-type c5.xlarge   # override size
#   ./infrastructure/spot_data_weekly.sh --branch my-branch          # override branch
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
RUN_MODE="full"  # full | smoke-only | rag-smoke-only | rag-only
while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke-only) RUN_MODE="smoke-only"; shift ;;
        --rag-smoke-only) RUN_MODE="rag-smoke-only"; shift ;;
        --rag-only) RUN_MODE="rag-only"; shift ;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
        --branch) BRANCH="$2"; shift 2 ;;
        *) echo "Unknown flag: $1"; exit 1 ;;
    esac
done

echo "═══════════════════════════════════════════════════════════════"
echo "  Weekly Data Spot Run (Phase1 + RAG) — $(date +%Y-%m-%d)"
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
# Note: alpha-engine-lib PAT is fetched by the spot itself from SSM
# (/alpha-engine/lib-token) during the DEPS step — dispatcher never touches it.

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
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=alpha-engine-data-weekly-$(date +%Y%m%d)}]" \
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

# ── Upload .env BEFORE pip install ──────────────────────────────────────────
echo "==> Uploading .env to spot..."
scp $SSH_OPTS -i "$KEY_FILE" \
    "$ENV_FILE" \
    ec2-user@"$PUBLIC_IP":/home/ec2-user/alpha-engine-data/.env

# ── Upload alpha-engine-config/data/config.yaml ─────────────────────────────
# weekly_collector.py's load_config() searches /home/ec2-user/alpha-engine-config/data/config.yaml
# first. Private config repo — SCP from the dispatcher's clone (pulled daily by
# ae-dashboard's boot-pull) rather than cloning it on the spot (which would
# require broader git-auth setup than the lib-only insteadOf we use here).
CONFIG_SRC="/home/ec2-user/alpha-engine-config/data/config.yaml"
if [ ! -f "$CONFIG_SRC" ]; then
    CONFIG_SRC="$HOME/Development/alpha-engine-config/data/config.yaml"
fi
if [ ! -f "$CONFIG_SRC" ]; then
    echo "ERROR: dispatcher config not found at /home/ec2-user/alpha-engine-config/data/config.yaml or $HOME/Development/alpha-engine-config/data/config.yaml — is alpha-engine-config cloned + pulled?"
    exit 1
fi
echo "==> Uploading alpha-engine-config/data/config.yaml to spot..."
run_remote "mkdir -p /home/ec2-user/alpha-engine-config/data"
scp $SSH_OPTS -i "$KEY_FILE" \
    "$CONFIG_SRC" \
    ec2-user@"$PUBLIC_IP":/home/ec2-user/alpha-engine-config/data/config.yaml

# ── Install python deps ──────────────────────────────────────────────────────
# The spot pulls its own alpha-engine-lib PAT from SSM (same pattern as
# ae-trading's boot-pull.sh). Dispatcher never handles the secret. The
# spot's IAM profile (alpha-engine-executor-profile) grants ssm:GetParameter
# on /alpha-engine/*. Token is scoped to a local shell var, never exported
# or logged.
echo "==> Installing Python dependencies..."
run_remote bash -s <<'DEPS'
set -euo pipefail
cd /home/ec2-user/alpha-engine-data

set -a
# shellcheck disable=SC1091
source /home/ec2-user/alpha-engine-data/.env
set +a

LIB_TOKEN=$(aws ssm get-parameter --name /alpha-engine/lib-token --with-decryption --query 'Parameter.Value' --output text --region us-east-1 2>/dev/null || echo "")
if [ -z "$LIB_TOKEN" ]; then
    echo "ERROR: could not fetch /alpha-engine/lib-token from SSM — required for alpha-engine-lib pip install"
    exit 1
fi
git config --global url."https://x-access-token:${LIB_TOKEN}@github.com/cipher813/alpha-engine-lib".insteadOf "https://github.com/cipher813/alpha-engine-lib"
unset LIB_TOKEN

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
# Export PYTHON_BIN so downstream bash scripts (e.g.
# rag/pipelines/run_weekly_ingestion.sh) inherit the interpreter we
# bootstrapped. AL2023 spots install python3.12 but have no bare `python`
# symlink — the RAG script's `python -m ...` fails without this. Origin:
# 2026-04-17 Saturday Step Function failure in RAG step-0 preflight.
ENV_SOURCE="set -a; [ -f /home/ec2-user/alpha-engine-data/.env ] && source /home/ec2-user/alpha-engine-data/.env; set +a; export XDG_CACHE_HOME=/tmp; export PYTHON_BIN=$REMOTE_PYTHON;"

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
# Show full output (was tail -30 — truncated error tracebacks from early
# collectors so their failure mode was invisible during debugging).
$REMOTE_PYTHON weekly_collector.py --phase 1 --dry-run 2>&1
SMOKE

    echo "==> Smoke complete — instance will be terminated."
    exit 0
fi

# ── RAG-smoke-only: SSM fetch + preflight + submodule imports + dry-run ──────
# Exercises the RAG-via-SSM path end-to-end on a real AL2023 spot without
# hitting production external state (no SEC fetches, no Voyage embeddings,
# no Postgres writes — everything gated by --dry-run in the submodules).
# Validates:
#   1. IAM: spot can fetch the 4 RAG secrets from SSM
#   2. PYTHON_BIN resolution under python3.12 on AL2023
#   3. All 5 env vars pass rag/preflight.py::RAGPreflight.check_env_vars
#   4. All 5 RAG submodules import under python3.12
#   5. run_weekly_ingestion.sh --dry-run executes each pipeline's CLI path
# Does NOT validate: Postgres reachability (dry-run doesn't connect),
# external API quotas (dry-run doesn't hit them), runtime bugs that only
# trigger on production-shape data.
if [ "$RUN_MODE" = "rag-smoke-only" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  RAG SMOKE TEST"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<RAG_SMOKE
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "==> RAG smoke: fetching secrets from SSM"
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done
echo "RAG secrets fetched: VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, RAG_DATABASE_URL"

echo ""
echo "==> RAG smoke: preflight env-var check"
\$PYTHON_BIN -m rag.preflight

echo ""
echo "==> RAG smoke: import all 5 RAG submodules"
\$PYTHON_BIN -c "
import rag.pipelines.ingest_sec_filings
import rag.pipelines.ingest_8k_filings
import rag.pipelines.ingest_earnings_finnhub
import rag.pipelines.ingest_theses
import rag.pipelines.filing_change_detection
print('all 5 rag submodules imported OK')
"

echo ""
echo "==> RAG smoke: run_weekly_ingestion.sh --dry-run"
bash rag/pipelines/run_weekly_ingestion.sh --dry-run 2>&1
RAG_SMOKE

    echo "==> RAG smoke complete — instance will be terminated."
    exit 0
fi

# ── RAG-only: skip DataPhase1, run only RAG ingestion ───────────────────────
# Use when DataPhase1 succeeded earlier (e.g. last Saturday's SF cleared
# DataPhase1 but RAG failed downstream and needs a standalone re-run). Fetches
# secrets from SSM, runs the real (non-dry-run) RAG ingestion, emits only the
# rag-ingestion heartbeat so CloudWatch state accurately reflects what ran.
if [ "$RUN_MODE" = "rag-only" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  RAG-ONLY RUN (skipping DataPhase1)"
    echo "═══════════════════════════════════════════════════════════════"
    run_remote bash -s <<RAG_ONLY
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "──────────────────────────────────────────────────────────────"
echo "Fetching RAG secrets from SSM at \$(date)"
echo "──────────────────────────────────────────────────────────────"
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM — required for RAG ingestion" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done
echo "RAG secrets fetched: VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, RAG_DATABASE_URL"

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Starting rag/pipelines/run_weekly_ingestion.sh at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! bash rag/pipelines/run_weekly_ingestion.sh 2>&1; then
    echo "ERROR: run_weekly_ingestion.sh failed." >&2
    exit 1
fi
echo "RAGIngestion complete at \$(date)"
RAG_ONLY

    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo "  RAG-only run complete. Instance will be terminated."
    echo "═══════════════════════════════════════════════════════════════"

    aws cloudwatch put-metric-data \
        --namespace "AlphaEngine" \
        --metric-name "Heartbeat" \
        --dimensions "Process=rag-ingestion" \
        --value 1 --unit "Count" \
        --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
        && echo "Heartbeat emitted: rag-ingestion" \
        || echo "WARNING: Failed to emit heartbeat for rag-ingestion (non-fatal)"
    exit 0
fi

# ── Full run: DataPhase1 + RAGIngestion sequentially ────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  FULL RUN: DataPhase1 + RAGIngestion"
echo "═══════════════════════════════════════════════════════════════"

run_remote bash -s <<WORKLOADS
set -euo pipefail
cd /home/ec2-user/alpha-engine-data
${ENV_SOURCE}

echo "──────────────────────────────────────────────────────────────"
echo "Starting weekly_collector.py --phase 1 at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! $REMOTE_PYTHON weekly_collector.py --phase 1 2>&1; then
    echo "ERROR: weekly_collector.py --phase 1 failed — aborting bundle before RAG." >&2
    exit 1
fi
echo "DataPhase1 complete at \$(date)"

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Fetching RAG secrets from SSM at \$(date)"
echo "──────────────────────────────────────────────────────────────"
# Phase 2 SSM migration — RAG secrets come from SSM Parameter Store, NOT
# from the SCP'd .env. Origin: 2026-04-17 Saturday Step Function failure
# where RAG_DATABASE_URL silently truncated at an unquoted & in the .env
# (a Postgres DSN query-param). Bash source on AL2023 spots dropped the
# tail of the value after the shell metachar. SSM stores the value as an
# opaque string — no shell-parse fragility, no cross-instance sync via
# push-secrets.sh needed, and the spot's IAM profile already has
# ssm:GetParameter (pattern already in use above for /alpha-engine/lib-token).
for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do
    val=\$(aws ssm get-parameter --name /alpha-engine/\$name --with-decryption --query 'Parameter.Value' --output text --region "\${AWS_REGION:-us-east-1}" 2>/dev/null || echo "")
    if [ -z "\$val" ]; then
        echo "ERROR: could not fetch /alpha-engine/\$name from SSM — required for RAG ingestion" >&2
        exit 1
    fi
    export \$name="\$val"
    unset val
done
echo "RAG secrets fetched: VOYAGE_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, RAG_DATABASE_URL"

echo ""
echo "──────────────────────────────────────────────────────────────"
echo "Starting rag/pipelines/run_weekly_ingestion.sh at \$(date)"
echo "──────────────────────────────────────────────────────────────"
if ! bash rag/pipelines/run_weekly_ingestion.sh 2>&1; then
    echo "ERROR: run_weekly_ingestion.sh failed." >&2
    exit 1
fi
echo "RAGIngestion complete at \$(date)"
WORKLOADS

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Weekly data bundle complete. Instance will be terminated."
echo "═══════════════════════════════════════════════════════════════"

# Heartbeat — one metric per sub-workload so CloudWatch alarms can
# distinguish between a missed Phase 1 and a missed RAG.
for proc in data-phase1 rag-ingestion; do
    aws cloudwatch put-metric-data \
        --namespace "AlphaEngine" \
        --metric-name "Heartbeat" \
        --dimensions "Process=$proc" \
        --value 1 --unit "Count" \
        --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
        && echo "Heartbeat emitted: $proc" \
        || echo "WARNING: Failed to emit heartbeat for $proc (non-fatal)"
done
