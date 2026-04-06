#!/usr/bin/env bash
# push-secrets.sh — Push master .env to all Lambda functions and EC2 instances.
#
# Single command to sync secrets everywhere after editing .env.
# Lambda gets env vars above the # LAMBDA_SKIP marker.
# EC2 gets the full .env via SCP.
#
# Usage:
#   bash infrastructure/push-secrets.sh              # push to all targets
#   bash infrastructure/push-secrets.sh --dry-run    # show what would be pushed
#   bash infrastructure/push-secrets.sh --lambda-only # Lambda functions only
#   bash infrastructure/push-secrets.sh --ec2-only    # EC2 instances only
#
# Prerequisites:
#   - .env file in repo root (copy from .env.example)
#   - SSH key at ~/.ssh/alpha-engine-key.pem (for EC2)
#   - AWS CLI configured (for Lambda)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
SSH_KEY="$HOME/.ssh/alpha-engine-key.pem"
REGION="${AWS_REGION:-us-east-1}"
REMOTE_PATH="/home/ec2-user/.alpha-engine.env"

# All Lambda functions that receive env vars from master .env
LAMBDA_FUNCTIONS=("alpha-engine-data-collector" "alpha-engine-research-runner" "alpha-engine-research-alerts" "alpha-engine-predictor-inference")

# EC2 instance IDs
TRADING_INSTANCE="i-018eb3307a21329bf"
MICRO_INSTANCE="i-09b539c844515d549"

# ── Parse args ──────────────────────────────────────────────────────────────

TARGET="all"
DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --lambda-only) TARGET="lambda-only" ;;
    --ec2-only) TARGET="ec2-only" ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

# ── Validate ────────────────────────────────────────────────────────────────

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found."
  echo "       Copy .env.example to .env and fill in your API keys."
  exit 1
fi

N_VARS=$(grep -cE "^[A-Z_]+=.+" "$ENV_FILE" 2>/dev/null || echo "0")
N_LAMBDA_VARS=$(awk '/^# LAMBDA_SKIP/{exit} /^[A-Z_]+=.+/{n++} END{print n+0}' "$ENV_FILE")
echo "Master .env: $N_VARS total variables ($N_LAMBDA_VARS for Lambda)"

if [ "$N_VARS" -lt 5 ]; then
  echo "WARNING: Only $N_VARS variables have values — did you fill in .env?"
fi

# ── Build Lambda env JSON ───────────────────────────────────────────────────
# Reads .env up to # LAMBDA_SKIP marker and outputs {"Variables": {...}}

build_lambda_env_json() {
  python3 -c "
import json
env = {}
with open('$ENV_FILE') as f:
    for line in f:
        line = line.strip()
        if line == '# LAMBDA_SKIP':
            break
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        key, val = line.split('=', 1)
        key, val = key.strip(), val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('\"', \"'\"):
            val = val[1:-1]
        if key and val:
            env[key] = val
if env:
    print(json.dumps({'Variables': env}))
else:
    print('')
"
}

# ── Dry run ─────────────────────────────────────────────────────────────────

if [ "$DRY_RUN" = true ]; then
  echo ""
  echo "Lambda variables (above # LAMBDA_SKIP):"
  awk '/^# LAMBDA_SKIP/{exit} /^[A-Z_]+=.+/{sub(/=.*/, "=***"); print "  " $0}' "$ENV_FILE" | sort
  echo ""
  if [ "$TARGET" != "ec2-only" ]; then
    echo "Lambda targets:"
    for fn in "${LAMBDA_FUNCTIONS[@]}"; do
      echo "  $fn"
    done
  fi
  if [ "$TARGET" != "lambda-only" ]; then
    echo "EC2 targets:"
    echo "  trading ($TRADING_INSTANCE)"
    echo "  micro ($MICRO_INSTANCE)"
  fi
  echo ""
  echo "(dry-run — no changes made)"
  exit 0
fi

# ── Push to Lambda ──────────────────────────────────────────────────────────

FAILED=0
LAMBDA_OK=0
EC2_OK=0

if [ "$TARGET" != "ec2-only" ]; then
  LAMBDA_ENV_JSON=$(build_lambda_env_json)

  if [ -z "$LAMBDA_ENV_JSON" ]; then
    echo "WARNING: No Lambda env vars found — skipping Lambda push"
  else
    echo ""
    echo "=== Pushing env vars to Lambda functions ==="
    for fn in "${LAMBDA_FUNCTIONS[@]}"; do
      echo -n "  $fn ... "
      result=$(aws lambda update-function-configuration --function-name "$fn" --environment "$LAMBDA_ENV_JSON" --region "$REGION" --query "LastUpdateStatus" --output text 2>&1) || true
      if echo "$result" | grep -qE "Successful|InProgress"; then
        echo "$result"
        LAMBDA_OK=$((LAMBDA_OK + 1))
      else
        echo "FAILED: $result"
        FAILED=$((FAILED + 1))
      fi
    done
  fi
fi

# ── Push to EC2 ─────────────────────────────────────────────────────────────

sync_to_instance() {
  local name="$1"
  local instance_id="$2"

  echo -n "  $name ($instance_id) ... "

  if [ ! -f "$SSH_KEY" ]; then
    echo "SKIP: SSH key not found at $SSH_KEY"
    return 1
  fi

  local ip
  ip=$(aws ec2 describe-instances --instance-ids "$instance_id" --region "$REGION" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)

  if [ -z "$ip" ] || [ "$ip" = "None" ]; then
    echo "SKIP: not running or no public IP"
    return 1
  fi

  scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$ENV_FILE" "ec2-user@${ip}:${REMOTE_PATH}" 2>/dev/null

  if [ $? -eq 0 ]; then
    local remote_vars
    remote_vars=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "ec2-user@${ip}" "grep -cE '^[A-Z_]+=.+' $REMOTE_PATH 2>/dev/null" 2>/dev/null || echo "0")
    echo "OK ($remote_vars vars written)"
  else
    echo "FAILED: SCP failed"
    return 1
  fi
}

if [ "$TARGET" != "lambda-only" ]; then
  echo ""
  echo "=== Pushing .env to EC2 instances ==="
  if sync_to_instance "trading" "$TRADING_INSTANCE"; then
    EC2_OK=$((EC2_OK + 1))
  else
    FAILED=$((FAILED + 1))
  fi
  if sync_to_instance "micro" "$MICRO_INSTANCE"; then
    EC2_OK=$((EC2_OK + 1))
  else
    FAILED=$((FAILED + 1))
  fi
fi

# ── Summary ─────────────────────────────────────────────────────────────────

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo "All targets updated successfully."
else
  echo "Completed with $FAILED failure(s)."
fi
[ "$LAMBDA_OK" -gt 0 ] && echo "  Lambda: $LAMBDA_OK/${#LAMBDA_FUNCTIONS[@]} functions"
[ "$EC2_OK" -gt 0 ] && echo "  EC2: $EC2_OK/2 instances"
echo ""
echo "Note: EC2 systemd services read env on next restart."
echo "      Lambda env vars take effect on next cold start."
