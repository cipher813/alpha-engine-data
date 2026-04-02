#!/usr/bin/env bash
# sync-secrets.sh — Push master .env to all EC2 instances.
#
# This script is the ONLY way secrets should reach EC2. Never SSH in
# and edit ~/.alpha-engine.env manually — edit .env locally and sync.
#
# What it does:
#   1. Reads .env from alpha-engine-data repo (master source of truth)
#   2. SCPs it to both EC2 instances as ~/.alpha-engine.env
#   3. Verifies the file landed correctly
#
# Usage:
#   bash infrastructure/sync-secrets.sh           # sync to all instances
#   bash infrastructure/sync-secrets.sh --trading  # sync to trading only
#   bash infrastructure/sync-secrets.sh --micro    # sync to micro only
#   bash infrastructure/sync-secrets.sh --dry-run  # show what would be synced
#
# Prerequisites:
#   - .env file in repo root (copy from .env.example)
#   - SSH key at ~/.ssh/alpha-engine-key.pem
#   - EC2 instances running and accessible

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
SSH_KEY="$HOME/.ssh/alpha-engine-key.pem"
REGION="${AWS_REGION:-us-east-1}"
REMOTE_PATH="/home/ec2-user/.alpha-engine.env"

# EC2 instance IDs
TRADING_INSTANCE="i-018eb3307a21329bf"
MICRO_INSTANCE="i-09b539c844515d549"

# ── Parse args ──────────────────────────────────────────────────────────────

TARGET="${1:-all}"
DRY_RUN=false
if [ "$TARGET" = "--dry-run" ]; then
  DRY_RUN=true
  TARGET="all"
fi

# ── Validate ────────────────────────────────────────────────────────────────

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found."
  echo "       Copy .env.example to .env and fill in your API keys."
  exit 1
fi

if [ ! -f "$SSH_KEY" ]; then
  echo "ERROR: SSH key not found at $SSH_KEY"
  exit 1
fi

# Count non-empty, non-comment lines
N_VARS=$(grep -cE "^[A-Z_]+=.+" "$ENV_FILE" 2>/dev/null || echo "0")
echo "Master .env: $N_VARS variables set"

if [ "$N_VARS" -lt 5 ]; then
  echo "WARNING: Only $N_VARS variables have values — did you fill in .env?"
fi

# ── Dry run ─────────────────────────────────────────────────────────────────

if [ "$DRY_RUN" = true ]; then
  echo ""
  echo "Variables that would be synced:"
  grep -E "^[A-Z_]+=.+" "$ENV_FILE" | sed 's/=.*/=***/' | sort
  echo ""
  echo "Targets: trading ($TRADING_INSTANCE), micro ($MICRO_INSTANCE)"
  echo "(dry-run — no changes made)"
  exit 0
fi

# ── Helper: resolve IP and SCP ──────────────────────────────────────────────

sync_to_instance() {
  local name="$1"
  local instance_id="$2"

  echo ""
  echo "=== Syncing to $name ($instance_id) ==="

  # Get public IP
  local ip
  ip=$(aws ec2 describe-instances \
    --instance-ids "$instance_id" \
    --region "$REGION" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text 2>/dev/null)

  if [ -z "$ip" ] || [ "$ip" = "None" ]; then
    echo "  SKIP: Instance not running or no public IP"
    return 1
  fi

  echo "  IP: $ip"

  # SCP the .env file
  scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
    "$ENV_FILE" "ec2-user@${ip}:${REMOTE_PATH}" 2>/dev/null

  if [ $? -eq 0 ]; then
    # Verify
    local remote_vars
    remote_vars=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
      "ec2-user@${ip}" "grep -cE '^[A-Z_]+=.+' $REMOTE_PATH 2>/dev/null" 2>/dev/null || echo "0")
    echo "  OK: $remote_vars variables written to $REMOTE_PATH"
  else
    echo "  FAILED: SCP failed (instance may be stopped or unreachable)"
    return 1
  fi
}

# ── Sync ────────────────────────────────────────────────────────────────────

FAILED=0

if [ "$TARGET" = "all" ] || [ "$TARGET" = "--trading" ]; then
  sync_to_instance "trading" "$TRADING_INSTANCE" || FAILED=$((FAILED + 1))
fi

if [ "$TARGET" = "all" ] || [ "$TARGET" = "--micro" ]; then
  sync_to_instance "micro" "$MICRO_INSTANCE" || FAILED=$((FAILED + 1))
fi

# ── Summary ─────────────────────────────────────────────────────────────────

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo "Sync complete. All instances updated."
else
  echo "Sync complete with $FAILED failure(s). Check instance status."
fi
echo ""
echo "Lambda env vars are synced separately during deploy:"
echo "  cd ~/Development/alpha-engine-research && bash infrastructure/deploy.sh"
echo "  cd ~/Development/alpha-engine-data && bash infrastructure/deploy.sh"
echo ""
