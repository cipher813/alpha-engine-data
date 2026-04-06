#!/usr/bin/env bash
# push-configs.sh — Push gitignored config files from local repos to EC2 instances.
#
# Configs are gitignored and never committed. Edit locally, then push with this script.
# Each config goes to the instance(s) that need it.
#
# Config mapping:
#   alpha-engine/config/risk.yaml       → trading EC2: ~/alpha-engine/config/risk.yaml
#   alpha-engine-data/config.yaml       → micro EC2:   ~/alpha-engine-data/config.yaml
#   alpha-engine-backtester/config.yaml → micro EC2:   ~/alpha-engine-backtester/config.yaml
#
# Usage:
#   bash infrastructure/push-configs.sh              # push all configs
#   bash infrastructure/push-configs.sh --dry-run    # show what would be pushed
#   bash infrastructure/push-configs.sh --trading    # trading instance only
#   bash infrastructure/push-configs.sh --micro      # micro instance only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SSH_KEY="$HOME/.ssh/alpha-engine-key.pem"
REGION="${AWS_REGION:-us-east-1}"

TRADING_INSTANCE="i-018eb3307a21329bf"
MICRO_INSTANCE="i-09b539c844515d549"

# Config file mapping: local_path → instance_type → remote_path
# Trading instance configs
TRADING_CONFIGS=("$DEV_ROOT/alpha-engine/config/risk.yaml:/home/ec2-user/alpha-engine/config/risk.yaml")

# Micro instance configs
MICRO_CONFIGS=("$DEV_ROOT/alpha-engine-data/config.yaml:/home/ec2-user/alpha-engine-data/config.yaml" "$DEV_ROOT/alpha-engine-backtester/config.yaml:/home/ec2-user/alpha-engine-backtester/config.yaml")

# ── Parse args ──────────────────────────────────────────────────────────────

TARGET="all"
DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --trading) TARGET="trading" ;;
    --micro) TARGET="micro" ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

# ── Validate ────────────────────────────────────────────────────────────────

if [ ! -f "$SSH_KEY" ]; then
  echo "ERROR: SSH key not found at $SSH_KEY"
  exit 1
fi

# ── Helpers ─────────────────────────────────────────────────────────────────

get_instance_ip() {
  local instance_id="$1"
  aws ec2 describe-instances --instance-ids "$instance_id" --region "$REGION" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null
}

push_config() {
  local local_path="$1"
  local remote_path="$2"
  local ip="$3"
  local name="$4"
  local basename
  basename=$(basename "$local_path")

  if [ ! -f "$local_path" ]; then
    echo "  SKIP $basename: local file not found at $local_path"
    return 1
  fi

  if [ "$DRY_RUN" = true ]; then
    echo "  $basename → $name:$remote_path"
    return 0
  fi

  echo -n "  $basename → $name ... "
  scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$local_path" "ec2-user@${ip}:${remote_path}" 2>/dev/null
  if [ $? -eq 0 ]; then
    echo "OK"
    return 0
  else
    echo "FAILED"
    return 1
  fi
}

push_to_instance() {
  local name="$1"
  local instance_id="$2"
  shift 2
  local configs=("$@")

  local ip
  ip=$(get_instance_ip "$instance_id")
  if [ -z "$ip" ] || [ "$ip" = "None" ]; then
    echo "  SKIP $name: instance not running or no public IP"
    return 1
  fi

  [ "$DRY_RUN" = false ] && echo "=== $name ($ip) ==="
  [ "$DRY_RUN" = true ] && echo "=== $name ($instance_id) ==="

  local failed=0
  for entry in "${configs[@]}"; do
    local local_path="${entry%%:*}"
    local remote_path="${entry#*:}"
    push_config "$local_path" "$remote_path" "$ip" "$name" || failed=$((failed + 1))
  done
  return $failed
}

# ── Dry run header ──────────────────────────────────────────────────────────

if [ "$DRY_RUN" = true ]; then
  echo "Configs that would be pushed:"
  echo ""
fi

# ── Push ────────────────────────────────────────────────────────────────────

FAILED=0

if [ "$TARGET" = "all" ] || [ "$TARGET" = "trading" ]; then
  push_to_instance "trading" "$TRADING_INSTANCE" "${TRADING_CONFIGS[@]}" || FAILED=$((FAILED + 1))
  echo ""
fi

if [ "$TARGET" = "all" ] || [ "$TARGET" = "micro" ]; then
  push_to_instance "micro" "$MICRO_INSTANCE" "${MICRO_CONFIGS[@]}" || FAILED=$((FAILED + 1))
  echo ""
fi

# ── Summary ─────────────────────────────────────────────────────────────────

if [ "$DRY_RUN" = true ]; then
  echo "(dry-run — no changes made)"
else
  if [ "$FAILED" -eq 0 ]; then
    echo "All configs pushed successfully."
  else
    echo "Completed with $FAILED failure(s)."
  fi
  echo ""
  echo "Note: Executor reads risk.yaml on each run (morning batch + daemon start)."
  echo "      Changes take effect on next trading day boot."
fi
