#!/usr/bin/env bash
# push-configs.sh — Push gitignored config files to S3 for EC2 boot-pull.
#
# Configs are gitignored and never committed. Edit locally, push to S3 with
# this script. EC2 instances pull from S3 on every boot (boot-pull.sh).
#
# S3 layout:
#   s3://alpha-engine-research/config/risk.yaml         → trading EC2
#   s3://alpha-engine-research/config/data.yaml          → micro EC2 (alpha-engine-data)
#   s3://alpha-engine-research/config/backtester.yaml    → micro EC2 (backtester)
#
# Usage:
#   bash infrastructure/push-configs.sh              # push all configs to S3
#   bash infrastructure/push-configs.sh --dry-run    # show what would be pushed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REGION="${AWS_REGION:-us-east-1}"
BUCKET="alpha-engine-research"
S3_PREFIX="config"

# Config mapping: local_path → S3 key
CONFIGS=(
  "$DEV_ROOT/alpha-engine/config/risk.yaml:${S3_PREFIX}/risk.yaml"
  "$DEV_ROOT/alpha-engine/flow-doctor.yaml:${S3_PREFIX}/flow-doctor-executor.yaml"
  "$DEV_ROOT/alpha-engine-data/config.yaml:${S3_PREFIX}/data.yaml"
  "$DEV_ROOT/alpha-engine-backtester/config.yaml:${S3_PREFIX}/backtester.yaml"
  "$DEV_ROOT/alpha-engine-backtester/flow-doctor.yaml:${S3_PREFIX}/flow-doctor-backtester.yaml"
)

# ── Parse args ──────────────────────────────────────────────────────────────

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

# ── Push to S3 ──────────────────────────────────────────────────────────────

PUSHED=0
SKIPPED=0
FAILED=0

echo "Pushing configs to s3://${BUCKET}/${S3_PREFIX}/"
echo ""

for entry in "${CONFIGS[@]}"; do
  local_path="${entry%%:*}"
  s3_key="${entry#*:}"
  basename=$(basename "$local_path")

  if [ ! -f "$local_path" ]; then
    echo "  SKIP $basename (not found: $local_path)"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  if [ "$DRY_RUN" = true ]; then
    echo "  $basename → s3://${BUCKET}/${s3_key}"
    PUSHED=$((PUSHED + 1))
    continue
  fi

  if aws s3 cp "$local_path" "s3://${BUCKET}/${s3_key}" --region "$REGION" --quiet 2>/dev/null; then
    echo "  OK   $basename → s3://${BUCKET}/${s3_key}"
    PUSHED=$((PUSHED + 1))
  else
    echo "  FAIL $basename"
    FAILED=$((FAILED + 1))
  fi
done

echo ""
if [ "$DRY_RUN" = true ]; then
  echo "Would push $PUSHED config(s), $SKIPPED skipped. (dry-run — no changes made)"
else
  echo "Pushed $PUSHED config(s), $SKIPPED skipped, $FAILED failed."
  echo ""
  echo "EC2 instances pull these on every boot (boot-pull.sh)."
  echo "Changes take effect on next boot (or run: ae-trading 'bash ~/alpha-engine/infrastructure/pull-configs.sh')"
fi
