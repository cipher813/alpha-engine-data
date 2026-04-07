#!/usr/bin/env bash
# push-configs.sh — Sync local config files to the private alpha-engine-config repo.
#
# Copies gitignored configs from each module into alpha-engine-config/,
# then commits and pushes. EC2 instances pull on boot via boot-pull.sh.
#
# Usage:
#   bash infrastructure/push-configs.sh              # sync + commit + push
#   bash infrastructure/push-configs.sh --dry-run    # show what would be synced

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_REPO="$DEV_ROOT/alpha-engine-config"

if [ ! -d "$CONFIG_REPO/.git" ]; then
  echo "ERROR: alpha-engine-config not found at $CONFIG_REPO"
  echo "Clone it: cd $DEV_ROOT && git clone git@github.com:cipher813/alpha-engine-config.git"
  exit 1
fi

# Config mapping: source → config repo destination
CONFIGS=(
  "$DEV_ROOT/alpha-engine/config/risk.yaml:executor/risk.yaml"
  "$DEV_ROOT/alpha-engine/flow-doctor.yaml:executor/flow-doctor.yaml"
  "$DEV_ROOT/alpha-engine-predictor/config/predictor.yaml:predictor/predictor.yaml"
  "$DEV_ROOT/alpha-engine-research/config/universe.yaml:research/universe.yaml"
  "$DEV_ROOT/alpha-engine-research/config/scoring.yaml:research/scoring.yaml"
  "$DEV_ROOT/alpha-engine-research/config.py:research/config.py"
  "$DEV_ROOT/alpha-engine-data/config.yaml:data/config.yaml"
  "$DEV_ROOT/alpha-engine-backtester/config.yaml:backtester/config.yaml"
  "$DEV_ROOT/alpha-engine-backtester/flow-doctor.yaml:backtester/flow-doctor.yaml"
)

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

SYNCED=0
SKIPPED=0

echo "Syncing configs to $CONFIG_REPO"
echo ""

for entry in "${CONFIGS[@]}"; do
  src="${entry%%:*}"
  dst="$CONFIG_REPO/${entry#*:}"
  name=$(basename "$src")

  if [ ! -f "$src" ]; then
    echo "  SKIP $name (not found: $src)"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  if [ "$DRY_RUN" = true ]; then
    echo "  $name → ${entry#*:}"
    SYNCED=$((SYNCED + 1))
    continue
  fi

  cp "$src" "$dst"
  echo "  OK   $name → ${entry#*:}"
  SYNCED=$((SYNCED + 1))
done

echo ""

if [ "$DRY_RUN" = true ]; then
  echo "Would sync $SYNCED config(s), $SKIPPED skipped. (dry-run)"
  exit 0
fi

# Commit and push if there are changes
cd "$CONFIG_REPO"
if git diff --quiet && git diff --cached --quiet; then
  echo "No config changes to push."
else
  git add -A
  echo "Changes detected:"
  git diff --cached --stat
  echo ""
  git commit -m "Sync configs from local modules ($(date +%Y-%m-%d))"
  git push origin main
  echo ""
  echo "Configs pushed. EC2 picks up on next boot, or run:"
  echo "  ae-trading \"cd ~/alpha-engine-config && git pull\""
fi
