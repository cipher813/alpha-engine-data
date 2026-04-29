#!/usr/bin/env bash
#
# apply_s3_lifecycle.sh — Apply S3 lifecycle policy on alpha-engine-research.
#
# The policy file (s3_lifecycle_staging.json) is the single source of truth
# for ALL bucket lifecycle rules — put-bucket-lifecycle-configuration replaces
# the entire document on apply. Current rules:
#
#   - staging/        7-day expiration (added 2026-04-29 in PR #112; the
#                     daily_closes parquet is intermediate state between API
#                     fetch and ArcticDB ingest by builders/daily_append.py).
#   - features/       90-day STANDARD_IA transition + 365-day expiration
#                     (pre-existing rule, preserved when staging/ was added).
#
# Add new rules INTO this file's Rules array — never split across files.
# Idempotent — re-running pushes the same policy.
#
# Usage:
#   ./infrastructure/apply_s3_lifecycle.sh                 # apply
#   ./infrastructure/apply_s3_lifecycle.sh --dry-run       # print planned cmd
#
# Prerequisites:
#   - AWS CLI with s3:PutLifecycleConfiguration on the bucket
#
# WARNING: this script REPLACES the full lifecycle policy on the bucket.
# If other prefixes need lifecycle rules in the future, append them to
# s3_lifecycle_staging.json (or a successor file) — do not split rules
# across multiple invocations of this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUCKET="${ALPHA_ENGINE_BUCKET:-alpha-engine-research}"
REGION="${AWS_REGION:-us-east-1}"
POLICY_FILE="$SCRIPT_DIR/s3_lifecycle_staging.json"

DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
  esac
done

if [ ! -f "$POLICY_FILE" ]; then
  echo "ERROR: $POLICY_FILE not found" >&2
  exit 1
fi

if ! python3 -c "import json; json.load(open('$POLICY_FILE'))" 2>/dev/null; then
  echo "ERROR: $POLICY_FILE is not valid JSON" >&2
  exit 1
fi

echo "Applying lifecycle policy to s3://$BUCKET"
echo "Policy file: $POLICY_FILE"

if [ "$DRY_RUN" = 1 ]; then
  echo "  [dry-run] aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET --lifecycle-configuration file://$POLICY_FILE --region $REGION"
  exit 0
fi

aws s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET" \
  --lifecycle-configuration "file://$POLICY_FILE" \
  --region "$REGION"

echo "  OK"
echo ""
echo "Verify:"
echo "  aws s3api get-bucket-lifecycle-configuration --bucket $BUCKET --region $REGION"
