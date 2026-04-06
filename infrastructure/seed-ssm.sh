#!/usr/bin/env bash
# seed-ssm.sh — Seed AWS SSM Parameter Store from master .env file.
#
# One-time migration: reads .env and creates SecureString parameters
# under /alpha-engine/ prefix. After this, all modules read from SSM
# at startup — no more pushing .env to Lambda/EC2.
#
# Usage:
#   bash infrastructure/seed-ssm.sh              # create/update all params
#   bash infrastructure/seed-ssm.sh --dry-run    # show what would be created
#   bash infrastructure/seed-ssm.sh --delete     # remove all /alpha-engine/ params
#
# To update a single secret later:
#   aws ssm put-parameter --name "/alpha-engine/POLYGON_API_KEY" \
#     --type SecureString --value "new-key" --overwrite \
#     --query "Version" --output text

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
REGION="${AWS_REGION:-us-east-1}"
PREFIX="/alpha-engine"

# ── Parse args ──────────────────────────────────────────────────────────────

DRY_RUN=false
DELETE=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --delete) DELETE=true ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

# ── Delete mode ─────────────────────────────────────────────────────────────

if [ "$DELETE" = true ]; then
  echo "Deleting all parameters under $PREFIX/..."
  params=$(aws ssm get-parameters-by-path --path "$PREFIX/" --region "$REGION" --query "Parameters[].Name" --output text 2>/dev/null || echo "")
  if [ -z "$params" ]; then
    echo "  No parameters found."
    exit 0
  fi
  for name in $params; do
    if [ "$DRY_RUN" = true ]; then
      echo "  Would delete: $name"
    else
      aws ssm delete-parameter --name "$name" --region "$REGION" --output text 2>/dev/null && echo "  Deleted: $name" || echo "  Failed: $name"
    fi
  done
  exit 0
fi

# ── Validate ────────────────────────────────────────────────────────────────

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found."
  echo "       Copy .env.example to .env and fill in your API keys."
  exit 1
fi

# ── Read .env and seed SSM ──────────────────────────────────────────────────

echo "Seeding SSM Parameter Store from $ENV_FILE"
echo "Prefix: $PREFIX/"
echo "Region: $REGION"
echo ""

CREATED=0
SKIPPED=0
FAILED=0

while IFS= read -r line; do
  line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
  [[ -z "$line" || "$line" == \#* ]] && continue
  [[ "$line" != *=* ]] && continue

  key="${line%%=*}"
  val="${line#*=}"
  key=$(echo "$key" | xargs)
  val=$(echo "$val" | xargs)

  # Strip surrounding quotes
  if [[ ${#val} -ge 2 && ( ("${val:0:1}" == '"' && "${val: -1}" == '"') || ("${val:0:1}" == "'" && "${val: -1}" == "'") ) ]]; then
    val="${val:1:${#val}-2}"
  fi

  # Skip empty values
  if [ -z "$val" ]; then
    continue
  fi

  param_name="$PREFIX/$key"

  if [ "$DRY_RUN" = true ]; then
    echo "  $param_name = ***"
    CREATED=$((CREATED + 1))
    continue
  fi

  result=$(aws ssm put-parameter --name "$param_name" --type SecureString --value "$val" --overwrite --region "$REGION" --query "Version" --output text 2>&1) || true

  if echo "$result" | grep -qE "^[0-9]+$"; then
    echo "  $param_name (v$result)"
    CREATED=$((CREATED + 1))
  else
    echo "  FAILED $param_name: $result"
    FAILED=$((FAILED + 1))
  fi
done < "$ENV_FILE"

echo ""
if [ "$DRY_RUN" = true ]; then
  echo "$CREATED parameters would be created/updated."
  echo "(dry-run — no changes made)"
else
  echo "Done: $CREATED created/updated, $FAILED failed."
  echo ""
  echo "Next steps:"
  echo "  1. Add ssm:GetParametersByPath to Lambda/EC2 IAM roles"
  echo "  2. Deploy modules with ssm_secrets.py loader"
  echo "  3. Verify: aws ssm get-parameters-by-path --path '$PREFIX/' --region $REGION --query 'Parameters[].Name' --output text"
fi
