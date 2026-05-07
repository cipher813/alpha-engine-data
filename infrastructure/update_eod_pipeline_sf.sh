#!/usr/bin/env bash
# update_eod_pipeline_sf.sh — Apply the canonical EOD pipeline SF definition.
#
# Reads the state-machine definition from
# infrastructure/step_function_eod.json (single source of truth, same
# pattern as deploy_step_function.sh for the Saturday SF) and applies
# it to alpha-engine-eod-pipeline. The JSON file is the authoritative
# definition — wiring tests pin its contents.
#
# Idempotent: re-running with the same definition is a no-op (AWS only
# bumps the revision when the definition actually changes).
#
# Usage:
#   ./infrastructure/update_eod_pipeline_sf.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFN_FILE="$SCRIPT_DIR/step_function_eod.json"

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:alpha-engine-eod-pipeline"

echo "=== Alpha Engine EOD Pipeline — SF Definition Update ==="
echo "  Region:        $REGION"
echo "  State machine: $SM_ARN"
echo "  Definition:    $DEFN_FILE"
echo ""

if [ ! -f "$DEFN_FILE" ]; then
    echo "ERROR: $DEFN_FILE not found" >&2
    exit 1
fi

# Validate JSON before sending it to AWS.
python3 -c "import json,sys; json.load(open(sys.argv[1])); print('  Definition: JSON valid')" "$DEFN_FILE"

aws stepfunctions update-state-machine \
    --state-machine-arn "$SM_ARN" \
    --definition "file://$DEFN_FILE" \
    --region "$REGION" > /dev/null

echo "  State machine: definition updated"
echo ""
echo "=== EOD Pipeline SF Update Complete ==="
echo ""
echo "Verify:"
echo "  aws stepfunctions describe-state-machine --state-machine-arn $SM_ARN --query 'definition' --output text | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(\"States:\", list(d[\"States\"].keys()))'"
echo ""
echo "First run with new chain: next daemon-triggered firing"
echo "(daemon shutdown, weekday market close + IB delay grace)."
