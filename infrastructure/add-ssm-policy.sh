#!/usr/bin/env bash
# add-ssm-policy.sh — Add SSM Parameter Store read access to all Alpha Engine IAM roles.
#
# Adds an inline policy allowing ssm:GetParametersByPath and ssm:GetParameter
# on /alpha-engine/* to each Lambda execution role and the EC2 instance role.
#
# Usage:
#   bash infrastructure/add-ssm-policy.sh              # apply to all roles
#   bash infrastructure/add-ssm-policy.sh --dry-run    # show what would be applied

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text 2>/dev/null)

if [ -z "$ACCOUNT_ID" ]; then
  echo "ERROR: Could not determine AWS account ID. Check AWS credentials."
  exit 1
fi

POLICY_NAME="alpha-engine-ssm-read"
POLICY_DOC=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SSMParameterStoreRead",
      "Effect": "Allow",
      "Action": [
        "ssm:GetParametersByPath",
        "ssm:GetParameter"
      ],
      "Resource": "arn:aws:ssm:${REGION}:${ACCOUNT_ID}:parameter/alpha-engine/*"
    }
  ]
}
EOF
)

# All IAM roles that need SSM access.
#
# Codified roles handle their own SSM-read inline policy via apply.sh in
# their home repo's infrastructure/iam/ directory:
#   - alpha-engine-executor-role  → alpha-engine
#   - alpha-engine-predictor-role → alpha-engine-predictor
# This script only writes to roles that don't have codified IAM yet.
ROLES=("alpha-engine-data-role" "alpha-engine-research-role")

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

echo "Adding SSM read policy to Alpha Engine IAM roles"
echo "Account: $ACCOUNT_ID"
echo "Region: $REGION"
echo "Resource: arn:aws:ssm:${REGION}:${ACCOUNT_ID}:parameter/alpha-engine/*"
echo ""

for role in "${ROLES[@]}"; do
  if [ "$DRY_RUN" = true ]; then
    echo "  Would add $POLICY_NAME to $role"
    continue
  fi

  if ! aws iam get-role --role-name "$role" --region "$REGION" &>/dev/null; then
    echo "  SKIP $role: role does not exist"
    continue
  fi

  aws iam put-role-policy --role-name "$role" --policy-name "$POLICY_NAME" --policy-document "$POLICY_DOC" 2>&1 && echo "  OK: $role" || echo "  FAILED: $role"
done

echo ""
if [ "$DRY_RUN" = true ]; then
  echo "(dry-run — no changes made)"
else
  echo "Done. Verify with:"
  echo "  aws iam get-role-policy --role-name alpha-engine-data-role --policy-name $POLICY_NAME --query 'PolicyDocument' --output json"
fi
