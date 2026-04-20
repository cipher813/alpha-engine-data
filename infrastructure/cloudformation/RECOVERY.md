# Recovering from ROLLBACK_COMPLETE

If `deploy-infrastructure.sh` aborts with "stack is in terminal state
ROLLBACK_COMPLETE" (or similar), the CloudFormation stack failed on
creation and needs to be rebuilt via import. CloudFormation cannot
update a stack in ROLLBACK_COMPLETE — the only path forward is delete
+ import + update.

## Why this happens

`create-stack` rolls back when a resource in the template already
exists outside CloudFormation management. Our 2026-04-20 incident: the
state machines, EventBridge rules, Scheduler, SNS topic, and most
alarms were created directly via `aws stepfunctions create-state-machine`
/ `aws events put-rule` etc. earlier in the system's life, so the first
`create-stack` attempt saw "State machine already exists" on the very
first resource and bailed.

## Remediation (one-time per incident)

```bash
cd infrastructure/cloudformation
STACK=alpha-engine-orchestration
REGION=us-east-1

# 1. Delete the failed stack registration. Since create rolled back on
#    the first resource, there's nothing real to delete — this just
#    clears the ROLLBACK_COMPLETE marker.
aws cloudformation delete-stack --stack-name "$STACK" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "$STACK" --region "$REGION"

# 2. Import existing resources. resources-to-import.json lists every
#    template resource that was created outside CloudFormation.
#    Any resource in the template but NOT in that JSON will NOT be
#    tracked by the stack until step 4.
aws cloudformation create-change-set \
    --stack-name "$STACK" \
    --change-set-name bootstrap-import \
    --change-set-type IMPORT \
    --resources-to-import file://resources-to-import.json \
    --template-body file://alpha-engine-orchestration.yaml \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION"

# 3. Review the change-set in the console or via describe-change-set,
#    then execute. Import is safe — no existing resources are modified
#    or replaced; CF just starts tracking them.
aws cloudformation describe-change-set \
    --stack-name "$STACK" \
    --change-set-name bootstrap-import \
    --query "Status" --output text --region "$REGION"

aws cloudformation execute-change-set \
    --stack-name "$STACK" \
    --change-set-name bootstrap-import \
    --region "$REGION"

aws cloudformation wait stack-import-complete --stack-name "$STACK" --region "$REGION"

# 4. Now run deploy-infrastructure.sh normally — the stack is tracking
#    everything in resources-to-import.json, so a regular update-stack
#    call will pick up the NEW resources (ResearchAlertsPermission +
#    UnscoredBuyCandidatesGap) and apply the git-sha tag.
cd ..
bash deploy-infrastructure.sh
```

## Verify post-recovery

```bash
aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].[StackStatus,Tags]" --output table --region "$REGION"

aws cloudwatch describe-alarms \
    --alarm-names alpha-engine-predictor-unscored-buy-candidates \
    --query "MetricAlarms[0].AlarmName" --output text --region "$REGION"
```

Expected: `StackStatus=UPDATE_COMPLETE`, `Tags` contains `git-sha=<sha>`,
alarm name returns (not "None").

## Keeping `resources-to-import.json` current

When adding a new resource to `alpha-engine-orchestration.yaml`:
- If the resource is genuinely new (never existed outside CF), it does
  NOT go in `resources-to-import.json`. Future deploys create it via
  update-stack.
- If the resource was provisioned manually before being added to the
  template (e.g. a shell script created it first), add it here so
  future recovery operations know to adopt it. Include physical ID via
  the `ResourceIdentifier` field.
