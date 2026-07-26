"""Revert: remove all --correlation-id flags from the SF definition.
The dashboard box was rolled back to pre-fleet-logging-standard krepis."""
import boto3, json

session = boto3.Session(profile_name="ne-admin", region_name="us-east-1")
sfn = session.client("stepfunctions")

ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
sm = sfn.describe_state_machine(stateMachineArn=ARN)
definition = json.loads(sm["definition"])

FIXED = 0
# All three broken patterns to remove:
PATTERNS = [
    ' --correlation-id run --slug',     # just the flag before run
    " run --correlation-id",            # broken placement after run
    ' --correlation-id "sf-$(date +%s)-$$" run --slug',  # flag with value
]

def apply_fixes(obj):
    global FIXED
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "commands.$" and isinstance(v, str) and "--correlation-id" in v:
                old = v
                new = v
                for pat in PATTERNS:
                    new = new.replace(pat, " run --slug" if "run" in pat else "")
                # Clean up any double "run run" artifacts
                new = new.replace(" run  run ", " run ")
                new = new.replace("ssm_log_capture  run", "ssm_log_capture run")
                if new != old:
                    FIXED += 1
                obj[k] = new
            else:
                apply_fixes(v)
    elif isinstance(obj, list):
        for item in obj:
            apply_fixes(item)

apply_fixes(definition)
print(f"Reverted {FIXED} ssm_log_capture calls")

sfn.update_state_machine(stateMachineArn=ARN, definition=json.dumps(definition))
print("State machine reverted to pre-correlation-id state. Done.")
