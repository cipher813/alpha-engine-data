"""Apply --correlation-id flag to all ssm_log_capture calls.
--correlation-id is a boolean FLAG (not a value argument)."""
import boto3, json

session = boto3.Session(profile_name="ne-admin", region_name="us-east-1")
sfn = session.client("stepfunctions")

ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
sm = sfn.describe_state_machine(stateMachineArn=ARN)
definition = json.loads(sm["definition"])

FIXED = 0

def apply_fixes(obj):
    global FIXED
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "commands.$" and isinstance(v, str) and "ssm_log_capture" in v:
                old = v
                new = v
                # Remove any previously-botched --correlation-id patterns
                # Pattern 1 (after run, with value): run --correlation-id "sf-$(date +%s)-$$"
                new = new.replace(' run --correlation-id "sf-$(date +%s)-$$"', "")
                # Pattern 2 (before run, with value): --correlation-id "sf-$(date +%s)-$$" run
                new = new.replace(' --correlation-id "sf-$(date +%s)-$$" run', " run")
                # Pattern 3 (correct flag, already present): --correlation-id run
                if "--correlation-id run" not in new and "--correlation-id" not in new:
                    # Inject the flag BEFORE 'run'
                    new = new.replace(
                        "ssm_log_capture run --slug",
                        "ssm_log_capture --correlation-id run --slug",
                    )
                if new != old:
                    FIXED += 1
                obj[k] = new
            else:
                apply_fixes(v)
    elif isinstance(obj, list):
        for item in obj:
            apply_fixes(item)


apply_fixes(definition)
print(f"Fixed {FIXED} ssm_log_capture calls")

sfn.update_state_machine(stateMachineArn=ARN, definition=json.dumps(definition))
print("State machine updated. Done.")
