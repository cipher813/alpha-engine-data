"""Apply --correlation-id using SF-native States.Format with $.run_date."""
import boto3, json, re
s = boto3.Session(profile_name="ne-admin", region_name="us-east-1")
f = s.client("stepfunctions")
ARN = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-weekly-freshness-pipeline"
sm = f.describe_state_machine(stateMachineArn=ARN)
d = json.loads(sm["definition"])
FIXED = 0

def fix_cmd(v):
    v = re.sub(r'\s*--correlation-id\s+(?:"[^"]*"|\S+)', '', v)
    if '--correlation-id' not in v:
        v = v.replace("ssm_log_capture run --slug", "ssm_log_capture run --correlation-id {} --slug")
        v = re.sub(r"(States\.Format\('.*?ssm_log_capture.*?)'(\s*,\s*)", r"\1',$.run_date\2", v)
    return v

def walk(obj):
    global FIXED
    if isinstance(obj, dict):
        for k in obj:
            if k == "commands.$" and isinstance(obj[k], str) and "ssm_log_capture" in obj[k]:
                n = fix_cmd(obj[k])
                if n != obj[k]: FIXED += 1; obj[k] = n
            else: walk(obj[k])
    elif isinstance(obj, list):
        for x in obj: walk(x)

walk(d)
print(f"Fixed {FIXED} ssm_log_capture calls")
f.update_state_machine(stateMachineArn=ARN, definition=json.dumps(d))
print("State machine updated. Done.")
