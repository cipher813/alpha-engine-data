#!/usr/bin/env bash
# update_eod_pipeline_sf.sh — Apply the Phase 2 EOD-SF update.
#
# Inserts the CaptureSnapshot step between PostMarketData and
# EODReconcile in alpha-engine-eod-pipeline. Companion code in
# alpha-engine repo: executor/snapshot_capturer.py + the eod_reconcile
# refactor that reads from S3 instead of live IB.
#
# Idempotent: re-running with the same definition is a no-op (AWS only
# bumps the revision when the definition actually changes).
#
# Usage:
#   ./infrastructure/update_eod_pipeline_sf.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION")
SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:alpha-engine-eod-pipeline"

echo "=== Alpha Engine EOD Pipeline — SF Definition Update ==="
echo "  Region:       $REGION"
echo "  State machine: $SM_ARN"
echo ""

DEFN=$(cat <<'JSON'
{
  "Comment": "Alpha Engine EOD Pipeline — post-market data capture, snapshot capture, reconciliation, instance shutdown. Triggered by daemon shutdown on trading EC2.",
  "StartAt": "PostMarketData",
  "States": {
    "PostMarketData": {
      "Type": "Task",
      "Comment": "Capture today's closing prices via yfinance + append to ArcticDB (runs on ae-trading). Hard-fails on non-zero exit via pipefail so tee does not mask python failures.",
      "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
      "Parameters": {
        "DocumentName": "AWS-RunShellScript",
        "InstanceIds.$": "$.trading_instance_id",
        "Parameters": {
          "commands": [
            "set -o pipefail",
            "cd /home/ec2-user/alpha-engine-data",
            "set -a && source /home/ec2-user/.alpha-engine.env && set +a",
            "source .venv/bin/activate",
            "python weekly_collector.py --daily --only daily_closes 2>&1 | tee /var/log/postmarket-data.log",
            "python -m builders.daily_append 2>&1 | tee -a /var/log/postmarket-data.log"
          ],
          "executionTimeout": ["1200"]
        },
        "TimeoutSeconds": 1200
      },
      "TimeoutSeconds": 1260,
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "MaxAttempts": 1,
          "IntervalSeconds": 30,
          "BackoffRate": 1.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Comment": "Hard-fail: EOD reconcile requires fresh ArcticDB closes (price_cache raises RuntimeError on miss). Don't paper over a PostMarketData failure.",
          "Next": "HandleFailure",
          "ResultPath": "$.error"
        }
      ],
      "ResultPath": "$.postmarket_result",
      "Next": "WaitForPostMarketData"
    },
    "WaitForPostMarketData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:ssm:getCommandInvocation",
      "Parameters": {
        "CommandId.$": "$.postmarket_result.Command.CommandId",
        "InstanceId.$": "$.trading_instance_id[0]"
      },
      "Retry": [
        {
          "ErrorEquals": ["Ssm.InvocationDoesNotExistException"],
          "MaxAttempts": 10,
          "IntervalSeconds": 10,
          "BackoffRate": 1.5
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure",
          "ResultPath": "$.error"
        }
      ],
      "ResultPath": "$.postmarket_poll",
      "Next": "CheckPostMarketStatus"
    },
    "CheckPostMarketStatus": {
      "Type": "Choice",
      "Comment": "Default routes to HandleFailure so any unexpected SSM status (Failed, Cancelled, TimedOut, ...) is surfaced, not silently swallowed.",
      "Choices": [
        { "Variable": "$.postmarket_poll.Status", "StringEquals": "Success", "Next": "CaptureSnapshot" },
        { "Variable": "$.postmarket_poll.Status", "StringEquals": "InProgress", "Next": "PostMarketWait" },
        { "Variable": "$.postmarket_poll.Status", "StringEquals": "Pending", "Next": "PostMarketWait" },
        { "Variable": "$.postmarket_poll.Status", "StringEquals": "Delayed", "Next": "PostMarketWait" }
      ],
      "Default": "PostMarketStatusError"
    },
    "PostMarketStatusError": {
      "Type": "Pass",
      "Comment": "Choice Default path — synthesize $.error from poll result so HandleFailure's JsonToString($.error) has something to stringify.",
      "Parameters": {
        "source": "post_market_status",
        "status.$": "$.postmarket_poll.Status",
        "status_details.$": "$.postmarket_poll.StatusDetails",
        "response_code.$": "$.postmarket_poll.ResponseCode",
        "command_id.$": "$.postmarket_poll.CommandId",
        "instance_id.$": "$.postmarket_poll.InstanceId"
      },
      "ResultPath": "$.error",
      "Next": "HandleFailure"
    },
    "PostMarketWait": {
      "Type": "Wait",
      "Seconds": 15,
      "Next": "WaitForPostMarketData"
    },
    "CaptureSnapshot": {
      "Type": "Task",
      "Comment": "Phase 2 of EOD-SF cutover: capture live IB state once at end-of-day and persist to s3://...trades/snapshots/{run_date}.json. EODReconcile then reads the snapshot instead of querying live IB. Decouples capture from reconciliation so the row keyed by run_date=X sources from observations made at time X.",
      "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
      "Parameters": {
        "DocumentName": "AWS-RunShellScript",
        "InstanceIds.$": "$.trading_instance_id",
        "Parameters": {
          "commands": [
            "set -o pipefail",
            "cd /home/ec2-user/alpha-engine",
            "set -a && source /home/ec2-user/.alpha-engine.env && set +a",
            "source .venv/bin/activate",
            "python executor/snapshot_capturer.py 2>&1 | tee /var/log/snapshot.log"
          ],
          "executionTimeout": ["120"]
        },
        "TimeoutSeconds": 120
      },
      "TimeoutSeconds": 180,
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "MaxAttempts": 1,
          "IntervalSeconds": 30,
          "BackoffRate": 1.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Comment": "Hard-fail: EODReconcile depends on this snapshot. No silent fallback to live IB — that's the whole point of Phase 2.",
          "Next": "HandleFailure",
          "ResultPath": "$.error"
        }
      ],
      "ResultPath": "$.snapshot_result",
      "Next": "WaitForCaptureSnapshot"
    },
    "WaitForCaptureSnapshot": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:ssm:getCommandInvocation",
      "Parameters": {
        "CommandId.$": "$.snapshot_result.Command.CommandId",
        "InstanceId.$": "$.trading_instance_id[0]"
      },
      "Retry": [
        {
          "ErrorEquals": ["Ssm.InvocationDoesNotExistException"],
          "MaxAttempts": 10,
          "IntervalSeconds": 10,
          "BackoffRate": 1.5
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure",
          "ResultPath": "$.error"
        }
      ],
      "ResultPath": "$.snapshot_poll",
      "Next": "CheckSnapshotStatus"
    },
    "CheckSnapshotStatus": {
      "Type": "Choice",
      "Comment": "Default routes to HandleFailure so any unexpected SSM status is surfaced, not silently swallowed.",
      "Choices": [
        { "Variable": "$.snapshot_poll.Status", "StringEquals": "Success", "Next": "EODReconcile" },
        { "Variable": "$.snapshot_poll.Status", "StringEquals": "InProgress", "Next": "SnapshotWait" },
        { "Variable": "$.snapshot_poll.Status", "StringEquals": "Pending", "Next": "SnapshotWait" },
        { "Variable": "$.snapshot_poll.Status", "StringEquals": "Delayed", "Next": "SnapshotWait" }
      ],
      "Default": "SnapshotStatusError"
    },
    "SnapshotStatusError": {
      "Type": "Pass",
      "Comment": "Choice Default path — synthesize $.error from poll result.",
      "Parameters": {
        "source": "snapshot_status",
        "status.$": "$.snapshot_poll.Status",
        "status_details.$": "$.snapshot_poll.StatusDetails",
        "response_code.$": "$.snapshot_poll.ResponseCode",
        "command_id.$": "$.snapshot_poll.CommandId",
        "instance_id.$": "$.snapshot_poll.InstanceId"
      },
      "ResultPath": "$.error",
      "Next": "HandleFailure"
    },
    "SnapshotWait": {
      "Type": "Wait",
      "Seconds": 5,
      "Next": "WaitForCaptureSnapshot"
    },
    "EODReconcile": {
      "Type": "Task",
      "Comment": "Run EOD reconciliation on trading EC2 — reads closes from ArcticDB + state from S3 snapshot (no live IB), computes P&L, sends email. pipefail surfaces python crashes that tee would otherwise mask.",
      "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
      "Parameters": {
        "DocumentName": "AWS-RunShellScript",
        "InstanceIds.$": "$.trading_instance_id",
        "Parameters": {
          "commands": [
            "set -o pipefail",
            "cd /home/ec2-user/alpha-engine",
            "set -a && source /home/ec2-user/.alpha-engine.env && set +a",
            "source .venv/bin/activate",
            "python executor/eod_reconcile.py 2>&1 | tee /var/log/eod.log"
          ],
          "executionTimeout": ["120"]
        },
        "TimeoutSeconds": 120
      },
      "TimeoutSeconds": 180,
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "MaxAttempts": 1,
          "IntervalSeconds": 30,
          "BackoffRate": 1.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure",
          "ResultPath": "$.error"
        }
      ],
      "ResultPath": "$.eod_result",
      "Next": "WaitForEOD"
    },
    "WaitForEOD": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:ssm:getCommandInvocation",
      "Parameters": {
        "CommandId.$": "$.eod_result.Command.CommandId",
        "InstanceId.$": "$.trading_instance_id[0]"
      },
      "Retry": [
        {
          "ErrorEquals": ["Ssm.InvocationDoesNotExistException"],
          "MaxAttempts": 10,
          "IntervalSeconds": 10,
          "BackoffRate": 1.5
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure",
          "ResultPath": "$.error"
        }
      ],
      "ResultPath": "$.eod_poll",
      "Next": "CheckEODStatus"
    },
    "CheckEODStatus": {
      "Type": "Choice",
      "Comment": "Default routes to HandleFailure so any unexpected SSM status (Failed, Cancelled, TimedOut, ...) is surfaced, not silently swallowed.",
      "Choices": [
        { "Variable": "$.eod_poll.Status", "StringEquals": "Success", "Next": "StopTradingInstance" },
        { "Variable": "$.eod_poll.Status", "StringEquals": "InProgress", "Next": "EODWait" },
        { "Variable": "$.eod_poll.Status", "StringEquals": "Pending", "Next": "EODWait" },
        { "Variable": "$.eod_poll.Status", "StringEquals": "Delayed", "Next": "EODWait" }
      ],
      "Default": "EODStatusError"
    },
    "EODStatusError": {
      "Type": "Pass",
      "Comment": "Choice Default path — synthesize $.error from poll result so HandleFailure's JsonToString($.error) has something to stringify.",
      "Parameters": {
        "source": "eod_status",
        "status.$": "$.eod_poll.Status",
        "status_details.$": "$.eod_poll.StatusDetails",
        "response_code.$": "$.eod_poll.ResponseCode",
        "command_id.$": "$.eod_poll.CommandId",
        "instance_id.$": "$.eod_poll.InstanceId"
      },
      "ResultPath": "$.error",
      "Next": "HandleFailure"
    },
    "EODWait": {
      "Type": "Wait",
      "Seconds": 10,
      "Next": "WaitForEOD"
    },
    "StopTradingInstance": {
      "Type": "Task",
      "Comment": "Stop trading EC2 instance after EOD completes",
      "Resource": "arn:aws:states:::aws-sdk:ec2:stopInstances",
      "Parameters": {
        "InstanceIds.$": "$.trading_instance_id"
      },
      "Retry": [
        {
          "ErrorEquals": ["States.TaskFailed"],
          "MaxAttempts": 2,
          "IntervalSeconds": 30,
          "BackoffRate": 1.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure",
          "ResultPath": "$.error"
        }
      ],
      "ResultPath": "$.stop_result",
      "End": true
    },
    "HandleFailure": {
      "Type": "Task",
      "Comment": "Failure alert via SNS — instance still stops to avoid cost",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn.$": "$.sns_topic_arn",
        "Subject": "Alpha Engine EOD Pipeline — FAILED",
        "Message.$": "States.Format('EOD pipeline failed. Error: {}', States.JsonToString($.error))"
      },
      "ResultPath": "$.failure_notify",
      "Next": "ForceStopInstance"
    },
    "ForceStopInstance": {
      "Type": "Task",
      "Comment": "Always stop trading instance even on failure — avoid cost overrun",
      "Resource": "arn:aws:states:::aws-sdk:ec2:stopInstances",
      "Parameters": {
        "InstanceIds.$": "$.trading_instance_id"
      },
      "ResultPath": "$.force_stop_result",
      "Next": "FailExecution"
    },
    "FailExecution": {
      "Type": "Fail",
      "Error": "EODPipelineFailure",
      "Cause": "EOD pipeline failed — trading instance stopped, check logs."
    }
  }
}
JSON
)

# Validate JSON before sending it to AWS.
echo "$DEFN" | python3 -c "import json,sys; json.loads(sys.stdin.read()); print('  Definition: JSON valid')"

aws stepfunctions update-state-machine \
    --state-machine-arn "$SM_ARN" \
    --definition "$DEFN" \
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
