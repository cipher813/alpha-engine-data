"""SNS-to-S3 mirror for the system-wide changelog.

Subscribed to arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts.
For every SNS message, writes one JSON entry under
s3://alpha-engine-research/changelog/incidents/{YYYY}/{MM}/
{DD}T{HH-MM-SS}_{topic}_{hash7}.json.

This is the "incident" half of the system-wide event-mining changelog
(deploys are written by the alpha-engine-docs append-changelog
composite action; manual + recovery entries by the changelog-log CLI).

Managed outside CloudFormation — see ../../README.md and the sibling
deploy.sh in this directory. The decision to orphan from CF (rather
than keep it in the alpha-engine-orchestration stack) was made
2026-05-01 to avoid a perm cascade on the github-actions-lambda-deploy
OIDC role; trade-off + reconsideration triggers documented in the
private/ROADMAP.md "Observability" section.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from hashlib import sha1

import boto3

_S3 = boto3.client("s3")
_BUCKET = os.environ["CHANGELOG_BUCKET"]
_PREFIX = os.environ.get("CHANGELOG_PREFIX", "changelog/incidents")


def handler(event, context):
    wrote = 0
    for record in event.get("Records", []):
        sns = record.get("Sns", {})
        message = sns.get("Message", "") or ""
        subject = sns.get("Subject", "") or ""
        topic_arn = sns.get("TopicArn", "") or ""
        message_id = sns.get("MessageId", "") or ""
        ts_iso = sns.get("Timestamp") or datetime.now(timezone.utc).isoformat()

        ts_iso_clean = ts_iso.replace("Z", "+00:00")
        try:
            ts = datetime.fromisoformat(ts_iso_clean)
        except ValueError:
            ts = datetime.now(timezone.utc)

        ts_utc = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        ts_key = ts.strftime("%Y/%m/%dT%H-%M-%S")

        topic_name = topic_arn.split(":")[-1] if topic_arn else "sns"
        summary_src = subject or (message.splitlines()[0] if message else "(empty)")
        summary = summary_src[:240]

        hash_id = sha1(message_id.encode()).hexdigest()[:7] if message_id else "0000000"

        entry = {
            "ts_utc": ts_utc,
            "event_type": "incident",
            "source": topic_name,
            "subject": subject,
            "summary": summary,
            "details": message,
            "sns_message_id": message_id,
            "topic_arn": topic_arn,
        }

        key = f"{_PREFIX}/{ts_key}_{topic_name}_{hash_id}.json"
        _S3.put_object(
            Bucket=_BUCKET,
            Key=key,
            Body=json.dumps(entry).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Wrote s3://{_BUCKET}/{key} subject={subject[:80]!r}")
        wrote += 1

    return {"statusCode": 200, "wrote": wrote}
