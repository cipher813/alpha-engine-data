"""alpha-engine-weekly-run-scope — the weekly pipeline's own scope, derived.

Tracked as ``alpha-engine-config-I7620``.

**What it answers.** Which stages did THIS run actually dispatch, and which did
an operator flag switch off? Nothing in the fleet has ever recorded that. The
Director grades the week's numbers without knowing which producers were
disabled, so on 2026-08-14 it reported the deliberate absence of ``pit_parity``
(``skip_parity: true``, set on the Saturday EventBridge target on 2026-08-13 by
a recorded ruling) as ``contamination: UNKNOWN — the producer never ran this
cycle``, and withheld ``issue_filing`` and ``loop_verification`` on the strength
of it. A deliberately-off stage and a dead stage were indistinguishable on the
page.

**Why it derives instead of reading a registry.** ``private-docs/`` already
carries thirteen registries. Every one exists because its fact had no
machine-readable home; a fourteenth listing enabled stages would be a COPY of a
fact that has two authoritative homes already — the state-machine definition
(what stages exist, what flag gates each) and the execution history (which
branch every gate took). A hand-kept copy drifts the first time somebody adds a
stage, which is the failure mode the other thirteen were built to prevent. So:
one flag flip in the CFN preset changes the pipeline, this artifact, and the
Director's purview together, because all three read the same two sources.

**Where it runs.** As the SF state ``RunScope``, immediately before
``ReportCard``, so every work stage is already in the history it reads. Its own
row, ``ReportCard``'s and ``Director``'s are the three the history cannot yet
contain; they resolve from the run's input flags, and each row records which of
the two sources decided it (``source``).

**Failure posture — deliberately fail-open, and only here.** A scope block that
could not be built must not kill a run that produced real trading artifacts, so
the handler catches, publishes nothing, and returns a block whose every stage is
``NOT_REACHED``. That degradation is SAFE because ``NOT_REACHED`` is never
graded and never reads as disabled: the consumer's denominator collapses to
zero and the report card says so out loud, rather than rendering a narrow green
page. The one thing this must never do is emit a scope that looks complete.
"""
from __future__ import annotations

import json
import logging
import os

import boto3

from run_scope import build_run_scope

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("RESEARCH_BUCKET", "alpha-engine-research")
KEY_TEMPLATE = "backtest/{run_date}/run_scope.json"

#: Every ``skip_*`` key the SF understands is threaded in by the caller. Read
#: only to EXPLAIN a NOT_REACHED row, never to decide a disposition — the
#: execution record decides, the input merely says what was asked for.
_FLAG_PREFIX = "skip_"


def _history(client, execution_arn: str) -> list[dict]:
    """The execution's own event history, paginated.

    ``includeExecutionData=False``: the derivation reads state names and the
    event chain only, and the payloads are large enough to matter at 1000+
    events.
    """
    events: list[dict] = []
    paginator = client.get_paginator("get_execution_history")
    for page in paginator.paginate(
        executionArn=execution_arn,
        includeExecutionData=False,
        reverseOrder=False,
    ):
        events.extend(page.get("events", []))
    return events


def _definition(client, state_machine_arn: str) -> dict:
    described = client.describe_state_machine(stateMachineArn=state_machine_arn)
    return json.loads(described["definition"])


def handler(event, _context):
    run_date = event.get("run_date") or ""
    execution_arn = event.get("execution_arn") or ""
    state_machine_arn = event.get("state_machine_arn") or ""
    dry_run = bool(event.get("dry_run"))
    flags = {
        key: value for key, value in (event.get("execution_input") or {}).items()
        if key.startswith(_FLAG_PREFIX)
    }

    try:
        states = boto3.client("stepfunctions")
        definition = _definition(states, state_machine_arn)
        history = _history(states, execution_arn)
        scope = build_run_scope(
            definition,
            history,
            run_date=run_date,
            execution_arn=execution_arn,
            state_machine_arn=state_machine_arn,
            input_flags=flags,
        )
    except Exception as exc:  # noqa: BLE001
        # Fail-open, and loudly. See the module docstring: the degraded block
        # grades NOTHING, so a consumer reading it cannot mistake this for a
        # narrow-but-clean run. Raising here would terminate a weekly execution
        # over an advisory artifact.
        logger.exception("run-scope derivation failed — emitting an empty scope")
        scope = build_run_scope(
            {}, [], run_date=run_date, execution_arn=execution_arn,
            state_machine_arn=state_machine_arn,
        )
        scope["degraded"] = True
        scope["degraded_reason"] = f"{type(exc).__name__}: {exc}"
        scope["statement"] = (
            "SCOPE UNAVAILABLE — the run's own scope could not be derived, so "
            "NOTHING on this cycle is established as having been dispatched. "
            "No stage is graded. This is not a narrow run; it is an unmeasured "
            f"one. Cause: {type(exc).__name__}."
        )

    logger.info("run scope: %s", scope["statement"])

    if dry_run:
        # The Friday-PM shell run exercises the whole read+derive path (client
        # construction, the two API grants, the derivation) and writes nothing —
        # the same dry contract every advisory producer on this pipeline honours.
        scope["dry_run"] = True
        return scope

    key = KEY_TEMPLATE.format(run_date=run_date)
    boto3.client("s3").put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(scope, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("wrote s3://%s/%s", BUCKET, key)
    return scope
