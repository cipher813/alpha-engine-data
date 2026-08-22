"""
Lambda handler for the WeeklyPreflight pre-spend gate —
invoked by `ne-weekly-freshness-pipeline` before AcquireMutex.

Runs the sf_preflight checks against live AWS state and returns a pass/fail
verdict. Failures halt the pipeline; a pass proceeds to the mutex + spot
launch.

The Lambda runs read-only API calls (IAM SimulatePrincipalPolicy, Lambda
GetFunctionConfiguration, CloudWatch GetMetricStatistics, Step Functions
DescribeStateMachine) and zero-cost static analysis.

Check selection is by CAPABILITY, not by hope. This Lambda declares
``sf_preflight.LAMBDA_CAPABILITIES`` (the AWS control plane and nothing
else); checks needing ArcticDB, the repo's collector modules, a Polygon key,
or sibling checkouts on local disk are reported status="skip" and excluded
from the failure count. Before 2026-08-10 this docstring claimed those checks
"gracefully skip" while the handler in fact ran the FULL profile — they
returned status="fail" (ModuleNotFoundError / "not checked out as sibling"),
which would have halted the Saturday pipeline by construction.

Usage:
    Invoked by Step Functions. Returns:
    {"status": "OK", "has_violation": false}              → proceed
    {"status": "FAIL", "has_violation": true, ...}        → halt via ExtractWeeklyPreflightError
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from dataclasses import asdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Lambda-specific bucket override (not the same as the data-plane bucket,
# since this Lambda reads the SF definition, not the research data).
_SF_DEFINITION_BUCKET = os.environ.get("SF_DEFINITION_BUCKET", "alpha-engine-research")
_WEEKLY_SF_NAME = os.environ.get("WEEKLY_SF_NAME", "ne-weekly-freshness-pipeline")


def handler(event: dict, context) -> dict:
    """
    AWS Lambda handler for the weekly preflight.

    Event payload (from Step Functions):
        bucket: str (optional, S3 bucket override)
        sf_name: str (optional, state machine name override)

    Returns:
        dict with status, has_violation, and detailed results.
    """
    # Captured at handler ENTRY: the stage-coverage window must predate any
    # write this invocation makes, or it would be trivially satisfied by it
    # (alpha-engine-config-I7214).
    started = datetime.now(timezone.utc)
    bucket = event.get("bucket", _SF_DEFINITION_BUCKET)
    sf_name = event.get("sf_name", _WEEKLY_SF_NAME)

    try:
        # Import sf_preflight — all heavy imports are deferred inside
        # individual check functions, so this never fails at module level.
        import sf_preflight as sfp  # type: ignore[import-untyped]
    except ImportError as exc:
        return {
            "status": "ERROR",
            "has_violation": True,
            "error": f"sf_preflight import failed: {exc}",
            "traceback": traceback.format_exc(),
        }

    # alpha-engine-config-I7443: the SF Task passes no explicit Payload, so
    # `event` IS the whole state input — run_date and every skip_* flag are
    # already here. Forwarding them lets check_skip_flag_artifact_coherence
    # assert that each skip CLAIM is backed by a real artifact for this
    # run_date, before AcquireMutex and before any spot dispatch. Absent
    # (a bare {} test invoke) => that check reports "nothing claimed", never
    # a failure: this gate must not start halting the pipeline over a payload
    # shape it previously ignored.
    run_date = event.get("run_date")
    skip_flags = {k: v for k, v in event.items() if k.startswith("skip_")}

    try:
        n_fail, results = sfp.run_preflight(
            bucket=bucket,
            capabilities=sfp.LAMBDA_CAPABILITIES,
            run_date=run_date,
            skip_flags=skip_flags,
        )
    except Exception as exc:
        return {
            "status": "ERROR",
            "has_violation": True,
            "error": f"run_preflight raised: {exc}",
            "traceback": traceback.format_exc(),
        }

    result_dicts = [asdict(r) for r in results]
    fail_results = [r for r in result_dicts if r.get("status") == "fail"]
    warn_results = [r for r in result_dicts if r.get("status") == "warn"]
    skip_results = [r for r in result_dicts if r.get("status") == "skip"]
    ran_count = len(result_dicts) - len(skip_results)

    if n_fail > 0:
        return {
            "status": "FAIL",
            "has_violation": True,
            "fail_count": n_fail,
            "warn_count": len(warn_results),
            "skip_count": len(skip_results),
            "ran_count": ran_count,
            "failures": [r["name"] for r in fail_results],
            "results": result_dicts,
        }

    # A gate that ran ZERO checks is not a pass — it is an unobserved
    # system reporting green (principles.md §2.7). Fail closed: this can
    # only happen if CHECKS or CHECK_CAPABILITIES drifted such that no
    # check is eligible under LAMBDA_CAPABILITIES.
    if ran_count == 0:
        return {
            "status": "ERROR",
            "has_violation": True,
            "error": (
                "preflight ran 0 checks — every check was skipped for missing "
                "capabilities; the gate observed nothing"
            ),
            "skip_count": len(skip_results),
            "results": result_dicts,
        }

    return {
        "status": "OK",
        "has_violation": False,
        "warn_count": len(warn_results),
        "skip_count": len(skip_results),
        "ran_count": ran_count,
        "results": result_dicts,
        "stage_coverage": _assert_stage_coverage("WeeklyPreflight", started, run_date),
    }


def _assert_stage_coverage(stage: str, started: datetime, run_date: str | None) -> dict:
    """Record this stage's own output verdict (alpha-engine-config-I7214).

    `WeeklyPreflight` is an INFRASTRUCTURE/GATE stage: it positively declares
    in `ARTIFACT_REGISTRY.yaml`'s `pipeline_stages:` that it writes no durable
    artifact, so the verdict is `COVERED_NO_OUTPUT`. It asserts nothing and
    still RECORDS that it declared nothing — "declares nothing" and "was never
    considered" must not be the same absence, which is the whole point of that
    registry section.

    Never alters the handler's outcome: an observer that can change the stage
    it observes is a new failure mode bolted onto the one it reports. The
    ImportError branch is loud rather than silent because the nousergon-lib
    pin may predate the module, and an inert assertion must be distinguishable
    from a covered stage.

    ``run_date`` comes from the state input (``$.run_date`` — the SF Task
    passes ``Payload.$="$"`` so ``event`` already carries it, alpha-engine-
    config-I8155). Never fabricated: a missing/blank run_date is reported
    UNMEASURED rather than defaulting to any derived date, because a stage
    that invents its own timestamp is exactly the defect this mechanism
    exists to catch (alpha-engine-config-I8155 — EXECUTION_RUN_DATE is
    deliberately the ONLY carrier, never $RUN_DATE, which other launchers in
    the fleet reassign to the trading day).
    """
    if not run_date:
        print(f"ERROR: stage-coverage assertion has no run_date for {stage} — event carried none")
        return {"stage": stage, "status": "UNMEASURED", "reason": "no run_date on state input"}
    try:
        from krepis.stage_coverage import assert_stage_coverage
    except ImportError as exc:
        print(f"ERROR: stage-coverage assertion unavailable for {stage}: {exc}")
        return {"stage": stage, "status": "UNMEASURED", "reason": str(exc)}
    return assert_stage_coverage(stage, window_start=started, run_date=run_date)
