"""alpha-engine-weekly-coverage-sweep — the caller the stage-coverage sweep never had.

Tracked as ``alpha-engine-config-I8214`` (carrying ``I8154`` deliverable 4 and
``I8186``'s state-machine half).

**What it answers.** Did this weekly CYCLE actually cover the stages it
declares, and does the completion marker say so? ``nousergon_lib.pipeline_status``
shipped the reader — the cycle's real shape, the union over its contributing
executions, the coverage verdict against the artifact registry — and nothing
called it on a schedule, so it detected nothing. This handler is its caller.

**Why the marker needs it.** On 2026-08-22 the marker at
``_sf_completion/ne-weekly-freshness-pipeline/2026-08-22.json`` was written by
``watch-rerun-2026-08-22-3``, an execution that entered **1 of 16** declared
spine stages. The marker's name is a claim the run could not support: the SF
writes it on reaching its own terminal, which is a narrower fact than "the
cycle completed". So the SF now stamps ``claim: sf_execution_terminal`` and
``cycle_verdict: unknown`` on the object, and this handler AUGMENTS it with the
cycle's real shape — which executions contributed, what each entered, what the
union adds up to. A consumer reading the marker before this runs resolves to
UNKNOWN, never to an implied pass (``sf-pipeline-policy.md`` §2.3a).

**Why a Lambda and not an SSM command on the run's box.** The weekly SF has no
always-on instance: ``$.ec2_instance_id`` is an ephemeral spot, and since
``alpha-engine-config-I8162`` a recovery run whose every box stage is skipped
carries **no instance id at all** — which is exactly the shape of the run that
most needs a coverage verdict. A sweep that dereferences the instance id would
throw ``States.Runtime`` on those runs, one state after the pipeline's own
success terminal. The sweep reads Step Functions and S3 and writes S3 and one
metric; it has no reason to need a box. Mirrors ``weekly-run-scope``, the
sibling that reads the same execution history for the same pipeline.

**Failure posture — fail-open, and never silent.** Three outcomes, and the
third is the one that must not be collapsed into either of the others:

- ``clean`` — the sweep ran and found no gap.
- ``findings`` — the sweep ran and found a gap. It has already paged from
  inside ``nousergon_lib`` (``krepis.alerts``, deduped per pipeline+run_date).
- ``unavailable`` — **the sweep did not run.** "Found nothing" and "did not
  run" are different facts and only the second means the surface is unobserved
  (``principles.md`` §2.7). This handler returns it as its own outcome so the
  SF can page for it, because a sweep that never ran cannot page for itself.

It never raises: this state sits DOWNSTREAM of the pipeline's real success
terminal, and an observe-only tail that fails a completed run is a worse defect
than the one it was added to detect (``sf-pipeline-policy.md`` §2.1 blast
radius). The outcome is carried in the return value instead, where the SF's
Choice reads it.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET = os.environ.get("RESEARCH_BUCKET", "alpha-engine-research")
PIPELINE = os.environ.get("PIPELINE", "ne-weekly-freshness-pipeline")

#: The three outcomes, closed. The SF's Choice matches these literals; adding a
#: fourth without a matching Choice branch lands on the Default, which is
#: ``unavailable`` — the honest fall-through, never ``clean``.
OUTCOME_CLEAN = "clean"
OUTCOME_FINDINGS = "findings"
OUTCOME_UNAVAILABLE = "unavailable"


def handler(event, _context):
    run_date = (event or {}).get("run_date") or ""
    state_machine_arn = (event or {}).get("state_machine_arn") or ""
    dry_run = bool((event or {}).get("dry_run"))

    if not run_date:
        # No cycle key means no cycle to sweep. Unobserved, not clean.
        return {
            "outcome": OUTCOME_UNAVAILABLE,
            "reason": "no run_date supplied — there is no cycle key to sweep",
            "run_date": run_date,
        }

    try:
        import boto3
        from krepis.aws_region import resolve_region
        from nousergon_lib.pipeline_status.completion_marker import augment_marker
        from nousergon_lib.pipeline_status.coverage import (
            publish_sweep,
            read_coverage_sweep,
        )

        region = resolve_region()
        s3_client = boto3.client("s3", region_name=region)
        sweep = read_coverage_sweep(
            pipeline=PIPELINE,
            run_date=run_date,
            state_machine_arn=state_machine_arn or None,
            bucket=BUCKET,
            s3_client=s3_client,
        )
    except Exception as exc:  # noqa: BLE001 — a sweep that cannot run says so
        # Deliberate broad catch with a named recording surface: the return
        # value below IS the recording surface, and the SF pages on it. The
        # failure class swallowed is "the sweep could not be performed"; the
        # primary deliverable (the weekly run, already complete) is untouched.
        logger.exception("coverage sweep could not run for %s", run_date)
        return {
            "outcome": OUTCOME_UNAVAILABLE,
            "reason": f"{type(exc).__name__}: {exc}",
            "run_date": run_date,
        }

    explanation = sweep.explain()
    logger.info("coverage sweep %s: %s", run_date, explanation)

    if dry_run:
        # The Friday-PM shell run exercises the whole read path — client
        # construction, every IAM grant the real run needs, the derivation —
        # and writes nothing. The same dry contract every advisory producer on
        # this pipeline honours.
        return {
            "outcome": OUTCOME_CLEAN,
            "dry_run": True,
            "run_date": run_date,
            "explanation": explanation,
        }

    published = False
    augmented = False
    write_error: str | None = None
    try:
        import boto3

        publish_sweep(
            sweep,
            s3_client=s3_client,
            cloudwatch_client=boto3.client("cloudwatch", region_name=region),
            bucket=BUCKET,
        )
        published = True
        if sweep.cycle is not None:
            augment_marker(sweep.cycle, s3_client=s3_client, bucket=BUCKET)
            augmented = True
        else:
            logger.warning(
                "the cycle could not be read, so the marker keeps its bare "
                "envelope claim — a marker with no cycle block resolves to "
                "UNKNOWN, never to a pass"
            )
    except Exception as exc:  # noqa: BLE001
        # The sweep RAN; only its write failed. That is still an unobserved
        # surface for every downstream reader of the artifact and the marker,
        # so it reports as unavailable rather than as a clean sweep whose
        # result nobody can read.
        logger.exception("coverage sweep ran but could not publish for %s", run_date)
        write_error = f"{type(exc).__name__}: {exc}"

    if write_error is not None:
        return {
            "outcome": OUTCOME_UNAVAILABLE,
            "reason": f"the sweep ran but could not publish its result: {write_error}",
            "run_date": run_date,
            "explanation": explanation,
        }

    if sweep.should_alert:
        try:
            from krepis import alerts

            alerts.publish(
                explanation,
                severity="error",
                source=f"stage-coverage-sweep/{PIPELINE}",
                dedup_key=f"stage-coverage-sweep/{PIPELINE}/{run_date}",
            )
        except Exception:  # noqa: BLE001
            # A failed page must not turn a real finding into a clean result.
            # The outcome below still says findings, and the SF records it.
            logger.exception("coverage sweep finding could not be paged")

    return {
        "outcome": OUTCOME_FINDINGS if sweep.should_alert else OUTCOME_CLEAN,
        "run_date": run_date,
        "explanation": explanation,
        "published": published,
        "marker_augmented": augmented,
    }
