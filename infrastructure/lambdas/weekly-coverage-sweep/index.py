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

**Failure posture — fail-open, and never silent.** FOUR outcomes, and the
last two are the ones that must not be collapsed into either of the first:

- ``clean`` — the sweep ran and found no gap.
- ``findings`` — the sweep ran and found a gap. It has already paged from
  inside ``nousergon_lib`` (``krepis.alerts``, deduped per pipeline+run_date).
- ``deferred`` — **the sweep ran and the cycle could not support the claim.**
  ``alpha-engine-config-I10170``. ``absent`` means "expected, entered, and
  recorded nothing", which only a cycle that has stopped changing can support;
  when the cycle is still in flight, or its contributor walk was truncated on
  a non-COMPLETED verdict, the sweep WITHHOLDS the absent count rather than
  asserting it. Withholding is not silence: the sweep still pages, naming
  every would-be-absent stage and the re-sweep command, and
  ``publish_sweep`` emits ``StageCoverageSweepDeferred=1`` on its own metric
  rather than letting the deferral be inferred from another metric's silence.
- ``unavailable`` — **the sweep did not run.** "Found nothing", "could not
  establish" and "did not run" are three different facts and only the last
  means the reader itself is dead (``principles.md`` §2.7). This handler
  returns it as its own outcome so the SF can page for it, because a sweep
  that never ran cannot page for itself.

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

#: The four outcomes, closed. The SF's Choice matches these literals; adding a
#: fifth without a matching Choice branch lands on the Default, which is
#: ``unavailable`` — the honest fall-through, never ``clean``.
OUTCOME_CLEAN = "clean"
OUTCOME_FINDINGS = "findings"
OUTCOME_UNAVAILABLE = "unavailable"
#: The sweep RAN, and the cycle could not support the claim it was asked to
#: make (``alpha-engine-config-I10170``). Distinct from all three above: it is
#: not clean, it is not a finding about a stage, and the sweep did not fail.
#: The SF Choice has a branch for it; without one it lands on the Default,
#: which is ``unavailable`` — still honest, just less precise.
OUTCOME_DEFERRED = "deferred"

#: The pipeline state this handler IS. It is executing while it grades, so it
#: can never have written a verdict about its own completion — reporting it
#: ``absent`` is a false positive guaranteed on every run, and it was one of
#: the 13 on 2026-09-04 (``alpha-engine-config-I10170``).
SWEEP_STAGE = os.environ.get("SWEEP_STAGE", "WeeklyCoverageSweep")


def _outcome_for(sweep) -> str:
    """The handler's verdict. ``deferred`` OUTRANKS ``findings``.

    ``alpha-engine-config-I10170``. When the cycle cannot support an absence
    claim, the honest headline is that coverage is not established — not that
    N stages are absent, which is the assertion the sweep just declined to
    make. Findings that DID land are still real and still page from inside
    ``nousergon_lib``; they are reported in the explanation either way.
    """
    if sweep.deferred:
        return OUTCOME_DEFERRED
    return OUTCOME_FINDINGS if sweep.should_alert else OUTCOME_CLEAN


def handler(event, _context):
    run_date = (event or {}).get("run_date") or ""
    # alpha-engine-config-I8809: the LEGACY partition. The sweep unions the
    # trading-day family (run_date) with the calendar family until the
    # 2026-09-05 cutover, so one cycle split across both reads as one cycle.
    # Absent => a single-partition sweep, which is the post-cutover shape.
    calendar_date = (event or {}).get("calendar_date") or ""
    state_machine_arn = (event or {}).get("state_machine_arn") or ""
    # alpha-engine-config-I10170: THE OBSERVER. This handler runs as a state
    # INSIDE the execution it is grading, so that execution is RUNNING at the
    # instant it grades — on every run, forever. Measured on the live
    # 2026-09-04 cycle: the sweep at 21:39:18Z declared the cycle in_flight
    # and paged 13 absences in the same artifact; watch-rerun-2026-09-04-4,
    # the execution it called RUNNING, stopped at 21:39:19Z, two hops later.
    # Naming the observer lets nousergon_lib count its entered states (real
    # work) without letting its status make its own cycle non-terminal.
    observer_execution_arn = (event or {}).get("observer_execution_arn") or ""
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
            calendar_date=calendar_date or None,
            state_machine_arn=state_machine_arn or None,
            bucket=BUCKET,
            s3_client=s3_client,
            observer_execution_arn=observer_execution_arn or None,
            observer_stage=SWEEP_STAGE,
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
        #
        # The outcome is the REAL one, not a hardcoded clean. Measured
        # 2026-08-22 on the first live dry invocation: the sweep found 28
        # absent verdicts and 1 finding, and this branch returned
        # ``outcome: clean`` anyway — a rehearsal that reports green whatever
        # it saw certifies nothing, and is the same "no data rendered as
        # healthy" defect (principles.md §2.7) the whole sweep exists to
        # detect. What ``dry_run`` withholds is the WRITES and the page, never
        # the verdict.
        return {
            "outcome": _outcome_for(sweep),
            "dry_run": True,
            "run_date": run_date,
            "partitions_read": list(sweep.partitions_read),
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
            # Both partitions the state machine dual-wrote get the cycle
            # verdict, or a consumer on the legacy family reads UNKNOWN beside
            # a known verdict (alpha-engine-config-I8809).
            augment_marker(
                sweep.cycle,
                s3_client=s3_client,
                bucket=BUCKET,
                also_dates=sweep.partitions_read,
            )
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
        "outcome": _outcome_for(sweep),
        "run_date": run_date,
        "coverage_established": sweep.coverage_established,
        "deferral_reason": sweep.deferral_reason,
        "partitions_read": list(sweep.partitions_read),
        "legacy_partition_rows": sweep.legacy_partition_rows,
        "explanation": explanation,
        "published": published,
        "marker_augmented": augmented,
    }
