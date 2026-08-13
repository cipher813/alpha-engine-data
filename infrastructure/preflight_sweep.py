"""Nightly all-stage ``--preflight-only`` sweep for the weekly freshness
pipeline — the driver.

Runs ON the shared launcher box (the same box, built by the same bootstrap,
that ``ne-weekly-freshness-pipeline`` itself runs its stages from). For every
stage the definition declares preflight-capable it runs that stage's OWN
command with ``--preflight-only``, INDEPENDENTLY, continuing past failures,
and reports the whole per-stage matrix in one pass.

WHAT MAKES THIS DIFFERENT FROM THE FRIDAY SHELL-RUN
---------------------------------------------------
``shell_run=true`` already threads ``--preflight-only`` into every stage — but
through the SF, which is a fail-fast chain. The first failing stage aborts the
execution, so a shell-run yields exactly ONE root cause per run. Sixteen
consecutive weekly executions since 2026-08-10 failed with sixteen DIFFERENT
root causes, and 2026-08-10 alone burned nine reruns converging on none of
them. Serial discovery of independent defects is the pathology; a
non-short-circuiting fan-out is the fix, and the SF cannot be that without a
``complexity:ultra`` topology change (``sf-pipeline-policy`` §5).

HONEST DEGRADATION (``sf-pipeline-policy`` §2.3, ``principles.md`` §2.7)
-----------------------------------------------------------------------
Three outcomes are kept distinct at every level, and no data is never rendered
as a pass:

* ``passed``      — the stage's preflight ran and exited 0.
* ``failed``      — the stage's preflight ran and exited non-zero. Real finding.
* ``unmeasured``  — the sweep could not run it, or could not read the result.
                    NOT a pass and NOT a defect. The verdict vocabulary mirrors
                    ``validators/stage_output_sweep.py`` (I7167) deliberately
                    rather than inventing a parallel one.
* ``unsweepable`` — the stage declares a dry path the sweep cannot exercise
                    (launcher missing, flag unimplemented, command unrenderable).
                    A coverage defect in its own right; fails the run.
* ``not_attempted`` — the run ended before reaching this stage.

A run that could not measure anything terminates ``failed`` with
``measured: false`` and a reason. It never writes the clean-run pointer.

The success-claim asymmetry (``observability-policy`` §3.1) is explicit here:
every run writes its full report, but only an all-``passed`` run advances
``_preflight_sweep/last_clean.json``. The failure path persists the work and
the cause of death; it never advances the artifact a detector reads as proof.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import subprocess
import sys
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from infrastructure.preflight_sweep_stages import (  # noqa: E402
    NO_DRY_PATH,
    SWEEPABLE,
    UNSWEEPABLE,
    Stage,
    apply_map_bindings,
    derive_required_map_bindings,
    derive_shell_run_bindings,
    derive_stages,
    load_manifest,
    manifest_disagreement,
    map_binding_disagreement,
)
from infrastructure.preflight_sweep_console import envelopes, load_cadence  # noqa: E402

# ── Verdict vocabulary (closed; mirrors validators/stage_output_sweep.py) ────
PASSED = "passed"
FAILED = "failed"
UNMEASURED = "unmeasured"
UNSWEEPABLE_VERDICT = "unsweepable"
NOT_ATTEMPTED = "not_attempted"

# Run-level outcome, from observability-policy §3.1's closed vocabulary.
OUTCOME_OK = "ok"
OUTCOME_DEGRADED = "degraded"
OUTCOME_FAILED = "failed"

COMPONENT_ID = "ae-preflight-sweep"
DEFAULT_BUCKET = os.environ.get("PREFLIGHT_SWEEP_BUCKET", "alpha-engine-research")
REPORT_PREFIX = "_preflight_sweep"
CW_NAMESPACE = "AlphaEngine"
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
DEFAULT_SNS_TOPIC = os.environ.get(
    "PREFLIGHT_SWEEP_SNS_TOPIC",
    "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
)
# Bounded fan-out: each stage's --preflight-only launches its OWN nested spot,
# so concurrency is a spot-API and capacity-pool decision, not a CPU one. Six
# keeps RunInstances well inside throttle limits and spreads across the six
# subnets the launchers rotate through.
DEFAULT_CONCURRENCY = int(os.environ.get("PREFLIGHT_SWEEP_CONCURRENCY", "6"))
# Per-stage ceiling. A --preflight-only run is boot + deps + smoke: minutes.
# 30 min is generous headroom over the observed worst case without letting a
# hung stage hold the whole sweep to its own timeout.
DEFAULT_STAGE_TIMEOUT = int(os.environ.get("PREFLIGHT_SWEEP_STAGE_TIMEOUT", "1800"))


@dataclass
class StageResult:
    stage: str
    verdict: str
    repo: str | None = None
    launcher: str | None = None
    returncode: int | None = None
    duration_seconds: float | None = None
    reason: str | None = None
    last_stderr_line: str | None = None
    log_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SweepReport:
    component_id: str
    run_id: str
    started_at: str
    finished_at: str | None = None
    outcome: str = OUTCOME_FAILED
    measured: bool = False
    unmeasured_reason: str | None = None
    definition_sha: str | None = None
    checkout_root: str | None = None
    stages_declared: int = 0
    stages_swept: int = 0
    stages_passed: int = 0
    stages_failed: int = 0
    stages_unmeasured: int = 0
    stages_unsweepable: int = 0
    stages_no_dry_path: int = 0
    coverage_findings: list[str] = field(default_factory=list)
    results: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── AWS side-effects, isolated so the driver is unit-testable ────────────────


class AwsSurface:
    """Every outward effect the sweep has. Injected so the driver can be
    tested without touching S3, CloudWatch or SNS."""

    def __init__(self, region: str = DEFAULT_REGION, bucket: str = DEFAULT_BUCKET):
        self.region = region
        self.bucket = bucket
        self._s3 = None
        self._cw = None
        self._sns = None

    def _client(self, name):
        import boto3

        return boto3.client(name, region_name=self.region)

    def put_json(self, key: str, payload: dict) -> None:
        if self._s3 is None:
            self._s3 = self._client("s3")
        self._s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )

    def put_text(self, key: str, body: str) -> None:
        if self._s3 is None:
            self._s3 = self._client("s3")
        self._s3.put_object(
            Bucket=self.bucket, Key=key, Body=body.encode(errors="replace"),
            ContentType="text/plain",
        )

    def put_metrics(self, metrics: list[tuple[str, float]]) -> None:
        if self._cw is None:
            self._cw = self._client("cloudwatch")
        self._cw.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    # Bounded dimension set only (observability-policy §4):
                    # the run id is high-cardinality and lives in the record,
                    # never in a metric dimension.
                    "Dimensions": [{"Name": "Component", "Value": COMPONENT_ID}],
                    "Value": float(value),
                    "Unit": "Count",
                }
                for name, value in metrics
            ],
        )

    def publish(self, topic_arn: str, subject: str, message: str) -> None:
        if self._sns is None:
            self._sns = self._client("sns")
        self._sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)


# ── Stage execution ──────────────────────────────────────────────────────────


def _stage_script(stage: Stage) -> str:
    """The shell body for one stage: exactly the commands the SF would send.

    Rendered from the definition, so the sweep runs the pipeline's own command
    rather than a re-implementation of it that can drift.
    """
    return "set -eo pipefail\n" + "\n".join(stage.commands) + "\n"


def run_stage(
    stage: Stage,
    checkout_root: str,
    timeout: int,
    runner=subprocess.run,
) -> StageResult:
    """Execute one stage's dry command. Never raises: a stage that could not be
    run is reported ``unmeasured``, which is neither a pass nor a defect."""
    script = _stage_script(stage)
    started = dt.datetime.now(dt.timezone.utc)
    try:
        proc = runner(
            ["bash", "-c", script],
            cwd=os.path.join(checkout_root, stage.box_dir or ""),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        # A timeout is a HARD FAIL, named as a resource kill in what the
        # operator reads — never retried on an unchanged workload, never
        # folded into a generic non-zero exit (Brian ruling 2026-08-13).
        return StageResult(
            stage=stage.name,
            verdict=FAILED,
            repo=stage.repo,
            launcher=stage.launcher,
            returncode=None,
            duration_seconds=(
                dt.datetime.now(dt.timezone.utc) - started
            ).total_seconds(),
            reason=(
                f"RESOURCE KILL — preflight exceeded the sweep's {timeout}s per-stage "
                "ceiling and was terminated. Not retried."
            ),
        )
    except OSError as exc:
        # The sweep could not start the process at all. That is a fact about
        # the sweep's own harness, not about the stage — reporting it as a
        # stage failure would be a detector reporting its harness fault as a
        # finding, always in the alarming direction.
        return StageResult(
            stage=stage.name,
            verdict=UNMEASURED,
            repo=stage.repo,
            launcher=stage.launcher,
            reason=f"sweep could not launch the stage command: {exc}",
        )

    duration = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    stderr_lines = [ln for ln in (proc.stderr or "").splitlines() if ln.strip()]
    last_err = stderr_lines[-1][:500] if stderr_lines else None
    verdict = PASSED if proc.returncode == 0 else FAILED
    reason = None
    if proc.returncode != 0:
        if proc.returncode == 137:
            reason = "RESOURCE KILL — rc=137 (OOM). Not retried."
        else:
            reason = f"preflight exited rc={proc.returncode}"
    return StageResult(
        stage=stage.name,
        verdict=verdict,
        repo=stage.repo,
        launcher=stage.launcher,
        returncode=proc.returncode,
        duration_seconds=duration,
        reason=reason,
        last_stderr_line=last_err,
    )


def _stage_log(stage: Stage, result: StageResult, script: str) -> str:
    return (
        f"stage: {stage.name}\nrepo: {stage.repo}\nlauncher: {stage.launcher}\n"
        f"verdict: {result.verdict}\nreturncode: {result.returncode}\n"
        f"reason: {result.reason}\n\n--- command ---\n{script}\n"
    )


# ── Report rendering ─────────────────────────────────────────────────────────


def render_notification(report: SweepReport) -> tuple[str, str]:
    """ONE notification for the whole run (observability-policy §7.2a: one per
    group failure, never one per member). Carries the full member list by name
    so the operator never has to open a console to learn which stages broke."""
    if not report.measured:
        subject = f"[preflight-sweep] COULD NOT MEASURE — {report.run_id}"
    elif report.outcome == OUTCOME_OK:
        subject = (
            f"[preflight-sweep] all {report.stages_passed} stages clean — {report.run_id}"
        )
    else:
        subject = (
            f"[preflight-sweep] {report.stages_failed} failed / "
            f"{report.stages_unsweepable} unsweepable / "
            f"{report.stages_unmeasured} unmeasured of {report.stages_declared} "
            f"— {report.run_id}"
        )

    lines = [
        f"component: {report.component_id}",
        f"run_id:    {report.run_id}",
        f"outcome:   {report.outcome}   measured: {report.measured}",
    ]
    if report.unmeasured_reason:
        lines.append(f"reason:    {report.unmeasured_reason}")
    lines += [
        "",
        f"declared {report.stages_declared} | swept {report.stages_swept} | "
        f"passed {report.stages_passed} | failed {report.stages_failed} | "
        f"unmeasured {report.stages_unmeasured} | unsweepable {report.stages_unsweepable} | "
        f"no-dry-path {report.stages_no_dry_path}",
        "",
    ]
    if report.coverage_findings:
        lines.append("COVERAGE FINDINGS (the sweep's own denominator is wrong):")
        lines += [f"  - {f}" for f in report.coverage_findings]
        lines.append("")
    for verdict, header in (
        (FAILED, "FAILED"),
        (UNSWEEPABLE_VERDICT, "UNSWEEPABLE"),
        (UNMEASURED, "UNMEASURED (not a pass, not a defect)"),
        (NOT_ATTEMPTED, "NOT ATTEMPTED"),
    ):
        members = [r for r in report.results if r["verdict"] == verdict]
        if members:
            lines.append(f"{header}:")
            for r in members:
                detail = r.get("reason") or ""
                err = r.get("last_stderr_line")
                lines.append(f"  - {r['stage']}: {detail}")
                if err:
                    lines.append(f"      last stderr: {err}")
            lines.append("")
    passed = [r["stage"] for r in report.results if r["verdict"] == PASSED]
    if passed:
        lines.append("PASSED: " + ", ".join(passed))
    lines.append("")
    lines.append(
        f"report: s3://{DEFAULT_BUCKET}/{REPORT_PREFIX}/{report.run_id}/report.json"
    )
    return subject, "\n".join(lines)


# ── Driver ───────────────────────────────────────────────────────────────────


def _definition_sha(path: Path) -> str | None:
    try:
        import hashlib

        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except OSError:
        # Not fatal to the report — the definition was already loaded by the
        # time this runs, so its absence here can only mean a concurrent
        # change. Recorded as null in the report rather than swallowed.
        return None


def sweep(
    definition_path: Path,
    manifest_path: Path,
    checkout_root: str,
    run_id: str,
    concurrency: int = DEFAULT_CONCURRENCY,
    stage_timeout: int = DEFAULT_STAGE_TIMEOUT,
    aws: AwsSurface | None = None,
    runner=subprocess.run,
) -> SweepReport:
    started = dt.datetime.now(dt.timezone.utc)
    report = SweepReport(
        component_id=COMPONENT_ID,
        run_id=run_id,
        started_at=started.isoformat(),
        checkout_root=checkout_root,
        definition_sha=_definition_sha(definition_path),
    )

    # ── Derivation. Any failure here means the sweep cannot know what it is
    # supposed to sweep — reported as UNMEASURED, never as a clean run.
    try:
        definition = json.loads(definition_path.read_text())
        manifest = load_manifest(manifest_path)
        base_bindings = derive_shell_run_bindings(definition)
        # run_date is computed by InitializeInput from the execution's own
        # start time (a States.Format, deliberately not read as a static
        # blob), so the RUNNER supplies it — exactly as the SF does. It is set
        # before the map-binding scan so it is not reported as an undeclared
        # Map-scoped variable.
        base_bindings.setdefault("run_date", started.date().isoformat())
        required_map = derive_required_map_bindings(definition, base_bindings)
        report.coverage_findings += map_binding_disagreement(required_map, manifest)
        bindings = apply_map_bindings(base_bindings, manifest)
        context = {"Execution": {"Name": run_id, "Id": f"preflight-sweep:{run_id}"}}
        stages = derive_stages(definition, bindings, context, checkout_root)
        report.coverage_findings += manifest_disagreement(stages, manifest)
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        report.finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        report.outcome = OUTCOME_FAILED
        report.measured = False
        report.unmeasured_reason = (
            f"stage derivation failed ({type(exc).__name__}: {exc}) — the sweep could "
            "not determine what it was supposed to sweep. This is NOT a clean run.\n"
            + traceback.format_exc(limit=6)
        )
        return report

    report.stages_declared = len(stages)
    report.stages_no_dry_path = sum(1 for s in stages if s.classification == NO_DRY_PATH)

    results: list[StageResult] = []
    for stage in stages:
        if stage.classification == UNSWEEPABLE:
            results.append(
                StageResult(
                    stage=stage.name,
                    verdict=UNSWEEPABLE_VERDICT,
                    repo=stage.repo,
                    launcher=stage.launcher,
                    reason=stage.reason,
                )
            )

    sweepable = [s for s in stages if s.classification == SWEEPABLE]
    report.measured = True

    if sweepable:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(run_stage, s, checkout_root, stage_timeout, runner): s
                for s in sweepable
            }
            for future in concurrent.futures.as_completed(futures):
                stage = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    # The worker itself died. The stage was not measured; it
                    # did not pass and it did not fail.
                    result = StageResult(
                        stage=stage.name,
                        verdict=UNMEASURED,
                        repo=stage.repo,
                        launcher=stage.launcher,
                        reason=f"sweep worker raised {type(exc).__name__}: {exc}",
                    )
                results.append(result)
                if aws is not None:
                    try:
                        aws.put_text(
                            f"{REPORT_PREFIX}/{run_id}/stages/{stage.name}.log",
                            _stage_log(stage, result, _stage_script(stage)),
                        )
                        result.log_key = f"{REPORT_PREFIX}/{run_id}/stages/{stage.name}.log"
                    except Exception as exc:  # noqa: BLE001
                        # Telemetry emission must never break the observed work
                        # (observability-policy §4), and must never be silent.
                        # The per-stage verdict is unaffected; only its log
                        # shipping failed, and it says so on the record.
                        print(
                            f"PREFLIGHT_SWEEP_TELEMETRY_DEGRADED stage={stage.name} "
                            f"could not ship stage log: {type(exc).__name__}: {exc}",
                            file=sys.stderr,
                        )

    order = {s.name: i for i, s in enumerate(stages)}
    results.sort(key=lambda r: order.get(r.stage, 10**6))
    report.results = [r.to_dict() for r in results]
    report.stages_swept = sum(1 for r in results if r.verdict in (PASSED, FAILED))
    report.stages_passed = sum(1 for r in results if r.verdict == PASSED)
    report.stages_failed = sum(1 for r in results if r.verdict == FAILED)
    report.stages_unmeasured = sum(1 for r in results if r.verdict == UNMEASURED)
    report.stages_unsweepable = sum(1 for r in results if r.verdict == UNSWEEPABLE_VERDICT)
    report.finished_at = dt.datetime.now(dt.timezone.utc).isoformat()

    clean = (
        report.stages_failed == 0
        and report.stages_unsweepable == 0
        and report.stages_unmeasured == 0
        and not report.coverage_findings
    )
    if clean and report.stages_passed > 0:
        report.outcome = OUTCOME_OK
    elif report.stages_failed or report.stages_unsweepable or report.coverage_findings:
        report.outcome = OUTCOME_FAILED
    else:
        # Nothing failed, but something could not be measured. Degraded, and
        # explicitly not a clean run — "no data" is never rendered as green.
        report.outcome = OUTCOME_DEGRADED
    if report.stages_passed == 0 and report.stages_swept == 0:
        report.measured = False
        report.unmeasured_reason = (
            "no stage produced a pass or a fail — the sweep exercised nothing"
        )
        report.outcome = OUTCOME_FAILED
    return report


def emit(report: SweepReport, aws: AwsSurface, sns_topic: str) -> None:
    """Publish the run's telemetry: durable report, console rows, metrics, one
    notification."""
    aws.put_json(f"{REPORT_PREFIX}/{report.run_id}/report.json", report.to_dict())
    aws.put_json(f"{REPORT_PREFIX}/latest.json", report.to_dict())

    # Console rows (console-policy §2.6): one check-result envelope per stage
    # plus the sweep's own roll-up. Published on EVERY run — a row that stops
    # being republished is exactly how the console renders STALE, so skipping
    # the write on a bad run would convert a loud failure into a quiet
    # ageing green.
    cadence = load_cadence()
    for key, body in envelopes(report.to_dict(), cadence):
        aws.put_json(key, body)
    if report.outcome == OUTCOME_OK:
        # Success-claim asymmetry (observability-policy §3.1): only a clean run
        # advances the pointer a detector reads as proof. The failure path
        # writes the report above and stops there.
        aws.put_json(f"{REPORT_PREFIX}/last_clean.json", report.to_dict())

    aws.put_metrics(
        [
            # The deadman's subject: emitted on EVERY terminal path, including
            # a run that could not measure. Its ABSENCE means the sweep never
            # reported at all, which is the only thing the alarm should page on.
            ("PreflightSweepRunCompleted", 1),
            ("PreflightSweepStagesDeclared", report.stages_declared),
            ("PreflightSweepStagesSwept", report.stages_swept),
            ("PreflightSweepStagesPassed", report.stages_passed),
            ("PreflightSweepStagesFailed", report.stages_failed),
            ("PreflightSweepStagesUnmeasured", report.stages_unmeasured),
            ("PreflightSweepStagesUnsweepable", report.stages_unsweepable),
            ("PreflightSweepCoverageFindings", len(report.coverage_findings)),
        ]
    )
    subject, message = render_notification(report)
    aws.publish(sns_topic, subject, message)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    repo_root = Path(__file__).resolve().parent.parent
    parser.add_argument("--definition", default=str(repo_root / "infrastructure" / "step_function.json"))
    parser.add_argument("--manifest", default=str(repo_root / "infrastructure" / "preflight_sweep_manifest.json"))
    parser.add_argument("--checkout-root", default="/home/ec2-user")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--stage-timeout", type=int, default=DEFAULT_STAGE_TIMEOUT)
    parser.add_argument("--sns-topic", default=DEFAULT_SNS_TOPIC)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--derive-only",
        action="store_true",
        help="derive and print the stage matrix without running anything or "
        "emitting telemetry (the zero-spend rehearsal path)",
    )
    args = parser.parse_args(argv)

    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime(
        "preflight-sweep-%Y%m%dT%H%M%SZ"
    )

    if args.derive_only:
        definition = json.loads(Path(args.definition).read_text())
        manifest = load_manifest(args.manifest)
        base = derive_shell_run_bindings(definition)
        required_map = derive_required_map_bindings(definition, base)
        findings = map_binding_disagreement(required_map, manifest)
        bindings = apply_map_bindings(base, manifest)
        bindings.setdefault("run_date", dt.date.today().isoformat())
        stages = derive_stages(
            definition,
            bindings,
            {"Execution": {"Name": run_id, "Id": f"preflight-sweep:{run_id}"}},
            args.checkout_root,
        )
        findings += manifest_disagreement(stages, manifest)
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "coverage_findings": findings,
                    "stages": [s.to_dict() for s in stages],
                },
                indent=2,
            )
        )
        return 1 if findings else 0

    aws = AwsSurface(region=args.region, bucket=args.bucket)
    report = sweep(
        definition_path=Path(args.definition),
        manifest_path=Path(args.manifest),
        checkout_root=args.checkout_root,
        run_id=run_id,
        concurrency=args.concurrency,
        stage_timeout=args.stage_timeout,
        aws=aws,
    )
    try:
        emit(report, aws, args.sns_topic)
    except Exception as exc:  # noqa: BLE001
        # Telemetry emission must not crash the observed work, and must not be
        # silently swallowed (observability-policy §4). The greppable marker
        # below IS the signal; the non-zero exit below still fires.
        print(
            f"PREFLIGHT_SWEEP_TELEMETRY_FAILED could not emit report: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        traceback.print_exc()

    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    subject, message = render_notification(report)
    print(f"\n{subject}\n{message}", file=sys.stderr)
    return 0 if report.outcome == OUTCOME_OK else 1


if __name__ == "__main__":
    raise SystemExit(main())

