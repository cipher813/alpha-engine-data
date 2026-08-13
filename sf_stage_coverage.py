"""
Post-stage coverage assertion for `ne-weekly-freshness-pipeline`
(alpha-engine-config-I7214).

## The failure this closes

Five stages of the 2026-08-08 weekly run produced no output while the run
reported SUCCEEDED (`alpha-engine-config-I7167`). Every one of them exited zero.
A Step Functions pipeline asserts that its stages did not *throw*; nothing
asserted that they *wrote*. Those are different claims, and only the second is
what the trading week depends on.

This module answers, for one execution: **for each stage that entered, does its
declared artifact exist, and was it written by THIS run?** A stage whose
artifact is missing, or is left over from a previous cycle, is not covered — and
a stale artifact is the more dangerous of the two, because every freshness probe
keyed on existence alone reads it as green.

## Where the declaration comes from

`ARTIFACT_REGISTRY.yaml`, read from
`s3://alpha-engine-research/_freshness_monitor/ARTIFACT_REGISTRY.yaml` — the
copy `alpha-engine-config`'s `sync-artifact-registry.yml` refreshes on every
merge that touches it. The consumer is refreshed by the same event that changes
the declaration, which is the property a registry read from a stale checkout
loses. Nothing here carries its own list of stages: a hand-maintained
denominator drifts, and its drift is invisible because the missing rows produce
no signal.

## Three properties this deliberately has

**It cannot fail the run.** Every path returns a verdict; nothing raises. A
coverage assertion that can kill the pipeline it observes is a new failure mode
bolted onto the one it was meant to report, and this pipeline is fragile
(`sf-pipeline-policy.md` §2.3a: withholding the guarantee is not the same as
failing the pipeline).

**It cannot report a pass it did not establish.** Registry unreadable, S3
unreachable, no artifacts probed — every one of those is `UNMEASURED`, never
`COVERED`. §2.3a rule 2: a missing verdict propagates as UNKNOWN and never as a
pass. `UNMEASURED` is a loud value; it is not silence.

**It has an OBSERVE mode and ships in it.** `enforce=False` computes and returns
the full verdict and sets nothing. The SF's Choice reads `enforce`, so promotion
to degrading the run is a one-literal diff in the definition — after one clean
cycle has shown what a healthy verdict actually looks like. Turning it on before
that would be enforcing a threshold whose distribution nobody has seen.
"""

from __future__ import annotations

import datetime as _dt
import logging
import os
import traceback
from typing import Any

logger = logging.getLogger(__name__)

WEEKLY_PIPELINE = "ne-weekly-freshness-pipeline"
REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET", "alpha-engine-research")
REGISTRY_KEY = os.environ.get(
    "REGISTRY_KEY", "_freshness_monitor/ARTIFACT_REGISTRY.yaml"
)
DEFAULT_ARTIFACT_BUCKET = "alpha-engine-research"

# Verdicts. Closed set — a consumer that cannot place a value must not fall
# through to a benign reading.
COVERED = "COVERED"
MISSING = "MISSING"
UNMEASURED = "UNMEASURED"

# Stage -> the execution-input flag that legitimately suppresses it. A stage
# whose flag is set did not run, so its artifact's absence is a fact about the
# input, not about the producer. Derived from the definition's own
# `CheckSkip*` Choice states; a stage with no entry here cannot be skipped.
#
# This is the ONE list here that mirrors something outside the registry, and it
# is guarded by `tests/test_sf_stage_coverage.py::test_every_skip_flag_is_real`,
# which reads the definition and fails on a flag that no Choice state consults.
STAGE_SKIP_FLAGS: dict[str, str] = {
    "MorningEnrich": "skip_morning_enrich",
    "DataPhase1": "skip_data_phase1",
    "DataPhase2": "skip_data_phase2",
    "Scanner": "skip_scanner",
    "SignalsEnvelope": "skip_signals_envelope",
    "ChallengerShadow": "skip_challenger_shadow",
    "RAGIngestion": "skip_rag_ingestion",
    "RegimeSubstrate": "skip_regime_substrate",
    "RegimeRetrospectiveEval": "skip_regime_retrospective_eval",
    "PredictorTraining": "skip_predictor_training",
    "Backtester": "skip_backtester",
    "PredictorBacktest": "skip_predictor_backtest",
    "PortfolioOptimizerBacktest": "skip_portfolio_optimizer_backtest",
    "PitParityLookahead": "skip_pit_parity_lookahead",
    "PitParityWalkforward": "skip_pit_parity_walkforward",
    "PitParityCompare": "skip_pit_parity_compare",
    "ParityReplay": "skip_parity_replay",
    "EvalJudgeProcess": "skip_eval_judge",
    "EvalRollingMean": "skip_eval_judge",
    "RationaleClustering": "skip_rationale_clustering",
    "ReplayConcordance": "skip_replay_concordance",
    "Counterfactual": "skip_counterfactual",
    "AggregateCosts": "skip_aggregate_costs",
    "EvaluatorDiagnostics": "skip_evaluator",
    "EvaluatorOptimize": "skip_evaluator",
    "ReportCard": "skip_report_card",
    "Director": "skip_director",
}
# Coarser gates that suppress several stages at once.
GROUP_SKIP_FLAGS: dict[str, tuple[str, ...]] = {
    "skip_parity": (
        "PitParityLookahead", "PitParityWalkforward", "ParityReplay",
        "PitParityCompare",
    ),
    "skip_post_eval": ("SaturdayHealthCheck", "WeeklySubstrateHealthCheck"),
}


def _client(s3_client=None):
    if s3_client is not None:
        return s3_client
    import boto3

    return boto3.client("s3")


def load_declaration(s3_client=None) -> dict:
    """Return ``{stage: {"artifacts": [{artifact_id, bucket, key_template}]}}``
    for every weekly-pipeline stage declaring registered output.

    Raises on any problem. The caller turns that into ``UNMEASURED``; it must
    never become an empty declaration, because an empty declaration probes
    nothing and would report a clean pass.
    """
    import yaml

    body = _client(s3_client).get_object(
        Bucket=REGISTRY_BUCKET, Key=REGISTRY_KEY
    )["Body"].read()
    registry = yaml.safe_load(body)

    by_id = {r["artifact_id"]: r for r in registry["artifacts"]}
    stages = registry["pipeline_stages"]
    if not stages:
        raise ValueError("registry carries an empty pipeline_stages section")

    out: dict[str, dict] = {}
    for row in stages:
        entry: dict[str, Any] = {
            "stage_class": row.get("stage_class"),
            "output": row.get("output"),
            "artifacts": [],
        }
        for aid in row.get("artifacts") or []:
            art = by_id[aid]
            entry["artifacts"].append({
                "artifact_id": aid,
                "bucket": art.get("s3_bucket", DEFAULT_ARTIFACT_BUCKET),
                "key_template": art["s3_key_template"],
            })
        out[row["stage"]] = entry
    return out


def _resolve_key(template: str, run_date: str) -> str:
    """`{date}` and `{trading_day}` both resolve to this run's date.

    A template carrying a placeholder this function does not know is returned
    with the literal braces intact — which fails the head_object loudly, rather
    than probing a wrong key and reporting a confident wrong answer.
    """
    return template.replace("{date}", run_date).replace("{trading_day}", run_date)


def _parse_iso(value: str) -> _dt.datetime:
    """Parse an SF `$$.Execution.StartTime` into an aware UTC datetime."""
    text = value.replace("Z", "+00:00")
    parsed = _dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed.astimezone(_dt.timezone.utc)


def _skipped(stage: str, execution_input: dict) -> bool:
    flag = STAGE_SKIP_FLAGS.get(stage)
    if flag and execution_input.get(flag) is True:
        return True
    for group_flag, members in GROUP_SKIP_FLAGS.items():
        if stage in members and execution_input.get(group_flag) is True:
            return True
    return False


def assert_stage_coverage(
    *,
    run_date: str,
    execution_start_time: str,
    execution_input: dict | None = None,
    enforce: bool = False,
    s3_client=None,
) -> dict:
    """Assert every entered stage's declared artifact exists and is of this run.

    Returns a verdict dict. NEVER raises — see the module docstring.
    """
    execution_input = execution_input or {}
    verdict: dict[str, Any] = {
        "pipeline": WEEKLY_PIPELINE,
        "run_date": run_date,
        "enforce": bool(enforce),
        "mode": "enforce" if enforce else "observe",
        "status": UNMEASURED,
        "stages_declared": 0,
        "stages_expected": 0,
        "stages_covered": 0,
        "stages_skipped": [],
        "stages_no_artifact": [],
        "missing": [],
        "stale": [],
        "unmeasured": [],
    }

    try:
        window_start = _parse_iso(execution_start_time)
    except Exception as exc:  # noqa: BLE001 — an unparseable window is UNMEASURED
        verdict["error"] = f"could not parse execution_start_time: {exc}"
        return verdict

    try:
        declaration = load_declaration(s3_client=s3_client)
    except Exception as exc:  # noqa: BLE001 — no declaration is UNMEASURED
        verdict["error"] = f"could not load the stage declaration: {exc}"
        verdict["traceback"] = traceback.format_exc()
        return verdict

    verdict["stages_declared"] = len(declaration)
    s3 = _client(s3_client)

    for stage, entry in sorted(declaration.items()):
        if _skipped(stage, execution_input):
            verdict["stages_skipped"].append(stage)
            continue
        if entry["output"] != "registered":
            # A POSITIVE declaration that this stage writes no registerable
            # key. It is covered by having said so — which is exactly what
            # distinguishes it from a stage nobody ever considered.
            verdict["stages_no_artifact"].append(stage)
            continue

        verdict["stages_expected"] += 1
        stage_ok = True
        for art in entry["artifacts"]:
            key = _resolve_key(art["key_template"], run_date)
            try:
                head = s3.head_object(Bucket=art["bucket"], Key=key)
            except Exception as exc:  # noqa: BLE001 — classified below
                name = type(exc).__name__
                code = getattr(exc, "response", {}).get(
                    "Error", {}
                ).get("Code", name)
                if code in ("404", "NoSuchKey", "NotFound"):
                    verdict["missing"].append({
                        "stage": stage,
                        "artifact_id": art["artifact_id"],
                        "key": key,
                        "reason": "absent",
                    })
                else:
                    # Could-not-measure is NOT a finding about the producer.
                    # Conflating the two reports a harness fault as a defect,
                    # always in the alarming direction.
                    verdict["unmeasured"].append({
                        "stage": stage,
                        "artifact_id": art["artifact_id"],
                        "key": key,
                        "reason": f"probe failed: {code}",
                    })
                stage_ok = False
                continue

            last_modified = head.get("LastModified")
            if last_modified is None:
                verdict["unmeasured"].append({
                    "stage": stage,
                    "artifact_id": art["artifact_id"],
                    "key": key,
                    "reason": "head_object returned no LastModified",
                })
                stage_ok = False
                continue
            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=_dt.timezone.utc)
            if last_modified < window_start:
                # The dangerous case. The key exists, so every existence-only
                # probe reads green, and the value being consumed is last
                # cycle's.
                verdict["stale"].append({
                    "stage": stage,
                    "artifact_id": art["artifact_id"],
                    "key": key,
                    "last_modified": last_modified.isoformat(),
                    "window_start": window_start.isoformat(),
                    "reason": "predates this execution",
                })
                stage_ok = False
        if stage_ok:
            verdict["stages_covered"] += 1

    if verdict["stages_expected"] == 0:
        # Probed nothing. That is not a clean run; it is an unobserved one.
        verdict["status"] = UNMEASURED
        verdict["error"] = (
            "no stage was expected to have written an artifact — every declared "
            "stage was skipped or declares no registerable key, so this run "
            "observed nothing"
        )
        return verdict

    if verdict["missing"] or verdict["stale"]:
        verdict["status"] = MISSING
    elif verdict["unmeasured"]:
        verdict["status"] = UNMEASURED
    else:
        verdict["status"] = COVERED

    verdict["summary"] = (
        f"{verdict['stages_covered']}/{verdict['stages_expected']} stages wrote "
        f"their declared artifact within this execution's window; "
        f"{len(verdict['missing'])} missing, {len(verdict['stale'])} stale, "
        f"{len(verdict['unmeasured'])} unmeasured, "
        f"{len(verdict['stages_skipped'])} skipped by input, "
        f"{len(verdict['stages_no_artifact'])} declare no registerable key"
    )
    return verdict


def handle(event: dict) -> dict:
    """Entry point for the Lambda's ``action: assert_stage_coverage`` mode.

    The `degrade` field is what the SF's Choice reads. It is true ONLY when the
    caller asked for enforcement AND the verdict is a real miss — so shipping
    with `enforce: false` in the definition makes this state observationally
    complete and operationally inert, which is the whole point before the first
    live run (`sf-pipeline-policy.md` §7: a withheld guarantee beats a failed
    run).
    """
    try:
        verdict = assert_stage_coverage(
            run_date=event["run_date"],
            execution_start_time=event["execution_start_time"],
            execution_input=event.get("execution_input") or {},
            enforce=bool(event.get("enforce", False)),
        )
    except Exception as exc:  # noqa: BLE001 — belt and braces; must not raise
        logger.error("stage coverage assertion raised: %s", exc, exc_info=True)
        verdict = {
            "pipeline": WEEKLY_PIPELINE,
            "status": UNMEASURED,
            "enforce": bool(event.get("enforce", False)),
            "mode": "enforce" if event.get("enforce") else "observe",
            "error": f"assertion raised: {exc}",
            "traceback": traceback.format_exc(),
        }
    verdict["degrade"] = bool(verdict.get("enforce")) and verdict["status"] == MISSING
    logger.info("stage coverage: %s", verdict.get("summary") or verdict.get("error"))
    return verdict
