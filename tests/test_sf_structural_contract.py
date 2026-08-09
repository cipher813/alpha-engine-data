"""alpha-engine-config#6684 — structural contract over every
``infrastructure/step_function*.json`` orchestration definition.

weekly-sf-policy.md §4 ("every stage declares a timeout") and §6 ("a new
stage lands complete") name two clause rows that were, until this module,
``kind: none`` — no enforcing artifact:

  * WSF-4-every-stage-declares-a-timeout
  * WSF-6-new-stage-lands-complete

This module is the enforcing artifact for both, plus a third structural
guard not named by a clause row but required by the same policy intent (a
stage that silently points at a deleted/renamed script is exactly the kind
of "lands complete" gap WSF-6 exists to catch):

  1. every ``Task`` state declares ``TimeoutSeconds``, unless the state name
     is in that file's ``_TIMEOUT_EXEMPT`` dict — each entry a one-line
     justification, not a bare pass;
  2. every ``Task`` state declares ``Catch``, same exemption mechanism;
  3. every script this repo's own EC2 target (``alpha-engine-data``) is
     told to run via SSM ``AWS-RunShellScript`` resolves to a file that
     actually exists in the tree (regression guard for the I4442 / I4975
     per-stage ``spot_data_weekly.sh`` splits — a state pointing at a
     deleted or renamed script fails here, not on Saturday).

New Task states are non-exempt by DEFAULT — the exemption dicts are an
enumerated allowlist populated from the definitions as measured on
``nousergon-data@b5b42b74`` (2026-08-09, post config#6408 Director-Catch
fix, PR #1233), not a wildcard. A state added later with no timeout/Catch
fails this suite until it is either fixed or explicitly, individually
exempted with its own justification.

Per config#6684's constraint: this PR does NOT add timeouts/Catch to any
state. Every currently-missing declaration is enumerated as an exemption
below, sourced from that state's own ``Comment`` field where one exists.
Tightening any individual exemption is separate, reviewable follow-up work
(alpha-engine-config#6684 remains open as the tracker for that).

Cross-repo scope note (deliverable 3): states whose SSM command list ``cd``s
into a sibling repo's EC2 checkout (``alpha-engine`` == crucible-executor,
``alpha-engine-predictor``, ``alpha-engine-backtester``,
``alpha-engine-dashboard``, ...) reference scripts this repo does not own
and cannot verify without a network clone — out of scope by the "pure file
read, no network" constraint in config#6684's deliverables. Only scripts
invoked after ``cd /home/ec2-user/alpha-engine-data`` (this repo's own EC2
deploy target, confirmed in OVERVIEW.md) are checked for existence.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterator

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"

# ---------------------------------------------------------------------------
# Exemption registry — one entry per file. Every listed state is a measured,
# justified gap as of the commit named in the module docstring above, NOT a
# blanket allowance. A state absent from a file's Task-state set (renamed or
# removed) but still listed here fails test_no_stale_*_exemptions — the
# allowlist is not permitted to silently outlive what it names.
# ---------------------------------------------------------------------------

_TIMEOUT_EXEMPT: dict[str, dict[str, str]] = {
    "step_function.json": {
        # sns:publish — fire-and-forget notify, seconds-scale API call.
        "WeeklyRunDayGateFailed": "sns:publish fail-open notifier — SDK call, not a wait",
        "PublishLibPinGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "PublishPipelineContractGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "ResearchPredictorParallel.PublishResearchFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
        "ResearchPredictorParallel.PublishPredictorFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
        "ResearchPredictorParallel.PublishModelZooFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
        "PublishReportCardDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "PublishParityDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait (alpha-engine-config-I6025)",
        "NotifyCompleteGatesDegraded": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyCompleteHealthDegraded": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyCompleteGatesAndHealthDegraded": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyCompleteReportCardDegraded": "sns:publish completion notifier — SDK call, not a wait (config#6685)",
        "NotifyCompleteMultipleDegraded": "sns:publish completion notifier — SDK call, not a wait (config#6685)",
        "NotifyCompleteParityDegraded": "sns:publish completion notifier — SDK call, not a wait (alpha-engine-config-I6025)",
        "NotifyShellRunComplete": "sns:publish completion notifier — SDK call, not a wait",
        "NotifyComplete": "sns:publish completion notifier — SDK call, not a wait",
        "HandleFailure": "sns:publish failure notifier — SDK call, not a wait",
        "PublishEvaluatorGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        "PublishEvaluatorDirectorGateDegraded": "sns:publish degraded-gate notifier — SDK call, not a wait",
        # lambda:invoke — synchronous gate Lambda, seconds-scale.
        "WeeklyRunDayGate": "lambda:invoke synchronous gate call — SDK call, not a wait",
        # dynamodb:putItem — single-item write, sub-second.
        "AcquireMutex": "dynamodb:putItem mutex acquire — SDK call, not a wait",
        # ssm:getCommandInvocation — a single POLL of an in-flight SSM
        # command; the actual long-running work is bounded by the
        # invoking state's own executionTimeout / the box-side script,
        # not this poll call.
        "WaitForMorningEnrich": "ssm:getCommandInvocation single poll — bounded by MorningEnrich's own executionTimeout",
        "WaitForDataPhase1": "ssm:getCommandInvocation single poll — bounded by DataPhase1's own executionTimeout",
        "ResearchPredictorParallel.WaitForRAGIngestion": "ssm:getCommandInvocation single poll — bounded by RAGIngestion's own executionTimeout",
        "ResearchPredictorParallel.WaitForThinkTank": "ssm:getCommandInvocation single poll — bounded by ThinkTankCoverage's own executionTimeout",
        "ResearchPredictorParallel.WaitForDataPhase2": "ssm:getCommandInvocation single poll — bounded by DataPhase2's own executionTimeout",
        "ResearchPredictorParallel.WaitForPredictorTraining": "ssm:getCommandInvocation single poll — bounded by PredictorTraining's own executionTimeout",
        "ResearchPredictorParallel.WaitResolveZoo": "ssm:getCommandInvocation single poll — bounded by ResolveZooSpecs' own executionTimeout",
        "ResearchPredictorParallel.ModelZooTrainMap.WaitTrainSpec": "ssm:getCommandInvocation single poll — bounded by TrainSpecDispatch's own executionTimeout",
        "ResearchPredictorParallel.WaitForModelZoo": "ssm:getCommandInvocation single poll — bounded by ModelZooSelect's own executionTimeout",
        "WaitForBacktester": "ssm:getCommandInvocation single poll — bounded by Backtester's own executionTimeout",
        "WaitForPredictorBacktest": "ssm:getCommandInvocation single poll — bounded by PredictorBacktest's own executionTimeout",
        "WaitForPortfolioOptimizerBacktest": "ssm:getCommandInvocation single poll — bounded by PortfolioOptimizerBacktest's own executionTimeout",
        "WaitForParity": "ssm:getCommandInvocation single poll — bounded by Parity's own executionTimeout",
        "WaitForEvaluator": "ssm:getCommandInvocation single poll — bounded by Evaluator's own executionTimeout",
        "WaitForSaturdayHealthCheck": "ssm:getCommandInvocation single poll — bounded by SaturdayHealthCheck's own executionTimeout",
        "WaitForWeeklySubstrateHealthCheck": "ssm:getCommandInvocation single poll — bounded by WeeklySubstrateHealthCheck's own executionTimeout",
        "WaitForWeeklyFreshnessSpotBootstrap": "ssm:getCommandInvocation single poll — bounded by DispatchWeeklyFreshnessSpot's own executionTimeout",
        # s3:headObject / s3:putObject — single-object API call, sub-second.
        "ResearchPredictorParallel.ValidatePredictorSkipWeightsFresh": "s3:headObject freshness check — SDK call, not a wait",
        "WriteCompletionMarker": "s3:putObject completion marker — SDK call, not a wait (config#2857)",
    },
    "step_function_daily.json": {
        "AcquireMutex": "dynamodb:putItem mutex acquire — SDK call, not a wait",
        "TradingDayGate": "lambda:invoke synchronous gate call — SDK call, not a wait",
        "TradingDayGateFailed": "sns:publish fail-open notifier — SDK call, not a wait",
        "NotifyHolidaySkip": "sns:publish terminal skip notifier — SDK call, not a wait",
        "StartExecutorEC2": "ec2:startInstances — SDK call, not a wait",
        "WriteCompletionMarker": "s3:putObject completion marker — SDK call, not a wait (config#2857)",
        "HandleFailure": "sns:publish failure notifier — SDK call, not a wait",
        "PollMorningEnrichSpot": "ssm:getCommandInvocation single poll — bounded by the spot dispatch Lambda's own budget",
        "PollMorningArcticAppendSpot": "ssm:getCommandInvocation single poll — bounded by the spot dispatch Lambda's own budget",
        "PublishDataSpotFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
    },
    "step_function_eod.json": {
        "AcquireMutex": "dynamodb:putItem mutex acquire — SDK call, not a wait",
        "StartTradingInstance": "ec2:startInstances — SDK call, not a wait",
        "WaitForCaptureSnapshot": "ssm:getCommandInvocation single poll — bounded by CaptureSnapshot's own executionTimeout",
        "WaitForEOD": "ssm:getCommandInvocation single poll — bounded by the EOD daemon's own executionTimeout",
        "WaitForRefreshExecutorDeploy": "ssm:getCommandInvocation single poll — bounded by RefreshExecutorDeploy's own executionTimeout",
        "PollPostMarketDataSpot": "ssm:getCommandInvocation single poll — bounded by the spot dispatch Lambda's own budget",
        "PollPostMarketArcticAppendSpot": "ssm:getCommandInvocation single poll — bounded by the spot dispatch Lambda's own budget",
        "PublishDataSpotFailureImmediate": "sns:publish immediate-failure notifier — SDK call, not a wait",
        "SkipEODReconcileDataGap": "sns:publish skip notifier — SDK call, not a wait",
        "HealPollPostMarketDataSpot": "ssm:getCommandInvocation single poll — bounded by the heal-path spot dispatch's own budget",
        "HealPollArcticAppendSpot": "ssm:getCommandInvocation single poll — bounded by the heal-path spot dispatch's own budget",
        "HealReplayDispatchFailed": "sns:publish notifier — SDK call, not a wait",
        "HealConvergedNotify": "sns:publish notifier — SDK call, not a wait",
        "HealNonConvergent": "sns:publish notifier — SDK call, not a wait",
        "StopTradingInstance": "ec2:stopInstances — SDK call, not a wait",
        "LaunchWeeklyExerciseRun": "states:startExecution (async, not .sync) — fire-and-forget SDK call",
        "WeeklyExerciseLaunchFailed": "sns:publish notifier — SDK call, not a wait",
        "WriteCompletionMarkerNormal": "s3:putObject completion marker — SDK call, not a wait (config#2857)",
        "WriteCompletionMarkerDegraded": "s3:putObject completion marker — SDK call, not a wait (config#2857)",
        "HandleFailure": "sns:publish failure notifier — SDK call, not a wait",
        "ForceStopInstance": "ec2:stopInstances fail-safe teardown — SDK call, not a wait",
    },
    "step_function_groom.json": {},
}

_CATCH_EXEMPT: dict[str, dict[str, str]] = {
    "step_function.json": {
        "WeeklyRunDayGateFailed": "deliberate fail-open notify+proceed (own Comment); a Catch here would need its own Catch",
        "WriteCompletionMarker": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate as this execution's own unverifiable-completion signal, not be masked",
        "HandleFailure": "terminal failure notifier — routes to FailExecution; the shared failure sink itself, not something to re-catch into",
    },
    "step_function_daily.json": {
        "TradingDayGateFailed": "deliberate fail-open notify+proceed (own Comment); a Catch here would need its own Catch",
        "NotifyHolidaySkip": "terminal skip notifier (End: true) — nothing downstream to route a Catch to",
        "WriteCompletionMarker": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "HandleFailure": "terminal failure notifier — routes to FailExecution; the shared failure sink itself, not something to re-catch into",
    },
    "step_function_eod.json": {
        "WriteCompletionMarkerNormal": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "WriteCompletionMarkerDegraded": "config#2857/config#1724: deliberately UNCAUGHT — a marker write failure must propagate, not be masked",
        "ForceStopInstance": "fail-safe teardown reached FROM the failure path (own Comment: 'always stop... even on failure') — a Catch here risks looping back into the failure path it is cleaning up after",
    },
    "step_function_groom.json": {},
}

assert set(_TIMEOUT_EXEMPT) == set(_CATCH_EXEMPT), (
    "timeout and catch exemption dicts must enumerate the same file set"
)
_SF_FILE_NAMES = sorted(_TIMEOUT_EXEMPT)


# ---------------------------------------------------------------------------
# Definition-walking primitives — mirrors the Task-state discovery already
# used informally by test_sf_global_timeout.py / test_deploy_infrastructure_
# sf_coverage.py, but recurses into Parallel branches and Map
# ItemProcessor/Iterator sub-definitions so nested Task states (e.g. every
# state inside step_function.json's ResearchPredictorParallel branches, or
# inside ModelZooTrainMap) are not silently skipped.
# ---------------------------------------------------------------------------


def _iter_task_states(definition: dict) -> Iterator[tuple[str, dict]]:
    def _walk(states: dict, prefix: str) -> Iterator[tuple[str, dict]]:
        for name, state in states.items():
            path = f"{prefix}{name}"
            stype = state.get("Type")
            if stype == "Task":
                yield path, state
            if "States" in state:
                yield from _walk(state["States"], f"{path}.")
            if stype == "Parallel":
                for branch in state.get("Branches", []):
                    yield from _walk(branch.get("States", {}), f"{path}.")
            if stype == "Map":
                sub = (state.get("ItemProcessor") or state.get("Iterator") or {}).get(
                    "States"
                )
                if sub:
                    yield from _walk(sub, f"{path}.")

    yield from _walk(definition.get("States", {}), "")


def _missing_timeout(definition: dict, exempt: dict[str, str]) -> list[str]:
    return sorted(
        name
        for name, state in _iter_task_states(definition)
        if "TimeoutSeconds" not in state and name not in exempt
    )


def _missing_catch(definition: dict, exempt: dict[str, str]) -> list[str]:
    return sorted(
        name
        for name, state in _iter_task_states(definition)
        if "Catch" not in state and name not in exempt
    )


def _stale_exemptions(definition: dict, exempt: dict[str, str]) -> list[str]:
    present = {name for name, _ in _iter_task_states(definition)}
    return sorted(name for name in exempt if name not in present)


def _load(sf_file: str) -> dict:
    return json.loads((_INFRA / sf_file).read_text())


# ---------------------------------------------------------------------------
# Deliverable 3 — data-repo launcher-script existence.
#
# Only states whose SSM command list `cd`s into THIS repo's own EC2 deploy
# target (`/home/ec2-user/alpha-engine-data`, confirmed in OVERVIEW.md) are
# checked: a sibling repo's scripts (crucible-executor's `executor/main.py`,
# crucible-predictor's `infrastructure/spot_train.sh`, ...) cannot be
# verified from a pure file read of this repo and are out of scope.
# ---------------------------------------------------------------------------

_SCRIPT_REF_RE = re.compile(r"[A-Za-z0-9_./-]+\.(?:sh|py)\b")
_DATA_REPO_CD_RE = re.compile(r"cd /home/ec2-user/alpha-engine-data(?=['\"\s])")
_DATA_REPO_ABS_PREFIX = "/home/ec2-user/alpha-engine-data/"


def _data_repo_script_refs(definition: dict) -> list[tuple[str, str]]:
    """(state_name, script_ref) pairs for Task states that operate against
    this repo's own EC2 checkout, as evidenced by a `cd` into it appearing
    in the state's Parameters block."""
    refs = []
    for name, state in _iter_task_states(definition):
        params_text = json.dumps(state.get("Parameters", {}))
        if not _DATA_REPO_CD_RE.search(params_text):
            continue
        refs.extend((name, ref) for ref in _SCRIPT_REF_RE.findall(params_text))
    return refs


def _missing_data_repo_scripts(definition: dict, repo_root: Path) -> list[str]:
    missing = []
    for state_name, ref in _data_repo_script_refs(definition):
        rel = (
            ref[len(_DATA_REPO_ABS_PREFIX) :]
            if ref.startswith(_DATA_REPO_ABS_PREFIX)
            else ref
        )
        if not (repo_root / rel).is_file():
            missing.append(f"{state_name} -> {ref} (resolved: {rel})")
    return missing


# ---------------------------------------------------------------------------
# Meta-tests — prove the checkers themselves flag a bad definition, so this
# suite cannot silently pass on a parsing bug (config#6684 deliverable d).
# All synthetic — no dependency on the real definitions, so these stay
# stable regardless of what other in-flight PRs land on the real files.
# ---------------------------------------------------------------------------


def test_meta_walker_finds_nested_task_states():
    """Task states nested inside Parallel branches and Map
    ItemProcessor/Iterator sub-definitions must be discovered — the exact
    shapes step_function.json's ResearchPredictorParallel and
    ModelZooTrainMap use."""
    synthetic = {
        "States": {
            "TopTask": {"Type": "Task", "End": True},
            "AParallel": {
                "Type": "Parallel",
                "Branches": [
                    {"States": {"BranchTask": {"Type": "Task", "End": True}}}
                ],
                "End": True,
            },
            "AMapNew": {
                "Type": "Map",
                "ItemProcessor": {
                    "States": {"MapNewTask": {"Type": "Task", "End": True}}
                },
                "End": True,
            },
            "AMapLegacy": {
                "Type": "Map",
                "Iterator": {
                    "States": {"MapLegacyTask": {"Type": "Task", "End": True}}
                },
                "End": True,
            },
        }
    }
    found = {name for name, _ in _iter_task_states(synthetic)}
    assert found == {
        "TopTask",
        "AParallel.BranchTask",
        "AMapNew.MapNewTask",
        "AMapLegacy.MapLegacyTask",
    }


def test_meta_missing_timeout_is_flagged_and_exemption_clears_it():
    synthetic = {
        "States": {
            "Bare": {"Type": "Task", "Catch": [{"ErrorEquals": ["States.ALL"]}], "End": True},
            "Covered": {
                "Type": "Task",
                "TimeoutSeconds": 30,
                "Catch": [{"ErrorEquals": ["States.ALL"]}],
                "End": True,
            },
        }
    }
    assert _missing_timeout(synthetic, exempt={}) == ["Bare"]
    assert _missing_timeout(synthetic, exempt={"Bare": "synthetic exemption"}) == []


def test_meta_missing_catch_is_flagged_and_exemption_clears_it():
    synthetic = {
        "States": {
            "Bare": {"Type": "Task", "TimeoutSeconds": 30, "End": True},
            "Covered": {
                "Type": "Task",
                "TimeoutSeconds": 30,
                "Catch": [{"ErrorEquals": ["States.ALL"]}],
                "End": True,
            },
        }
    }
    assert _missing_catch(synthetic, exempt={}) == ["Bare"]
    assert _missing_catch(synthetic, exempt={"Bare": "synthetic exemption"}) == []


def test_meta_stale_exemption_is_flagged():
    synthetic = {
        "States": {
            "StillHere": {"Type": "Task", "TimeoutSeconds": 30, "End": True},
        }
    }
    exempt = {"StillHere": "ok", "Renamed": "no longer exists"}
    assert _stale_exemptions(synthetic, exempt) == ["Renamed"]


def test_meta_missing_data_repo_script_is_flagged():
    synthetic = {
        "States": {
            "RunsMissingScript": {
                "Type": "Task",
                "Parameters": {
                    "commands": [
                        "cd /home/ec2-user/alpha-engine-data",
                        "bash infrastructure/this_script_does_not_exist_ndm6684.sh",
                    ]
                },
                "End": True,
            },
            "RunsRealScript": {
                "Type": "Task",
                "Parameters": {
                    "commands": [
                        "cd /home/ec2-user/alpha-engine-data",
                        "bash infrastructure/spot_data_weekly.sh",
                    ]
                },
                "End": True,
            },
            "SiblingRepoNotChecked": {
                "Type": "Task",
                "Parameters": {
                    "commands": [
                        "cd /home/ec2-user/alpha-engine-predictor",
                        "bash infrastructure/this_also_does_not_exist.sh",
                    ]
                },
                "End": True,
            },
        }
    }
    missing = _missing_data_repo_scripts(synthetic, _REPO_ROOT)
    assert len(missing) == 1
    assert "RunsMissingScript" in missing[0]
    assert "this_script_does_not_exist_ndm6684.sh" in missing[0]


# ---------------------------------------------------------------------------
# Real-definition tests.
# ---------------------------------------------------------------------------


def test_sf_file_set_matches_exemption_registry():
    """Guards against a vacuous parametrize AND a new step_function_*.json
    landing with no exemption entries defined for it — either would make
    every test below silently skip or silently pass for the new file."""
    on_disk = sorted(p.name for p in _INFRA.glob("step_function*.json"))
    assert on_disk, "no infrastructure/step_function*.json files found"
    assert on_disk == _SF_FILE_NAMES, (
        f"infrastructure/ has {on_disk} but this module's exemption "
        f"registry covers {_SF_FILE_NAMES} — add/remove a top-level dict "
        f"entry in test_sf_structural_contract.py to match"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_every_task_state_declares_timeout_or_is_exempt(sf_file: str):
    definition = _load(sf_file)
    missing = _missing_timeout(definition, _TIMEOUT_EXEMPT[sf_file])
    assert not missing, (
        f"{sf_file}: Task state(s) with no TimeoutSeconds and no exemption: "
        f"{missing} — either the state genuinely needs a timeout (fix it; "
        f"config#6684 tracks tightening these) or add a one-line-justified "
        f"entry to _TIMEOUT_EXEMPT['{sf_file}'] in this file"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_every_task_state_declares_catch_or_is_exempt(sf_file: str):
    definition = _load(sf_file)
    missing = _missing_catch(definition, _CATCH_EXEMPT[sf_file])
    assert not missing, (
        f"{sf_file}: Task state(s) with no Catch and no exemption: "
        f"{missing} — either the state genuinely needs a Catch (fix it; "
        f"config#6684 tracks tightening these) or add a one-line-justified "
        f"entry to _CATCH_EXEMPT['{sf_file}'] in this file"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_no_stale_timeout_exemptions(sf_file: str):
    definition = _load(sf_file)
    stale = _stale_exemptions(definition, _TIMEOUT_EXEMPT[sf_file])
    assert not stale, (
        f"{sf_file}: _TIMEOUT_EXEMPT names state(s) no longer present as a "
        f"Task state: {stale} — remove the stale entry (renamed/removed "
        f"states must not linger as dead allowlist entries)"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_no_stale_catch_exemptions(sf_file: str):
    definition = _load(sf_file)
    stale = _stale_exemptions(definition, _CATCH_EXEMPT[sf_file])
    assert not stale, (
        f"{sf_file}: _CATCH_EXEMPT names state(s) no longer present as a "
        f"Task state: {stale} — remove the stale entry (renamed/removed "
        f"states must not linger as dead allowlist entries)"
    )


@pytest.mark.parametrize("sf_file", _SF_FILE_NAMES)
def test_data_repo_launcher_scripts_exist(sf_file: str):
    definition = _load(sf_file)
    missing = _missing_data_repo_scripts(definition, _REPO_ROOT)
    assert not missing, (
        f"{sf_file}: state(s) invoke a script under this repo's own EC2 "
        f"checkout (alpha-engine-data) that does not exist in the tree: "
        f"{missing} — I4442/I4975-class regression: a deleted/renamed "
        f"launcher script must fail here, not on Saturday"
    )
