"""A stage timeout that can never bind is not a budget (config-I6855).

`sf-pipeline-policy.md` §4: every stage carries a declared timeout sized to
observed p95 x 1.5, and a stage with no declared timeout is a defect.

There is a second way to have no budget, and it is invisible: declare one the
service can never reach. A `lambda:invoke` task has TWO ceilings — the state's
`TimeoutSeconds` and the function's own `Timeout` — and only the SMALLER one
ever fires. Where the state's value is the larger, it describes nothing.

Measured 2026-08-11 on `ne-preopen-trading-pipeline`. The `Scanner` state
declared `TimeoutSeconds: 600`; `alpha-engine-research-scanner` carried a
300s function timeout. Both invocations died at exactly 300.00s with
`Sandbox.Timedout`, the preopen pipeline terminated DEGRADED, and that day's
universe board, membership, leaderboard and trajectory were never written.
The 600 had never been reachable, so nobody reviewing the definition could
see the real budget — it lived in another repo's deploy script.

Which direction is correct is not symmetric:

  SF <  function   LEGITIMATE. The state's value binds, and a shared
                   multi-purpose Lambda (alpha-engine-predictor-inference is
                   invoked by nine states at budgets from 60s to 900s)
                   deliberately carries per-call budgets below its ceiling.
                   An overrun surfaces as States.Timeout naming the state.
  SF == function   RACE. Whichever fires first is undefined, so the error
                   type an operator sees is not reproducible.
  SF >  function   DEFECT. The declaration is decorative; the function's
                   ceiling silently governs.

So this file asserts `TimeoutSeconds < function timeout`, and that one is
declared at all.

WHY THE FUNCTION TIMEOUTS ARE CODIFIED HERE rather than read from AWS: this
runs in CI without credentials, and the functions are deployed from four
other repositories. `_FUNCTION_TIMEOUTS_SEC` is therefore a claim about the
live account that can go stale — `test_the_codified_function_timeouts_match_live`
is skipped without credentials and is what proves it has not.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterator

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_DEFS = ("step_function.json", "step_function_daily.json", "step_function_eod.json")

# Live function timeouts, measured 2026-08-11 via
# `aws lambda get-function-configuration --query Timeout`.
_FUNCTION_TIMEOUTS_SEC: dict[str, int] = {
    "alpha-engine-data-spot-dispatcher": 600,
    "alpha-engine-eod-precondition-probe": 30,
    "alpha-engine-evaluator": 300,
    "alpha-engine-evaluator-director": 900,
    "alpha-engine-predictor-inference": 900,
    "alpha-engine-predictor-regime-retrospective-eval": 600,
    "alpha-engine-predictor-regime-substrate": 300,
    "alpha-engine-replay-concordance": 900,
    "alpha-engine-replay-counterfactual": 600,
    "alpha-engine-research-aggregate-costs": 300,
    "alpha-engine-research-eval-judge-poll": 60,
    "alpha-engine-research-eval-judge-process": 900,
    "alpha-engine-research-eval-judge-submit": 300,
    "alpha-engine-research-eval-rolling-mean": 300,
    "alpha-engine-research-rationale-clustering": 900,
    "alpha-engine-research-runner": 900,
    # 300 -> 450 by crucible-research-PR601 (p95 x 1.5). The Scanner states
    # moved 600 -> 440 in the same arc so the SF budget binds again.
    "alpha-engine-research-scanner": 450,
    "alpha-engine-research-signals-envelope": 300,
    "alpha-engine-weekly-freshness-spot-dispatcher": 600,
    "alpha-engine-weekly-preflight": 120,
}

# States whose SF budget is >= the function timeout, measured 2026-08-11.
#
# NOT fixed here. Each needs its stage runtime looked at before a number is
# chosen — sf-pipeline-policy.md §4: "Timeouts are budgets, not
# accommodations. Raise only with a stated reason for the new baseline."
# Picking values to make a test pass is the move that policy forbids, and
# most of these sit on the weekly pipeline, whose behaviour this arc did not
# measure. Tracked as alpha-engine-config-I6897 with the full table.
#
# Pinned as an exact set: the gap cannot widen while that issue is open, and
# an entry cannot outlive its fix without failing.
_KNOWN_UNBOUND: frozenset[tuple[str, str]] = frozenset(
    {
        ("step_function.json", "WeeklyRunDayGate"),  # no TimeoutSeconds at all
        ("step_function.json", "SignalsEnvelope"),
        ("step_function.json", "ChallengerShadow"),
        ("step_function.json", "EvalJudgeSubmitFirstSaturday"),
        ("step_function.json", "EvalJudgeSubmitWeekly"),
        ("step_function.json", "EvalJudgePoll"),
        ("step_function.json", "EvalJudgeProcess"),
        ("step_function.json", "EvalRollingMean"),
        ("step_function.json", "RationaleClustering"),
        ("step_function.json", "ReplayConcordance"),
        ("step_function.json", "Counterfactual"),
        ("step_function.json", "ReportCard"),
        ("step_function.json", "DispatchWeeklyFreshnessSpot"),
        # SF > function: the declaration is decorative, the function governs.
        ("step_function.json", "RegimeSubstrate"),
        ("step_function.json", "RegimeRetrospectiveEval"),
        ("step_function.json", "AggregateCosts"),
        ("step_function.json", "Director"),
        ("step_function_daily.json", "PredictorInference"),
        ("step_function_daily.json", "ReinvokePredictor"),
        ("step_function_eod.json", "ProbeEODReconcilePrecondition"),
        ("step_function_eod.json", "HealReProbe"),
    }
)


def _lambda_invoke_states(states: dict) -> Iterator[tuple[str, str, int | None]]:
    """``(state_name, function_base_name, TimeoutSeconds)`` for every lambda:invoke.

    Recurses into Parallel branches and Map iterators — the weekly Scanner
    lives inside ``ResearchPredictorParallel``, and a top-level-only scan
    would silently exempt it, which is the state this test exists for.
    """
    for name, body in states.items():
        resource = body.get("Resource")
        if isinstance(resource, str) and resource.endswith(":lambda:invoke"):
            raw = str((body.get("Parameters") or {}).get("FunctionName") or "")
            # Either "name:alias" or a full ARN with an optional alias suffix.
            base = raw.split(":")[-2] if raw.startswith("arn:") else raw.split(":")[0]
            yield name, base, body.get("TimeoutSeconds")
        for branch in body.get("Branches") or []:
            yield from _lambda_invoke_states(branch.get("States") or {})
        iterator = body.get("Iterator") or body.get("ItemProcessor")
        if iterator:
            yield from _lambda_invoke_states(iterator.get("States") or {})


def _states(definition: str) -> dict:
    return json.loads((_INFRA / definition).read_text(encoding="utf-8"))["States"]


@pytest.mark.parametrize("definition", _DEFS)
def test_every_lambda_invoke_declares_a_timeout(definition: str) -> None:
    """§4: a stage with no declared timeout inherits the definition ceiling.

    Its hang is then indistinguishable from a slow run until everything
    times out together.
    """
    missing = sorted(
        name
        for name, _, timeout in _lambda_invoke_states(_states(definition))
        if timeout is None and (definition, name) not in _KNOWN_UNBOUND
    )
    assert not missing, f"{definition}: lambda:invoke states with no TimeoutSeconds: {missing}"


@pytest.mark.parametrize("definition", _DEFS)
def test_the_declared_stage_timeout_is_the_one_that_binds(definition: str) -> None:
    """`TimeoutSeconds` strictly below the function's own timeout.

    Equal is a race — whichever ceiling fires first is undefined, so the
    error an operator sees is not reproducible. Greater is decorative.
    """
    violations: list[str] = []
    for name, fn, timeout in _lambda_invoke_states(_states(definition)):
        if (definition, name) in _KNOWN_UNBOUND:
            continue
        fn_timeout = _FUNCTION_TIMEOUTS_SEC.get(fn)
        if fn_timeout is None:
            continue  # covered by test_every_invoked_function_has_a_codified_timeout
        if timeout is None:
            continue  # covered by test_every_lambda_invoke_declares_a_timeout
        if timeout >= fn_timeout:
            violations.append(
                f"{name}: TimeoutSeconds={timeout} >= {fn}'s own {fn_timeout}s "
                f"({'race' if timeout == fn_timeout else 'never binds'})"
            )
    assert not violations, f"{definition}:\n  " + "\n  ".join(violations)


@pytest.mark.parametrize("definition", _DEFS)
def test_every_invoked_function_has_a_codified_timeout(definition: str) -> None:
    """A function missing from the map is an unchecked stage, not a passing one.

    Without this, adding a Lambda no one codified would make the ordering
    test silently skip it — the same shape as the whitelist that made the
    weekday notifier render nothing (config-I6857).
    """
    unknown = sorted(
        {
            fn
            for _, fn, _ in _lambda_invoke_states(_states(definition))
            if fn not in _FUNCTION_TIMEOUTS_SEC and not fn.startswith("function")
        }
    )
    assert not unknown, (
        f"{definition} invokes functions absent from _FUNCTION_TIMEOUTS_SEC: {unknown}. "
        "Add each with its measured `aws lambda get-function-configuration --query Timeout`."
    )


def test_the_known_unbound_set_contains_no_stale_entries() -> None:
    """An exception that outlives its fix silently exempts a healthy stage."""
    live = {(d, name) for d in _DEFS for name, _, _ in _lambda_invoke_states(_states(d))}
    stale = sorted(entry for entry in _KNOWN_UNBOUND if entry not in live)
    assert not stale, f"_KNOWN_UNBOUND names states that no longer exist: {stale}"

    still_broken = set()
    for definition, name in _KNOWN_UNBOUND:
        for state, fn, timeout in _lambda_invoke_states(_states(definition)):
            if state != name:
                continue
            fn_timeout = _FUNCTION_TIMEOUTS_SEC.get(fn)
            if timeout is None or (fn_timeout is not None and timeout >= fn_timeout):
                still_broken.add((definition, name))
    fixed = sorted(set(_KNOWN_UNBOUND) - still_broken)
    assert not fixed, (
        f"these now bind correctly — remove them from _KNOWN_UNBOUND: {fixed}. "
        "Close alpha-engine-config-I6897 when the set is empty."
    )


@pytest.mark.skipif(
    not os.environ.get("SF_TIMEOUT_LIVE_CHECK"),
    reason="needs AWS credentials; set SF_TIMEOUT_LIVE_CHECK=1 to run",
)
def test_the_codified_function_timeouts_match_live() -> None:
    """The map is a claim about the live account; this is what proves it.

    `sf-pipeline-policy.md` §2.4 — verification reads the deployed artifact,
    not the source claiming to produce it. Without this the ordering guard
    would keep passing against timeouts that moved in another repo, which is
    precisely how the Scanner's 300s ceiling stayed invisible from here.
    """
    import boto3

    client = boto3.client("lambda")
    drift: list[str] = []
    for fn, codified in sorted(_FUNCTION_TIMEOUTS_SEC.items()):
        live = client.get_function_configuration(FunctionName=fn)["Timeout"]
        if live != codified:
            drift.append(f"{fn}: codified {codified}s, live {live}s")
    assert not drift, "\n  ".join(drift)
