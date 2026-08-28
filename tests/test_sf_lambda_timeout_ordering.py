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
  SF >  function   DEFECT *unless* the function sits at Lambda's 900s
                   SERVICE MAXIMUM. Normally the declaration is decorative
                   and the function's ceiling silently governs. But when the
                   function is already at 900 there is no larger value to
                   give it, and the choice inverts: SF slightly ABOVE 900
                   makes the LAMBDA's timeout fire first, which yields a
                   REPORT line with a duration and the function's logs.
                   SF below 900 makes States.Timeout abort the state while
                   the function keeps running — billed, orphaned, and
                   silent. A guard band above a service-max function is a
                   deliberate choice, not a decorative declaration.

So this file asserts `TimeoutSeconds < function timeout`, and that one is
declared at all — with `_SERVICE_MAX_GUARD_BAND` carved out below.

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
    # alpha-engine-config-I7620. Two read-only Step Functions API calls plus a
    # pure in-memory derivation — no S3 reads, no spot, no model call. Matches
    # the --bootstrap timeout in
    # infrastructure/lambdas/weekly-run-scope/deploy.sh; a generous ceiling here
    # would let an advisory state hold the tail of the weekly run.
    "alpha-engine-weekly-run-scope": 60,
    # alpha-engine-config-I8214. ListExecutions + one DescribeExecution and
    # GetExecutionHistory per contributing execution, a prefix listing, one
    # PUT and one marker read-modify-write. Bounded by the cycle's execution
    # count (single digits), not by the ticker universe. Matches the
    # --bootstrap timeout in
    # infrastructure/lambdas/weekly-coverage-sweep/deploy.sh; the SF state's
    # 240s ceiling is deliberately below it so the SF is the one that binds.
    # MEASURED 2026-08-22 on the first live invocation: 29.4s at 1024 MB,
    # 675 MB peak, over a 40-execution walk. The original 120s/256 MB sizing
    # timed out — at 256 MB the function used 256 of 256 and Lambda scales CPU
    # with memory, so it was CPU-throttled as well as memory-starved.
    "alpha-engine-weekly-coverage-sweep": 300,
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
        ("step_function.json", "SignalsEnvelope"),
        ("step_function.json", "ChallengerShadow"),
        ("step_function.json", "EvalJudgeSubmitFirstSaturday"),
        ("step_function.json", "EvalJudgeSubmitWeekly"),
        ("step_function.json", "EvalJudgePoll"),
        # EvalRollingMean REMOVED 2026-08-28 (alpha-engine-config#9102): it now
        # binds at 240s against the function's 300s. The reason the policy asks
        # for was measured, not chosen — the state burned its full 300s budget
        # after 1.6s of handler work, because an unbounded boto3 S3 client in
        # flow-doctor's notifier preflight (flow-doctor#93, fixed in 0.16.2) held
        # the invocation open. With SF and function both at 300 the stop arrived
        # as an opaque Lambda-side timeout rather than an SF error attributable
        # to this state, and it fail-opened the whole research/predictor branch.
        ("step_function.json", "RationaleClustering"),
        ("step_function.json", "Counterfactual"),
        ("step_function.json", "ReportCard"),
        ("step_function.json", "DispatchWeeklyFreshnessSpot"),
        ("step_function_daily.json", "PredictorInference"),
        ("step_function_daily.json", "ReinvokePredictor"),
        ("step_function_eod.json", "ProbeEODReconcilePrecondition"),
        ("step_function_eod.json", "HealReProbe"),
    }
)


# Lambda's hard service maximum. A function AT this value cannot be raised, so
# the SF-vs-function ordering rule inverts for it — see the module docstring.
_LAMBDA_SERVICE_MAX_SEC = 900

# States deliberately declaring a guard band ABOVE a service-max function.
# Not exceptions to the rule; instances of the rule's second branch.
_SERVICE_MAX_GUARD_BAND: dict[tuple[str, str], int] = {
    # alpha-engine-evaluator-director is pinned at the 900s service maximum
    # (crucible-evaluator-PR196, 2026-08-13: the measured requirement is ~195s
    # and the real defect was multiplicative retry loops, not call size —
    # 2 transport x 2 krepis body-level x 3 evaluator = up to 12 model calls
    # against a budget funding 2). 930 gives the function 30s to time out and
    # emit its REPORT line before the state would abort it.
    ("step_function.json", "Director"): 930,
    # alpha-engine-replay-concordance and alpha-engine-research-eval-judge-process
    # are both at the 900s maximum AND are now self-deadlining: their loops ask
    # before each item whether one more fits, then stop and return a PARTIAL with
    # the residue recorded (crucible-backtester#633 for concordance, measured live
    # 2026-08-11 returning at 622s with "141 of 150 artifacts not replayed";
    # crucible-research-PR613 for the judge). A self-deadlining function's clock
    # starts at INVOKE; the SF clock starts at SCHEDULE, earlier by dispatch plus
    # cold start. An equal ceiling therefore guarantees the SF fires first and
    # pre-empts the graceful partial return -- the state would be killed at the
    # wall precisely because the function learned not to be. 60s over the measured
    # 2.0-3.0s Init Duration, rounded an order of magnitude so a cold-start
    # regression cannot recreate the race. alpha-engine-config-I7181.
    ("step_function.json", "ReplayConcordance"): 960,
    ("step_function.json", "EvalJudgeProcess"): 960,
}


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
        band = _SERVICE_MAX_GUARD_BAND.get((definition, name))
        if band is not None:
            # The rule's second branch: the function is at Lambda's service
            # maximum and cannot be raised, so the guard band is deliberate.
            # Still pinned to an exact value — a band is a declared number, and
            # a drifting one is back to being decorative.
            assert fn_timeout == _LAMBDA_SERVICE_MAX_SEC, (
                f"{name} is carved out as a service-max guard band, but {fn} "
                f"is {fn_timeout}s, not the {_LAMBDA_SERVICE_MAX_SEC}s maximum "
                f"— raise the function instead, and drop the carve-out"
            )
            assert timeout == band, (
                f"{name}: TimeoutSeconds={timeout}, expected the declared "
                f"guard band {band}"
            )
            continue
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


# ── SSM sendCommand stages (alpha-engine-config-I6948) ───────────────────────
#
# The same "declare a ceiling the service can never reach" defect, in the other
# direction and with a worse failure mode. An `aws-sdk:ssm:sendCommand` task has
# two ceilings: the state's `TimeoutSeconds` and SSM's own `executionTimeout`
# parameter. Which one should bind is the OPPOSITE of the lambda:invoke case:
#
#   SSM <  SF     CORRECT. SSM kills the command, the agent returns
#                 `Status=TimedOut ResponseCode=137 ExecutionTimedOut`, the poll
#                 state reads it, and the operator gets a named cause.
#   SSM == SF     RACE. Undefined which fires; the error is not reproducible.
#   SSM >  SF     DEFECT, and expensive. The state gives up while the command
#                 KEEPS RUNNING on the box — Step Functions does not cancel an
#                 SSM invocation. The stage fails with a bare `States.Timeout`
#                 naming nothing, a spot instance carries on billing, and the
#                 declared SSM budget is decorative.
#
# Found by this guard on its first run: `RAGIngestion` declared
# `executionTimeout=21600` against `TimeoutSeconds=3660`. config#2938
# (2026-07-18) had deliberately widened that budget 3600 -> 14400 -> 21600 after
# live measurement — "the SEC-filings phase alone needs >=1h before the news
# sweep starts", with the Polygon leg covering ~944 tickers at 5 req/min
# (~3.15h). `TimeoutSeconds` was never moved with it, so the state has been
# killing at 61 minutes ever since and the measured widening has been inert. The
# commit message records the intent; nothing enforced it.
#
# I6948 asked for the ordering to be added to this file specifically, so it
# lives here rather than in a sibling — the defect class is identical ("a stage
# timeout that can never bind"), only the correct direction differs.


def _ssm_send_command_states(states: dict) -> Iterator[tuple[str, int | None, int | None]]:
    """``(state_name, TimeoutSeconds, executionTimeout)`` for every sendCommand.

    Recurses the same way as :func:`_lambda_invoke_states` — every spot-bearing
    stage of the weekly pipeline lives inside `ResearchPredictorParallel` or
    `ParityParallel`, so a top-level-only scan would exempt all of them.

    ``executionTimeout`` is an SSM *document parameter*, so it arrives as a list
    of strings: ``{"Parameters": {"executionTimeout": ["5400"]}}``.
    """
    for name, body in states.items():
        resource = body.get("Resource")
        if isinstance(resource, str) and resource.endswith(":ssm:sendCommand"):
            params = ((body.get("Parameters") or {}).get("Parameters") or {})
            raw = params.get("executionTimeout")
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            try:
                execution_timeout = int(raw) if raw is not None else None
            except (TypeError, ValueError):
                execution_timeout = None
            yield name, body.get("TimeoutSeconds"), execution_timeout
        for branch in body.get("Branches") or []:
            yield from _ssm_send_command_states(branch.get("States") or {})
        iterator = body.get("Iterator") or body.get("ItemProcessor")
        if iterator:
            yield from _ssm_send_command_states(iterator.get("States") or {})


@pytest.mark.parametrize("definition", _DEFS)
def test_every_ssm_send_command_declares_both_ceilings(definition: str) -> None:
    """Neither budget may be left implicit.

    A missing `executionTimeout` takes the SSM document default; a missing
    `TimeoutSeconds` takes the definition ceiling. Either way the stage's real
    budget is somewhere other than the stage.
    """
    missing = [
        f"{name}: TimeoutSeconds={sf} executionTimeout={ssm}"
        for name, sf, ssm in _ssm_send_command_states(_states(definition))
        if sf is None or ssm is None
    ]
    assert not missing, f"{definition}:\n  " + "\n  ".join(missing)


@pytest.mark.parametrize("definition", _DEFS)
def test_ssm_kills_the_command_before_the_state_gives_up(definition: str) -> None:
    """`executionTimeout` strictly below `TimeoutSeconds`.

    Ordered this way the operator sees `TimedOut/137/ExecutionTimedOut` naming
    the SSM command. Inverted, the state abandons a command that is still
    running: bare `States.Timeout`, no cause, and a spot instance still billing.
    """
    violations: list[str] = []
    for name, sf, ssm in _ssm_send_command_states(_states(definition)):
        if sf is None or ssm is None:
            continue  # covered by the declaration test above
        if ssm >= sf:
            violations.append(
                f"{name}: executionTimeout={ssm} >= TimeoutSeconds={sf} "
                f"({'race' if ssm == sf else 'the state abandons a live command'})"
            )
    assert not violations, f"{definition}:\n  " + "\n  ".join(violations)


@pytest.mark.parametrize("definition", _DEFS)
def test_the_ssm_scan_is_not_empty(definition: str) -> None:
    """A recursion bug would make every assertion above vacuously true.

    `step_function.json` alone carries 19 spot-bearing stages, all of them
    nested inside Parallel branches. A scan returning nothing would report
    clean — the empty-denominator shape this fleet keeps re-shipping.
    """
    found = list(_ssm_send_command_states(_states(definition)))
    assert found, f"{definition}: no ssm:sendCommand states found — the walk is broken"
