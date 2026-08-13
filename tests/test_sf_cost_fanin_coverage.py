"""The weekly SF's declaration of which stages produce cost records.

**What this guards, and why a count could not.** ``AggregateCosts`` rolls
``decision_artifacts/_cost_raw/{date}/`` into a daily parquet. On
2026-08-13 that prefix was measured on five separate dates and every
record on every one of them came from a single producer — the Think Tank,
which was removed from this pipeline on 2026-08-10 — while
``ReplayConcordance`` (a per-artifact call, up to 150 artifacts) and the
rest of the weekly chain were attributed to nothing at all
(``alpha-engine-config-I7179``). Five files under the prefix is a healthy
count. The defect is only visible as a **set difference**, so the
declaration this module guards is a set, and the aggregator's check is a
set comparison.

**Why the declaration lives in the Step Function.** The set of stages that
can produce a cost record is a property of the pipeline, and the pipeline
is this file. Putting it in the aggregator's own code would put the
denominator inside the thing being measured, one repo away from every
change that alters it.

**Two directions, both enforced — here structurally, and at runtime by the
aggregator.**

- *Declared but absent* — a stage that ran and emitted nothing. The
  I7179 defect.
- *Present but undeclared* — a producer nobody registered. Since
  ``krepis>=0.57`` emits from the environment rather than from a
  per-call-site argument, a newly added LLM stage emits **by
  construction**, so it arrives in the prefix as an undeclared producer
  and the aggregator refuses it until this map is updated. That is what
  keeps the next stage added from silently reproducing the gap, without
  this test having to guess from the ASL which stages call an LLM — a
  guess that would be wrong today: ``dry_run_llm`` is threaded into
  ``Scanner``, ``RationaleClustering``, ``Counterfactual`` and
  ``AggregateCosts``, none of which make an LLM call, and is absent from
  ``ChallengerShadow`` and ``Director``, which do.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    flat: dict = dict(sf["States"])
    for st in sf["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat


@pytest.fixture(scope="module")
def branch_a(sf) -> dict:
    """The branch AggregateCosts itself lives in.

    Reachability is only meaningful inside one branch: a stage in a
    sibling branch of the same Parallel, or at the top level after it,
    has no ordering relationship with the aggregator that the aggregator
    can rely on.
    """
    for st in sf["States"].values():
        if st.get("Type") != "Parallel":
            continue
        for branch in st["Branches"]:
            if "AggregateCosts" in branch["States"]:
                return branch["States"]
    raise AssertionError("AggregateCosts is not inside a Parallel branch")


@pytest.fixture(scope="module")
def coverage(states) -> dict:
    return states["AggregateCosts"]["Parameters"]["Payload"]["coverage"]


def _successors(state: dict) -> set:
    """Every state name this state can hand control to."""
    out = set()
    if state.get("Next"):
        out.add(state["Next"])
    if state.get("Default"):
        out.add(state["Default"])
    for choice in state.get("Choices", []) or []:
        if choice.get("Next"):
            out.add(choice["Next"])
    for catch in state.get("Catch", []) or []:
        if catch.get("Next"):
            out.add(catch["Next"])
    return out


def _reaches(start: str, target: str, branch: dict) -> bool:
    seen, frontier = set(), {start}
    while frontier:
        name = frontier.pop()
        if name == target:
            return True
        if name in seen or name not in branch:
            continue
        seen.add(name)
        frontier |= _successors(branch[name])
    return False


class TestDeclarationShape:
    def test_coverage_block_exists(self, coverage):
        assert set(coverage) >= {
            "execution_arn.$",
            "required_producers",
            "conditional_producers",
            "allowed_producers",
        }

    def test_execution_arn_is_threaded(self, coverage):
        """Without the execution ARN the aggregator cannot tell a stage
        that ran and went silent from one that never ran — and a check
        that cannot tell those apart fires on every degraded run and is
        then turned off."""
        assert coverage["execution_arn.$"] == "$$.Execution.Id"

    def test_required_producers_is_not_empty(self, coverage):
        """An empty required set is a check that can never fail, which is
        indistinguishable on every surface from a check that passes."""
        assert coverage["required_producers"]

    @pytest.mark.parametrize("key", ["required_producers", "conditional_producers"])
    def test_producer_ids_are_non_empty_strings(self, coverage, key):
        for stage, ids in coverage[key].items():
            assert isinstance(ids, list) and ids, stage
            for callsite_id in ids:
                assert isinstance(callsite_id, str) and callsite_id.strip(), stage

    def test_allowed_producers_is_a_list_of_patterns(self, coverage):
        assert isinstance(coverage["allowed_producers"], list)
        for pattern in coverage["allowed_producers"]:
            assert isinstance(pattern, str) and pattern.strip()


class TestDeclaredStagesAreReal:
    """A renamed state silently empties the required set, and the check
    then passes for the same reason it was written to fail."""

    @pytest.mark.parametrize("key", ["required_producers", "conditional_producers"])
    def test_every_declared_stage_exists(self, coverage, states, key):
        for stage in coverage[key]:
            assert stage in states, f"{key} names a state that does not exist: {stage}"

    def test_every_declared_stage_is_a_task(self, coverage, states):
        for stage in coverage["required_producers"]:
            assert states[stage]["Type"] == "Task", stage


class TestDeclaredStagesCanActuallyBeSeen:
    """The ordering half — the failure that was live when this was written.

    ``Director`` makes the pipeline's single most expensive call and runs
    at the TOP LEVEL, after the Parallel that contains ``AggregateCosts``.
    Its rows cannot be in the parquet the aggregator writes, so demanding
    them would make the coverage check fail on every healthy run. It is
    listed under ``allowed_producers`` instead, and moving the aggregator
    is tracked separately. This test is what stops a later edit from
    quietly promoting a post-aggregator stage into ``required_producers``.
    """

    def test_every_required_stage_precedes_the_aggregator(self, coverage, branch_a):
        for stage in coverage["required_producers"]:
            assert stage in branch_a, (
                f"{stage} is required to have emitted cost rows before "
                f"AggregateCosts runs, but it is not in the aggregator's own "
                f"Parallel branch — the aggregator cannot see its rows"
            )
            assert _reaches(stage, "AggregateCosts", branch_a), (
                f"{stage} cannot reach AggregateCosts, so its cost rows are "
                f"not guaranteed to exist when the aggregator reads the prefix"
            )

    def test_the_aggregator_does_not_require_itself(self, coverage):
        assert "AggregateCosts" not in coverage["required_producers"]


class TestKnownProducersStayDeclared:
    """Pins the four producers measured on 2026-08-13, so a future edit
    that drops one has to say so in a diff rather than in a silence."""

    def test_replay_concordance_is_required(self, coverage):
        """I7176 item A2: ReplayConcordance is wall-killed in 58% of runs
        and its budget cannot be sized until its spend is visible."""
        assert coverage["required_producers"]["ReplayConcordance"] == [
            "replay-concordance"
        ]

    def test_challenger_shadow_is_required(self, coverage):
        assert coverage["required_producers"]["ChallengerShadow"] == [
            "single-agent-quant"
        ]

    def test_eval_judge_batch_is_required(self, coverage):
        """The Anthropic Batches carve-out (`model-router-policy` §4) means
        this spend has no router client to emit at, so EvalJudgeProcess
        emits it at batch decode. It is still required."""
        assert coverage["required_producers"]["EvalJudgeProcess"] == [
            "evaljudge-batch"
        ]

    def test_eval_judge_sync_escalation_is_conditional_not_required(self, coverage):
        """The parse-retry / Sonnet-escalation tail genuinely does not fire
        on every run. Requiring it would make the detector cry wolf, and a
        detector that cries wolf is turned off."""
        assert coverage["conditional_producers"]["EvalJudgeProcess"] == [
            "evaljudge-sync"
        ]

    def test_thinktank_is_allowed_but_never_required(self, coverage):
        """The Think Tank left this pipeline on 2026-08-10 and now runs on
        its own daily cadence, but still writes to the same prefix. It is
        allowed so it does not read as an undeclared producer, and never
        required, because its absence says nothing about this pipeline."""
        assert "thinktank-*" in coverage["allowed_producers"]
        flat_required = {
            cid for ids in coverage["required_producers"].values() for cid in ids
        }
        assert not any(cid.startswith("thinktank-") for cid in flat_required)
