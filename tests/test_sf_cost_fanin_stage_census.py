"""Every work-dispatching weekly stage is classified for cost attribution.

**The half that was missing.** ``tests/test_sf_cost_fanin_coverage.py`` checks
the declaration against the definition: every stage NAMED in the
``AggregateCosts`` coverage block must exist, be a Task, and precede the
aggregator. That catches a rename. It cannot catch the other direction, which
is the one that costs money — a stage present in the definition and named in
NO set at all. Such a stage is not "allowed"; it is *unconsidered*, and its
silence at fan-in time is indistinguishable from a stage that legitimately
spends nothing.

That is exactly the shape of ``alpha-engine-config-I7179``, where the
aggregator was rolling up Think Tank spend while every LLM-calling stage of
the pipeline emitted nothing, and every scalar on the dashboard was right.
The fix then was to name four producers. Nothing made the naming *exhaustive*,
so the next stage added inherits the same silence.

``infrastructure/cost_fanin_stage_census.json`` is that exhaustive statement,
and this file is what makes it binding: a new Lambda or SSM stage fails CI
until someone writes down which of three things it is and why. `principles.md`
2.7 — a component emitting nothing is not healthy, it is unobserved, and *no
data* is never rendered as green.

**Why three dispositions and not two.** An ``exempt``/``emits`` binary forces
a stage that really does spend money without emitting into the exempt column,
where it reads as "costs nothing" forever. ``unattributed`` is the honest
third answer, and it must carry a tracked issue — so a real gap is a dated
debt rather than a laundered exemption. ``RAGIngestion`` is the live one:
measured 2026-08-31, it calls the paid Voyage embedding API through
``voyageai.Client`` directly rather than through ``krepis.llm.LLMClient``, so
it writes no ``_cost_raw`` object and the fan-in check structurally cannot see
it.

**Scope: work-dispatching stages only.** A stage is in scope when it invokes a
Lambda or runs an SSM shell command — the only two ways this definition causes
code to execute. ``sns:publish``, ``Pass``, ``Choice``, ``Wait`` and the
``aws-sdk`` integrations (``s3:putObject``, ``ssm:getCommandInvocation``,
``dynamodb:putItem``) run no code of ours and cannot call a model. Scoping by
the mechanism rather than by a hand-kept skip list means a new state is in
scope the moment it can execute anything.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_SF = _INFRA / "step_function.json"
_CENSUS = _INFRA / "cost_fanin_stage_census.json"

_VALID_DISPOSITIONS = {"emits", "exempt", "unattributed"}


def _walk(states: dict):
    """Every state in the definition, including inside Branches/Iterators."""
    for name, state in states.items():
        yield name, state
        for branch in state.get("Branches", []) or []:
            yield from _walk(branch["States"])
        inner = state.get("Iterator") or state.get("ItemProcessor")
        if inner:
            yield from _walk(inner["States"])


def _work_dispatching(states: dict) -> dict:
    """Stages that cause code of ours to run: Lambda invoke or SSM shell."""
    out = {}
    for name, state in _walk(states):
        if state.get("Type") != "Task":
            continue
        params = state.get("Parameters") or {}
        resource = state.get("Resource", "")
        if (
            params.get("FunctionName")
            or params.get("DocumentName") == "AWS-RunShellScript"
            or resource.startswith("arn:aws:lambda")
        ):
            out[name] = state
    return out


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF.read_text())


@pytest.fixture(scope="module")
def census() -> dict:
    return json.loads(_CENSUS.read_text())


@pytest.fixture(scope="module")
def stages(sf) -> dict:
    return _work_dispatching(sf["States"])


@pytest.fixture(scope="module")
def coverage(sf) -> dict:
    return sf["States"]["AggregateCosts"]["Parameters"]["Payload"]["coverage"]


def test_the_scope_filter_finds_stages(stages):
    """Guards the check itself: an empty scope would pass every test below
    vacuously, which is the failure mode this whole file exists to refuse."""
    assert len(stages) >= 40, (
        f"only {len(stages)} work-dispatching stage(s) found — the scope filter "
        f"is broken, and a vacuous census is worse than none"
    )


def test_every_work_dispatching_stage_is_classified(stages, census):
    """The load-bearing assertion. A new Lambda or SSM stage cannot merge
    without a written answer to 'does this spend money, and where does the
    record go?'"""
    missing = sorted(set(stages) - set(census["stages"]))
    assert not missing, (
        f"{len(missing)} weekly stage(s) run code and are classified nowhere: "
        f"{', '.join(missing)}. Add each to "
        f"infrastructure/cost_fanin_stage_census.json as 'emits' (and declare "
        f"its producer in the AggregateCosts coverage block), 'exempt' (with a "
        f"reason naming why its substrate cannot call a paid model), or "
        f"'unattributed' (with the tracked issue for the gap). An unclassified "
        f"stage's silence at fan-in time is indistinguishable from a stage that "
        f"costs nothing."
    )


def test_the_census_names_no_stage_that_left_the_definition(stages, census):
    """A stale row is a claim about a topology that no longer exists, and it
    would keep a deleted stage's exemption alive for whatever is added under
    that name next."""
    stale = sorted(set(census["stages"]) - set(stages))
    assert not stale, (
        f"census names {len(stale)} stage(s) the definition no longer "
        f"dispatches work to: {', '.join(stale)}"
    )


@pytest.mark.parametrize("field", ["disposition", "substrate", "reason"])
def test_every_row_is_complete(census, field):
    bad = sorted(n for n, e in census["stages"].items() if not str(e.get(field, "")).strip())
    assert not bad, f"census rows with an empty '{field}': {', '.join(bad)}"


def test_every_disposition_is_one_of_the_three(census):
    bad = {
        n: e.get("disposition")
        for n, e in census["stages"].items()
        if e.get("disposition") not in _VALID_DISPOSITIONS
    }
    assert not bad, f"unknown disposition(s): {bad}"


def test_reasons_are_reasons(census):
    """A one-word reason ('n/a', 'none', 'no') is an absence wearing a
    justification's clothes."""
    thin = sorted(
        n for n, e in census["stages"].items() if len(e["reason"].split()) < 5
    )
    assert not thin, (
        f"census rows whose reason says nothing: {', '.join(thin)}. The reason "
        f"has to name why this stage's substrate cannot spend, specifically "
        f"enough that the next reader can check it."
    )


def test_every_unattributed_row_names_a_tracked_issue(census):
    """An unattributed row is a DEBT. Without an issue it is a permanent
    exemption with a more honest label, which is worse than an exemption
    because it looks like it is being tracked."""
    bad = sorted(
        n for n, e in census["stages"].items()
        if e["disposition"] == "unattributed"
        and not str(e.get("issue", "")).strip()
    )
    assert not bad, (
        f"unattributed stage(s) with no tracked issue: {', '.join(bad)}"
    )


def test_the_emits_set_is_exactly_the_declared_set(census, coverage):
    """The census and the coverage block cannot drift.

    Two files stating the same fact is a defect unless something forces them
    to agree. Declaring a producer without censusing the stage would let the
    census under-report; censusing a stage as 'emits' without declaring a
    producer would claim an attribution the aggregator never checks.
    """
    censused = {n for n, e in census["stages"].items() if e["disposition"] == "emits"}
    declared = set(coverage.get("required_producers") or {}) | set(
        coverage.get("conditional_producers") or {}
    )
    assert censused == declared, (
        f"census 'emits' set and the AggregateCosts coverage block disagree — "
        f"census only: {sorted(censused - declared)}; "
        f"coverage only: {sorted(declared - censused)}"
    )


def test_the_live_unattributed_gap_is_still_recorded(census):
    """RAGIngestion, measured 2026-08-31.

    Pinned so that flipping it to 'exempt' has to happen in a diff someone
    reads, not in a quiet edit. It becomes 'emits' when the Voyage call is
    routed through krepis and declares a producer; it becomes 'exempt' only if
    the paid embedding call leaves the stage entirely.
    """
    row = census["stages"].get("RAGIngestion")
    assert row is not None, "RAGIngestion left the census"
    assert row["disposition"] in {"unattributed", "emits"}, (
        f"RAGIngestion is recorded as '{row['disposition']}'. It calls the paid "
        f"Voyage embedding API (nousergon_lib/rag/embeddings.py, voyageai.Client "
        f"— not krepis.llm.LLMClient), so it cannot be exempt while that call "
        f"stands."
    )
