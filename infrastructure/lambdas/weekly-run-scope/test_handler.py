"""The run-scope derivation and handler, against REAL definitions and executions.

Named ``test_handler.py`` because that is the ONLY filename either gate looks
for: `.github/workflows/ci.yml` globs `infrastructure/lambdas/*/test_handler.py`
pre-merge, and `_shared/run_handler_tests.sh` returns 0 for a lambda that has
none. A file called anything else here would be run by neither gate, which is
the exact drift that helper's own docstring was written to kill.

Both fixtures are verbatim captures of live executions of
``ne-weekly-freshness-pipeline`` (Comments stripped from the definition — they
are ~90% of its bytes and nothing here reads them):

``history_all_skip_shell.json``
    ``watch-rerun-2026-08-16-4`` — the execution that terminated SUCCEEDED
    while carrying 22 ``skip_*`` flags. Nothing ran, and every surface in the
    fleet recorded it as a successful weekly run.

``history_real_run_failed.json``
    ``watch-rerun-2026-08-15-1`` — the last execution that did real work, with
    ``skip_parity`` set by the 2026-08-13 ruling.

Synthetic payloads are used only for the degenerate cases. Every structural rule
in ``run_scope.py`` was established by running it against these two files and
finding it wrong — reachability, dominance, sequence adjacency and
``Choices[0]`` each produced a confident, plausible, incorrect answer on one of
them. A synthetic fixture would have agreed with all four.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from run_scope import (
    DISABLED,
    DISPOSITIONS,
    ENABLED_COMPLETED,
    ENABLED_FAILED,
    GRADED_DISPOSITIONS,
    NOT_REACHED,
    build_run_scope,
    derive_gates,
    entered_sequence,
    gate_decisions,
    graded_stage_names,
    work_entry,
)

_FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"


def _load(name: str):
    return json.loads((_FIXTURES / name).read_text())


@pytest.fixture(scope="module")
def definition():
    return _load("definition_2026-08-18.json")


@pytest.fixture(scope="module")
def all_skip_history():
    return _load("history_all_skip_shell.json")


@pytest.fixture(scope="module")
def real_run_history():
    return _load("history_real_run_failed.json")


# ---------------------------------------------------------------------------
# The definition half
# ---------------------------------------------------------------------------


def test_every_gate_is_discovered_from_the_definition_alone(definition):
    """No hand-maintained list anywhere — the denominator IS the definition."""
    gates = derive_gates(definition)
    assert len(gates) == 29
    assert all(name.startswith("CheckSkip") for name in gates)
    assert all(gate["flag"].startswith("skip_") for gate in gates.values())


def test_a_gate_with_two_skip_branches_records_both(definition):
    """`Choices[0]` alone read a real skip as neither branch.

    `CheckSkipPredictorTraining` declares two branches on `skip_predictor_
    training` — a marker state and a weights-freshness assertion. Reading only
    the first target silently degraded PredictorTraining to NOT_REACHED on both
    fixtures.
    """
    gate = derive_gates(definition)["CheckSkipPredictorTraining"]
    assert len(gate["on_disabled"]) == 2
    assert "PredictorTrainingSkipped" in gate["on_disabled"]


def test_a_routing_gate_resolves_to_the_work_state_behind_it(definition):
    """`CheckSkipEvalJudge` points at a Pass, three hops from the real Task."""
    entry, nested = work_entry(definition, "ComputeEvalCadence")
    assert entry == "EvalJudgeSubmitWeekly"
    assert nested == []


def test_the_work_walk_is_bounded(definition):
    """An unbounded walk is how the earlier reachability attempt reached 132
    states from a single branch. A missing work state degrades to None."""
    assert work_entry(definition, "NoSuchState") == (None, [])


# ---------------------------------------------------------------------------
# The execution half
# ---------------------------------------------------------------------------


def test_gate_decisions_follow_the_event_chain_not_adjacency(
    definition, all_skip_history
):
    """Six gates live inside `ResearchPredictorParallel`, whose events interleave.

    Adjacency in the entered-order sequence read `CheckSkipScanner` as followed
    by `CheckSkipPredictorTraining` — a different branch entirely, matching
    neither declared target, silently degrading six stages to NOT_REACHED. The
    `previousEventId` chain resolves it to the skip branch, correctly.
    """
    gates = derive_gates(definition)
    sequence = entered_sequence(all_skip_history)
    index = sequence.index("CheckSkipScanner")
    assert sequence[index + 1] not in gates["CheckSkipScanner"]["on_disabled"]

    assert gate_decisions(gates, all_skip_history)["CheckSkipScanner"] == DISABLED


# ---------------------------------------------------------------------------
# The two real executions, end to end
# ---------------------------------------------------------------------------


def test_the_all_skip_shell_run_grades_almost_nothing(definition, all_skip_history):
    """The execution that terminated SUCCEEDED having run nothing.

    This is the run every surface in the fleet counted toward `sf_success_rate`.
    Scope says what the status code could not: 3 of 29.
    """
    scope = build_run_scope(definition, all_skip_history, run_date="2026-08-15")
    assert scope["counts"][ENABLED_COMPLETED] == 3
    assert scope["counts"][DISABLED] >= 18
    assert scope["graded_stages"] == [
        "ChallengerShadow", "LibPinDriftCheck", "PostEval",
    ]
    assert scope["statement"].startswith("3 of 29 gated stages ran")


def test_the_real_run_names_the_flag_that_disabled_parity(
    definition, real_run_history
):
    """`skip_parity` has been set on the live Saturday trigger since 2026-08-13.

    The Director reported the resulting absence as "the producer never ran this
    cycle" and withheld its acting authority. The scope block says which flag
    did it, which is the fact that was missing.
    """
    scope = build_run_scope(definition, real_run_history, run_date="2026-08-14")
    parity = scope["stages"]["Parity"]
    assert parity["disposition"] == DISABLED
    assert parity["disabled_by"] == "skip_parity"
    assert parity["source"] == "execution_history"


def test_an_outer_gate_over_an_inner_skip_names_the_inner_flag(
    definition, real_run_history
):
    """`CheckSkipBacktester` said run; `CheckSkipBacktesterStageOnly` skipped.

    Naming the outer gate would hand an operator a flag that is not the one to
    flip, and reporting ENABLED_FAILED would invent a failure that did not
    happen.
    """
    scope = build_run_scope(definition, real_run_history, run_date="2026-08-14")
    row = scope["stages"]["Backtester"]
    assert row["disposition"] == DISABLED
    assert row["disabled_by"] == "skip_backtester_stage_only"
    assert row["source"] == "nested_gate"


def test_never_reached_is_never_collapsed_into_disabled(
    definition, all_skip_history
):
    """The whole reason the vocabulary has four values and not three.

    A run that ends upstream leaves stages that are neither switched off nor
    failed. Reading them as disabled would let an execution that died early
    render as a deliberately narrow, fully green cycle.
    """
    scope = build_run_scope(definition, all_skip_history, run_date="2026-08-15")
    not_reached = [
        name for name, row in scope["stages"].items()
        if row["disposition"] == NOT_REACHED
    ]
    assert "PitParityWalkforward" in not_reached
    for name in not_reached:
        assert scope["stages"][name].get("disabled_by") is None
        assert name not in scope["graded_stages"]


def test_the_input_flag_is_reported_without_being_treated_as_a_verdict(
    definition, all_skip_history
):
    """A flag in the input explains a NOT_REACHED row; it does not decide it.

    Blame inferred over the state graph was tried and got this wrong — the
    shared `RouteAfterBootstrapSuccess` relaunch hub makes containment
    unresolvable, and a wrong parent flag is worse than none.
    """
    scope = build_run_scope(
        definition, all_skip_history, run_date="2026-08-15",
        input_flags={"skip_parity": True},
    )
    row = scope["stages"]["Parity"]
    assert row["disposition"] == NOT_REACHED
    assert row["input_flag"] is True
    assert "skip_parity=true" in row["reason"]


# ---------------------------------------------------------------------------
# Invariants that must hold on any input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("history_name", [
    "history_all_skip_shell.json", "history_real_run_failed.json",
])
def test_every_gate_gets_exactly_one_row_from_the_closed_vocabulary(
    definition, history_name
):
    scope = build_run_scope(definition, _load(history_name), run_date="2026-08-14")
    assert len(scope["stages"]) == len(derive_gates(definition))
    assert sum(scope["counts"].values()) == len(scope["stages"])
    for row in scope["stages"].values():
        assert row["disposition"] in DISPOSITIONS
        assert row["reason"]
        assert row["source"] in {"execution_history", "nested_gate"}


@pytest.mark.parametrize("history_name", [
    "history_all_skip_shell.json", "history_real_run_failed.json",
])
def test_graded_stages_match_the_graded_dispositions(definition, history_name):
    scope = build_run_scope(definition, _load(history_name), run_date="2026-08-14")
    assert graded_stage_names(scope) == scope["graded_stages"]
    for name in scope["graded_stages"]:
        assert scope["stages"][name]["disposition"] in GRADED_DISPOSITIONS


@pytest.mark.parametrize("degenerate", [
    None, {}, {"States": {}}, [], "not a definition", 7,
])
def test_no_degenerate_definition_raises_or_grades_anything(degenerate):
    """A producer that cannot describe the run must yield an empty scope, never
    a scope that happens to look complete."""
    scope = build_run_scope(
        degenerate if isinstance(degenerate, dict) else {},
        [], run_date="2026-08-14",
    )
    assert scope["stages"] == {}
    assert scope["graded_stages"] == []
    assert scope["schema_version"] == 1


def test_an_empty_history_never_reads_as_disabled(definition):
    """No history at all is the strongest possible absence of evidence.

    Every stage must land on NOT_REACHED. If it landed on DISABLED, a Lambda
    that failed to fetch its own history would render as a deliberately
    narrowed, fully green run.
    """
    scope = build_run_scope(definition, [], run_date="2026-08-14")
    assert scope["counts"][NOT_REACHED] == len(scope["stages"])
    assert scope["counts"][DISABLED] == 0
    assert scope["graded_stages"] == []


def test_graded_stage_names_withholds_an_unknown_disposition():
    """A fifth disposition from a future producer withholds rather than grades."""
    block = {"stages": {"X": {"disposition": "PROBABLY_FINE"}}}
    assert graded_stage_names(block) == []
    assert graded_stage_names(None) == []
    assert graded_stage_names({"stages": "not a mapping"}) == []



# ---------------------------------------------------------------------------
# The handler — its failure posture, which is the part that can hurt
# ---------------------------------------------------------------------------


def _index(monkeypatch, *, definition=None, history=None, raises=None):
    """Import the handler with boto3 replaced by a recording double."""
    import sys
    import types

    written = {}

    class _States:
        def describe_state_machine(self, **_kw):
            if raises:
                raise raises
            return {"definition": json.dumps(definition or {})}

        def get_paginator(self, _name):
            class _P:
                def paginate(self_inner, **_kw):
                    return [{"events": history or []}]
            return _P()

    class _S3:
        def put_object(self, **kwargs):
            written.update(kwargs)

    fake = types.SimpleNamespace(
        client=lambda name: _States() if name == "stepfunctions" else _S3()
    )
    monkeypatch.setitem(sys.modules, "boto3", fake)
    sys.modules.pop("index", None)
    import index  # noqa: PLC0415
    return index, written


def test_the_handler_writes_the_artifact_for_the_run_date(
    monkeypatch, definition, real_run_history
):
    """``event['run_date']` is the SF's CALENDAR date — a Saturday for this
    pipeline (`InitializeInput` sets it from `$$.Execution.StartTime`). The
    written Key must land on the normalized TRADING day (Friday), the same
    key the sole consumer (crucible-evaluator) reads — alpha-engine-config-
    I8373: it previously landed on the raw Saturday, where nothing ever
    read it.
    """
    index, written = _index(
        monkeypatch, definition=definition, history=real_run_history
    )
    result = index.handler(
        {
            "run_date": "2026-08-15",  # Saturday
            "execution_arn": "arn:x",
            "state_machine_arn": "arn:y",
            "execution_input": {"skip_parity": True, "sns_topic_arn": "arn:z"},
        },
        None,
    )
    assert written["Key"] == "backtest/2026-08-14/run_scope.json"
    assert result["run_date"] == "2026-08-14"
    assert result["calendar_run_date"] == "2026-08-15"
    assert result["stages"]["Parity"]["disabled_by"] == "skip_parity"


def test_a_saturday_run_date_normalizes_to_the_preceding_nyse_session(
    monkeypatch, definition, all_skip_history
):
    """The real live case, alpha-engine-config-I8373: the 2026-08-22 weekly
    execution wrote ``backtest/2026-08-22/run_scope.json`` while the cycle's
    ~49 other artifacts were under ``backtest/2026-08-21/`` — the artifact
    was written where its only consumer could never read it. 2026-08-22 is a
    Saturday; 2026-08-21 is the preceding NYSE trading day (a Friday, no
    intervening holiday).
    """
    index, written = _index(
        monkeypatch, definition=definition, history=all_skip_history
    )
    result = index.handler(
        {
            "run_date": "2026-08-22",
            "execution_arn": "arn:x",
            "state_machine_arn": "arn:y",
        },
        None,
    )
    assert written["Key"] == "backtest/2026-08-21/run_scope.json"
    assert result["run_date"] == "2026-08-21"
    assert result["calendar_run_date"] == "2026-08-22"


def test_an_empty_run_date_raises_rather_than_writing_a_double_slash_key(
    monkeypatch, definition, all_skip_history
):
    """An empty ``run_date`` must never reach ``KEY_TEMPLATE.format`` — that
    would silently write ``backtest//run_scope.json``, a key nothing reads
    and every later listing of ``backtest/`` would misparse. The SF's own
    Catch on the ``RunScope`` state routes any raised exception to
    ``CheckSkipReportCard`` without failing the run (module docstring), so
    raising here is free and turns a silently-misplaced artifact into an
    honestly-absent one.
    """
    index, written = _index(
        monkeypatch, definition=definition, history=all_skip_history
    )
    result = index.handler(
        {"run_date": "", "execution_arn": "arn:x", "state_machine_arn": "arn:y"},
        None,
    )
    assert result["degraded"] is True
    assert "EmptyRunDateError" in result["degraded_reason"]
    assert result["statement"].startswith("SCOPE UNAVAILABLE")
    # Nothing was persisted — an absent artifact, not a misplaced one.
    assert written == {}


def test_only_skip_flags_are_carried_off_the_execution_input(
    monkeypatch, definition, all_skip_history
):
    """The input blob carries SNS ARNs and instance ids too. Only the flags are
    read, and only to explain a row."""
    index, _ = _index(
        monkeypatch, definition=definition, history=all_skip_history
    )
    result = index.handler(
        {
            "run_date": "2026-08-15", "execution_arn": "arn:x",
            "state_machine_arn": "arn:y",
            "execution_input": {"skip_parity": True, "ec2_instance_id": ["i-1"]},
        },
        None,
    )
    flags = {
        row.get("input_flag") for row in result["stages"].values()
        if "input_flag" in row
    }
    assert flags <= {True, False, None}


def test_dry_run_derives_everything_and_writes_nothing(
    monkeypatch, definition, real_run_history
):
    """The Friday-PM shell run must exercise both API grants and the derivation
    without leaving an artifact for a day that had no run."""
    index, written = _index(
        monkeypatch, definition=definition, history=real_run_history
    )
    result = index.handler(
        {"run_date": "2026-08-14", "execution_arn": "a", "state_machine_arn": "b",
         "dry_run": True},
        None,
    )
    assert result["dry_run"] is True
    assert result["stages"]
    assert written == {}


def test_a_failed_derivation_grades_nothing_rather_than_failing_the_run(
    monkeypatch, definition
):
    """The fail-open path, and the reason it is safe.

    An advisory artifact must not kill a run that produced real trading
    artifacts — but failing open is only defensible because the degraded block
    grades NOTHING. If it emitted a scope that looked complete, a broken Lambda
    would render as a narrow, clean, fully green cycle.
    """
    index, written = _index(monkeypatch, raises=RuntimeError("no such execution"))
    result = index.handler(
        {"run_date": "2026-08-14", "execution_arn": "a", "state_machine_arn": "b"},
        None,
    )
    assert result["degraded"] is True
    assert result["graded_stages"] == []
    assert result["statement"].startswith("SCOPE UNAVAILABLE")
    assert "RuntimeError" in result["degraded_reason"]
    # Still persisted: an absent artifact and an unmeasured one must not look
    # the same to the consumer.
    assert written["Key"] == "backtest/2026-08-14/run_scope.json"
