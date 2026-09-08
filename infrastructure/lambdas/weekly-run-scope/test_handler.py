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

import io
import json
import logging
import pathlib

import pytest

from run_scope import (
    AUTHORITY,
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
    merge_run_scopes,
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


class _S3Error(Exception):
    """A botocore-shaped error. ``index._error_code`` is duck-typed against the
    ``response`` envelope precisely so these branches can be driven without
    botocore in the test path."""

    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _FakeS3:
    """An S3 double with the ONE behaviour this fix depends on: conditional
    writes.

    ``IfNoneMatch: "*"`` (create-only) and ``IfMatch: <etag>`` (compare-and-swap)
    are enforced here rather than assumed, because they are the mechanism that
    makes the guard hold when the Lambda cannot read the incumbent at all.
    """

    def __init__(self, store=None, *, read_error=None):
        self.store = dict(store or {})
        self.etags = {k: f'"etag-{i}"' for i, k in enumerate(self.store)}
        self.read_error = read_error
        self.writes = []

    def get_object(self, *, Bucket, Key):  # noqa: N803
        if self.read_error:
            raise _S3Error(self.read_error)
        if Key not in self.store:
            raise _S3Error("NoSuchKey")
        body = json.dumps(self.store[Key]).encode()
        return {"Body": io.BytesIO(body), "ETag": self.etags[Key]}

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        if kwargs.get("IfNoneMatch") == "*" and key in self.store:
            raise _S3Error("PreconditionFailed")
        if "IfMatch" in kwargs and self.etags.get(key) != kwargs["IfMatch"]:
            raise _S3Error("PreconditionFailed")
        self.store[key] = json.loads(kwargs["Body"].decode())
        self.etags[key] = f'"etag-{len(self.writes)}"'
        self.writes.append(kwargs)
        return {}


def _index(monkeypatch, *, definition=None, history=None, raises=None, s3=None):
    """Import the handler with boto3 replaced by a recording double."""
    import sys
    import types

    written = {}
    s3 = s3 if s3 is not None else _FakeS3()

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

    class _RecordingS3:
        def __getattr__(self, name):
            attr = getattr(s3, name)
            if name != "put_object":
                return attr

            def _put(**kwargs):
                result = attr(**kwargs)
                written.clear()
                written.update(kwargs)
                return result
            return _put

    fake = types.SimpleNamespace(
        client=lambda name: _States() if name == "stepfunctions" else _RecordingS3()
    )
    monkeypatch.setitem(sys.modules, "boto3", fake)
    sys.modules.pop("index", None)
    import index  # noqa: PLC0415
    index.fake_s3 = s3
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
    # alpha-engine-config-I10172: the stage-coverage self-assertion — never
    # raises without live AWS credentials (record_verdict is fail-soft) and
    # keyed on the same normalized TRADING day the artifact itself landed on.
    assert result["stage_coverage"]["stage"] == "RunScope"


def test_stage_coverage_assertion_is_import_guarded_and_loud(monkeypatch, definition, real_run_history):
    """The nousergon-lib/krepis pin may predate the module; an inert
    assertion must stay distinguishable from a covered stage."""
    index, _written = _index(monkeypatch, definition=definition, history=real_run_history)
    body = pathlib.Path(index.__file__).read_text()
    assert "from krepis.stage_coverage import assert_stage_coverage" in body
    assert "except ImportError as exc:" in body
    assert '"status": "UNMEASURED"' in body


def test_stage_coverage_is_unmeasured_without_a_run_date(monkeypatch, definition, real_run_history):
    """`EmptyRunDateError` (or any total derivation failure) must never
    fabricate a run_date for the coverage assertion — UNMEASURED, not a
    guessed date (alpha-engine-config-I8155)."""
    index, _written = _index(monkeypatch, definition=definition, history=real_run_history)
    result = index.handler({"run_date": "", "execution_arn": "arn:x", "state_machine_arn": "arn:y"}, None)
    assert result["stage_coverage"] == {
        "stage": "RunScope",
        "status": "UNMEASURED",
        "reason": "no run_date on state input",
    }


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


# ---------------------------------------------------------------------------
# The clobber guard — one key per cycle, several executions writing it
# (alpha-engine-config-I8811)
# ---------------------------------------------------------------------------

_KEY = "backtest/2026-08-14/run_scope.json"


def _scheduled_then_rerun(monkeypatch, definition, first_history, second_history):
    """Write a cycle's scope from one execution, then attempt it from another.

    Both bodies are derived from REAL captured executions, and the second write
    goes through the same conditional-write path production uses — the store is
    not hand-assembled between the two.
    """
    s3 = _FakeS3()
    index, _ = _index(monkeypatch, definition=definition, history=first_history, s3=s3)
    scheduled = index.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:scheduled",
         "state_machine_arn": "arn:y",
         "execution_input": {"skip_parity": True}},
        None,
    )
    index2, written = _index(
        monkeypatch, definition=definition, history=second_history, s3=s3
    )
    rerun = index2.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:watch-rerun-2026-08-28-13",
         "state_machine_arn": "arn:y",
         "execution_input": {"skip_backtester": True, "skip_parity": True}},
        None,
    )
    return s3, scheduled, rerun, written


def test_a_skip_flagged_rerun_cannot_demote_a_scheduled_runs_scope(
    monkeypatch, definition, real_run_history, all_skip_history, caplog
):
    """THE regression, in the exact 2026-08-28 shape.

    A scheduled run writes the cycle's scope; a later skip-flagged recovery
    rerun runs ``RunScope`` over the same ``run_date`` and derives a scope in
    which almost nothing ran. Pre-fix, the rerun's body simply replaced the
    scheduled run's — which is how both artifacts on S3 on 2026-08-31 came to
    claim ``Backtester: DISABLED`` for cycles whose backtester artifacts exist,
    and how ``Parity`` came to read ``NOT_REACHED`` on a cycle where it was
    deliberately ``DISABLED`` by ``skip_parity``.

    Both halves of that matter for clause 5: ``NOT_REACHED`` does not arm
    ``NOT_IN_SCOPE`` in ``crucible-evaluator``'s
    ``attestation._mark_scope_out_of_scope`` and ``DISABLED`` does, so the
    demotion alone is enough to hold ``contamination_verdict`` at UNKNOWN over a
    clean week.
    """
    with caplog.at_level(logging.ERROR):
        s3, scheduled, rerun, _ = _scheduled_then_rerun(
            monkeypatch, definition, real_run_history, all_skip_history
        )

    stored = s3.store[_KEY]
    # The scheduled run established these; the rerun claimed weaker ones.
    assert scheduled["stages"]["Parity"]["disposition"] == DISABLED
    assert scheduled["stages"]["Evaluator"]["disposition"] == ENABLED_COMPLETED
    assert stored["stages"]["Parity"]["disposition"] == DISABLED
    assert stored["stages"]["Parity"]["disabled_by"] == "skip_parity"
    assert stored["stages"]["Evaluator"]["disposition"] == ENABLED_COMPLETED
    assert stored["stages"]["Parity"]["recorded_by_execution_arn"] == "arn:scheduled"

    # The refusal is DURABLE (in the artifact) and LOUD (an ERROR line naming
    # both sides) — a guard whose refusal leaves no trace is indistinguishable
    # from the silent overwrite it replaced.
    refused = {r["stage"]: r for r in stored["scope_merge"]["rejected"]}
    assert refused["Parity"]["kept"] == DISABLED
    assert refused["Parity"]["refused"] == NOT_REACHED
    assert refused["Evaluator"]["refused"] == DISABLED
    assert refused["Parity"]["kept_from"] == "arn:scheduled"
    assert any("write REFUSED for stage Parity" in r.message for r in caplog.records)
    # The returned block is the merged truth, not this execution's derivation.
    assert rerun["stages"]["Parity"]["disposition"] == DISABLED


def test_the_guard_fails_when_removed(
    monkeypatch, definition, real_run_history, all_skip_history
):
    """Prove the guard is load-bearing: with the merge neutralised, the same
    two executions reproduce the live defect.

    A guard not verified to fail is not a guard. Neutralising is done by
    replacing ``merge_run_scopes`` with the pre-fix behaviour — last writer
    wins — rather than by editing the assertion, so what is demonstrated is the
    mechanism and not the test.
    """
    import index as _probe  # noqa: PLC0415,F401

    s3 = _FakeS3()
    index, _ = _index(monkeypatch, definition=definition, history=real_run_history, s3=s3)
    index.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:scheduled",
         "state_machine_arn": "arn:y", "execution_input": {"skip_parity": True}},
        None,
    )
    index2, _ = _index(monkeypatch, definition=definition, history=all_skip_history, s3=s3)
    monkeypatch.setattr(
        index2, "merge_run_scopes",
        lambda _incumbent, incoming: (incoming, {"merged": False, "accepted": [],
                                                 "rejected": []}),
    )
    index2.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:rerun",
         "state_machine_arn": "arn:y", "execution_input": {"skip_backtester": True}},
        None,
    )
    # Pre-fix behaviour, reproduced exactly: the scheduled run's deliberate
    # DISABLED becomes NOT_REACHED, which is what denies clause 5.
    assert s3.store[_KEY]["stages"]["Parity"]["disposition"] == NOT_REACHED
    assert s3.store[_KEY]["stages"]["Evaluator"]["disposition"] == DISABLED


def test_a_rerun_that_genuinely_re_runs_a_stage_still_records_it(
    monkeypatch, definition, all_skip_history, real_run_history
):
    """The other half of the requirement, and the reason immutability was
    rejected.

    Reversed order: the skip-flagged shell run writes first, then an execution
    that really dispatched the stages writes. Every stronger claim lands —
    otherwise the fix would have traded a fail-open for a fail-stuck, and a
    genuine recovery could never record what it re-ran.
    """
    s3, first, second, _ = _scheduled_then_rerun(
        monkeypatch, definition, all_skip_history, real_run_history
    )
    stored = s3.store[_KEY]
    assert first["stages"]["Evaluator"]["disposition"] == DISABLED
    assert stored["stages"]["Evaluator"]["disposition"] == ENABLED_COMPLETED
    accepted = {a["stage"] for a in stored["scope_merge"]["accepted"]}
    assert "Evaluator" in accepted
    assert stored["stages"]["Evaluator"]["recorded_by_execution_arn"] \
        == "arn:watch-rerun-2026-08-28-13"
    # Both executions are named on the artifact, per row and in aggregate.
    assert len(stored["contributing_executions"]) == 2


def test_a_failed_derivation_never_erases_an_established_scope(
    monkeypatch, definition, real_run_history
):
    """The fail-open path is itself a clobber vector, and was one.

    A later execution whose derivation raises publishes a block with
    ``degraded: True`` and zero rows — and the consumer reads ``degraded`` as
    SCOPE UNKNOWN. Writing that over a good scope would take a measured cycle
    to unmeasured, which is the same destruction in a different costume.
    """
    s3 = _FakeS3()
    index, _ = _index(monkeypatch, definition=definition, history=real_run_history, s3=s3)
    index.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:scheduled",
         "state_machine_arn": "arn:y", "execution_input": {"skip_parity": True}},
        None,
    )
    index2, _ = _index(monkeypatch, raises=RuntimeError("no such execution"), s3=s3)
    result = index2.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:broken",
         "state_machine_arn": "arn:y"},
        None,
    )
    stored = s3.store[_KEY]
    assert "degraded" not in stored
    assert stored["stages"]["Evaluator"]["disposition"] == ENABLED_COMPLETED
    assert "RuntimeError" in stored["scope_merge"]["incoming_degraded_reason"]
    assert result["scope_merge"]["incoming_degraded"] is True


def test_an_unreadable_incumbent_is_declined_not_overwritten(
    monkeypatch, definition, real_run_history, all_skip_history, caplog
):
    """The posture while the ``s3:GetObject`` grant lags the code deploy.

    ``deploy-weekly-run-scope.yml`` ships CODE ONLY — its OIDC role has no
    ``iam:PutRolePolicy`` by design — so the new grant in ``iam-policy.json``
    reaches the role on a later operator ``--apply-iam``. In between, the
    Lambda cannot read the incumbent. It must therefore not be able to destroy
    it either: the write degrades to create-only (``IfNoneMatch: "*"``), S3
    itself refuses it, and the handler declines at ERROR. The fix is safe from
    the moment the code lands, not from the moment the grant does.
    """
    s3 = _FakeS3()
    index, _ = _index(monkeypatch, definition=definition, history=real_run_history, s3=s3)
    index.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:scheduled",
         "state_machine_arn": "arn:y", "execution_input": {"skip_parity": True}},
        None,
    )
    established = json.loads(json.dumps(s3.store[_KEY]))

    s3.read_error = "AccessDenied"
    index2, _ = _index(monkeypatch, definition=definition, history=all_skip_history, s3=s3)
    with caplog.at_level(logging.ERROR):
        result = index2.handler(
            {"run_date": "2026-08-15", "execution_arn": "arn:rerun",
             "state_machine_arn": "arn:y"},
            None,
        )
    assert s3.store[_KEY] == established          # untouched
    assert result["write_declined"]["key"] == _KEY
    assert "AccessDenied" in result["write_declined"]["reason"]
    assert any("write DECLINED" in r.message for r in caplog.records)


def test_the_first_write_of_a_cycle_is_create_only(
    monkeypatch, definition, real_run_history
):
    """No incumbent -> ``IfNoneMatch: "*"``. A concurrent execution that wrote
    between our read and our write loses the race instead of being silently
    replaced."""
    index, written = _index(
        monkeypatch, definition=definition, history=real_run_history
    )
    index.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:scheduled",
         "state_machine_arn": "arn:y"},
        None,
    )
    assert written["IfNoneMatch"] == "*"
    assert "IfMatch" not in written


def test_a_merge_write_is_compare_and_swap(
    monkeypatch, definition, real_run_history, all_skip_history
):
    """Incumbent read -> ``IfMatch`` its ETag. Read-merge-write without a
    condition is a race with a longer window, not a fix."""
    s3, _, _, written = _scheduled_then_rerun(
        monkeypatch, definition, real_run_history, all_skip_history
    )
    assert written["IfMatch"].startswith('"etag-')
    assert "IfNoneMatch" not in written


def test_a_lost_race_is_retried_against_the_new_incumbent(
    monkeypatch, definition, real_run_history, all_skip_history
):
    """A 412 on a MERGE write means somebody else wrote between our read and
    our put. That is a retry, not a refusal — the merge is simply recomputed
    against what is now there."""
    s3 = _FakeS3()
    index, _ = _index(monkeypatch, definition=definition, history=real_run_history, s3=s3)
    index.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:scheduled",
         "state_machine_arn": "arn:y", "execution_input": {"skip_parity": True}},
        None,
    )
    real_put = s3.put_object
    calls = {"n": 0}

    def _flaky(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            s3.etags[_KEY] = '"etag-moved"'   # somebody else wrote first
            raise _S3Error("PreconditionFailed")
        return real_put(**kwargs)

    s3.put_object = _flaky
    index2, _ = _index(monkeypatch, definition=definition, history=all_skip_history, s3=s3)
    index2.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:rerun",
         "state_machine_arn": "arn:y"},
        None,
    )
    assert calls["n"] == 2
    assert s3.store[_KEY]["stages"]["Parity"]["disposition"] == DISABLED


def test_a_permanent_race_raises_rather_than_forcing_the_write(
    monkeypatch, definition, real_run_history, all_skip_history
):
    """Exhausting the attempts raises. The SF's Catch on ``RunScope`` routes it
    to ``CheckSkipReportCard`` without failing the weekly run, and the cycle
    keeps the scope already on S3 — never a forced overwrite."""
    s3 = _FakeS3()
    index, _ = _index(monkeypatch, definition=definition, history=real_run_history, s3=s3)
    index.handler(
        {"run_date": "2026-08-15", "execution_arn": "arn:scheduled",
         "state_machine_arn": "arn:y"},
        None,
    )

    def _always_conflict(**_kwargs):
        raise _S3Error("PreconditionFailed")

    s3.put_object = _always_conflict
    index2, _ = _index(monkeypatch, definition=definition, history=all_skip_history, s3=s3)
    with pytest.raises(index2.ScopeWriteConflictError):
        index2.handler(
            {"run_date": "2026-08-15", "execution_arn": "arn:rerun",
             "state_machine_arn": "arn:y"},
            None,
        )


def test_the_dry_run_exercises_the_incumbent_read_and_writes_nothing(
    monkeypatch, definition, real_run_history
):
    """The Friday rehearsal is where a missing grant must surface — a rehearsal
    that skipped the new read would leave it to be discovered by the Saturday
    run it exists to protect."""
    s3 = _FakeS3(read_error="AccessDenied")
    index, written = _index(
        monkeypatch, definition=definition, history=real_run_history, s3=s3
    )
    result = index.handler(
        {"run_date": "2026-08-14", "execution_arn": "a", "state_machine_arn": "b",
         "dry_run": True},
        None,
    )
    assert result["dry_run"] is True
    assert result["incumbent_read"]["ok"] is False
    assert "AccessDenied" in result["incumbent_read"]["note"]
    assert written == {}
    assert s3.store == {}


# ---------------------------------------------------------------------------
# The merge rule itself
# ---------------------------------------------------------------------------


def test_authority_ranks_dispatch_above_a_later_skip():
    """The ordering IS the rule, so it is asserted directly rather than only
    through its consequences. Dispatch is a fact about the cycle that a later
    skip cannot unmake; an absence of evidence ranks below a decision."""
    assert AUTHORITY[NOT_REACHED] < AUTHORITY[DISABLED] \
        < AUTHORITY[ENABLED_FAILED] < AUTHORITY[ENABLED_COMPLETED]


@pytest.mark.parametrize(
    "held,offered,expected",
    [
        (ENABLED_COMPLETED, DISABLED, ENABLED_COMPLETED),
        (ENABLED_COMPLETED, NOT_REACHED, ENABLED_COMPLETED),
        (ENABLED_FAILED, DISABLED, ENABLED_FAILED),
        (DISABLED, NOT_REACHED, DISABLED),
        (NOT_REACHED, DISABLED, DISABLED),
        (NOT_REACHED, ENABLED_COMPLETED, ENABLED_COMPLETED),
        (ENABLED_FAILED, ENABLED_COMPLETED, ENABLED_COMPLETED),
        (ENABLED_COMPLETED, "PROBABLY_FINE", ENABLED_COMPLETED),
    ],
)
def test_a_row_is_replaced_only_by_a_strictly_stronger_claim(held, offered, expected):
    incumbent = {"run_date": "2026-08-14", "execution_arn": "arn:first",
                 "stages": {"Parity": {"disposition": held,
                                       "recorded_by_execution_arn": "arn:first"}}}
    incoming = {"run_date": "2026-08-14", "execution_arn": "arn:second",
                "stages": {"Parity": {"disposition": offered,
                                      "recorded_by_execution_arn": "arn:second"}}}
    merged, ledger = merge_run_scopes(incumbent, incoming)
    assert merged["stages"]["Parity"]["disposition"] == expected
    assert merged["counts"][expected] == 1 if expected in DISPOSITIONS else True
    assert ledger["merged"] is True


def test_the_merge_never_raises_on_a_degenerate_incumbent():
    """An unreadable or foreign incumbent must not strand a cycle forever on a
    corrupt artifact — it is treated as no incumbent and recorded as such."""
    incoming = {"run_date": "2026-08-14", "stages":
                {"Parity": {"disposition": DISABLED}}}
    for junk in (None, "", [], {"stages": "nope"}, {}):
        merged, ledger = merge_run_scopes(junk, json.loads(json.dumps(incoming)))
        assert merged["stages"]["Parity"]["disposition"] == DISABLED
        assert ledger["merged"] is False


def test_the_merged_statement_is_recomputed_not_carried():
    """The statement is the denominator every downstream grade is rendered
    against. Carrying either input's is how a surface goes quietly green."""
    incumbent = {"run_date": "2026-08-14", "statement": "stale.",
                 "stages": {"A": {"disposition": ENABLED_COMPLETED},
                            "B": {"disposition": ENABLED_COMPLETED}}}
    incoming = {"run_date": "2026-08-14", "statement": "also stale.",
                "stages": {"A": {"disposition": NOT_REACHED},
                           "C": {"disposition": DISABLED}}}
    merged, _ = merge_run_scopes(incumbent, incoming)
    assert merged["statement"] == (
        "2 of 3 gated stages ran and are graded; 1 disabled by operator flag."
    )
    assert merged["graded_stages"] == ["A", "B"]
