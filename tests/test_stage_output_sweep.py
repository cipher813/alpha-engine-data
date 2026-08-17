"""
Tests for validators/stage_output_sweep.py — the stage-level output assertion
(alpha-engine-config-I7167).

Two obligations shape this file, both from measured fleet failures rather than
from a coverage target:

1. **The assertion must be PROVEN able to fail.** This fleet has shipped three
   detectors that could not — an assertion whose failing branch is never
   exercised is indistinguishable from one that always passes, and the
   difference only shows up on the run you needed it for. Every verdict here
   has a test that produces it, and ``test_enforce_exits_1_on_a_defect`` walks
   the whole path from a stale key to a non-zero process exit.

2. **"Could not measure" must never be reported as "found a defect"**, and
   never as health either. The ``unmeasured`` verdict has its own tests in both
   directions, including the one that matters most —
   ``test_s3_permission_error_is_unmeasured_not_missing``, because a 403
   rendered as a missing artifact is a detector reporting its own harness fault
   as a finding about the system, always in the alarming direction.

The five 2026-08-08 instances are replayed in ``TestAugust08Replay`` against
the live evidence recorded on I7167, each asserting the sweep WOULD have caught
what no surface reported.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from validators import stage_output_sweep as sos

RUN_DATE = "2026-08-08"
# The scheduled 2026-08-08 weekly run, execution
# 9b34ac0f-5e2f-f70b-d668-61b4c4751654_6fb6adf9-55fa-4426-6d06-79e4579bcaff:
# 02:00:49 → 06:20:44 PDT, terminated SUCCEEDED.
EXEC_START = datetime(2026, 8, 8, 9, 0, 49, tzinfo=timezone.utc)

PIPELINE = "ne-weekly-freshness-pipeline"


def _row(artifact_id, template, stage, pipeline=PIPELINE, **kw):
    row = {
        "artifact_id": artifact_id,
        "s3_key_template": template,
        "cadence": "eod_sf",
        "sla_minutes_after_cron": 60,
        "severity": "warning",
        "owner_repo": "alpha-engine-data",
        "created_at": "2026-06-01",
        "produced_by": [{"pipeline": pipeline, "stage": stage}],
    }
    row.update(kw)
    return row


def _head_from(mapping):
    """Build a ``head`` callable from ``{key: last_modified | 'error:<why>'}``.

    A key absent from the mapping is an absent object; a value beginning
    ``error:`` is an S3 fault. Injecting the head is what makes every branch —
    including the ones S3 will not produce on demand — actually reachable.
    """

    def head(bucket, key):
        if key not in mapping:
            return "absent", None
        value = mapping[key]
        if isinstance(value, str) and value.startswith("error:"):
            return "error", value[len("error:"):]
        return "found", value

    return head


# ---------------------------------------------------------------------------
# resolve_key
# ---------------------------------------------------------------------------


class TestResolveKey:
    @pytest.mark.parametrize("placeholder", ["date", "trading_day", "run_date"])
    def test_run_date_placeholders_resolve(self, placeholder):
        got = sos.resolve_key("backtest/{%s}/report.md" % placeholder, run_date=RUN_DATE, cycle_date=RUN_DATE)
        assert got == f"backtest/{RUN_DATE}/report.md"

    def test_unplaceholdered_template_passes_through(self):
        assert (
            sos.resolve_key("market_data/macro_history.parquet", run_date=RUN_DATE, cycle_date=RUN_DATE)
            == "market_data/macro_history.parquet"
        )

    def test_unresolvable_placeholder_returns_none(self):
        """A key this sweep cannot build is not a key that is missing.

        head-ing a literal ``{cycle_label}`` would 404 every time and report a
        confident, permanent, entirely false `missing` — the most convincing
        wrong finding this sweep could produce.
        """
        assert sos.resolve_key("x/{cycle_label}/y.json", run_date=RUN_DATE, cycle_date=RUN_DATE) is None

    def test_mixed_resolvable_and_unresolvable_is_none(self):
        assert sos.resolve_key("a/{date}/{cycle_label}.json", run_date=RUN_DATE, cycle_date=RUN_DATE) is None


class TestCycleDate:
    """The regression guard for this module's own worst bug.

    Resolving ``{date}`` / ``{trading_day}`` to ``$.run_date`` produced 28
    confident false ``missing`` verdicts against the 2026-08-08 run — measured,
    not hypothetical: that run's ``$.run_date`` was ``2026-08-08`` while every
    artifact it wrote landed under ``2026-08-07``, and ``backtest/2026-08-08/``
    does not exist at all.

    A detector wrong in that direction, at that volume, is worse than no
    detector: it gets muted within a week and takes the real finding with it.
    """

    def test_saturday_run_resolves_to_the_prior_trading_day(self):
        assert sos.resolve_cycle_date("2026-08-08") == "2026-08-07"

    def test_a_trading_day_resolves_to_itself(self):
        assert sos.resolve_cycle_date("2026-08-07") == "2026-08-07"

    def test_cycle_date_is_not_run_date_by_default_in_main(
        self, monkeypatch, registry_file
    ):
        """``main()`` must NOT fall back to run_date for the cycle date.

        Pinned as its own test because every other test in TestMain passes
        ``--cycle-date`` explicitly, so none of them would notice the default
        regressing.
        """
        captured = {}

        def _fake_sweep(**kw):
            captured.update(kw)
            return {"status": "ok", "enforce": False, "defects": [], "unmeasured": [],
                    "checked": 0}

        monkeypatch.setattr(sos, "sweep", _fake_sweep)
        sos.main(["--run-date", "2026-08-08", "--registry", registry_file, "--no-alert"])
        assert captured["cycle_date"] is None, (
            "main() must pass cycle_date=None so sweep() resolves it via "
            "expected_last_close rather than silently reusing run_date"
        )

    def test_unresolved_cycle_date_is_unmeasured_not_missing(self):
        """When the resolver is unavailable, a dated key is UNMEASURED.

        Falling back to run_date here would reintroduce the 28-false-positive
        failure in the one situation where nobody would be looking for it.
        """
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "S")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({}), cycle_date=None,
        )[0]
        assert f["verdict"] == sos.UNMEASURED
        assert "cycle date unresolved" in f["detail"]

    def test_undated_keys_still_check_without_a_cycle_date(self):
        """A pointer-pattern key carries no placeholder, so losing the cycle
        date must not take it down too."""
        decls = sos.declared_outputs([_row("a", "pointer/latest.json", "S")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from({}), cycle_date=None,
        )[0]
        assert f["verdict"] == sos.MISSING


class TestParseExecutionStart:
    def test_z_suffix(self):
        assert sos.parse_execution_start("2026-08-08T09:00:49Z") == EXEC_START

    def test_offset_form(self):
        assert sos.parse_execution_start("2026-08-08T09:00:49+00:00") == EXEC_START

    def test_naive_is_assumed_utc(self):
        assert sos.parse_execution_start("2026-08-08T09:00:49") == EXEC_START

    def test_garbage_raises(self):
        with pytest.raises(ValueError):
            sos.parse_execution_start("not-a-timestamp")


# ---------------------------------------------------------------------------
# Registry loading — the empty-producer-set trap
# ---------------------------------------------------------------------------


class TestLoadRegistry:
    def test_local_path_round_trip(self, tmp_path):
        import yaml

        path = tmp_path / "reg.yaml"
        path.write_text(yaml.safe_dump({"artifacts": [_row("a", "k", "S")]}))
        assert len(sos.load_registry_rows(registry_path=str(path))) == 1

    def test_missing_file_raises_rather_than_returning_empty(self, tmp_path):
        with pytest.raises(sos.RegistryUnreadable):
            sos.load_registry_rows(registry_path=str(tmp_path / "nope.yaml"))

    def test_empty_artifacts_list_raises(self, tmp_path):
        """An assertion over an empty producer set always passes.

        Returning ``[]`` here would make an unreadable/misparsed registry
        indistinguishable from a perfectly clean run — the failure mode this
        whole module exists to end, reproduced inside the detector.
        """
        import yaml

        path = tmp_path / "reg.yaml"
        path.write_text(yaml.safe_dump({"artifacts": []}))
        with pytest.raises(sos.RegistryUnreadable):
            sos.load_registry_rows(registry_path=str(path))

    def test_unparseable_yaml_raises(self, tmp_path):
        path = tmp_path / "reg.yaml"
        path.write_text("artifacts: [oops\n  - : :\n")
        with pytest.raises(sos.RegistryUnreadable):
            sos.load_registry_rows(registry_path=str(path))


class TestDeclaredOutputs:
    def test_filters_to_the_named_pipeline(self):
        rows = [
            _row("weekly", "a/{date}.json", "Backtester"),
            _row("preopen", "b/{date}.json", "Scanner", pipeline="ne-preopen-trading-pipeline"),
        ]
        got = sos.declared_outputs(rows, pipeline=PIPELINE)
        assert [d["artifact_id"] for d in got] == ["weekly"]

    def test_dual_producer_row_contributes_to_both(self):
        """The Scanner artifacts are written by the weekday preopen SF AND the
        weekly SF. A registry that can name only one is why a stopped weekly
        stage hides behind a live daily one — so both must be emitted.
        """
        row = _row("scanner", "scanner/{date}.json", "Scanner")
        row["produced_by"].append(
            {"pipeline": "ne-preopen-trading-pipeline", "stage": "Scanner"}
        )
        assert len(sos.declared_outputs([row], pipeline=PIPELINE)) == 1
        assert len(
            sos.declared_outputs([row], pipeline="ne-preopen-trading-pipeline")
        ) == 1

    def test_rows_without_produced_by_are_ignored(self):
        row = _row("x", "k", "S")
        del row["produced_by"]
        assert sos.declared_outputs([row], pipeline=PIPELINE) == []


# ---------------------------------------------------------------------------
# evaluate — one test per verdict, both polarities
# ---------------------------------------------------------------------------


class TestVerdicts:
    def _one(self, template, head_map, **kw):
        decls = sos.declared_outputs([_row("a", template, "Backtester")], pipeline=PIPELINE)
        kw.setdefault("execution_start", EXEC_START)
        kw.setdefault("cycle_date", RUN_DATE)
        return sos.evaluate(decls, run_date=RUN_DATE, head=_head_from(head_map), **kw)[0]

    def test_wrote_when_written_after_execution_start(self):
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": EXEC_START + timedelta(hours=2)})
        assert f["verdict"] == sos.WROTE

    def test_wrote_on_exact_boundary(self):
        """LastModified == execution start is INSIDE the window.

        An exclusive boundary would report a stage that wrote in the run's
        first second as stale — a false defect at the one moment the pipeline
        is least likely to be watched.
        """
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": EXEC_START})
        assert f["verdict"] == sos.WROTE

    def test_stale_when_the_key_predates_the_run(self):
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": EXEC_START - timedelta(days=87)})
        assert f["verdict"] == sos.STALE
        assert "87d" in f["detail"]

    def test_missing_when_no_object_exists(self):
        f = self._one("b/{date}.json", {})
        assert f["verdict"] == sos.MISSING

    def test_s3_permission_error_is_unmeasured_not_missing(self):
        """A 403 is not an absent artifact.

        Reporting it as ``missing`` is a detector filing its own harness fault
        as a defect in the system it watches — this fleet's most-repeated
        detector bug, and always in the alarming direction.
        """
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": "error:AccessDenied"})
        assert f["verdict"] == sos.UNMEASURED
        assert f["verdict"] != sos.MISSING
        assert "AccessDenied" in f["detail"]

    def test_transient_s3_fault_is_unmeasured_not_wrote(self):
        """The other polarity: an unanswerable check is not health either."""
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": "error:SlowDown"})
        assert f["verdict"] == sos.UNMEASURED
        assert f["verdict"] != sos.WROTE

    def test_present_key_without_a_window_is_unmeasured_not_wrote(self):
        """Absence is measurable without an execution window; staleness is not.

        Degrading a present key to ``wrote`` here is exactly how instances 2, 3
        and 4 stayed invisible for months: the key existed, so everything
        looked fine.
        """
        f = self._one(
            "b/{date}.json",
            {f"b/{RUN_DATE}.json": EXEC_START - timedelta(days=87)},
            execution_start=None,
        )
        assert f["verdict"] == sos.UNMEASURED

    def test_absence_is_still_a_defect_without_a_window(self):
        f = self._one("b/{date}.json", {}, execution_start=None)
        assert f["verdict"] == sos.MISSING

    def test_missing_last_modified_is_unmeasured(self):
        f = self._one("b/{date}.json", {f"b/{RUN_DATE}.json": None})
        assert f["verdict"] == sos.UNMEASURED

    def test_unresolvable_template_is_unmeasured(self):
        f = self._one("b/{cycle_label}.json", {})
        assert f["verdict"] == sos.UNMEASURED

    def test_unresolved_producer_row_is_its_own_verdict(self):
        """config-I7180's ratcheted gap is neither a pass nor a defect.

        Folding UNRESOLVED into ``wrote`` would let 20 unattributed artifacts
        inflate the green number; folding it into ``missing`` would fail every
        run for a registry gap that is already tracked and published.
        """
        decls = sos.declared_outputs([_row("a", "k", "UNRESOLVED")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, head=_head_from({}), cycle_date=RUN_DATE
        )[0]
        assert f["verdict"] == sos.UNRESOLVED
        assert f["verdict"] not in sos.DEFECT_VERDICTS


class TestEnteredStages:
    def test_stage_not_entered_is_skipped_when_the_set_is_known(self):
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, cycle_date=RUN_DATE,
            head=_head_from({}), entered_stages=frozenset({"MorningEnrich"}),
        )[0]
        assert f["verdict"] == sos.SKIPPED

    def test_unknown_entered_set_never_excuses_a_stage(self):
        """"I don't know which stages ran" must not become "the stage was
        allowed not to run" — that reasoning turns every degraded run into a
        blanket amnesty and is how a silent stage buys itself cover.
        """
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, cycle_date=RUN_DATE,
            head=_head_from({}), entered_stages=None,
        )[0]
        assert f["verdict"] == sos.MISSING

    def test_entered_stage_is_still_asserted(self):
        decls = sos.declared_outputs([_row("a", "b/{date}.json", "Backtester")], pipeline=PIPELINE)
        f = sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START, cycle_date=RUN_DATE,
            head=_head_from({}), entered_stages=frozenset({"Backtester"}),
        )[0]
        assert f["verdict"] == sos.MISSING


# ---------------------------------------------------------------------------
# summarise / exit_code — the enforcement ladder
# ---------------------------------------------------------------------------


class TestSummarise:
    def test_defect_outranks_unmeasured_in_the_status(self):
        findings = [
            {"verdict": sos.MISSING}, {"verdict": sos.UNMEASURED}, {"verdict": sos.WROTE},
        ]
        s = sos.summarise(findings)
        assert s["status"] == "stage_output_missing"
        assert len(s["defects"]) == 1 and len(s["unmeasured"]) == 1

    def test_unmeasured_alone_is_not_ok(self):
        """A sweep that could not look has not reported a clean run."""
        s = sos.summarise([{"verdict": sos.UNMEASURED}, {"verdict": sos.WROTE}])
        assert s["status"] == "stage_output_unmeasured"

    def test_all_wrote_is_ok(self):
        assert sos.summarise([{"verdict": sos.WROTE}])["status"] == "ok"

    def test_unresolved_and_skipped_do_not_break_ok(self):
        s = sos.summarise(
            [{"verdict": sos.WROTE}, {"verdict": sos.UNRESOLVED}, {"verdict": sos.SKIPPED}]
        )
        assert s["status"] == "ok"


class TestExitCode:
    def _doc(self, **kw):
        base = {
            "status": "ok", "enforce": False, "defects": [], "unmeasured": [],
        }
        base.update(kw)
        return base

    def test_observe_mode_exits_0_on_a_defect(self):
        """The I6891 blast radius, encoded.

        A non-zero exit here fails WeeklySubstrateHealthCheck → degraded →
        DegradedRun (a Fail state) → the whole four-hour run terminates FAILED.
        Three of I7167's five instances are still open, so an enforcing default
        would hard-fail the next scheduled run for defects that predate the
        detector.
        """
        assert sos.exit_code(self._doc(status="stage_output_missing", defects=[{}])) == 0

    def test_enforce_exits_1_on_a_defect(self):
        assert sos.exit_code(
            self._doc(enforce=True, status="stage_output_missing", defects=[{}])
        ) == 1

    def test_enforce_exits_2_on_unmeasured(self):
        """2, not 1 — the operator's next move differs, and collapsing them is
        how a permissions gap gets filed as a data defect."""
        assert sos.exit_code(
            self._doc(enforce=True, status="stage_output_unmeasured", unmeasured=[{}])
        ) == 2

    def test_unreadable_registry_exits_2_even_in_observe_mode(self):
        """Observe mode excuses FINDINGS, never a sweep that could not run."""
        assert sos.exit_code(self._doc(status="registry_unreadable")) == 2

    def test_clean_run_exits_0_in_both_modes(self):
        assert sos.exit_code(self._doc()) == 0
        assert sos.exit_code(self._doc(enforce=True)) == 0


# ---------------------------------------------------------------------------
# The five 2026-08-08 instances, replayed
# ---------------------------------------------------------------------------


class TestAugust08Replay:
    """Each instance from alpha-engine-config-I7167's root-cause table, replayed
    against the live evidence recorded there. Every one asserts the sweep would
    have produced a non-``wrote`` verdict on a run that terminated SUCCEEDED
    with nothing on any surface.

    The point of replaying all five is that they had FIVE DIFFERENT root causes
    — a shell trap, a caller/contract disagreement, a masked sibling, a
    wall-kill and a prefix cutover — and the assertion is indifferent to all of
    them. That indifference is the deliverable: it is what covers the sixth
    cause, which nobody has found yet.
    """

    def _verdict(self, artifact_id, template, stage, head_map):
        decls = sos.declared_outputs(
            [_row(artifact_id, template, stage)], pipeline=PIPELINE
        )
        return sos.evaluate(
            decls, run_date=RUN_DATE, execution_start=EXEC_START,
            head=_head_from(head_map), cycle_date=RUN_DATE,
        )[0]

    def test_instance_1_substrate_health_check_log_absent(self):
        """SSM exit 127 at line 5 — ``trap: --only-show-errors: invalid signal
        specification``. The stage died before its first check ran, so its log
        object never existed. (Root cause since fixed on main: the stage now
        runs through ``krepis.ssm_log_capture``.)"""
        f = self._verdict(
            "substrate_health_check_log",
            "_ssm_logs/substrate-health-check/{date}/run.log",
            "WeeklySubstrateHealthCheck",
            {},
        )
        assert f["verdict"] == sos.MISSING

    def test_instance_2_executor_optimizer_recommendation_87_days_stale(self):
        """``produce_artifact()`` is documented to write unconditionally; its
        only caller fires it when ``status == "ok"``. The call site wins, so
        the key froze at 2026-05-18 — 87 days before this run, and present the
        whole time."""
        key = f"config/executor_params/recommendations/{RUN_DATE}/from_executor_optimizer.json"
        f = self._verdict(
            "executor_optimizer_recommendation",
            "config/executor_params/recommendations/{date}/from_executor_optimizer.json",
            "Backtester",
            {key: datetime(2026, 5, 18, 12, tzinfo=timezone.utc)},
        )
        assert f["verdict"] == sos.STALE

    def test_instance_3_parity_report_masked_by_a_fresh_sibling(self):
        """``parity_report.json`` last wrote 2026-07-17, but ``report.md`` in
        the SAME prefix updated on this run. A prefix scan sees recent
        activity; a per-key, producer-anchored assertion does not.
        """
        head = {
            f"backtest/{RUN_DATE}/parity_report.json":
                datetime(2026, 7, 17, 12, tzinfo=timezone.utc),
            f"backtest/{RUN_DATE}/report.md": EXEC_START + timedelta(hours=4),
        }
        stale = self._verdict(
            "parity_report", "backtest/{date}/parity_report.json", "ParityReplay", head
        )
        fresh = self._verdict(
            "backtest_report", "backtest/{date}/report.md", "Backtester", head
        )
        assert stale["verdict"] == sos.STALE
        assert fresh["verdict"] == sos.WROTE, (
            "the masking sibling must still read healthy — a check that fails "
            "the whole prefix on one dead key is a check that gets muted"
        )

    def test_instance_4_replay_concordance_wall_killed(self):
        """Killed at its 900s ceiling in 22 of 38 observed runs. Killed at the
        wall means no artifact AND no cause — the stage cannot report its own
        death, which is precisely why the assertion has to be external to it.
        """
        f = self._verdict(
            "replay_summary",
            "decision_artifacts/_replay_summary/{date}/summary.json",
            "ReplayConcordance",
            {},
        )
        assert f["verdict"] == sos.MISSING

    def test_instance_5_aggregate_costs_parquet_absent(self):
        """``_cost_raw/2026-08-08/`` held zero objects (a prefix cutover skew,
        per I7179), so the aggregator honestly no-opped and wrote no parquet.
        The stage was the honest one here — and the assertion still fires,
        because the assertion is about the ARTIFACT, not about whether the
        stage had a good reason.
        """
        f = self._verdict(
            "cost_parquet",
            "decision_artifacts/_cost/{date}/cost.parquet",
            "AggregateCosts",
            {},
        )
        assert f["verdict"] == sos.MISSING

    def test_the_whole_08_08_run_would_have_been_flagged(self):
        """End to end: the five instances plus healthy stages, in one sweep.

        The run terminated SUCCEEDED. In enforce mode this sweep exits 1, and
        in observe mode it exits 0 while still reporting five defects — which
        is the difference between the two modes stated as a test rather than as
        a comment.
        """
        rows = [
            _row("substrate_log", "_ssm_logs/substrate-health-check/{date}/run.log",
                 "WeeklySubstrateHealthCheck"),
            _row("executor_rec",
                 "config/executor_params/recommendations/{date}/from_executor_optimizer.json",
                 "Backtester"),
            _row("parity_report", "backtest/{date}/parity_report.json", "ParityReplay"),
            _row("replay_summary", "decision_artifacts/_replay_summary/{date}/summary.json",
                 "ReplayConcordance"),
            _row("cost_parquet", "decision_artifacts/_cost/{date}/cost.parquet",
                 "AggregateCosts"),
            _row("backtest_report", "backtest/{date}/report.md", "Backtester"),
            _row("predictor_manifest", "predictor/weights/meta/manifest.json",
                 "PredictorTraining"),
        ]
        head = _head_from({
            f"config/executor_params/recommendations/{RUN_DATE}/from_executor_optimizer.json":
                datetime(2026, 5, 18, 12, tzinfo=timezone.utc),
            f"backtest/{RUN_DATE}/parity_report.json":
                datetime(2026, 7, 17, 12, tzinfo=timezone.utc),
            f"backtest/{RUN_DATE}/report.md": EXEC_START + timedelta(hours=4),
            "predictor/weights/meta/manifest.json": EXEC_START + timedelta(hours=1),
        })
        findings = sos.evaluate(
            sos.declared_outputs(rows, pipeline=PIPELINE),
            run_date=RUN_DATE, execution_start=EXEC_START, head=head,
            cycle_date=RUN_DATE,
        )
        summary = sos.summarise(findings)

        assert summary["counts"].get(sos.WROTE) == 2
        assert len(summary["defects"]) == 5
        assert {f["stage"] for f in summary["defects"]} == {
            "WeeklySubstrateHealthCheck", "Backtester", "ParityReplay",
            "ReplayConcordance", "AggregateCosts",
        }
        assert sos.exit_code({**summary, "enforce": True}) == 1
        assert sos.exit_code({**summary, "enforce": False}) == 0


# ---------------------------------------------------------------------------
# sweep() / main() — publish, alert and the end-to-end exit path
# ---------------------------------------------------------------------------


class _FakeS3:
    def __init__(self, objects=None, put_raises=None):
        self.objects = objects or {}
        self.puts = []
        self.put_raises = put_raises

    def head_object(self, Bucket, Key):  # noqa: N803 - boto3 kwarg casing
        if Key not in self.objects:
            err = Exception("not found")
            err.response = {"Error": {"Code": "404"}}
            raise err
        return {"LastModified": self.objects[Key]}

    def put_object(self, **kw):
        if self.put_raises:
            raise self.put_raises
        self.puts.append(kw)
        return {}


@pytest.fixture()
def registry_file(tmp_path):
    import yaml

    path = tmp_path / "ARTIFACT_REGISTRY.yaml"
    path.write_text(yaml.safe_dump({"artifacts": [
        _row("good", "good/{date}.json", "PredictorTraining"),
        _row("bad", "bad/{date}.json", "ReplayConcordance"),
    ]}))
    return str(path)


class TestSweep:
    def test_finds_the_defect_and_publishes_the_verdict(self, registry_file):
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START, registry_path=registry_file,
            s3_client=s3, alert=False,
        )
        assert doc["status"] == "stage_output_missing"
        assert [f["stage"] for f in doc["defects"]] == ["ReplayConcordance"]
        assert len(s3.puts) == 1
        published = json.loads(s3.puts[0]["Body"])
        assert published["schema"] == "stage_output_sweep-1.0.0"
        assert s3.puts[0]["Key"] == f"_stage_outputs/{PIPELINE}/{RUN_DATE}.json"

    def test_verdict_publish_failure_does_not_lose_the_verdict(self, registry_file):
        """The reporter must not destroy what it reports.

        An exception on the verdict WRITE would convert an observability fault
        into a pipeline failure — and, in enforce mode, into a hard-failed
        four-hour run caused by the detector rather than by the pipeline.
        """
        s3 = _FakeS3(
            {f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)},
            put_raises=RuntimeError("s3 down"),
        )
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, execution_start=EXEC_START, registry_path=registry_file,
            s3_client=s3, alert=False,
        )
        assert doc["status"] == "stage_output_missing"
        assert len(doc["defects"]) == 1

    def test_unreadable_registry_returns_a_document_not_an_exception(self, tmp_path):
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=str(tmp_path / "absent.yaml"),
            s3_client=_FakeS3(), alert=False, publish=False,
        )
        assert doc["status"] == "registry_unreadable"
        assert sos.exit_code(doc) == 2

    def test_no_publish_writes_nothing(self, registry_file):
        s3 = _FakeS3()
        sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            alert=False, publish=False,
        )
        assert s3.puts == []


class TestMain:
    def _run(self, monkeypatch, argv, s3):
        monkeypatch.setattr(sos, "_alert", lambda doc: None)
        import boto3

        monkeypatch.setattr(boto3, "client", lambda *a, **k: s3)
        return sos.main(argv)

    def test_observe_mode_exits_0_with_a_real_defect(self, monkeypatch, registry_file):
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--no-alert",
        ], s3)
        assert code == 0

    def test_enforce_mode_exits_1_with_the_same_defect(self, monkeypatch, registry_file):
        """The proof the assertion CAN fail, walked end to end from a real
        absent key to a non-zero process exit."""
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--enforce", "--no-alert",
        ], s3)
        assert code == 1

    def test_enforce_mode_exits_0_on_a_clean_run(self, monkeypatch, registry_file):
        """The other polarity — an absence-alarm is blind in BOTH directions,
        and a detector that fires on a healthy run is the one that gets muted.
        """
        s3 = _FakeS3({
            f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1),
            f"bad/{RUN_DATE}.json": EXEC_START + timedelta(hours=2),
        })
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--enforce", "--no-alert",
        ], s3)
        assert code == 0

    def test_malformed_execution_start_exits_2_rather_than_dropping_the_window(
        self, monkeypatch, registry_file
    ):
        """A mis-typed timestamp must not silently degrade every assertion to
        ``unmeasured`` — that is a detector switched off by a typo.
        """
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "08/08/2026", "--no-alert",
        ], _FakeS3())
        assert code == 2

    def test_entered_stage_flag_excuses_only_the_absent_stage(
        self, monkeypatch, registry_file
    ):
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        code = self._run(monkeypatch, [
            "--run-date", RUN_DATE, "--cycle-date", RUN_DATE, "--registry", registry_file,
            "--execution-start", "2026-08-08T09:00:49Z", "--enforce", "--no-alert",
            "--entered-stage", "PredictorTraining",
        ], s3)
        assert code == 0, "ReplayConcordance never entered, so its artifact is not owed"


class _FakeSfn:
    """Step Functions double. ``describe_raises`` / ``history_raises`` exist so
    each half of the execution context can be failed INDEPENDENTLY — the two
    degrade separately by design and a shared failure switch would hide that.
    """

    def __init__(self, start=None, entered=(), describe_raises=None,
                 history_raises=None, pages=None):
        self.start = start
        self.entered = list(entered)
        self.describe_raises = describe_raises
        self.history_raises = history_raises
        self.pages = pages

    def describe_execution(self, executionArn):  # noqa: N803
        if self.describe_raises:
            raise self.describe_raises
        return {"startDate": self.start, "status": "SUCCEEDED"}

    def get_execution_history(self, **kw):
        if self.history_raises:
            raise self.history_raises
        if self.pages is not None:
            index = 0 if "nextToken" not in kw else int(kw["nextToken"])
            return self.pages[index]
        return {
            "events": [
                {"type": "TaskStateEntered",
                 "stateEnteredEventDetails": {"name": n}}
                for n in self.entered
            ]
        }


class TestExecutionContext:
    def test_reads_start_and_entered_stages(self):
        ctx = sos.read_execution_context(
            "arn:x", sfn_client=_FakeSfn(start=EXEC_START, entered=["A", "B"]),
        )
        assert ctx.start == EXEC_START
        assert ctx.entered_stages == frozenset({"A", "B"})
        assert ctx.notes == []

    def test_paginates_the_history(self):
        """A four-hour weekly run's history runs to thousands of events; a
        single un-paginated page would silently truncate the entered set and
        mark late stages skipped — false GREEN, the dangerous polarity."""
        pages = [
            {"events": [{"type": "TaskStateEntered",
                         "stateEnteredEventDetails": {"name": "Early"}}],
             "nextToken": "1"},
            {"events": [{"type": "TaskStateEntered",
                         "stateEnteredEventDetails": {"name": "Late"}}]},
        ]
        ctx = sos.read_execution_context("arn:x", sfn_client=_FakeSfn(pages=pages))
        assert ctx.entered_stages == frozenset({"Early", "Late"})

    def test_only_entered_events_count(self):
        """Filtering on EXIT events would excuse every stage that entered and
        died — precisely the population this sweep exists to catch."""
        class _Sfn(_FakeSfn):
            def get_execution_history(self, **kw):
                return {"events": [
                    {"type": "TaskStateEntered",
                     "stateEnteredEventDetails": {"name": "Ran"}},
                    {"type": "TaskStateExited",
                     "stateExitedEventDetails": {"name": "Exited"}},
                ]}

        ctx = sos.read_execution_context("arn:x", sfn_client=_Sfn())
        assert ctx.entered_stages == frozenset({"Ran"})

    def test_describe_denied_still_yields_entered_stages(self):
        ctx = sos.read_execution_context(
            "arn:x",
            sfn_client=_FakeSfn(entered=["A"], describe_raises=RuntimeError("AccessDenied")),
        )
        assert ctx.start is None
        assert ctx.entered_stages == frozenset({"A"})
        assert any("describe_execution failed" in n for n in ctx.notes)

    def test_history_denied_still_yields_the_start(self):
        ctx = sos.read_execution_context(
            "arn:x",
            sfn_client=_FakeSfn(start=EXEC_START, history_raises=RuntimeError("AccessDenied")),
        )
        assert ctx.start == EXEC_START
        assert ctx.entered_stages is None
        assert any("UNKNOWN" in n for n in ctx.notes)

    def test_both_denied_degrades_to_nothing_known_and_says_so(self):
        ctx = sos.read_execution_context(
            "arn:x",
            sfn_client=_FakeSfn(describe_raises=RuntimeError("x"),
                                history_raises=RuntimeError("y")),
        )
        assert ctx.start is None and ctx.entered_stages is None
        assert len(ctx.notes) == 2

    def test_no_injected_client_resolves_a_region_with_no_env_set(self, monkeypatch):
        """Regression for alpha-engine-config-I7428: SSM AWS-RunShellScript
        runs with neither AWS_REGION nor AWS_DEFAULT_REGION set and no
        per-user boto profile. Before the fix, ``boto3.client("stepfunctions")``
        with no ``sfn_client`` injected raised NoRegionError on every box
        run and the execution context degraded with
        "stepfunctions client unavailable: You must specify a region." on
        every weekly run. Clearing both env vars and forcing krepis's
        session/IMDS fallbacks to miss reproduces the exact box shape; the
        client must still be constructed (via
        krepis.aws_region.resolve_region()'s DEFAULT_REGION floor) rather
        than raising NoRegionError."""
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        from krepis import aws_region

        monkeypatch.setattr(aws_region, "_from_botocore_session", lambda: None)
        monkeypatch.setattr(aws_region, "_from_imds", lambda: None)

        ctx = sos.read_execution_context("arn:x")

        assert not any(
            "NoRegionError" in n or "You must specify a region" in n
            for n in ctx.notes
        )


class TestGatedOutRun:
    """The 2026-08-13 scheduled run terminated SUCCEEDED in 5.4 seconds,
    gated out by ``WeeklyRunDayGate`` having done no work. Roughly two of
    every three firings are such runs.

    Without the entered-stage set this sweep would report every declared
    artifact missing on those — ~30 false defects, twice a week, which is how
    a detector earns its mute. With it, the run is correctly silent.
    """

    def test_gated_out_run_reports_no_defects(self, registry_file):
        s3 = _FakeS3()
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_arn="arn:x",
            sfn_client=_FakeSfn(start=EXEC_START, entered=["WeeklyRunDayGate"]),
            alert=False, publish=False, enforce=True,
        )
        assert doc["defects"] == []
        assert doc["counts"].get(sos.SKIPPED) == 2
        assert sos.exit_code(doc) == 0

    def test_a_deep_run_with_a_silent_stage_still_fails(self, registry_file):
        """The other polarity — the skip excuse must not become a blanket
        amnesty. A stage that DID enter and wrote nothing is still a defect."""
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_arn="arn:x",
            sfn_client=_FakeSfn(
                start=EXEC_START, entered=["PredictorTraining", "ReplayConcordance"]
            ),
            alert=False, publish=False, enforce=True,
        )
        assert [f["stage"] for f in doc["defects"]] == ["ReplayConcordance"]
        assert sos.exit_code(doc) == 1

    def test_history_denied_does_not_excuse_anything(self, registry_file):
        """Losing the history must fail toward asserting, not toward amnesty."""
        s3 = _FakeS3({f"good/{RUN_DATE}.json": EXEC_START + timedelta(hours=1)})
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_arn="arn:x",
            sfn_client=_FakeSfn(start=EXEC_START, history_raises=RuntimeError("AccessDenied")),
            alert=False, publish=False, enforce=True,
        )
        assert doc["entered_stages_known"] is False
        assert [f["stage"] for f in doc["defects"]] == ["ReplayConcordance"]
        assert doc["context_notes"]

    def test_explicit_arguments_win_over_the_api(self, registry_file):
        """Replaying a historical run whose ARN no longer resolves must still
        honour an explicitly-supplied window."""
        s3 = _FakeS3()
        doc = sos.sweep(
            run_date=RUN_DATE, cycle_date=RUN_DATE, registry_path=registry_file, s3_client=s3,
            execution_start=EXEC_START, entered_stages=frozenset({"PredictorTraining"}),
            execution_arn="arn:x",
            sfn_client=_FakeSfn(start=datetime(2020, 1, 1, tzinfo=timezone.utc),
                                entered=["Something", "Else"]),
            alert=False, publish=False,
        )
        assert doc["execution_start"] == EXEC_START.isoformat()
        assert doc["entered_stage_count"] == 1


class TestAlertBody:
    def test_alert_names_the_mode_and_separates_the_two_finding_classes(
        self, monkeypatch
    ):
        published = {}

        class _Result:
            any_ok = True

        class _Alerts:
            @staticmethod
            def publish(message, **kw):
                published["message"] = message
                published["kw"] = kw
                return _Result()

        import sys as _sys

        monkeypatch.setitem(_sys.modules, "nousergon_lib", type("M", (), {"alerts": _Alerts}))
        monkeypatch.setitem(_sys.modules, "nousergon_lib.alerts", _Alerts)

        sos._alert({
            "pipeline": PIPELINE, "run_date": RUN_DATE, "enforce": False,
            "entered_stages_known": False,
            "defects": [{"stage": "ParityReplay", "artifact_id": "parity_report",
                         "verdict": sos.STALE}],
            "unmeasured": [{"stage": "Backtester", "artifact_id": "x",
                            "verdict": sos.UNMEASURED}],
        })
        message = published["message"]
        assert "OBSERVE" in message
        assert "DEFECT" in message and "UNMEASURED" in message
        assert "NOT evidence of health" in message
        assert published["kw"]["severity"] == "error"

    def test_unmeasured_only_alerts_at_warn_not_error(self, monkeypatch):
        published = {}

        class _Result:
            any_ok = True

        class _Alerts:
            @staticmethod
            def publish(message, **kw):
                published["kw"] = kw
                return _Result()

        import sys as _sys

        monkeypatch.setitem(_sys.modules, "nousergon_lib", type("M", (), {"alerts": _Alerts}))
        monkeypatch.setitem(_sys.modules, "nousergon_lib.alerts", _Alerts)

        sos._alert({
            "pipeline": PIPELINE, "run_date": RUN_DATE, "enforce": True,
            "entered_stages_known": True, "defects": [],
            "unmeasured": [{"stage": "S", "artifact_id": "a", "verdict": sos.UNMEASURED}],
        })
        assert published["kw"]["severity"] == "warn"
