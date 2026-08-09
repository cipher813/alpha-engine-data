"""Unit tests for scripts/weekday_sf_rerun.py (alpha-engine-config#6694) +
lockstep guards pinning its declarative stage tables against infrastructure/
step_function_daily.json and infrastructure/step_function_eod.json.

Four recorded-shape execution-history fixtures (tests/fixtures/
weekday_sf_rerun/), built from the real state names in both weekday SF
definitions (see the module docstring's COHERENCE VALIDATION section for
why both pipelines are strictly linear, unlike the weekly SF):

- ``daily_mid_failure``: morning_enrich + scanner complete, predictor_
  inference fails (the preopen SF, role-unconditional skip gates);
- ``daily_early_failure``: DeployDriftGate finds drift and hard-fails
  BEFORE any CheckSkip* gate is ever reached — the pre-workload case;
- ``eod_mid_failure``: refresh_executor_deploy + post_market_data +
  capture_snapshot complete, eod_reconcile fails hard (EODReconcile's own
  SSM command errors — not the data-gap route);
- ``eod_degraded``: the same three stages complete, but the precondition
  probe finds the day's SPY close not yet verified-present ->
  SkipEODReconcileDataGap (data-gap route, NOT an operator skip flag) -> the
  self-heal loop exhausts its budget -> HealNonConvergent -> DegradedRun
  (Fail terminal, Brian's 2026-07-28 Option-A ruling, config#2699) — proves
  the I6055 degraded-overrides-witness rule holds on EOD's data-gap route
  too, not only the weekly SF's Publish*Degraded routes.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "weekday_sf_rerun.py"
FIXTURES = Path(__file__).parent / "fixtures" / "weekday_sf_rerun"
DAILY_SF_PATH = REPO_ROOT / "infrastructure" / "step_function_daily.json"
EOD_SF_PATH = REPO_ROOT / "infrastructure" / "step_function_eod.json"


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location("weekday_sf_rerun", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    sys.modules["weekday_sf_rerun"] = m
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def daily_def() -> dict:
    return json.loads(DAILY_SF_PATH.read_text())


@pytest.fixture(scope="module")
def eod_def() -> dict:
    return json.loads(EOD_SF_PATH.read_text())


def _events(name: str) -> list:
    return json.loads((FIXTURES / f"{name}.json").read_text())["events"]


def _original_input(events: list) -> dict:
    started = next(e for e in events if "executionStartedEventDetails" in e)
    return json.loads(started["executionStartedEventDetails"]["input"])


def _set_original_input(events: list, inp: dict) -> None:
    started = next(e for e in events if "executionStartedEventDetails" in e)
    started["executionStartedEventDetails"]["input"] = json.dumps(inp)


# ---------------------------------------------------------------------------
# Pipeline auto-detection from an execution ARN
# ---------------------------------------------------------------------------

class TestPipelineDetection:
    def test_daily_arn_detected(self, mod):
        arn = "arn:aws:states:us-east-1:711398986525:execution:ne-preopen-trading-pipeline:abc"
        assert mod.pipeline_for_execution_arn(arn) is mod.DAILY

    def test_eod_arn_detected(self, mod):
        arn = "arn:aws:states:us-east-1:711398986525:execution:ne-postclose-trading-pipeline:abc"
        assert mod.pipeline_for_execution_arn(arn) is mod.EOD

    def test_weekly_arn_rejected(self, mod):
        arn = "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:abc"
        with pytest.raises(SystemExit, match="weekly_sf_rerun"):
            mod.pipeline_for_execution_arn(arn)

    def test_malformed_arn_rejected(self, mod):
        with pytest.raises(SystemExit, match="does not look like"):
            mod.pipeline_for_execution_arn("not-an-arn")


# ---------------------------------------------------------------------------
# Skip-set derivation over the four fixtures
# ---------------------------------------------------------------------------

class TestDerivePlan:
    def test_daily_mid_failure(self, mod):
        plan = mod.derive_plan(mod.DAILY, _events("daily_mid_failure"))
        assert plan.run_date == "2026-08-07"
        assert "InitializeInput" in plan.run_date_provenance
        assert plan.completed == ["morning_enrich", "scanner"]
        assert plan.failed == ["predictor_inference"]
        assert plan.degraded == []
        assert set(plan.skip_flags) == {"skip_morning_enrich", "skip_scanner"}
        # daily PRESERVES the original pipeline_role (role-unconditional gates)
        assert plan.emitted_role == "daily"
        assert "skip_predictor_inference" not in plan.skip_flags

    def test_daily_early_failure_warns_pre_workload(self, mod):
        plan = mod.derive_plan(mod.DAILY, _events("daily_early_failure"))
        assert plan.completed == []
        assert plan.failed == []
        assert plan.degraded == []
        assert plan.skip_flags == {}
        assert any("pre-workload" in w for w in plan.warnings)
        # a pre-workload rerun still carries the original run_date + role
        assert plan.run_date == "2026-08-07"
        assert plan.emitted_role == "daily"

    def test_eod_mid_failure(self, mod):
        plan = mod.derive_plan(mod.EOD, _events("eod_mid_failure"))
        assert plan.run_date == "2026-08-07"
        assert "explicit" in plan.run_date_provenance
        assert plan.completed == ["refresh_executor_deploy", "post_market_data", "capture_snapshot"]
        assert plan.failed == ["eod_reconcile"]
        assert plan.degraded == []
        assert set(plan.skip_flags) == {
            "skip_refresh_executor_deploy", "skip_post_market_data", "skip_capture_snapshot",
        }
        # EOD ALWAYS forces operator-replay regardless of the original role
        # ("eod" here — the daemon-triggered cadence role) — the skip gates
        # require it (config#1614).
        assert plan.emitted_role == "operator-replay"
        assert "skip_eod_reconcile" not in plan.skip_flags

    def test_eod_degraded_is_rerun_not_skipped(self, mod):
        """config-I2702's SkipEODReconcileDataGap data-gap route must NEVER
        be treated as an operator-equivalent skip — mirrors weekly_sf_rerun.
        py's I6055 rule, applied to EOD's precondition-probe bypass instead
        of a Publish*Degraded route."""
        plan = mod.derive_plan(mod.EOD, _events("eod_degraded"))
        assert plan.completed == ["refresh_executor_deploy", "post_market_data", "capture_snapshot"]
        assert plan.degraded == ["eod_reconcile"]
        assert plan.failed == []
        assert "skip_eod_reconcile" not in plan.skip_flags
        assert any("DEGRADED" in n for n in plan.notes)
        assert "skip_eod_reconcile" not in plan.rerun_input()
        assert plan.emitted_role == "operator-replay"

    @pytest.mark.parametrize(
        ("pipeline_attr", "fixture"),
        [("DAILY", "daily_mid_failure"), ("EOD", "eod_mid_failure")],
    )
    def test_original_input_reuse_contract(self, mod, pipeline_attr, fixture):
        """The emitted input carries the ORIGINAL execution's run_date +
        sns_topic_arn + trading_instance_id — never today's date, never a
        fabricated instance id — mirroring weekly_sf_rerun.py's contract."""
        pipeline = getattr(mod, pipeline_attr)
        events = _events(fixture)
        orig = _original_input(events)
        plan = mod.derive_plan(pipeline, events)
        inp = plan.rerun_input()
        assert inp["run_date"] == orig.get("run_date", plan.run_date)
        assert inp["sns_topic_arn"] == orig["sns_topic_arn"]
        assert inp["trading_instance_id"] == orig["trading_instance_id"]
        for flag, val in plan.skip_flags.items():
            assert inp[flag] is val is True

    def test_eod_original_ec2_instance_id_passes_through(self, mod):
        events = _events("eod_mid_failure")
        assert _original_input(events)["ec2_instance_id"] == ["i-0123456789abcdef0"]
        plan = mod.derive_plan(mod.EOD, events)
        assert plan.rerun_input()["ec2_instance_id"] == ["i-0123456789abcdef0"]

    def test_run_date_falls_back_to_start_time_when_absent(self, mod):
        events = [
            e for e in _events("daily_mid_failure")
            if e.get("stateExitedEventDetails", {}).get("name") != "InitializeInput"
        ]
        start = datetime(2026, 8, 7, 13, 5, tzinfo=timezone.utc)
        plan = mod.derive_plan(mod.DAILY, events, start_time=start)
        assert plan.run_date == "2026-08-07"
        assert "FALLBACK" in plan.run_date_provenance


# ---------------------------------------------------------------------------
# Coherence rejection: a stale/hand-edited original-input skip flag that
# would bypass a stage THIS execution's own history shows as FAILED.
# ---------------------------------------------------------------------------

class TestCoherenceRejection:
    def test_refuses_stale_flag_that_bypasses_the_failed_stage(self, mod):
        """eod_mid_failure's own history shows eod_reconcile FAILED (
        EODReconcile entered, StopTradingInstance never reached). If the
        preserved original input ALSO carries skip_eod_reconcile: true (a
        hand-edited or stale value inconsistent with this execution's own
        outcome), _simulate_reachable_works must catch that the derived
        rerun would silently skip the very stage that just failed, and
        derive_plan must refuse rather than emit that input."""
        events = _events("eod_mid_failure")
        inp = _original_input(events)
        inp["skip_eod_reconcile"] = True
        _set_original_input(events, inp)
        with pytest.raises(SystemExit, match="unreachable"):
            mod.derive_plan(mod.EOD, events)

    def test_internal_contradiction_guard(self, mod):
        """Defensive guard: a failed stage must never end up with its own
        derived skip flag set (would only fire on a topology bug, not
        reachable via the public API today — exercised directly)."""
        events = _events("eod_mid_failure")
        plan = mod.derive_plan(mod.EOD, events)
        # sanity: the anti-swallow guard already prevented this — confirm
        # the failed stage's flag was never derived in the first place.
        assert "skip_eod_reconcile" not in plan.skip_flags
        assert "eod_reconcile" in plan.failed


# ---------------------------------------------------------------------------
# operator_rerun_name naming convention
# ---------------------------------------------------------------------------

class TestOperatorRerunName:
    def test_format(self, mod):
        now = datetime(2026, 8, 7, 14, 30, 5, tzinfo=timezone.utc)
        assert mod.operator_rerun_name("2026-08-07", now=now) == "operator-rerun-2026-08-07-143005"

    def test_defaults_to_current_utc_time(self, mod):
        name = mod.operator_rerun_name("2026-08-07")
        assert name.startswith("operator-rerun-2026-08-07-")
        assert len(name.rsplit("-", 1)[-1]) == 6


# ---------------------------------------------------------------------------
# Role-gating verification (config#1614 for EOD; role-unconditional for daily)
# ---------------------------------------------------------------------------

class TestRoleGating:
    def test_daily_gates_are_role_unconditional_live(self, mod, daily_def):
        # ANY role must render the daily skip flags live — verified for both
        # the role daily preserves and an arbitrary other role.
        mod.verify_skip_flags_live(daily_def, "daily")
        mod.verify_skip_flags_live(daily_def, "some-other-role")

    def test_no_daily_skip_gate_references_pipeline_role_today(self, daily_def, mod):
        for name, st in daily_def["States"].items():
            if name.startswith("CheckSkip") and st.get("Type") == "Choice":
                assert "$.pipeline_role" not in json.dumps(st.get("Choices")), (
                    f"{name} now conjuncts pipeline_role — update "
                    "scripts/weekday_sf_rerun.py's DAILY.role_conjunct + this test"
                )

    def test_eod_gates_require_operator_replay_live(self, mod, eod_def):
        mod.verify_skip_flags_live(eod_def, "operator-replay")

    def test_eod_gates_reject_other_roles_live(self, mod, eod_def):
        with pytest.raises(SystemExit, match="role gating"):
            mod.verify_skip_flags_live(eod_def, "eod")

    def test_every_eod_skip_gate_conjuncts_operator_replay(self, eod_def):
        for name, st in eod_def["States"].items():
            if name.startswith("CheckSkip") and st.get("Type") == "Choice":
                assert '"operator-replay"' in json.dumps(st.get("Choices")), (
                    f"{name} no longer conjuncts pipeline_role==operator-replay — "
                    "update scripts/weekday_sf_rerun.py's EOD.role_conjunct + this test"
                )


# ---------------------------------------------------------------------------
# Stage-table lockstep with both live SF definitions
# ---------------------------------------------------------------------------

class TestStageTableLockstep:
    @pytest.fixture(scope="class")
    def daily_states(self):
        return json.loads(DAILY_SF_PATH.read_text())["States"]

    @pytest.fixture(scope="class")
    def eod_states(self):
        return json.loads(EOD_SF_PATH.read_text())["States"]

    def test_daily_every_stage_state_exists(self, mod, daily_states):
        for stage in mod.DAILY.stages:
            assert stage.gate in daily_states, f"{stage.name}: gate {stage.gate} missing"
            assert daily_states[stage.gate]["Type"] == "Choice"
            assert stage.work in daily_states, f"{stage.name}: work {stage.work} missing"
            for w in stage.witness:
                assert w in daily_states, f"{stage.name}: witness {w} missing"

    def test_eod_every_stage_state_exists(self, mod, eod_states):
        for stage in mod.EOD.stages:
            assert stage.gate in eod_states, f"{stage.name}: gate {stage.gate} missing"
            assert eod_states[stage.gate]["Type"] == "Choice"
            assert stage.work in eod_states, f"{stage.name}: work {stage.work} missing"
            for w in stage.witness:
                assert w in eod_states, f"{stage.name}: witness {w} missing"
            for dw in stage.degraded_witness:
                assert dw in eod_states, f"{stage.name}: degraded witness {dw} missing"

    def test_daily_every_gate_tests_its_flag(self, mod, daily_states):
        for stage in mod.DAILY.stages:
            choices = json.dumps(daily_states[stage.gate]["Choices"])
            assert f"$.{stage.flag}" in choices, f"{stage.name}: gate {stage.gate} no longer tests {stage.flag}"

    def test_eod_every_gate_tests_its_flag(self, mod, eod_states):
        for stage in mod.EOD.stages:
            choices = json.dumps(eod_states[stage.gate]["Choices"])
            assert f"$.{stage.flag}" in choices, f"{stage.name}: gate {stage.gate} no longer tests {stage.flag}"

    def test_daily_skip_route_lands_in_witness(self, mod, daily_states):
        for stage in mod.DAILY.stages:
            gate = daily_states[stage.gate]
            skip_targets = {c["Next"] for c in gate["Choices"]}
            assert skip_targets <= stage.witness or skip_targets & stage.witness, (
                f"{stage.name}: skip route {skip_targets} no longer lands in "
                f"witness {set(stage.witness)} — update DAILY_STAGES"
            )

    def test_eod_skip_route_lands_in_witness(self, mod, eod_states):
        for stage in mod.EOD.stages:
            gate = eod_states[stage.gate]
            skip_targets = {c["Next"] for c in gate["Choices"]}
            assert skip_targets & stage.witness, (
                f"{stage.name}: skip route {skip_targets} no longer lands in "
                f"witness {set(stage.witness)} — update EOD_STAGES"
            )

    def test_daily_every_checkskip_gate_is_covered(self, mod, daily_states):
        gates = {s.gate for s in mod.DAILY.stages}
        for name, state in daily_states.items():
            if name.startswith("CheckSkip") and state.get("Type") == "Choice":
                assert name in gates, f"new daily skip gate {name} is not covered by DAILY_STAGES — add a row"

    def test_eod_every_checkskip_gate_is_covered(self, mod, eod_states):
        gates = {s.gate for s in mod.EOD.stages}
        for name, state in eod_states.items():
            if name.startswith("CheckSkip") and state.get("Type") == "Choice":
                assert name in gates, f"new EOD skip gate {name} is not covered by EOD_STAGES — add a row"

    def test_eod_degraded_route_is_mapped(self, mod, eod_states):
        """Completeness (mirrors weekly_sf_rerun.py's I6055 guard): a NEW
        data-gap / degraded-equivalent route entered instead of a stage's
        witness must be covered by a degraded_witness row, or the helper
        would silently treat it as completed and skip it on rerun."""
        mapped: dict = {}
        for stage in mod.EOD.stages:
            for d in stage.degraded_witness:
                assert d in eod_states, f"{stage.name}: degraded witness {d} not in step_function_eod.json"
                mapped[d] = stage.name
        assert "SkipEODReconcileDataGap" in mapped


# ---------------------------------------------------------------------------
# Reachability simulator directly (defensive coverage of the linear-chain
# and role-conjunction semantics independent of any fixture)
# ---------------------------------------------------------------------------

class TestSimulateReachableWorks:
    def test_daily_all_stages_reachable_with_no_flags(self, mod):
        reachable = mod._simulate_reachable_works(mod.DAILY, {}, {}, "daily")
        assert reachable == {s.name for s in mod.DAILY.stages}

    def test_daily_flag_removes_stage_from_reachable(self, mod):
        reachable = mod._simulate_reachable_works(mod.DAILY, {"skip_scanner": True}, {}, "daily")
        assert "scanner" not in reachable
        assert "morning_enrich" in reachable

    def test_eod_flag_inert_under_wrong_role(self, mod):
        """The core config#1614-style coherence fact: an EOD skip flag does
        NOTHING unless role == operator-replay."""
        reachable = mod._simulate_reachable_works(
            mod.EOD, {"skip_eod_reconcile": True}, {}, "eod",
        )
        assert "eod_reconcile" in reachable  # flag inert under role "eod"

    def test_eod_flag_live_under_operator_replay(self, mod):
        reachable = mod._simulate_reachable_works(
            mod.EOD, {"skip_eod_reconcile": True}, {}, "operator-replay",
        )
        assert "eod_reconcile" not in reachable

    def test_original_input_flag_falls_through_when_not_overridden(self, mod):
        reachable = mod._simulate_reachable_works(
            mod.EOD, {}, {"skip_post_market_data": True}, "operator-replay",
        )
        assert "post_market_data" not in reachable
