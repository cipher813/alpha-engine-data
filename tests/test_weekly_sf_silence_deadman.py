"""weekly_sf_silence_deadman.py — the silence deadman for the weekly
pipeline's two trigger roles (alpha-engine-config#6689 deliverable 3,
resolving #5599).

Structured in three layers, each tested at its own layer so a boto3 mock
never has to stand in for calendar or classification logic:

  1. Expected-slot derivation (pure calendar math — no AWS, no executions).
  2. Slot <-> execution matching (pure — takes pre-built ExecutionRecords,
     including the WeeklyRunDayGateChoice no-op-vs-real-run distinction).
  3. CLI plumbing (boto3 mocked at the client-factory level) — the
     --no-notify flag and the exit-code contract.

The measured-shape regression (`TestMeasured20260805And0806Shape`) replays
the exact failure this script exists to catch: postclose never fired at all
on 2026-08-05/08-06, with zero signal anywhere else.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "weekly_sf_silence_deadman.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("weekly_sf_silence_deadman", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["weekly_sf_silence_deadman"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


# nousergon_lib.trading_calendar re-exports krepis's NYSE calendar — reuse
# directly rather than faking it, since the whole point of #6689's slot
# derivation is to agree with the SAME calendar the live SF gates use.
from nousergon_lib.trading_calendar import is_trading_day, next_trading_day  # noqa: E402


def _utc(d: date, hour: int = 21) -> datetime:
    return datetime(d.year, d.month, d.day, hour, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Layer 1: expected-slot derivation
# ---------------------------------------------------------------------------

class TestLastSessionOfWeek:
    def test_friday_is_last_session_on_a_normal_week(self, mod):
        # 2026-08-07 is a Friday; 2026-08-10 (next trading day) is Monday —
        # a different ISO week.
        assert mod._is_last_session_of_week(date(2026, 8, 7), is_trading_day, next_trading_day) is True

    def test_wednesday_is_not_last_session(self, mod):
        assert mod._is_last_session_of_week(date(2026, 8, 5), is_trading_day, next_trading_day) is False

    def test_a_weekend_day_is_not_a_session_at_all(self, mod):
        assert mod._is_last_session_of_week(date(2026, 8, 8), is_trading_day, next_trading_day) is False


class TestComputeExpectedSlots:
    def test_daily_cadence_expects_exercise_on_every_trading_day_ungated(self, mod):
        today = date(2026, 8, 9)  # Sunday
        slots = mod.compute_expected_slots(today, "daily", 5, is_trading_day, next_trading_day)
        exercise_days = {s.day for s in slots if s.role == "exercise"}
        assert exercise_days == {date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)}
        assert all(not s.gated_off for s in slots if s.role == "exercise")

    def test_weekly_only_gates_every_exercise_slot(self, mod):
        today = date(2026, 8, 9)
        slots = mod.compute_expected_slots(today, "weekly-only", 5, is_trading_day, next_trading_day)
        exercise = [s for s in slots if s.role == "exercise"]
        assert exercise  # still derived — just gated
        assert all(s.gated_off for s in exercise)

    def test_off_gates_every_exercise_slot(self, mod):
        today = date(2026, 8, 9)
        slots = mod.compute_expected_slots(today, "off", 5, is_trading_day, next_trading_day)
        exercise = [s for s in slots if s.role == "exercise"]
        assert exercise
        assert all(s.gated_off for s in exercise)

    def test_weekly_slot_is_never_gated_by_cadence(self, mod):
        """The knob has exactly one insertion point (the exercise launch);
        the Saturday cron is untouched regardless of the declared value."""
        today = date(2026, 8, 9)
        for cadence in ("daily", "weekly-only", "off"):
            slots = mod.compute_expected_slots(today, cadence, 5, is_trading_day, next_trading_day)
            weekly = [s for s in slots if s.role == "weekly"]
            assert weekly and all(not s.gated_off for s in weekly), cadence

    def test_weekly_slot_lands_the_day_after_the_last_session(self, mod):
        """Friday 2026-08-07 is the week's last session; the real weekly run
        (WeeklyRunDayGateChoice self-gated true) lands 2026-08-08 (Saturday) —
        deliberately the day AFTER the session, not the session day itself.
        See this script's module docstring for why this refines the filing
        issue's 'weekly for the week's last session' wording."""
        today = date(2026, 8, 9)
        slots = mod.compute_expected_slots(today, "daily", 5, is_trading_day, next_trading_day)
        weekly_days = {s.day for s in slots if s.role == "weekly"}
        assert date(2026, 8, 8) in weekly_days
        assert date(2026, 8, 7) not in weekly_days


# ---------------------------------------------------------------------------
# Layer 2: slot <-> execution matching
# ---------------------------------------------------------------------------

class TestEvaluateSlot:
    def test_gated_off_is_ok_without_touching_executions(self, mod):
        slot = mod.ExpectedSlot(day=date(2026, 8, 5), role="exercise", gated_off=True, note="gated")
        result = mod.evaluate_slot(slot, executions=[])
        assert result.classification == "GATED_OFF"

    def test_exercise_ok_when_a_matching_execution_exists(self, mod):
        slot = mod.ExpectedSlot(day=date(2026, 8, 4), role="exercise")
        execs = [mod.ExecutionRecord(name="abc", role="exercise", status="SUCCEEDED",
                                      start=_utc(date(2026, 8, 4)), stop=_utc(date(2026, 8, 4), 22))]
        result = mod.evaluate_slot(slot, execs)
        assert result.classification == "OK"
        assert result.matched_execution == "abc"

    def test_exercise_critical_when_absent(self, mod):
        slot = mod.ExpectedSlot(day=date(2026, 8, 5), role="exercise")
        result = mod.evaluate_slot(slot, executions=[])
        assert result.classification == "CRITICAL"

    def test_exercise_ignores_a_weekly_role_execution_on_the_same_day(self, mod):
        slot = mod.ExpectedSlot(day=date(2026, 8, 8), role="exercise")
        execs = [mod.ExecutionRecord(name="weekly1", role="weekly", status="SUCCEEDED",
                                      start=_utc(date(2026, 8, 8)), stop=_utc(date(2026, 8, 8), 23))]
        assert mod.evaluate_slot(slot, execs).classification == "CRITICAL"

    def test_weekly_ok_with_a_real_run(self, mod):
        slot = mod.ExpectedSlot(day=date(2026, 8, 8), role="weekly")
        start = _utc(date(2026, 8, 8), 9)
        execs = [mod.ExecutionRecord(name="w1", role="weekly", status="SUCCEEDED",
                                      start=start, stop=start + timedelta(hours=3))]
        result = mod.evaluate_slot(slot, execs)
        assert result.classification == "OK"
        assert result.matched_execution == "w1"

    def test_weekly_critical_when_only_the_run_day_gate_noop_exists(self, mod):
        """A 2-second SUCCEEDED weekly-role execution is
        WeeklyRunDayGateChoice's designed skip on a non-run day, not a real
        run — counting it as 'ran' would hide a genuinely silent week."""
        slot = mod.ExpectedSlot(day=date(2026, 8, 8), role="weekly")
        start = _utc(date(2026, 8, 8), 9)
        execs = [mod.ExecutionRecord(name="noop", role="weekly", status="SUCCEEDED",
                                      start=start, stop=start + timedelta(seconds=2))]
        result = mod.evaluate_slot(slot, execs)
        assert result.classification == "CRITICAL"
        assert "no-op" in result.detail

    def test_weekly_critical_when_nothing_at_all(self, mod):
        slot = mod.ExpectedSlot(day=date(2026, 8, 8), role="weekly")
        assert mod.evaluate_slot(slot, executions=[]).classification == "CRITICAL"

    def test_weekly_real_run_status_need_not_be_succeeded(self, mod):
        """Existence, not success, is what the SILENCE deadman checks — a
        FAILED weekly execution is a different (already-alerted) problem, but
        it is not SILENCE."""
        slot = mod.ExpectedSlot(day=date(2026, 8, 8), role="weekly")
        start = _utc(date(2026, 8, 8), 9)
        execs = [mod.ExecutionRecord(name="w1", role="weekly", status="FAILED",
                                      start=start, stop=start + timedelta(hours=1))]
        assert mod.evaluate_slot(slot, execs).classification == "OK"


class TestNormalizeRole:
    @pytest.mark.parametrize("raw,expected", [
        ("exercise", "exercise"),
        ("weekly", "weekly"),
        ("operator-replay", None),
        ("watch-rerun", None),
        (None, None),
        ("", None),
    ])
    def test_only_the_two_declared_roles_pass_through(self, mod, raw, expected):
        assert mod.normalize_role(raw) == expected


# ---------------------------------------------------------------------------
# Measured-shape regression: 2026-08-05 / 2026-08-06 (postclose never fired)
# ---------------------------------------------------------------------------

class TestMeasured20260805And0806Shape:
    """Replays alpha-engine-config#6689's filing evidence: postclose FAILED
    before the launch state on 07-27/07-28 and never fired AT ALL on
    08-05/08-06 — silence, not failure, with zero prior signal. cadence=daily
    throughout (the value in effect when this was measured, and unchanged by
    this PR)."""

    TODAY = date(2026, 8, 9)

    def _executions(self, mod):
        # Real exercise runs on the trading days that DID chain successfully.
        return [
            _record(mod, "e0804", "exercise", "SUCCEEDED", date(2026, 8, 4)),
            _record(mod, "e0807", "exercise", "SUCCEEDED", date(2026, 8, 7)),
            # A real weekly run landed on the gate day (Saturday 08-08) — this
            # regression is scoped to the exercise silence, not the weekly leg.
            _record(mod, "w0808", "weekly", "SUCCEEDED", date(2026, 8, 8), hours=3),
        ]

    def test_the_two_silent_days_are_critical(self, mod):
        slots = mod.compute_expected_slots(self.TODAY, "daily", 5, is_trading_day, next_trading_day)
        results = {(r.slot.day, r.slot.role): r.classification for r in mod.evaluate(slots, self._executions(mod))}
        assert results[(date(2026, 8, 5), "exercise")] == "CRITICAL"
        assert results[(date(2026, 8, 6), "exercise")] == "CRITICAL"

    def test_the_days_that_actually_ran_are_ok(self, mod):
        slots = mod.compute_expected_slots(self.TODAY, "daily", 5, is_trading_day, next_trading_day)
        results = {(r.slot.day, r.slot.role): r.classification for r in mod.evaluate(slots, self._executions(mod))}
        assert results[(date(2026, 8, 4), "exercise")] == "OK"
        assert results[(date(2026, 8, 7), "exercise")] == "OK"
        assert results[(date(2026, 8, 8), "weekly")] == "OK"

    def test_exactly_two_criticals_fleet_wide_in_this_window(self, mod):
        slots = mod.compute_expected_slots(self.TODAY, "daily", 5, is_trading_day, next_trading_day)
        results = mod.evaluate(slots, self._executions(mod))
        critical = [r for r in results if r.classification == "CRITICAL"]
        assert len(critical) == 2
        assert {(r.slot.day, r.slot.role) for r in critical} == {
            (date(2026, 8, 5), "exercise"), (date(2026, 8, 6), "exercise"),
        }


class TestWeeklyOnlyCadenceShape:
    """No exercise runs at all — every exercise slot is a designed gate-off,
    not silence. The weekly leg is unaffected and still separately checked."""

    TODAY = date(2026, 8, 9)

    def test_exercise_slots_are_gated_off_not_critical(self, mod):
        slots = mod.compute_expected_slots(self.TODAY, "weekly-only", 5, is_trading_day, next_trading_day)
        results = mod.evaluate(slots, executions=[])
        exercise_results = [r for r in results if r.slot.role == "exercise"]
        assert exercise_results
        assert all(r.classification == "GATED_OFF" for r in exercise_results)

    def test_weekly_slot_is_still_checked_and_critical_when_silent(self, mod):
        slots = mod.compute_expected_slots(self.TODAY, "weekly-only", 5, is_trading_day, next_trading_day)
        results = mod.evaluate(slots, executions=[])
        weekly_results = [r for r in results if r.slot.role == "weekly"]
        assert weekly_results and all(r.classification == "CRITICAL" for r in weekly_results)

    def test_no_gated_off_result_ever_publishes(self, mod):
        slots = mod.compute_expected_slots(self.TODAY, "weekly-only", 5, is_trading_day, next_trading_day)
        results = mod.evaluate(slots, executions=[_record(mod, "w0808", "weekly", "SUCCEEDED", date(2026, 8, 8), hours=3)])
        critical = [r for r in results if r.classification == "CRITICAL"]
        assert not critical  # weekly ran; exercise is gated-off — nothing to page


def _record(mod, name, role, status, day, hours=1):
    start = _utc(day, 21)
    stop = start + timedelta(hours=hours)
    return mod.ExecutionRecord(name=name, role=role, status=status, start=start, stop=stop)


# ---------------------------------------------------------------------------
# Layer 3: CLI plumbing (boto3 mocked)
# ---------------------------------------------------------------------------

class _FakeSF:
    def __init__(self, executions):
        self._executions = executions

    def list_executions(self, **kwargs):
        return {"executions": self._executions, "nextToken": None}

    def describe_execution(self, executionArn):
        for e in self._executions:
            if e["executionArn"] == executionArn:
                return {"input": json.dumps({"pipeline_role": e["_role"]})}
        raise KeyError(executionArn)


class _FakeSNS:
    def __init__(self):
        self.published = []

    def publish(self, **kwargs):
        self.published.append(kwargs)
        return {}


def _fake_execution(name, role, status, day, hours=1):
    start = _utc(day, 21)
    stop = start + timedelta(hours=hours)
    return {
        "executionArn": f"arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:{name}",
        "name": name,
        "status": status,
        "startDate": start,
        "stopDate": stop,
        "_role": role,
    }


class TestMainCLI:
    def _patch_boto3(self, monkeypatch, mod, executions, sns):
        import boto3
        fake_sf = _FakeSF(executions)

        def _client(service, region_name=None):
            if service == "stepfunctions":
                return fake_sf
            if service == "sns":
                return sns
            if service == "ssm":
                raise AssertionError("ssm should not be called without --live")
            raise AssertionError(f"unexpected client: {service}")

        monkeypatch.setattr(boto3, "client", _client)
        monkeypatch.setattr(mod, "load_cadence_from_manifest", lambda: "daily")
        monkeypatch.setattr(mod, "_today", lambda: date(2026, 8, 9))

    def test_exit_zero_when_every_expected_slot_is_covered(self, mod, monkeypatch, capsys):
        # today=2026-08-09, window=5 -> exercise slots on 08-04..08-07, weekly
        # slot on 08-08 (day after the week's last session, 08-07).
        execs = [
            _fake_execution("e0804", "exercise", "SUCCEEDED", date(2026, 8, 4)),
            _fake_execution("e0805", "exercise", "SUCCEEDED", date(2026, 8, 5)),
            _fake_execution("e0806", "exercise", "SUCCEEDED", date(2026, 8, 6)),
            _fake_execution("e0807", "exercise", "SUCCEEDED", date(2026, 8, 7)),
            _fake_execution("w0808", "weekly", "SUCCEEDED", date(2026, 8, 8), hours=3),
        ]
        sns = _FakeSNS()
        self._patch_boto3(monkeypatch, mod, execs, sns)
        rc = mod.main(["--window-days", "5", "--no-notify"])
        assert rc == 0
        assert sns.published == []

    def test_critical_triggers_sns_publish_by_default(self, mod, monkeypatch):
        sns = _FakeSNS()
        self._patch_boto3(monkeypatch, mod, executions=[], sns=sns)
        rc = mod.main(["--window-days", "10"])
        assert rc == 1
        assert len(sns.published) == 1
        assert sns.published[0]["TopicArn"] == mod.SNS_TOPIC_ARN

    def test_no_notify_suppresses_the_publish_but_exit_code_is_unchanged(self, mod, monkeypatch):
        sns = _FakeSNS()
        self._patch_boto3(monkeypatch, mod, executions=[], sns=sns)
        rc = mod.main(["--window-days", "10", "--no-notify"])
        assert rc == 1
        assert sns.published == []

    def test_json_output_is_valid_json(self, mod, monkeypatch, capsys):
        sns = _FakeSNS()
        self._patch_boto3(monkeypatch, mod, executions=[], sns=sns)
        mod.main(["--window-days", "3", "--no-notify", "--json"])
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed["cadence"] == "daily"
        assert "results" in parsed
