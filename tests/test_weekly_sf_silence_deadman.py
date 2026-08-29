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

    def test_a_slot_predating_the_first_declared_entry_is_gated_off(self, mod):
        """alpha-engine-config-I7175: a slot on a date before any
        exercise_cadence declaration existed is GATED_OFF ('the declaration
        did not exist yet'), never CRITICAL ('expected and absent') — the
        two are different states even though both are exit-0/non-paging."""
        history = [mod.CadenceEntry(value="daily", effective_from=date(2026, 7, 27))]
        today = date(2026, 7, 20)  # entirely before the only entry
        slots = mod.compute_expected_slots(today, "daily", 3, is_trading_day, next_trading_day, cadence_history=history)
        exercise = [s for s in slots if s.role == "exercise"]
        assert exercise
        assert all(s.gated_off for s in exercise)
        results = mod.evaluate(slots, executions=[])
        assert all(r.classification != "CRITICAL" for r in results if r.slot.role == "exercise")
        assert all(r.classification == "GATED_OFF" for r in results if r.slot.role == "exercise")

    def test_a_slot_on_or_after_the_first_entry_is_judged_normally(self, mod):
        history = [
            mod.CadenceEntry(value="weekly-only", effective_from=date(2026, 1, 1)),
            mod.CadenceEntry(value="daily", effective_from=date(2026, 7, 27)),
        ]
        today = date(2026, 7, 28)
        slots = mod.compute_expected_slots(today, "daily", 5, is_trading_day, next_trading_day, cadence_history=history)
        by_day = {s.day: s for s in slots if s.role == "exercise"}
        # 07-24 (Fri) predates the 'daily' entry -> gated off under weekly-only
        assert by_day[date(2026, 7, 24)].gated_off is True
        # 07-27/07-28 are on/after the daily entry -> ungated, a real slot
        assert by_day[date(2026, 7, 27)].gated_off is False
        assert by_day[date(2026, 7, 28)].gated_off is False
        results = {r.slot.day: r.classification for r in mod.evaluate(slots, executions=[])}
        assert results[date(2026, 7, 24)] == "GATED_OFF"
        assert results[date(2026, 7, 27)] == "CRITICAL"
        assert results[date(2026, 7, 28)] == "CRITICAL"

    def test_omitting_cadence_history_preserves_the_flat_cadence_behavior(self, mod):
        """The pipeline-watchdog Lambda calls compute_expected_slots
        positionally without this argument — must be indistinguishable from
        before I7175 when omitted."""
        today = date(2026, 8, 9)
        slots = mod.compute_expected_slots(today, "daily", 5, is_trading_day, next_trading_day)
        assert all(not s.gated_off for s in slots if s.role == "exercise")

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
        """A SUCCEEDED weekly-role execution whose output proves the gate
        skipped it is WeeklyRunDayGateChoice's designed skip on a non-run
        day, not a real run — counting it as 'ran' would hide a genuinely
        silent week. `is_gate_out` is set from the output verdict, never
        from duration (alpha-engine-config-I8057)."""
        slot = mod.ExpectedSlot(day=date(2026, 8, 8), role="weekly")
        start = _utc(date(2026, 8, 8), 9)
        execs = [mod.ExecutionRecord(name="noop", role="weekly", status="SUCCEEDED",
                                      start=start, stop=start + timedelta(seconds=2),
                                      is_gate_out=True)]
        result = mod.evaluate_slot(slot, execs)
        assert result.classification == "CRITICAL"
        assert "no-op" in result.detail


    def test_weekly_ok_with_a_slow_real_run_despite_short_old_threshold(self, mod):
        """alpha-engine-config-I8057's measured fact: the fleet's shortest
        genuine weekly execution ran 12.6s — a duration heuristic would have
        misclassified it. Without `is_gate_out` set, a short SUCCEEDED
        execution is a real run regardless of how fast it finished."""
        slot = mod.ExpectedSlot(day=date(2026, 8, 8), role="weekly")
        start = _utc(date(2026, 8, 8), 9)
        execs = [mod.ExecutionRecord(name="director-verify-0731-003355Z", role="weekly",
                                      status="SUCCEEDED", start=start,
                                      stop=start + timedelta(seconds=12.6))]
        result = mod.evaluate_slot(slot, execs)
        assert result.classification == "OK"


    def test_weekly_critical_when_a_slow_gate_out_is_the_only_execution(self, mod):
        """Mirror fact: a gate-out that happened to run long is still a
        gate-out — `is_gate_out` is never re-derived from duration."""
        slot = mod.ExpectedSlot(day=date(2026, 8, 22), role="weekly")
        start = _utc(date(2026, 8, 22), 9)
        execs = [mod.ExecutionRecord(name="watch-rerun-2026-08-22-3", role="weekly",
                                      status="SUCCEEDED", start=start,
                                      stop=start + timedelta(seconds=871.8),
                                      is_gate_out=True)]
        result = mod.evaluate_slot(slot, execs)
        assert result.classification == "CRITICAL"

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


class TestCadenceHistory:
    """alpha-engine-config-I7175: the dated exercise_cadence history and its
    date -> value resolver, independent of AWS or the calendar."""

    def test_cadence_for_date_returns_none_before_the_first_entry(self, mod):
        history = [mod.CadenceEntry(value="daily", effective_from=date(2026, 7, 27))]
        assert mod.cadence_for_date(history, date(2026, 7, 26)) is None

    def test_cadence_for_date_returns_the_entry_in_force(self, mod):
        history = [
            mod.CadenceEntry(value="weekly-only", effective_from=date(2026, 1, 1)),
            mod.CadenceEntry(value="daily", effective_from=date(2026, 7, 27)),
        ]
        assert mod.cadence_for_date(history, date(2026, 7, 26)) == "weekly-only"
        assert mod.cadence_for_date(history, date(2026, 7, 27)) == "daily"
        assert mod.cadence_for_date(history, date(2026, 12, 31)) == "daily"

    def test_parse_cadence_history_sorts_out_of_order_entries(self, mod):
        raw = [
            {"value": "daily", "effective_from": "2026-07-27"},
            {"value": "weekly-only", "effective_from": "2026-01-01"},
        ]
        entries = mod._parse_cadence_history(raw, "test")
        assert [e.effective_from for e in entries] == [date(2026, 1, 1), date(2026, 7, 27)]

    def test_parse_cadence_history_rejects_a_bare_scalar(self, mod):
        with pytest.raises(ValueError):
            mod._parse_cadence_history("daily", "test")

    def test_parse_cadence_history_rejects_an_empty_list(self, mod):
        with pytest.raises(ValueError):
            mod._parse_cadence_history([], "test")

    def test_load_cadence_history_reads_the_real_manifest(self, mod):
        history = mod.load_cadence_history()
        assert history
        assert history == sorted(history, key=lambda e: e.effective_from)
        # Assert the PROPERTY, not today's literal value. This used to pin
        # ``history[-1].value == "daily"``, which made every legitimate cadence
        # ruling fail a test whose purpose is "the loader reads the real
        # manifest" — the current value is incidental to that. Brian's
        # 2026-08-13 ruling (daily -> off) was the first to hit it. The manifest
        # is still the source of truth for what is legal, so a typo'd value
        # still fails here.
        raw = json.loads(mod.MANIFEST_PATH.read_text())
        assert history[-1].value in raw["allowed_values"]


# ---------------------------------------------------------------------------
# Acceptance regression: the real 45-day window (alpha-engine-config-I7175)
# ---------------------------------------------------------------------------

class TestMeasuredFortyFiveDayShape:
    """Replays the exact live run measured 2026-08-13
    (``--live --no-notify --window-days 45 --through today``): 23 CRITICAL
    before this fix (19 false positives 2026-06-29..2026-07-24, predating the
    exercise cadence, plus the 4 real silences), 4 after. Uses the REAL
    checked-in manifest history, not a synthetic one, so a future edit to
    ``weekly_cadence.json`` that reintroduces the retroactivity bug fails
    this test."""

    TODAY = date(2026, 8, 13)
    REAL_CRITICAL_DAYS = {date(2026, 7, 27), date(2026, 7, 28), date(2026, 8, 5), date(2026, 8, 6)}

    def _executions(self, mod):
        # Every trading day from the first real exercise run (2026-07-29)
        # onward EXCEPT the two real silences (08-05/08-06) got a matching
        # exercise-role execution, per the live output measured 2026-08-13.
        execs = []
        d = date(2026, 7, 29)
        while d <= self.TODAY:
            if is_trading_day(d) and d not in self.REAL_CRITICAL_DAYS:
                execs.append(_record(mod, f"e{d.isoformat()}", "exercise", "SUCCEEDED", d))
            d += timedelta(days=1)
        # Every weekly-role run that actually landed in this window, per the
        # live output measured 2026-08-13 — the weekly leg is not itself
        # silent here and is out of scope for this regression (it is a
        # separate, unaffected code path: never gated by cadence at all).
        for w in (
            date(2026, 7, 3), date(2026, 7, 11), date(2026, 7, 18),
            date(2026, 7, 25), date(2026, 8, 1), date(2026, 8, 8),
        ):
            execs.append(_record(mod, f"w{w.isoformat()}", "weekly", "SUCCEEDED", w, hours=3))
        return execs

    def test_exactly_four_critical_and_they_are_the_named_four(self, mod):
        history = mod.load_cadence_history()
        slots = mod.compute_expected_slots(
            self.TODAY, "daily", 45, is_trading_day, next_trading_day, cadence_history=history,
        )
        results = mod.evaluate(slots, self._executions(mod))
        critical = [r for r in results if r.classification == "CRITICAL"]
        assert {r.slot.day for r in critical} == self.REAL_CRITICAL_DAYS
        assert len(critical) == 4

    def test_pre_cadence_days_are_gated_off_not_critical(self, mod):
        history = mod.load_cadence_history()
        slots = mod.compute_expected_slots(
            self.TODAY, "daily", 45, is_trading_day, next_trading_day, cadence_history=history,
        )
        results = mod.evaluate(slots, self._executions(mod))
        pre_cadence = [
            r for r in results
            if r.slot.role == "exercise" and r.slot.day < date(2026, 7, 27)
        ]
        assert pre_cadence  # the window does reach back that far
        assert all(r.classification == "GATED_OFF" for r in pre_cadence)

    def test_2026_07_04_derives_no_weekly_slot(self, mod):
        """NOT a missed Saturday: NYSE observed July 4th on Friday 07-03, so
        WeeklyRunDayGate selects 07-03 and the deadman correctly derives no
        weekly slot for 07-04 at all. Confirmed unchanged by this fix."""
        history = mod.load_cadence_history()
        slots = mod.compute_expected_slots(
            self.TODAY, "daily", 45, is_trading_day, next_trading_day, cadence_history=history,
        )
        weekly_days = {s.day for s in slots if s.role == "weekly"}
        assert date(2026, 7, 4) not in weekly_days
        assert date(2026, 7, 3) in weekly_days


class TestNormalizeRole:
    """Amended 2026-08-16 (alpha-engine-config-I7440).

    This class previously asserted that ``watch-rerun`` normalizes to
    ``None`` — it pinned the defect rather than forbidding it. That
    collapsing is precisely what disarmed ``pipeline-watchdog``'s weekly
    failed-cycle check: its ``WEEKLY_CADENCE_ROLES`` filter names
    ``watch-rerun`` and ``recovery``, and this function guaranteed no record
    could ever carry either, so a weekly cycle recovered by the §2.5
    mechanical rerun could never clear the detector watching it. The test
    passed for exactly as long as the bug existed and would have turned CI
    red on the fix.

    Roles are now passed through for the whole KNOWN_ROLES vocabulary, and
    the closed-vocabulary property — an UNRECOGNIZED role still normalizes to
    None, so a new launch path cannot silently start counting as a declared
    cycle — is what this class asserts instead.
    """

    @pytest.mark.parametrize("raw,expected", [
        ("exercise", "exercise"),
        ("weekly", "weekly"),
        ("watch-rerun", "watch-rerun"),
        ("recovery", "recovery"),
        ("shell-run", "shell-run"),
        # Not a weekly-SF launch role (it is the postclose SF's operator
        # replay role) — still discarded.
        ("operator-replay", None),
        ("something-new", None),
        (None, None),
        ("", None),
    ])
    def test_known_roles_pass_through_and_unknown_ones_are_discarded(self, mod, raw, expected):
        assert mod.normalize_role(raw) == expected

    def test_slot_matching_is_unchanged_by_the_widening(self, mod):
        """Behaviour-preservation guard for THIS module. Slot roles are only
        weekly/exercise, so records that used to be None and matched no slot
        are now named and must still match no slot — the widening may not
        let a rerun start satisfying a cron slot."""
        day = date(2026, 8, 15)
        start = datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc)
        rerun = mod.ExecutionRecord(
            name="watch-rerun-2026-08-15-1", role="watch-rerun",
            status="SUCCEEDED", start=start,
            stop=start + timedelta(seconds=1800), run_date=day,
        )
        slot = mod.ExpectedSlot(day=day, role="weekly", gated_off=False, note="")
        result = mod.evaluate_slot(slot, [rerun])
        assert result.classification == "CRITICAL"


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


class _FakeSFWithOutput:
    """Minimal Step Functions fake carrying `output`, for the fetch-level
    `is_gate_out` wiring test — `TestMainCLI`'s `_FakeSF` below never sets
    `output` because no existing CLI test needed the gate axis."""

    def __init__(self, executions, output_by_arn):
        self._executions = executions
        self._output_by_arn = output_by_arn

    def list_executions(self, **kwargs):
        return {"executions": self._executions, "nextToken": None}

    def describe_execution(self, executionArn):
        for e in self._executions:
            if e["executionArn"] == executionArn:
                resp = {"input": json.dumps({"pipeline_role": e["_role"]})}
                if executionArn in self._output_by_arn:
                    resp["output"] = json.dumps(self._output_by_arn[executionArn])
                return resp
        raise KeyError(executionArn)


class TestFetchExecutionRecordsGateOut:
    """alpha-engine-config-I8057: `is_gate_out` is read from the SAME
    `describe_execution` call already made for role/run_date — no extra AWS
    call, no clock."""

    def test_gate_out_output_sets_is_gate_out(self, mod):
        arn = "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:g1"
        start = _utc(date(2026, 8, 21), 9)
        ex = {"executionArn": arn, "name": "g1", "status": "SUCCEEDED",
              "startDate": start, "stopDate": start + timedelta(seconds=5.7),
              "_role": "weekly"}
        sf = _FakeSFWithOutput([ex], {arn: {"weekly_run_day_gate": {"Payload": {
            "is_weekly_run_day": False, "marker": "NOT_WEEKLY_RUN_DAY"}}}})
        records = mod.fetch_execution_records(sf, window_days=5, today=date(2026, 8, 23))
        assert records[0].is_gate_out is True

    def test_real_run_output_leaves_is_gate_out_false(self, mod):
        arn = "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:r1"
        start = _utc(date(2026, 8, 3), 9)
        ex = {"executionArn": arn, "name": "r1", "status": "SUCCEEDED",
              "startDate": start, "stopDate": start + timedelta(seconds=12.6),
              "_role": "weekly"}
        sf = _FakeSFWithOutput([ex], {arn: {"some_other_stage": {"ok": True}}})
        records = mod.fetch_execution_records(sf, window_days=5, today=date(2026, 8, 5))
        assert records[0].is_gate_out is False


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
        # --through today, today=2026-08-09 (a Sunday), window=5 -> exercise
        # slots on 08-04..08-07, weekly slot on 08-08 (day after the week's
        # last session, 08-07). 08-09 itself is not a trading day.
        execs = [
            _fake_execution("e0804", "exercise", "SUCCEEDED", date(2026, 8, 4)),
            _fake_execution("e0805", "exercise", "SUCCEEDED", date(2026, 8, 5)),
            _fake_execution("e0806", "exercise", "SUCCEEDED", date(2026, 8, 6)),
            _fake_execution("e0807", "exercise", "SUCCEEDED", date(2026, 8, 7)),
            _fake_execution("w0808", "weekly", "SUCCEEDED", date(2026, 8, 8), hours=3),
        ]
        sns = _FakeSNS()
        self._patch_boto3(monkeypatch, mod, execs, sns)
        rc = mod.main(["--window-days", "5", "--no-notify", "--through", "today"])
        assert rc == 0
        assert sns.published == []

    def test_default_anchor_excludes_todays_not_yet_due_slot(self, mod, monkeypatch, capsys):
        """alpha-engine-config#6738. Measured live 2026-08-09: run at 21:00 PT
        (= 2026-08-10 UTC), the CLI reported the 2026-08-10 exercise slot
        CRITICAL — Monday's postclose was ~23h in the FUTURE. The default
        anchor stops at the last DUE day so a not-yet-due slot can never be
        reported as silence."""
        # anchor 08-09, window 5 -> [08-04 .. 08-09]
        execs = [
            _fake_execution("e0804", "exercise", "SUCCEEDED", date(2026, 8, 4)),
            _fake_execution("e0805", "exercise", "SUCCEEDED", date(2026, 8, 5)),
            _fake_execution("e0806", "exercise", "SUCCEEDED", date(2026, 8, 6)),
            _fake_execution("e0807", "exercise", "SUCCEEDED", date(2026, 8, 7)),
            _fake_execution("w0808", "weekly", "SUCCEEDED", date(2026, 8, 8), hours=3),
        ]
        sns = _FakeSNS()
        self._patch_boto3(monkeypatch, mod, execs, sns)
        monkeypatch.setattr(mod, "_today", lambda: date(2026, 8, 10))

        rc = mod.main(["--window-days", "5", "--no-notify", "--json"])
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["evaluated_through"] == "2026-08-09"
        assert "2026-08-10" not in [r["day"] for r in parsed["results"]]
        assert parsed["critical"] == 0
        assert rc == 0

        # ...and opting back in reports exactly the one not-yet-due slot.
        rc_today = mod.main(
            ["--window-days", "5", "--no-notify", "--json", "--through", "today"]
        )
        parsed_today = json.loads(capsys.readouterr().out)
        assert parsed_today["evaluated_through"] == "2026-08-10"
        assert [
            r["day"] for r in parsed_today["results"] if r["classification"] == "CRITICAL"
        ] == ["2026-08-10"]
        assert rc_today == 1

    def test_last_due_day_is_the_shared_anchor_for_both_entry_points(self, mod):
        assert mod.last_due_day(date(2026, 8, 10)) == date(2026, 8, 9)
        # Purely calendar arithmetic — no trading-day skipping. The slot
        # derivation already drops non-sessions, so a Sunday anchor simply
        # yields no Sunday slot rather than silently sliding the window.
        assert mod.last_due_day(date(2026, 8, 9)) == date(2026, 8, 8)

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


class TestScheduledSubstrateWiring:
    """alpha-engine-config#6738 — the deadman's schedule surface is the
    already-scheduled, already-un-paused ``alpha-engine-pipeline-watchdog``
    Lambda rather than a second EventBridge trigger of its own.

    These are packaging invariants, not behaviour: each one, if it silently
    broke, would leave the scheduled check importing nothing, reading nothing,
    or drifting from the CLI an operator reruns by hand — with the Lambda
    still reporting a clean summary. The detector for a detector.
    """

    WATCHDOG_DIR = REPO_ROOT / "infrastructure" / "lambdas" / "pipeline-watchdog"

    def test_deploy_packages_the_deadman_module_into_the_zip(self):
        """index.py imports ``weekly_sf_silence_deadman`` at module scope; if
        deploy.sh stops copying it the Lambda dies on cold start."""
        deploy = (self.WATCHDOG_DIR / "deploy.sh").read_text(encoding="utf-8")
        assert "scripts/weekly_sf_silence_deadman.py" in deploy
        assert '"${PKG}/weekly_sf_silence_deadman.py"' in deploy

    def test_watchdog_imports_the_shared_slot_logic_rather_than_copying_it(self):
        index_src = (self.WATCHDOG_DIR / "index.py").read_text(encoding="utf-8")
        assert "from weekly_sf_silence_deadman import" in index_src
        for symbol in (
            "compute_expected_slots",
            "evaluate",
            "fetch_execution_records",
            "load_cadence_from_ssm",
        ):
            assert symbol in index_src
        # A second, local re-derivation of the slot rules is the drift this
        # single-implementation packaging exists to prevent.
        assert "def compute_expected_slots" not in index_src

    def test_watchdog_role_is_granted_the_cadence_parameter_read(self):
        """The substrate identity needs its own grant — the existing SSM read
        for /alpha-engine/weekly-sf/exercise-cadence covers the SF role only
        (config#6701)."""
        policy = json.loads(
            (self.WATCHDOG_DIR / "iam-policy.json").read_text(encoding="utf-8")
        )
        wanted = (
            "arn:aws:ssm:us-east-1:711398986525:parameter"
            "/alpha-engine/weekly-sf/exercise-cadence"
        )
        granted = [
            stmt
            for stmt in policy["Statement"]
            if stmt.get("Effect") == "Allow"
            and "ssm:GetParameter" in stmt.get("Action", [])
            and wanted in (
                stmt["Resource"]
                if isinstance(stmt["Resource"], list)
                else [stmt["Resource"]]
            )
        ]
        assert granted, "watchdog exec role cannot read the cadence declaration"

    def test_the_deploy_workflow_fires_when_this_module_changes(self):
        """Packaging a file into another component's zip creates a second way
        for a merged fix to never go live: the deploy workflow's path filter
        must cover the packaged file, not just the Lambda directory."""
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "deploy-pipeline-watchdog.yml"
        ).read_text(encoding="utf-8")
        assert "scripts/weekly_sf_silence_deadman.py" in workflow

    def test_no_second_trigger_was_introduced_for_the_deadman(self):
        """Design choice (A): one liveness surface. A new EventBridge rule
        would have to be born DISABLED under the 2026-08-07 pause and named in
        automation_pause.json's pending block — this asserts the alternative
        was not half-built."""
        manifest = json.loads(
            (REPO_ROOT / "infrastructure" / "automation_pause.json").read_text(
                encoding="utf-8"
            )
        )
        names = (
            set(manifest.get("pending", {}))
            | set(manifest["paused"]["events_rules"])
            | set(manifest["paused"]["scheduler_schedules"])
            | set(manifest["not_paused"])
        )
        assert not [n for n in names if "silence-deadman" in n]
        # ...and the surface it rides on is the un-paused watchdog trigger.
        assert "alpha-engine-pipeline-watchdog-daily" in manifest["not_paused"]
