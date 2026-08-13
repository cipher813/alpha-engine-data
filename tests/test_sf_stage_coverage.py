"""Tests for sf_stage_coverage — the weekly-SF post-stage coverage assertion
(alpha-engine-config-I7214).

The assertion exists because a Step Functions run asserts a stage did not
THROW and nothing asserted that it WROTE — five stages produced no output on the
2026-08-08 run while it reported SUCCEEDED (config-I7167). What these tests pin
is not the happy path but the three properties that make it safe to ship against
a fragile pipeline the Saturday before it runs:

  * it can never report a pass it did not establish (UNMEASURED, never COVERED);
  * it can never fail the run — no path raises, and observe mode never degrades;
  * a STALE artifact is a miss, because an existence-only probe reads it green.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest
import yaml

import sf_stage_coverage as cov

_REPO_ROOT = Path(__file__).resolve().parent.parent
_WEEKLY_SF = _REPO_ROOT / "infrastructure" / "step_function.json"

WINDOW_START = "2026-08-15T09:00:00.000Z"
FRESH = _dt.datetime(2026, 8, 15, 10, 0, tzinfo=_dt.timezone.utc)
STALE = _dt.datetime(2026, 8, 8, 10, 0, tzinfo=_dt.timezone.utc)


class _FakeS3:
    """Serves the registry from `_freshness_monitor/` and HEADs from a map."""

    def __init__(self, registry: dict, heads: dict, registry_error=None):
        self._registry = registry
        self._heads = heads
        self._registry_error = registry_error

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        if self._registry_error is not None:
            raise self._registry_error
        assert Key == cov.REGISTRY_KEY

        class _B:
            @staticmethod
            def read():
                return yaml.safe_dump(self._registry).encode()

        return {"Body": _B()}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self._heads:
            raise _NotFound()
        value = self._heads[Key]
        if isinstance(value, Exception):
            raise value
        return {"LastModified": value}


class _NotFound(Exception):
    response = {"Error": {"Code": "404"}}


class _Throttled(Exception):
    response = {"Error": {"Code": "SlowDown"}}


def _registry(stage_rows, artifact_rows=None):
    arts = artifact_rows or [{
        "artifact_id": "thing",
        "s3_key_template": "path/{date}/x.json",
    }]
    return {"artifacts": arts, "pipeline_stages": stage_rows}


_REGISTERED = [{
    "stage": "Doer", "stage_class": "product",
    "output": "registered", "artifacts": ["thing"],
}]


def _run(s3, **kw):
    kw.setdefault("run_date", "2026-08-15")
    kw.setdefault("execution_start_time", WINDOW_START)
    return cov.assert_stage_coverage(s3_client=s3, **kw)


# ── it establishes what it reports ───────────────────────────────────────────


def test_fresh_artifact_is_covered():
    s3 = _FakeS3(_registry(_REGISTERED), {"path/2026-08-15/x.json": FRESH})
    v = _run(s3)
    assert v["status"] == cov.COVERED
    assert v["stages_covered"] == 1 and v["stages_expected"] == 1
    assert v["missing"] == [] and v["stale"] == []


def test_absent_artifact_is_missing_and_names_the_key():
    s3 = _FakeS3(_registry(_REGISTERED), {})
    v = _run(s3)
    assert v["status"] == cov.MISSING
    assert v["missing"][0]["stage"] == "Doer"
    assert v["missing"][0]["key"] == "path/2026-08-15/x.json"
    assert v["stages_covered"] == 0


def test_artifact_from_a_previous_cycle_is_stale_not_covered():
    """The dangerous case, and the reason LastModified is checked at all: the
    key EXISTS, so every existence-only freshness probe reads it green while the
    consumer is served last cycle's belief."""
    s3 = _FakeS3(_registry(_REGISTERED), {"path/2026-08-15/x.json": STALE})
    v = _run(s3)
    assert v["status"] == cov.MISSING
    assert v["stale"][0]["reason"] == "predates this execution"
    assert v["stale"][0]["last_modified"].startswith("2026-08-08")
    assert v["missing"] == [], "a stale key is not an absent key; keep them apart"


# ── it can never report a pass it did not establish ──────────────────────────


def test_unreadable_registry_is_unmeasured_never_covered():
    s3 = _FakeS3(_registry(_REGISTERED), {}, registry_error=RuntimeError("no"))
    v = _run(s3)
    assert v["status"] == cov.UNMEASURED
    assert "could not load" in v["error"]


def test_a_probe_failure_that_is_not_a_404_is_unmeasured_not_a_finding():
    """Could-not-measure is not a finding about the producer. Conflating them
    reports a harness fault AS a defect, always in the alarming direction."""
    s3 = _FakeS3(_registry(_REGISTERED), {"path/2026-08-15/x.json": _Throttled()})
    v = _run(s3)
    assert v["status"] == cov.UNMEASURED
    assert v["missing"] == [] and v["stale"] == []
    assert v["unmeasured"][0]["reason"] == "probe failed: SlowDown"


def test_probing_nothing_is_unmeasured_not_a_clean_run():
    """Every stage skipped: the run observed nothing. A checker that reports
    COVERED here is a check that can never fail, which is indistinguishable
    from one that passes."""
    s3 = _FakeS3(_registry(_REGISTERED), {})
    v = _run(s3, execution_input={"skip_scanner": True})
    # Doer has no skip flag, so force the other emptiness route instead:
    s3b = _FakeS3(_registry([{
        "stage": "SaturdayHealthCheck", "stage_class": "infrastructure",
        "output": "none", "reason": "reads only",
    }]), {})
    v = _run(s3b)
    assert v["status"] == cov.UNMEASURED
    assert "observed nothing" in v["error"]


def test_unparseable_window_is_unmeasured():
    s3 = _FakeS3(_registry(_REGISTERED), {"path/2026-08-15/x.json": FRESH})
    v = _run(s3, execution_start_time="not-a-time")
    assert v["status"] == cov.UNMEASURED


def test_a_missing_registry_never_yields_an_empty_clean_declaration():
    """An empty declaration probes nothing and would report a clean pass. The
    loader raises instead, and the caller turns that into UNMEASURED."""
    s3 = _FakeS3({"artifacts": [], "pipeline_stages": []}, {})
    v = _run(s3)
    assert v["status"] == cov.UNMEASURED


# ── declared-nothing is COVERAGE, not an absence ─────────────────────────────


def test_a_stage_declaring_no_artifact_is_recorded_not_ignored():
    """The whole reason the registry grew a stage-side section: 'writes
    nothing' must be a positive declaration. Such a stage is neither expected
    nor missing — it is accounted for, by name."""
    s3 = _FakeS3(_registry(_REGISTERED + [{
        "stage": "WeeklyRunDayGate", "stage_class": "infrastructure",
        "output": "none", "reason": "calendar arithmetic only",
    }]), {"path/2026-08-15/x.json": FRESH})
    v = _run(s3)
    assert v["status"] == cov.COVERED
    assert v["stages_no_artifact"] == ["WeeklyRunDayGate"]
    assert v["stages_expected"] == 1


def test_a_stage_skipped_by_input_is_not_a_miss():
    """Absence after a skip flag is a fact about the input, not the producer.
    A detector that pages on a deliberate skip gets turned off."""
    s3 = _FakeS3(_registry([{
        "stage": "Scanner", "stage_class": "product",
        "output": "registered", "artifacts": ["thing"],
    }, {
        "stage": "Backtester", "stage_class": "product",
        "output": "registered", "artifacts": ["thing"],
    }]), {"path/2026-08-15/x.json": FRESH})
    v = _run(s3, execution_input={"skip_scanner": True})
    assert v["stages_skipped"] == ["Scanner"]
    assert v["status"] == cov.COVERED


def test_a_group_skip_flag_suppresses_every_member():
    s3 = _FakeS3(_registry([{
        "stage": s, "stage_class": "product",
        "output": "registered", "artifacts": ["thing"],
    } for s in ("PitParityLookahead", "PitParityWalkforward", "Backtester")]),
        {"path/2026-08-15/x.json": FRESH})
    v = _run(s3, execution_input={"skip_parity": True})
    assert set(v["stages_skipped"]) == {"PitParityLookahead", "PitParityWalkforward"}


def test_a_skip_flag_that_is_not_true_does_not_suppress():
    """`skip_x: false` and an absent key must both mean 'it ran'. A truthiness
    test would let the string 'false' suppress a stage."""
    s3 = _FakeS3(_registry([{
        "stage": "Scanner", "stage_class": "product",
        "output": "registered", "artifacts": ["thing"],
    }]), {})
    assert _run(s3, execution_input={"skip_scanner": False})["status"] == cov.MISSING
    assert _run(s3, execution_input={"skip_scanner": "false"})["status"] == cov.MISSING


# ── it can never fail the run ────────────────────────────────────────────────


def test_handle_never_raises_even_on_a_malformed_event():
    v = cov.handle({})
    assert v["status"] == cov.UNMEASURED
    assert v["degrade"] is False


def test_observe_mode_never_degrades_however_bad_the_verdict(monkeypatch):
    """The hard constraint. OBSERVE records the full verdict and sets nothing,
    so the state is observationally complete and operationally inert until the
    enforce literal is flipped — after one clean cycle."""
    monkeypatch.setattr(cov, "assert_stage_coverage", lambda **kw: {
        "status": cov.MISSING, "enforce": False, "missing": [{"stage": "X"}],
    })
    v = cov.handle({"run_date": "d", "execution_start_time": WINDOW_START,
                    "enforce": False})
    assert v["status"] == cov.MISSING
    assert v["degrade"] is False


def test_enforce_mode_degrades_only_on_a_real_miss(monkeypatch):
    for status, expected in ((cov.MISSING, True), (cov.UNMEASURED, False),
                             (cov.COVERED, False)):
        def _stub(_s=status, **kw):
            return {"status": _s, "enforce": True}

        monkeypatch.setattr(cov, "assert_stage_coverage", _stub)
        v = cov.handle({"run_date": "d", "execution_start_time": WINDOW_START,
                        "enforce": True})
        assert v["degrade"] is expected, status
    # UNMEASURED never degrades even under enforcement: that would report the
    # observer's own fault as the producer's.


# ── key resolution ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("template,expected", [
    ("path/{date}/x.json", "path/2026-08-15/x.json"),
    ("backtest/{trading_day}/pit_parity.json", "backtest/2026-08-15/pit_parity.json"),
    ("config/apply_audit/latest.json", "config/apply_audit/latest.json"),
])
def test_both_date_placeholders_resolve_to_the_run_date(template, expected):
    assert cov._resolve_key(template, "2026-08-15") == expected


def test_an_unknown_placeholder_is_left_intact_so_the_probe_fails_loud():
    """Silently dropping a placeholder would probe a WRONG key and return a
    confident wrong answer. Leaving the braces makes the head_object miss."""
    assert "{cycle_label}" in cov._resolve_key("a/{cycle_label}/b", "2026-08-15")


# ── the skip map mirrors the definition ──────────────────────────────────────


def test_every_skip_flag_is_real():
    """The one list here that mirrors something outside the registry. A flag no
    Choice state consults would silently suppress a stage forever."""
    text = _WEEKLY_SF.read_text()
    flags = set(cov.STAGE_SKIP_FLAGS.values()) | set(cov.GROUP_SKIP_FLAGS)
    for flag in sorted(flags):
        assert f'"$.{flag}"' in text, f"{flag} is consulted by no state"


def test_every_mapped_stage_is_a_real_state():
    states = json.loads(_WEEKLY_SF.read_text())["States"]
    names: set[str] = set()

    def walk(s):
        for k, v in s.items():
            names.add(k)
            if v.get("Type") == "Parallel":
                for b in v["Branches"]:
                    walk(b["States"])
            if v.get("Type") == "Map":
                for key in ("ItemProcessor", "Iterator"):
                    if key in v:
                        walk(v[key]["States"])

    walk(states)
    mapped = set(cov.STAGE_SKIP_FLAGS) | {
        m for members in cov.GROUP_SKIP_FLAGS.values() for m in members
    }
    assert mapped <= names, sorted(mapped - names)


# ── the definition wires the assertion the way the module expects ────────────


def test_the_sf_ships_in_observe_mode():
    """The hard constraint, asserted against the artifact that actually runs.
    Flipping this to true is a deliberate promotion, and it must be a diff this
    test sees."""
    states = json.loads(_WEEKLY_SF.read_text())["States"]
    payload = states["StageCoverageAssert"]["Parameters"]["Payload"]
    assert payload["enforce"] is False, (
        "StageCoverageAssert must ship in OBSERVE mode — promote only after one "
        "clean scheduled run (alpha-engine-config-I7214)"
    )
    assert payload["action"] == "assert_stage_coverage"


def test_the_assertion_cannot_fail_the_run():
    states = json.loads(_WEEKLY_SF.read_text())["States"]
    catches = states["StageCoverageAssert"]["Catch"]
    assert any(c["ErrorEquals"] == ["States.ALL"] for c in catches)
    assert catches[0]["Next"] == "StageCoverageUnmeasured"
    assert states["StageCoverageUnmeasured"]["Result"]["stage_coverage"]["status"] \
        == "UNMEASURED"
    assert states["StageCoverageUnmeasured"]["Next"] == "CheckGateDegradedNotify", (
        "the observer's own failure must not route through the degraded summary"
    )


def test_it_degrades_through_the_existing_chokepoint():
    """Not a second mechanism: $.degraded_summary is the path CheckDegradedOutcome
    and the DegradedRun terminal already read."""
    states = json.loads(_WEEKLY_SF.read_text())["States"]
    setter = states["SetStageCoverageDegradedSummary"]
    assert setter["ResultPath"] == "$.degraded_summary"
    assert setter["Parameters"]["degraded"] is True
    assert setter["Parameters"]["reason"] == "weekly_stage_coverage_incomplete"
    assert states["CheckStageCoverageOutcome"]["Choices"][0]["Next"] \
        == "SetStageCoverageDegradedSummary"


def test_the_enforce_choice_is_ispresent_guarded():
    """An unguarded dereference throws States.Runtime at the tail of an
    otherwise successful weekly run — the config-I2767 shape."""
    states = json.loads(_WEEKLY_SF.read_text())["States"]
    rule = states["CheckStageCoverageOutcome"]["Choices"][0]["And"]
    assert any("IsPresent" in r for r in rule)


def test_the_friday_shell_run_is_not_asserted():
    """The Friday-PM dry pass writes nothing by design; asserting coverage on it
    would report every stage as a miss, every Friday."""
    states = json.loads(_WEEKLY_SF.read_text())["States"]
    shell = states["CheckShellRunNotify"]
    assert shell["Choices"][0]["Next"] == "NotifyShellRunComplete"
    assert shell["Default"] == "StageCoverageAssert"
