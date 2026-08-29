"""Weekly-SF enrollment into the pipeline-watchdog prior-day failed-run
check (alpha-engine-config-I7036).

``_FAILED_DAY_PIPELINES`` (infrastructure/lambdas/pipeline-watchdog/
index.py) reads S3 completion markers to catch a Step Function that started
but never succeeded. Until alpha-engine-config-I6891 (nousergon-data-PR1319)
the weekly SF (``ne-weekly-freshness-pipeline``) had no DEGRADED marker to
read, so it was never enrolled. I6891 shipped ``WriteCompletionMarkerDegraded``
and the ``DegradedRun`` Fail terminal for it; this enrolls it.

The weekly SF's cadence is NOT weekday-trading-day like the two pre-existing
entries: it fires via a self-gating THU-SAT cron (real cycle day = the day
after the week's last trading session, not always Saturday) and is ALSO
chain-launched daily as a ``pipeline_role=exercise`` dry run. Counting a
``WeeklyRunDayGateChoice`` no-op skip (~2s SUCCEEDED) or an exercise run as
the weekly cycle would silently hide a genuinely failed real run — exactly
the defect class I7036 deliverable 2 warns against. This module tests that
``_check_failed_day(cadence="weekly")`` / ``_weekly_real_statuses_for_day``
reuse ``weekly_sf_silence_deadman._is_gate_noop`` (imported, not
re-derived) to make that discrimination, via the three cases named in the
issue's Deliverable 3 / Closes-when:

  1. a weekly marker DEGRADED clears the failed day (no page)
  2. a weekly marker ABSENT on an expected cycle day pages
  3. a gate-skip execution is not counted as the cycle (no false-clear,
     no false-page — deferred to the silence deadman instead)
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
LAMBDA_DIR = REPO_ROOT / "infrastructure" / "lambdas" / "pipeline-watchdog"
LAMBDAS_ROOT = REPO_ROOT / "infrastructure" / "lambdas"
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _load_module():
    # index.py imports `from flow_doctor_telegram import notify_via_flow_doctor`
    # as a bare top-level module (deployed flat next to index.py in prod;
    # in-repo it lives one directory up at infrastructure/lambdas/) and
    # falls back to inserting SCRIPTS_DIR itself for the
    # weekly_sf_silence_deadman import (in-repo layout branch already
    # coded in index.py) — both paths need to be importable before exec.
    for p in (str(LAMBDAS_ROOT), str(SCRIPTS_DIR)):
        if p not in sys.path:
            sys.path.insert(0, p)
    spec = importlib.util.spec_from_file_location(
        "pipeline_watchdog_index", LAMBDA_DIR / "index.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pipeline_watchdog_index"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load_module()


TARGET_DATE = date(2026, 8, 15)  # a Saturday — a real weekly cycle day
MARKER_BUCKET = "alpha-engine-research"
MARKER_KEY = f"_sf_completion/ne-weekly-freshness-pipeline/{TARGET_DATE.isoformat()}.json"


def _utc(d: date, hour: int, minute: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, 0, tzinfo=timezone.utc)


class _FakeSFClient:
    """Minimal stepfunctions client stub covering the two calls
    ``weekly_sf_silence_deadman.fetch_execution_records`` makes:
    ``list_executions`` (single page — small fixtures, no pagination
    needed) and ``describe_execution`` (role comes from execution INPUT,
    never the name — mirrors every real launch path)."""

    def __init__(self, executions: list[dict]):
        self._executions = executions

    def list_executions(self, **kwargs):
        return {"executions": self._executions, "nextToken": None}

    def describe_execution(self, *, executionArn: str):
        for ex in self._executions:
            if ex["executionArn"] == executionArn:
                resp = {"input": json.dumps(ex["_input"])}
                if ex.get("_output") is not None:
                    resp["output"] = json.dumps(ex["_output"])
                return resp
        raise AssertionError(f"no fixture execution for {executionArn}")  # pragma: no cover


# The run-day gate's own verdict, exactly as it appears in a real gate-skip
# execution's output (alpha-engine-config-I8057 — the gate-noop axis is
# read from this, never from duration).
_GATE_OUT_OUTPUT = {"weekly_run_day_gate": {"Payload": {
    "is_weekly_run_day": False, "marker": "NOT_WEEKLY_RUN_DAY",
}}}


def _execution(
    *, name: str, role: str | None, status: str, start: datetime, duration_s: float,
    gate_out: bool = False,
) -> dict:
    return {
        "executionArn": f"arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:{name}",
        "name": name,
        "status": status,
        "startDate": start,
        "stopDate": start + timedelta(seconds=duration_s) if status != "RUNNING" else None,
        "_input": {"pipeline_role": role} if role is not None else {},
        "_output": _GATE_OUT_OUTPUT if gate_out else None,
    }


class _FakeS3Client:
    def __init__(self, *, status: str | None):
        """``status=None`` simulates ABSENT (NoSuchKey)."""
        self._status = status

    def get_object(self, *, Bucket: str, Key: str):
        assert Bucket == MARKER_BUCKET
        assert Key == MARKER_KEY
        if self._status is None:

            class _NoSuchKey(Exception):
                response = {"Error": {"Code": "NoSuchKey"}}

            raise _NoSuchKey("not found")
        body = json.dumps({"status": self._status, "degraded_summary": {"reason": "test"}}).encode()
        return {"Body": io.BytesIO(body)}


@pytest.fixture(autouse=True)
def _no_real_alerts(mod, monkeypatch):
    """Never let a test reach SNS/Telegram — assert on the recorded calls
    instead. Mirrors the module's own dedup_key/severity contract without
    depending on live AWS credentials being present in CI."""
    calls: list[dict] = []

    def _fake_publish(message, *, severity, dedup_key, context, dedup_window_min=12 * 60):
        calls.append({"message": message, "severity": severity, "dedup_key": dedup_key})
        return f"published:{dedup_key}"

    monkeypatch.setattr(mod, "_publish_watchdog_alert", _fake_publish)
    return calls


# ---------------------------------------------------------------------------
# The three I7036 Deliverable-3 cases
# ---------------------------------------------------------------------------

class TestWeeklyFailedDayDegradedMarkerClears:
    def test_degraded_marker_clears_the_failed_day(self, mod, _no_real_alerts):
        # Real weekly-role execution, terminated FAILED (the DegradedRun
        # Fail-state terminal is a real AWS FAILED status), long enough not
        # to be a gate-noop. Noise: an unrelated same-day exercise-role
        # execution must not influence the outcome (role filtering).
        executions = [
            _execution(
                name="real-weekly-run",
                role="weekly",
                status="FAILED",
                start=_utc(TARGET_DATE, 9, 0),
                duration_s=5400,  # 90 minutes; it carries no gate-out verdict either
            ),
        ]
        sf_client = _FakeSFClient(executions)
        s3_client = _FakeS3Client(status="DEGRADED")

        result = mod._check_failed_day(
            sf_label="Weekly SF",
            sf_arn=mod.SATURDAY_SF_ARN,
            pipeline_name="ne-weekly-freshness-pipeline",
            target_date=TARGET_DATE,
            cadence="weekly",
            client=sf_client,
            s3_client=s3_client,
        )

        assert result.checked is True
        assert result.executions_on_day == 1
        assert result.succeeded_on_day == 0
        assert result.marker_status == "DEGRADED"
        assert result.alert_emitted is False
        assert _no_real_alerts == []  # no page — Option-A visible degrade


class TestWeeklyFailedDayAbsentMarkerPages:
    def test_absent_marker_on_expected_cycle_day_pages(self, mod, _no_real_alerts):
        executions = [
            _execution(
                name="real-weekly-run-failed-hard",
                role="weekly",
                status="FAILED",
                start=_utc(TARGET_DATE, 9, 0),
                duration_s=1800,
            ),
        ]
        sf_client = _FakeSFClient(executions)
        s3_client = _FakeS3Client(status=None)  # ABSENT

        result = mod._check_failed_day(
            sf_label="Weekly SF",
            sf_arn=mod.SATURDAY_SF_ARN,
            pipeline_name="ne-weekly-freshness-pipeline",
            target_date=TARGET_DATE,
            cadence="weekly",
            client=sf_client,
            s3_client=s3_client,
        )

        assert result.checked is True
        assert result.executions_on_day == 1
        assert result.succeeded_on_day == 0
        assert result.marker_status == "ABSENT"
        assert result.alert_emitted is True
        assert len(_no_real_alerts) == 1
        assert _no_real_alerts[0]["severity"] == "error"
        assert "ne-weekly-freshness-pipeline" in _no_real_alerts[0]["message"]
        assert "no completion marker was written" in _no_real_alerts[0]["message"]


class TestWeeklyFailedDayGateSkipIsNotACycle:
    def test_gate_skip_execution_is_not_counted_as_the_cycle(self, mod, _no_real_alerts):
        # WeeklyRunDayGateChoice's designed no-op: SUCCEEDED, declared by the
        # gate's OWN output verdict (`gate_out=True`), not by its ~2s duration
        # (alpha-engine-config-I8057). Must not read as either a
        # real SUCCEEDED cycle (which would silently hide a genuine miss)
        # NOR as a "0 executions" never-fired case that some OTHER
        # non-weekly execution masks — this is the exact ambiguity I7036
        # deliverable 2 exists to resolve. Also includes a same-day
        # exercise-role execution as noise to prove role filtering holds
        # even when gate-noop filtering wouldn't have excluded it alone.
        executions = [
            _execution(
                name="gate-skip",
                role="weekly",
                status="SUCCEEDED",
                start=_utc(TARGET_DATE, 9, 0),
                duration_s=2,
                gate_out=True,
            ),
            _execution(
                name="same-day-exercise-noise",
                role="exercise",
                status="SUCCEEDED",
                start=_utc(TARGET_DATE, 9, 5),
                duration_s=3600,
            ),
        ]
        sf_client = _FakeSFClient(executions)
        s3_client = _FakeS3Client(status=None)  # must never be consulted

        result = mod._check_failed_day(
            sf_label="Weekly SF",
            sf_arn=mod.SATURDAY_SF_ARN,
            pipeline_name="ne-weekly-freshness-pipeline",
            target_date=TARGET_DATE,
            cadence="weekly",
            client=sf_client,
            s3_client=s3_client,
        )

        assert result.checked is True
        assert result.executions_on_day == 0
        assert result.succeeded_on_day == 0
        assert result.marker_status is None  # never consulted — total==0 short-circuits first
        assert result.alert_emitted is False
        assert _no_real_alerts == []
        assert "gate-skip" in (result.skip_reason or "") or "weekly" in (result.skip_reason or "")


# ---------------------------------------------------------------------------
# Sanity: the generic (trading_day-cadence) path is unchanged by the new
# ``cadence`` parameter's default.
# ---------------------------------------------------------------------------

class TestTradingDayCadenceUnaffected:
    def test_weekday_sf_default_cadence_still_uses_plain_date_counting(self, mod, _no_real_alerts):
        class _FakeStatusesClient:
            def list_executions(self, *, stateMachineArn, statusFilter, maxResults, nextToken=None):
                if statusFilter != "SUCCEEDED":
                    return {"executions": []}
                return {
                    "executions": [
                        {
                            "executionArn": "arn:x",
                            "startDate": _utc(TARGET_DATE, 12, 45),
                        }
                    ]
                }

        result = mod._check_failed_day(
            sf_label="Weekday SF",
            sf_arn=mod.WEEKDAY_SF_ARN,
            pipeline_name="ne-preopen-trading-pipeline",
            target_date=TARGET_DATE,
            client=_FakeStatusesClient(),
        )
        assert result.checked is True
        assert result.succeeded_on_day == 1
        assert result.alert_emitted is False


# ---------------------------------------------------------------------------
# Target-date derivation: the real cycle day is not always Saturday
# ---------------------------------------------------------------------------

class TestLastDueWeeklyDay:
    def test_normal_week_returns_the_saturday(self, mod):
        # Evaluated Sunday 2026-08-16 (14:00 UTC watchdog firing) — the
        # week's last session was Friday 2026-08-14, so the real cycle day
        # is Saturday 2026-08-15.
        now = _utc(date(2026, 8, 16), 14)
        assert mod._last_due_weekly_day(now) == date(2026, 8, 15)

    def test_holiday_shortened_week_returns_friday_not_saturday(self, mod):
        # Real precedent (weekly_sf_silence_deadman docstring): the week
        # containing Independence Day 2026 (observed Friday 2026-07-03)
        # ends its trading on Thursday 2026-07-02, so the real cycle day is
        # Friday 2026-07-03 — a naive "always Saturday" assumption would
        # derive 2026-07-04 and find nothing there.
        now = _utc(date(2026, 7, 4), 14)
        assert mod._last_due_weekly_day(now) == date(2026, 7, 3)


# ---------------------------------------------------------------------------
# _FAILED_DAY_PIPELINES enrollment
# ---------------------------------------------------------------------------

def test_weekly_sf_is_enrolled_in_failed_day_pipelines(mod):
    labels = {(row[0], row[3]) for row in mod._FAILED_DAY_PIPELINES}
    assert ("Weekly SF", "weekly") in labels
    assert ("Weekday SF", "trading_day") in labels
    assert ("EOD SF", "trading_day") in labels
