"""Unit tests for scripts/weekly_sf_recovery_metric.py (alpha-engine-config#6686).

Covers:
  - classify_execution's three-way split (counted / excluded_exercise /
    excluded_gate_skip), including the gate-skip output-verdict check
    (config#1824's CheckWeeklyRunDayGate self-selection, alpha-engine-
    config-I8057: NOT duration-based — read directly from the execution's
    own `output`, positive-test-only, never absence-as-gate-out).
  - build_metric's derived fields against the two documented real shapes:
    a clean single-run day (0 reruns) and the 2026-08-01 shape (1 FAILED
    weekly + 6 watch-reruns, mixed FAILED-then-SUCCEEDED -> 6).
  - resolve_latest_run_date's completion-marker scan (excludes its own
    "-recovery.json" siblings).
  - main()'s no-data-is-not-green exit path, the S3 metric PUT, and the
    recovery_actions>1 alert fan-out via nousergon_lib.alerts.publish
    (mocked — mirrors tests/test_phase_marker_sweep.py's pattern).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from scripts import weekly_sf_recovery_metric as m


UTC = timezone.utc


def _dt(y, mo, d, h, mi, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=UTC)


class _FakeSF:
    """Minimal Step Functions client fake: single-page list_executions and
    describe_execution keyed by executionArn."""

    def __init__(self, executions, describe_by_arn=None):
        self._executions = executions
        self._describe = describe_by_arn or {}

    def list_executions(self, stateMachineArn, maxResults=200, nextToken=None, statusFilter=None):
        return {"executions": self._executions}

    def describe_execution(self, executionArn):
        return self._describe[executionArn]


class _FakeS3:
    def __init__(self, keys=(), *, capture_puts=None):
        self._keys = list(keys)
        self._puts = capture_puts if capture_puts is not None else []

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        contents = [{"Key": k} for k in self._keys if k.startswith(Prefix)]
        return {"Contents": contents, "IsTruncated": False}

    def put_object(self, **kwargs):
        self._puts.append(kwargs)
        return {}


def _weekly_exec(arn, name, status, start, stop, role="weekly", explicit_run_date=None,
                  output=None):
    inp = {"pipeline_role": role}
    if explicit_run_date:
        inp["run_date"] = explicit_run_date
    desc = {"status": status, "startDate": start, "stopDate": stop, "input": json.dumps(inp)}
    if output is not None:
        desc["output"] = json.dumps(output)
    return (
        {"executionArn": arn, "name": name, "status": status, "startDate": start, "stopDate": stop},
        desc,
    )


def _gate_out_output():
    """The gate's own verdict, as it appears in a real gate-skip execution's
    output (verified live 2026-08-21/08-22 against the 3.5s and 5.7s
    gate-outs — alpha-engine-config-I8057)."""
    return {"weekly_run_day_gate": {"Payload": {
        "is_weekly_run_day": False, "marker": "NOT_WEEKLY_RUN_DAY",
    }}}


# ── classify_execution ───────────────────────────────────────────────────


def test_classify_named_rerun_is_counted_without_describe():
    sf = _FakeSF(executions=[])  # describe_execution would KeyError if called
    ex = {"executionArn": "arn:1", "name": "watch-rerun-2026-08-01-6",
          "status": "SUCCEEDED", "startDate": _dt(2026, 8, 1, 14, 0), "stopDate": _dt(2026, 8, 1, 15, 0)}
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "counted"
    assert rec.run_date == "2026-08-01"
    assert rec.role == "watch-rerun"


def test_classify_scheduled_weekly_success_is_counted():
    ex, desc = _weekly_exec(
        "arn:weekly", "9f2a-uuid", "SUCCEEDED",
        _dt(2026, 8, 8, 9, 0), _dt(2026, 8, 8, 11, 30),
    )
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:weekly": desc})
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "counted"
    assert rec.run_date == "2026-08-08"  # UTC-date fallback, no explicit run_date
    assert rec.role == "weekly"


def test_classify_exercise_role_excluded():
    ex, desc = _weekly_exec(
        "arn:ex", "uuid-ex", "SUCCEEDED",
        _dt(2026, 8, 8, 9, 0), _dt(2026, 8, 8, 9, 20),
        role="exercise", explicit_run_date="2026-08-08",
    )
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:ex": desc})
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "excluded_exercise"
    assert rec.run_date == "2026-08-08"


def test_classify_weekly_exercise_role_variant_excluded():
    ex, desc = _weekly_exec(
        "arn:ex2", "uuid-ex2", "SUCCEEDED",
        _dt(2026, 8, 8, 9, 0), _dt(2026, 8, 8, 9, 20),
        role="weekly-exercise", explicit_run_date="2026-08-08",
    )
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:ex2": desc})
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "excluded_exercise"


def test_classify_gate_skip_confirmed_via_output():
    """A fast (2s) SUCCEEDED weekly-role execution whose OUTPUT carries the
    gate's `is_weekly_run_day: false` verdict must be excluded, not counted
    as a clean healthy run — the false positive this metric exists to
    prevent."""
    start = _dt(2026, 8, 6, 9, 0, 0)
    stop = _dt(2026, 8, 6, 9, 0, 2)
    ex, desc = _weekly_exec("arn:gate", "uuid-gate", "SUCCEEDED", start, stop,
                            output=_gate_out_output())
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:gate": desc})
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "excluded_gate_skip"
    assert rec.run_date == "2026-08-06"


def test_classify_short_real_run_not_mistaken_for_gate_skip():
    """alpha-engine-config-I8057's measured fact: the fleet's shortest
    genuine weekly execution ran 12.6s (`director-verify-0731-003355Z`,
    2026-08-03) — a duration threshold would have misclassified it. A short
    SUCCEEDED weekly execution with no `weekly_run_day_gate` key in its
    output — exactly what a real run's output looks like — must stay
    counted regardless of how fast it finished."""
    start = _dt(2026, 8, 8, 9, 0, 0)
    stop = start + timedelta(seconds=12.6)
    ex, desc = _weekly_exec("arn:short", "uuid-short", "SUCCEEDED", start, stop,
                            output={"some_other_stage": {"ok": True}})
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:short": desc})
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "counted"


def test_classify_slow_gate_out_still_excluded():
    """Mirror fact: a gate-out is never assumed fast. Even if the terminal
    state took longer than any duration threshold would have allowed, the
    output verdict alone decides — no clock involved."""
    start = _dt(2026, 8, 20, 9, 0, 0)
    stop = _dt(2026, 8, 20, 9, 2, 0)  # 120s — would have missed a 60s ceiling
    ex, desc = _weekly_exec("arn:slowgate", "uuid-slowgate", "SUCCEEDED", start, stop,
                            output=_gate_out_output())
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:slowgate": desc})
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "excluded_gate_skip"


def test_classify_failed_weekly_never_gate_checked():
    """status != SUCCEEDED short-circuits the gate-skip check entirely —
    a FAILED execution's `output` field does not exist on real AWS
    responses either, so this also pins that absence never raises."""
    ex, desc = _weekly_exec(
        "arn:failed", "uuid-failed", "FAILED",
        _dt(2026, 8, 1, 9, 0), _dt(2026, 8, 1, 9, 0, 2),
    )
    assert "output" not in desc
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:failed": desc})
    rec = m.classify_execution(sf, ex)
    assert rec.classification == "counted"


# ── gather_records + build_metric: the two documented real shapes ────────


def test_clean_single_run_day_zero_recovery_actions():
    ex, desc = _weekly_exec(
        "arn:clean", "uuid-clean", "SUCCEEDED",
        _dt(2026, 8, 8, 9, 0), _dt(2026, 8, 8, 11, 0),
    )
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:clean": desc})
    records = m.gather_records(sf, m.DEFAULT_STATE_MACHINE_ARN, "2026-08-08")
    metric = m.build_metric("2026-08-08", records)
    assert metric["executions_total"] == 1
    assert metric["recovery_actions"] == 0
    assert metric["first_terminal_status"] == "SUCCEEDED"
    assert metric["succeeded_after_n_reruns"] == 0


def test_20260801_shape_one_failed_plus_six_watch_reruns():
    weekly_ex, weekly_desc = _weekly_exec(
        "arn:w", "uuid-weekly-0801", "FAILED",
        _dt(2026, 8, 1, 9, 0), _dt(2026, 8, 1, 9, 45),
    )
    executions = [weekly_ex]
    describe = {"arn:w": weekly_desc}
    for i in range(1, 6):  # reruns 1-5 FAILED
        executions.append({
            "executionArn": f"arn:r{i}", "name": f"watch-rerun-2026-08-01-{i}",
            "status": "FAILED",
            "startDate": _dt(2026, 8, 1, 9 + i, 0), "stopDate": _dt(2026, 8, 1, 9 + i, 20),
        })
    executions.append({  # rerun 6 SUCCEEDED
        "executionArn": "arn:r6", "name": "watch-rerun-2026-08-01-6",
        "status": "SUCCEEDED",
        "startDate": _dt(2026, 8, 1, 15, 0), "stopDate": _dt(2026, 8, 1, 17, 0),
    })
    sf = _FakeSF(executions=executions, describe_by_arn=describe)
    records = m.gather_records(sf, m.DEFAULT_STATE_MACHINE_ARN, "2026-08-01")
    metric = m.build_metric("2026-08-01", records)
    assert metric["executions_total"] == 7
    assert metric["recovery_actions"] == 6
    assert metric["first_terminal_status"] == "FAILED"
    assert metric["succeeded_after_n_reruns"] == 6


def test_20260725_shape_sixteen_manual_attempts():
    weekly_ex, weekly_desc = _weekly_exec(
        "arn:w2", "uuid-weekly-0725", "FAILED",
        _dt(2026, 7, 25, 9, 0), _dt(2026, 7, 25, 9, 10),
    )
    executions = [weekly_ex]
    describe = {"arn:w2": weekly_desc}
    for i in range(1, 17):  # 16 reruns, last one SUCCEEDED
        executions.append({
            "executionArn": f"arn:m{i}", "name": f"rerun-2026-07-25-{i}",
            "status": "SUCCEEDED" if i == 16 else "FAILED",
            "startDate": _dt(2026, 7, 25, 10, i), "stopDate": _dt(2026, 7, 25, 10, i, 30),
        })
    sf = _FakeSF(executions=executions, describe_by_arn=describe)
    records = m.gather_records(sf, m.DEFAULT_STATE_MACHINE_ARN, "2026-07-25")
    metric = m.build_metric("2026-07-25", records)
    assert metric["executions_total"] == 17
    assert metric["recovery_actions"] == 16
    assert metric["succeeded_after_n_reruns"] == 16


def test_exercise_and_gate_skip_excluded_from_counted_but_visible():
    weekly_ex, weekly_desc = _weekly_exec(
        "arn:real", "uuid-real", "SUCCEEDED",
        _dt(2026, 8, 8, 9, 0), _dt(2026, 8, 8, 11, 0),
    )
    exercise_ex, exercise_desc = _weekly_exec(
        "arn:ex", "uuid-ex", "SUCCEEDED",
        _dt(2026, 8, 8, 12, 0), _dt(2026, 8, 8, 12, 20),
        role="exercise", explicit_run_date="2026-08-08",
    )
    sf = _FakeSF(
        executions=[weekly_ex, exercise_ex],
        describe_by_arn={"arn:real": weekly_desc, "arn:ex": exercise_desc},
    )
    records = m.gather_records(sf, m.DEFAULT_STATE_MACHINE_ARN, "2026-08-08")
    metric = m.build_metric("2026-08-08", records)
    assert metric["executions_total"] == 1
    assert metric["recovery_actions"] == 0
    assert metric["excluded_exercise"] == 1


def test_gate_skip_only_day_is_no_data_not_a_false_clean_run():
    """Querying the Thursday-that-gate-skipped's own date directly must
    surface as zero *counted* executions, never as a false healthy run."""
    start = _dt(2026, 8, 6, 9, 0, 0)
    stop = _dt(2026, 8, 6, 9, 0, 2)
    ex, desc = _weekly_exec("arn:gate2", "uuid-gate2", "SUCCEEDED", start, stop,
                            output=_gate_out_output())
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:gate2": desc})
    records = m.gather_records(sf, m.DEFAULT_STATE_MACHINE_ARN, "2026-08-06")
    counted = [r for r in records if r.classification == "counted"]
    assert counted == []
    metric = m.build_metric("2026-08-06", records)
    assert metric["executions_total"] == 0
    assert metric["excluded_gate_skip"] == 1


# ── resolve_latest_run_date ────────────────────────────────────────────


def test_resolve_latest_run_date_picks_max_excludes_recovery_siblings():
    s3 = _FakeS3(keys=[
        f"{m.COMPLETION_PREFIX}2026-07-25.json",
        f"{m.COMPLETION_PREFIX}2026-08-01.json",
        f"{m.COMPLETION_PREFIX}2026-08-01-recovery.json",
        f"{m.COMPLETION_PREFIX}2026-08-08.json",
    ])
    assert m.resolve_latest_run_date(s3, "alpha-engine-research") == "2026-08-08"


def test_resolve_latest_run_date_no_markers_raises():
    s3 = _FakeS3(keys=[])
    with pytest.raises(SystemExit):
        m.resolve_latest_run_date(s3, "alpha-engine-research")


# ── main(): no-data exit, S3 write, alert fan-out ─────────────────────


def _patch_boto3(monkeypatch, sf, s3):
    fake_boto3 = MagicMock()
    fake_boto3.client.side_effect = lambda service, region_name=None: {
        "stepfunctions": sf, "s3": s3,
    }[service]
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)


def test_main_no_data_exits_nonzero(monkeypatch, capsys):
    sf = _FakeSF(executions=[])
    s3 = _FakeS3(keys=[])
    _patch_boto3(monkeypatch, sf, s3)
    rc = m.main(["--run-date", "2026-09-01"])
    assert rc == 1
    assert "NO DATA" in capsys.readouterr().err


def test_main_writes_metric_and_skips_alert_below_threshold(monkeypatch, capsys):
    ex, desc = _weekly_exec(
        "arn:clean2", "uuid-clean2", "SUCCEEDED",
        _dt(2026, 8, 8, 9, 0), _dt(2026, 8, 8, 11, 0),
    )
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:clean2": desc})
    puts = []
    s3 = _FakeS3(keys=[], capture_puts=puts)
    _patch_boto3(monkeypatch, sf, s3)
    fake_alerts = MagicMock()
    with patch.dict("sys.modules", {"nousergon_lib": MagicMock(alerts=fake_alerts),
                                     "nousergon_lib.alerts": fake_alerts}):
        rc = m.main(["--run-date", "2026-08-08"])
    assert rc == 0
    assert len(puts) == 1
    assert puts[0]["Key"] == f"{m.COMPLETION_PREFIX}2026-08-08-recovery.json"
    body = json.loads(puts[0]["Body"])
    assert body["recovery_actions"] == 0
    fake_alerts.publish.assert_not_called()


def test_main_alerts_when_recovery_actions_exceeds_one(monkeypatch):
    weekly_ex, weekly_desc = _weekly_exec(
        "arn:w3", "uuid-weekly-alert", "FAILED",
        _dt(2026, 8, 1, 9, 0), _dt(2026, 8, 1, 9, 45),
    )
    executions = [weekly_ex]
    for i in range(1, 4):  # 3 reruns -> recovery_actions == 3 (> 1)
        executions.append({
            "executionArn": f"arn:ra{i}", "name": f"watch-rerun-2026-08-01-{i}",
            "status": "SUCCEEDED" if i == 3 else "FAILED",
            "startDate": _dt(2026, 8, 1, 9 + i, 0), "stopDate": _dt(2026, 8, 1, 9 + i, 20),
        })
    sf = _FakeSF(executions=executions, describe_by_arn={"arn:w3": weekly_desc})
    s3 = _FakeS3(keys=[])
    _patch_boto3(monkeypatch, sf, s3)
    fake_alerts = MagicMock()
    fake_alerts.publish.return_value = MagicMock(
        sns=MagicMock(ok=True), telegram=MagicMock(ok=True), any_ok=True, dedup_skipped=False,
    )
    with patch.dict("sys.modules", {"nousergon_lib": MagicMock(alerts=fake_alerts),
                                     "nousergon_lib.alerts": fake_alerts}):
        rc = m.main(["--run-date", "2026-08-01"])
    assert rc == 0
    fake_alerts.publish.assert_called_once()
    _, kwargs = fake_alerts.publish.call_args
    assert kwargs["dedup_key"] == "weekly_sf_recovery_2026-08-01_3"
    assert kwargs["severity"] == "error"


def test_main_no_alert_flag_suppresses_publish_even_above_threshold(monkeypatch):
    weekly_ex, weekly_desc = _weekly_exec(
        "arn:w4", "uuid-weekly-noalert", "FAILED",
        _dt(2026, 8, 1, 9, 0), _dt(2026, 8, 1, 9, 45),
    )
    executions = [weekly_ex]
    for i in range(1, 4):
        executions.append({
            "executionArn": f"arn:na{i}", "name": f"watch-rerun-2026-08-01-{i}",
            "status": "SUCCEEDED" if i == 3 else "FAILED",
            "startDate": _dt(2026, 8, 1, 9 + i, 0), "stopDate": _dt(2026, 8, 1, 9 + i, 20),
        })
    sf = _FakeSF(executions=executions, describe_by_arn={"arn:w4": weekly_desc})
    s3 = _FakeS3(keys=[])
    _patch_boto3(monkeypatch, sf, s3)
    fake_alerts = MagicMock()
    with patch.dict("sys.modules", {"nousergon_lib": MagicMock(alerts=fake_alerts),
                                     "nousergon_lib.alerts": fake_alerts}):
        rc = m.main(["--run-date", "2026-08-01", "--no-alert"])
    assert rc == 0
    fake_alerts.publish.assert_not_called()


def test_main_no_write_flag_skips_s3_put(monkeypatch):
    ex, desc = _weekly_exec(
        "arn:nw", "uuid-nowrite", "SUCCEEDED",
        _dt(2026, 8, 8, 9, 0), _dt(2026, 8, 8, 11, 0),
    )
    sf = _FakeSF(executions=[ex], describe_by_arn={"arn:nw": desc})
    puts = []
    s3 = _FakeS3(keys=[], capture_puts=puts)
    _patch_boto3(monkeypatch, sf, s3)
    rc = m.main(["--run-date", "2026-08-08", "--no-write", "--no-alert"])
    assert rc == 0
    assert puts == []
