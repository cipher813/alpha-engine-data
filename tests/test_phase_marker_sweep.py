"""Tests for the Weekly-SF post-run phase-marker sweep (config#2322)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return self._pages


class _FakeS3:
    """Minimal boto3 S3 client fake: list_objects_v2 pagination + get_object."""

    def __init__(self, keys_and_bodies: dict[str, bytes]):
        self._bodies = keys_and_bodies
        contents = [{"Key": k} for k in keys_and_bodies]
        self._pages = [{"Contents": contents}] if contents else [{}]

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self._pages)

    def get_object(self, Bucket, Key):
        body = MagicMock()
        body.read.return_value = self._bodies[Key]
        return {"Body": body}


def _marker(phase: str, status: str, error: str | None = None) -> bytes:
    return json.dumps({
        "schema_version": 1,
        "phase": phase,
        "date": "2026-07-18",
        "status": status,
        "started_at": "2026-07-18T09:00:00Z",
        "completed_at": "2026-07-18T09:00:01Z",
        "duration_s": 1.0,
        "artifact_keys": [],
        "error": error,
    }).encode()


def test_sweep_no_markers_returns_ok(monkeypatch):
    from validators import phase_marker_sweep

    monkeypatch.setattr(phase_marker_sweep, "boto3", MagicMock(
        client=lambda *a, **k: _FakeS3({})
    ))
    result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "ok"
    assert result["checked_count"] == 0
    assert result["error_phases"] == []


def test_sweep_all_ok_markers_returns_ok(monkeypatch):
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok"),
        "backtest/2026-07-18/.phases/evaluator.json": _marker("evaluator", "ok"),
    })
    monkeypatch.setattr(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake))
    result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "ok"
    assert result["checked_count"] == 2
    assert result["error_phases"] == []


def test_sweep_detects_error_marker():
    """The canonical 2026-07-11 scenario: one phase status=error."""
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok"),
        "backtest/2026-07-18/.phases/scanner_predictor_research_free_backfill.json": _marker(
            "scanner_predictor_research_free_backfill", "error",
            error="FileNotFoundError: missing local weights sync",
        ),
    })
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "phase_errors_detected"
    assert result["checked_count"] == 2
    assert len(result["error_phases"]) == 1
    assert result["error_phases"][0]["phase"] == "scanner_predictor_research_free_backfill"
    assert "FileNotFoundError" in result["error_phases"][0]["error"]


def test_sweep_unparseable_marker_is_a_named_finding_not_fatal():
    """An unparseable marker is reported by key — and is NOT scored `ok`.

    Contract change, alpha-engine-config-I9262. This test previously
    asserted `status == "ok"`: a corrupt marker was warned about and
    dropped, and the sweep then reported a clean bill of health for a run
    whose phase outcome it had just failed to read. That is `no data`
    rendered as green (`principles.md` §2.7), on the one surface that
    exists to say whether a non-fatal backtester phase errored.

    Still not fatal in the sense the old name meant: the sweep completes,
    reports every marker it COULD read, and exits 1 (a finding) rather
    than 2 (a sweep-infra fault).
    """
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/corrupt.json": b"not json {{{",
        "backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok"),
    })
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "malformed_markers_detected"
    assert result["checked_count"] == 1
    assert [m["s3_key"] for m in result["malformed_markers"]] == [
        "backtest/2026-07-18/.phases/corrupt.json"
    ]


def test_sweep_s3_failure_returns_error():
    from validators import phase_marker_sweep

    boom_client = MagicMock(side_effect=Exception("S3 unreachable"))
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=boom_client)):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "s3_list"


def test_sweep_publishes_alert_on_error_phase_with_dedup_key():
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/evaluator.json": _marker(
            "evaluator", "error", error="ValueError: bad input",
        ),
    })
    fake_alerts = MagicMock()
    fake_alerts.publish.return_value = MagicMock(
        sns=MagicMock(ok=True), telegram=MagicMock(ok=True), any_ok=True,
    )
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)), \
         patch.dict("sys.modules", {"nousergon_lib": MagicMock(alerts=fake_alerts),
                                     "nousergon_lib.alerts": fake_alerts}):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=True)

    assert result["status"] == "phase_errors_detected"
    fake_alerts.publish.assert_called_once()
    _, kwargs = fake_alerts.publish.call_args
    assert kwargs["dedup_key"] == "phase_marker_sweep_2026-07-18_evaluator"
    assert kwargs["severity"] == "error"


def test_sweep_no_alert_flag_skips_publish():
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/evaluator.json": _marker(
            "evaluator", "error", error="ValueError: bad input",
        ),
    })
    fake_alerts = MagicMock()
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)), \
         patch.dict("sys.modules", {"nousergon_lib": MagicMock(alerts=fake_alerts),
                                     "nousergon_lib.alerts": fake_alerts}):
        phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    fake_alerts.publish.assert_not_called()


def test_main_exit_code_ok():
    from validators.phase_marker_sweep import main

    fake = _FakeS3({"backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok")})
    with patch("validators.phase_marker_sweep.boto3", MagicMock(client=lambda *a, **k: fake)):
        rc = main(["--run-date", "2026-07-18", "--no-alert"])
    assert rc == 0


def test_main_exit_code_phase_errors_detected():
    from validators.phase_marker_sweep import main

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/evaluator.json": _marker("evaluator", "error", error="boom"),
    })
    with patch("validators.phase_marker_sweep.boto3", MagicMock(client=lambda *a, **k: fake)):
        rc = main(["--run-date", "2026-07-18", "--no-alert"])
    assert rc == 1


def test_main_requires_run_date():
    from validators.phase_marker_sweep import main

    with pytest.raises(SystemExit):
        main(["--no-alert"])


def test_abbreviated_long_option_is_rejected_not_rebound():
    """`--alert` must NOT be silently accepted as a prefix of `--alert-severity`.

    config-I7415: the weekly SF's substrate health check invoked the sweep with
    `--alert`, intending to turn alerting on (it is already the default).
    argparse's default `allow_abbrev=True` rebound it to `--alert-severity`,
    which then aborted for a missing value — so the sweep exited non-zero
    without ever sweeping, and the caller read that as a phase-marker finding.
    """
    from validators.phase_marker_sweep import main

    with pytest.raises(SystemExit) as excinfo:
        main(["--run-date", "2026-07-18", "--alert"])
    # argparse exits 2 for an unrecognised argument — NOT the sweep's own
    # rc=1 "phase errors detected", which is what the rebinding produced.
    assert excinfo.value.code == 2


def test_declared_long_options_still_parse_in_full():
    from validators.phase_marker_sweep import main

    fake = _FakeS3({"backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok")})
    with patch("validators.phase_marker_sweep.boto3", MagicMock(client=lambda *a, **k: fake)):
        rc = main(["--run-date", "2026-07-18", "--no-alert", "--alert-severity", "warn"])
    assert rc == 0


# ── alpha-engine-config-I9262 regressions ────────────────────────────────
#
# The 2026-08-29 weekly SF run terminated SubstrateHealthCheckDegraded on a
# TypeError inside this sweep, not on a health finding. Two independent
# defects, one test each. Both FAIL on the pre-fix implementation:
# the first with `TypeError: list indices must be integers or slices, not
# str`, the second the same way.


class _DelimiterAwareS3:
    """S3 fake that honours ``Delimiter`` the way the real API does.

    With ``Delimiter="/"`` the real ``list_objects_v2`` returns only keys
    with no further ``/`` after the prefix under ``Contents``; anything
    deeper is collapsed into ``CommonPrefixes``. The pre-existing
    ``_FakeS3`` in this module ignores ``Delimiter`` entirely, so it cannot
    tell the fixed implementation from the broken one — hence this second
    fake rather than a change to that one (other tests depend on it).
    """

    def __init__(self, keys_and_bodies: dict[str, bytes]):
        self._bodies = keys_and_bodies

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        outer = self

        class _P:
            def paginate(self, **kwargs):
                prefix = kwargs.get("Prefix", "")
                delimiter = kwargs.get("Delimiter")
                contents, common = [], set()
                for k in outer._bodies:
                    if not k.startswith(prefix):
                        continue
                    rest = k[len(prefix):]
                    if delimiter and delimiter in rest:
                        common.add(prefix + rest.split(delimiter, 1)[0] + delimiter)
                    else:
                        contents.append({"Key": k})
                page = {}
                if contents:
                    page["Contents"] = contents
                if common:
                    page["CommonPrefixes"] = [{"Prefix": p} for p in sorted(common)]
                return [page or {}]

        return _P()

    def get_object(self, Bucket, Key):
        body = MagicMock()
        body.read.return_value = self._bodies[Key]
        return {"Body": body}


def test_nested_phase_artifacts_are_not_read_as_markers(monkeypatch):
    """A phase artifact under .phases/{phase}/ is not a marker.

    Measured live on backtest/2026-08-28/.phases/: simulation_setup.json is
    a well-formed marker, and simulation_setup/dates.json — declared in
    that marker's own artifact_keys — is a JSON ARRAY. Without
    Delimiter="/" the sweep read the array as a marker and died.
    """
    from validators import phase_marker_sweep

    s3 = _DelimiterAwareS3({
        "backtest/2026-08-28/.phases/simulation_setup.json": _marker(
            "simulation_setup", "ok"
        ),
        "backtest/2026-08-28/.phases/simulation_setup/dates.json": json.dumps(
            ["2026-03-05", "2026-03-06"]
        ).encode(),
    })
    monkeypatch.setattr(
        phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: s3)
    )

    result = phase_marker_sweep.sweep(run_date="2026-08-28", alert=False)

    assert result["status"] == "ok", result
    assert result["checked_count"] == 1
    assert result["error_phases"] == []
    assert result["malformed_markers"] == []


def test_non_dict_top_level_marker_is_a_named_finding_not_a_crash(monkeypatch):
    """A top-level marker that parses to a non-object is reported, not fatal.

    json.loads accepts an array, so the pre-fix
    ``except (JSONDecodeError, ValueError)`` guard never saw it. Fail loud
    with the key named — a marker the sweep cannot read must not be
    indistinguishable from a healthy phase, and must not present as an
    opaque stage=s3_list harness fault.
    """
    from validators import phase_marker_sweep

    s3 = _DelimiterAwareS3({
        "backtest/2026-08-28/.phases/good.json": _marker("good", "ok"),
        "backtest/2026-08-28/.phases/bad.json": json.dumps([1, 2, 3]).encode(),
    })
    monkeypatch.setattr(
        phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: s3)
    )

    result = phase_marker_sweep.sweep(run_date="2026-08-28", alert=False)

    assert result["status"] == "malformed_markers_detected", result
    assert result["checked_count"] == 1
    assert [m["s3_key"] for m in result["malformed_markers"]] == [
        "backtest/2026-08-28/.phases/bad.json"
    ]
    assert "list" in result["malformed_markers"][0]["reason"]


def test_malformed_marker_exits_1_not_2(monkeypatch):
    """An unreadable marker is a finding (exit 1), not a sweep-infra fault (2).

    substrate_health_check.sh treats this sweep as a gating check, so the
    distinction is what separates "a phase marker is broken" from "the
    sweep could not run" on the surface that reports it.
    """
    from validators import phase_marker_sweep

    s3 = _DelimiterAwareS3({
        "backtest/2026-08-28/.phases/bad.json": json.dumps([1]).encode(),
    })
    monkeypatch.setattr(
        phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: s3)
    )

    rc = phase_marker_sweep.main(
        ["--run-date", "2026-08-28", "--no-alert"]
    )
    assert rc == 1
