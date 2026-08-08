"""Tests for infrastructure/systemd/check-systemd-unit-drift.py (config#2352).

Covers the installed-vs-repo systemd unit drift probe: clean match,
divergence detection, not-installed (box hosts neither pair) as non-error,
and missing repo source as a config error. No real AWS/systemd access —
purely local file comparison, so this is a plain tmp-dir fixture test
(mirrors the module-load pattern used by test_sf_definition_check_drift.py,
minus the subprocess mocking since this script never shells out to AWS).
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "infrastructure" / "systemd" / "check-systemd-unit-drift.py"


@pytest.fixture()
def cd(tmp_path, monkeypatch):
    """Load the module fresh per-test, pointed at an isolated repo+installed dir pair."""
    spec = importlib.util.spec_from_file_location("check_systemd_unit_drift", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    script_dir = tmp_path / "infrastructure" / "systemd"
    script_dir.mkdir(parents=True)
    installed_dir = tmp_path / "etc-systemd-system"
    installed_dir.mkdir()

    monkeypatch.setattr(module, "SCRIPT_DIR", script_dir)
    monkeypatch.setattr(module, "INSTALLED_DIR", installed_dir)

    return module, script_dir, installed_dir


def _write(path: Path, content: str) -> None:
    path.write_text(content)


def test_clean_when_installed_matches_repo(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "daily-news.timer", "UNIT A\n")
    _write(installed_dir / "daily-news.timer", "UNIT A\n")

    status, detail = module.check_unit("daily-news.timer")

    assert status == "clean"
    assert "OK" in detail


def test_drift_when_installed_diverges_from_repo(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "metron-intraday.service", "UNIT NEW\n")
    _write(installed_dir / "metron-intraday.service", "UNIT OLD (stale)\n")

    status, detail = module.check_unit("metron-intraday.service")

    assert status == "drift"
    assert "metron-intraday.service" in detail


def test_not_installed_when_box_never_had_the_unit(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "metron-intraday.timer", "UNIT A\n")
    # No file under installed_dir — this box never installed it (e.g. the
    # dashboard box probing for metron-intraday, which only the trading box
    # hosts).

    status, detail = module.check_unit("metron-intraday.timer")

    assert status == "not-installed"


def test_installed_with_no_repo_copy_is_uncodified_not_a_source_error(cd):
    """Reclassified 2026-08-08 (alpha-engine-config-I6656).

    This used to assert `source-error` — i.e. that a unit installed with no
    repo copy meant THIS SCRIPT was misconfigured. On the dashboard box 54 of
    60 units are in that state; they are not 54 configuration errors, they are
    the coverage gap, and naming them an error made the honest reading
    unavailable.
    """
    module, script_dir, installed_dir = cd
    _write(installed_dir / "ghost.service", "UNIT GHOST\n")

    status, detail = module.check_unit("ghost.service")

    assert status == "uncodified"
    assert "codified in no known root" in detail


def test_all_not_installed_when_box_hosts_neither_pair(cd):
    module, script_dir, installed_dir = cd
    for name in module.ALL_UNITS:
        _write(script_dir / name, f"UNIT {name}\n")
    # installed_dir stays empty — this box hosts none of the tracked units.

    statuses = [module.check_unit(name)[0] for name in module.ALL_UNITS]

    assert all(s == "not-installed" for s in statuses)


def test_main_reports_drift_exit_code_via_cli(cd, monkeypatch, capsys):
    module, script_dir, installed_dir = cd
    _write(script_dir / "daily-news.service", "UNIT NEW\n")
    _write(installed_dir / "daily-news.service", "UNIT OLD\n")
    for name in module.ALL_UNITS:
        if name != "daily-news.service":
            _write(script_dir / name, f"UNIT {name}\n")

    monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
    exit_code = module.main()

    out = capsys.readouterr().out
    assert exit_code == 1
    assert "drift" in out.lower()


class TestDriftReportingPath:
    """The FAILURE path must actually report (alpha-engine-config-I4509).

    These are the tests that were missing. `_report_drift` (formerly
    `_flow_doctor_report`) was broken for an unknown length of time and nobody
    noticed, because it only runs when drift is FOUND and the daily check
    normally passes. Two independent faults were live simultaneously:
    `flow_doctor.init()` does not exist in flow-doctor 0.8.7, and the env
    hydration for flow-doctor.yaml was incomplete regardless.

    A reporting path exercised only on failure needs a test that exercises
    failure. That is the whole lesson.
    """

    def test_report_publishes_via_krepis(self, cd, monkeypatch):
        module, _, _ = cd
        captured = {}

        def fake_publish(**kwargs):
            captured.update(kwargs)

        monkeypatch.setitem(
            __import__("sys").modules, "krepis.alerts",
            type("M", (), {"publish": staticmethod(fake_publish)})
        )
        module._report_drift(["daily-news.service: DIVERGED"])

        assert captured, "drift must publish an alert, not just print"
        assert captured["severity"] == "error"
        assert captured["source"] == "check-systemd-unit-drift"
        assert "daily-news.service" in captured["message"], (
            "the alert must name the drifting unit — an alert that says "
            "'drift detected' sends you to the box to find out what"
        )

    def test_report_dedups_on_findings_not_message(self, cd, monkeypatch):
        # Same drift persisting should alert once a day; DIFFERENT drift must
        # produce a different key so it pages immediately.
        module, _, _ = cd
        keys = []

        def fake_publish(**kwargs):
            keys.append(kwargs["dedup_key"])

        monkeypatch.setitem(
            __import__("sys").modules, "krepis.alerts",
            type("M", (), {"publish": staticmethod(fake_publish)})
        )
        module._report_drift(["a.service: DIVERGED"])
        module._report_drift(["a.service: DIVERGED"])
        module._report_drift(["b.service: DIVERGED"])

        assert keys[0] == keys[1], "identical findings must share a dedup key"
        assert keys[2] != keys[0], "different findings must not be deduped together"

    def test_publish_failure_is_loud_not_swallowed(self, cd, monkeypatch, capsys):
        # The exact defect this rewrite fixes: drift was detected and the
        # telling threw, silently.
        module, _, _ = cd

        def boom(**kwargs):
            raise RuntimeError("simulated transport failure")

        monkeypatch.setitem(
            __import__("sys").modules, "krepis.alerts",
            type("M", (), {"publish": staticmethod(boom)})
        )
        module._report_drift(["a.service: DIVERGED"])

        err = capsys.readouterr().err
        assert "UNREPORTED" in err, (
            "a failure to publish must be loud — silent failure here is "
            "indistinguishable from no drift"
        )

    def test_flow_doctor_init_is_not_resurrected(self):
        """`flow_doctor.init` has not existed for some time -- guard the CALL.

        Parsed with `ast` rather than grepped, deliberately. A source-text
        guard trips on the module's own docstring explaining why this API is
        gone, which pushes the next person to delete the explanation rather
        than keep the guard. Matching a real Call node means prose is free to
        name the dead API as often as it is useful to.
        """
        import ast

        tree = ast.parse(_SCRIPT_PATH.read_text())
        bad = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "init"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "flow_doctor"
        ]
        assert not bad, (
            "flow_doctor.init() does not exist in flow-doctor 0.8.7 -- it "
            "exports FlowDoctor/FlowDoctorBuilder. Use krepis.alerts (the "
            "canonical CLI, config#1649)."
        )


class TestCoverageAccounting:
    """alpha-engine-config-I6656 — the defect was not a wrong diff. The diff
    was right. It was that four units were compared and the closing line said
    "installed units match repo", on a box with 60 installed unit files. So
    these assert what the script SAYS as much as what it compares.
    """

    def test_an_uncodified_unit_is_never_reported_as_a_pass(self, cd, capsys, monkeypatch):
        module, script_dir, installed_dir = cd
        _write(script_dir / "covered.service", "X\n")
        _write(installed_dir / "covered.service", "X\n")
        _write(installed_dir / "hand-installed.timer", "Y\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        module.main()
        out = capsys.readouterr().out

        assert "PASSED" not in out
        assert "INCOMPLETE" in out
        assert "1/2 installed units are codified and clean" in out

    def test_full_coverage_is_the_only_thing_called_a_pass(self, cd, capsys, monkeypatch):
        module, script_dir, installed_dir = cd
        _write(script_dir / "covered.service", "X\n")
        _write(installed_dir / "covered.service", "X\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        code = module.main()
        out = capsys.readouterr().out

        assert code == 0
        assert "coverage PASSED" in out
        assert "uncodified=0" in out

    def test_a_baselined_uncodified_unit_does_not_fail_the_run(self, cd, capsys, monkeypatch):
        """54 of them exist. Failing on the backlog pages once per unit on the
        first run and teaches the reader to disable the check."""
        module, script_dir, installed_dir = cd
        _write(installed_dir / "known.service", "Y\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: {"known.service"})
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        code = module.main()
        out = capsys.readouterr().out

        assert code == 0
        assert "uncodified_new=0" in out
        assert "uncodified-NEW" not in out

    def test_an_unbaselined_uncodified_unit_is_named_individually(self, cd, capsys, monkeypatch):
        """New is the only kind a given run can act on, so it must not blend
        into the count."""
        module, script_dir, installed_dir = cd
        _write(installed_dir / "known.service", "Y\n")
        _write(installed_dir / "surprise.timer", "Z\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: {"known.service"})
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        module.main()
        cap = capsys.readouterr()

        assert "[uncodified-NEW] surprise.timer" in cap.out
        assert "[uncodified] known.service" in cap.out
        assert "uncodified=2 uncodified_new=1" in cap.out
        assert "absent from the baseline" in cap.err

    def test_strict_fails_on_any_uncodified_unit(self, cd, capsys, monkeypatch):
        """The end state, once the baseline is empty."""
        module, script_dir, installed_dir = cd
        _write(installed_dir / "known.service", "Y\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: {"known.service"})
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py", "--strict"])

        assert module.main() == 1

    def test_symlinked_units_are_not_counted(self, cd, capsys, monkeypatch):
        """`/etc/systemd/system` carries systemd's own aliases into
        /usr/lib/systemd as symlinks. Counting those puts `dbus.service` in
        the work queue."""
        module, script_dir, installed_dir = cd
        _write(script_dir / "real.service", "X\n")
        _write(installed_dir / "real.service", "X\n")
        (installed_dir / "alias.service").symlink_to(installed_dir / "real.service")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        code = module.main()
        out = capsys.readouterr().out

        assert code == 0
        assert "installed=1" in out
        assert "alias.service" not in out

    def test_metric_failure_is_loud_and_does_not_mask_the_check(self, cd, capsys, monkeypatch):
        """The counts are the surface that says whether coverage is real, so
        their absence has to be attributable rather than silent — and a broken
        metric must not take the drift check itself down with it.

        The failure is injected at the real boundary (the boto3 call), not by
        replacing `_emit_metrics`: stubbing the function under test would
        assert the behaviour of the stub.
        """
        module, script_dir, installed_dir = cd
        _write(script_dir / "covered.service", "X\n")
        _write(installed_dir / "covered.service", "EDITED\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py", "--metric"])

        import boto3

        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no credentials")),
        )

        code = module.main()
        cap = capsys.readouterr()

        # The drift is still detected and still reported.
        assert code == 1
        assert "drift=1" in cap.out
        # And the metric failure is named, not swallowed.
        assert "METRIC EMIT FAILED" in cap.err
        assert "UNOBSERVED" in cap.err

    def test_the_shipped_baseline_parses_and_holds_only_unit_names(self, cd):
        """The baseline is data the check depends on; a typo in it silently
        un-covers a unit."""
        module, _, _ = cd
        baseline = module.load_baseline(_REPO_ROOT / "infrastructure" / "systemd" / "uncodified-units-baseline.txt")

        assert baseline, "the shipped baseline should not be empty — 54 units are uncodified"
        for name in baseline:
            assert name.endswith((".service", ".timer")), name
            assert " " not in name, name

    def test_the_shipped_baseline_covers_the_measured_dashboard_box(self, cd):
        """Seeded 2026-08-08 from i-09b539c844515d549. Spot-checks the units
        this session actually touched, so an edit that drops them is caught."""
        module, _, _ = cd
        baseline = module.load_baseline(_REPO_ROOT / "infrastructure" / "systemd" / "uncodified-units-baseline.txt")

        for name in (
            "morning-signal.service",
            "morning-signal.timer",
            "morning-signal-bakeoff.service",
            "box-health.timer",
            "litellm-proxy.service",
        ):
            assert name in baseline, f"{name} missing from the uncodified baseline"
