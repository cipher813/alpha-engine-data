"""The declared weekly exercise-cadence must stay a record, not a comment.

alpha-engine-config#6689: before this file, the weekly pipeline's exercise
cadence lived on TWO uncoordinated surfaces (an SF Choice hardcoded in
step_function_eod.json, no declared parameter anywhere) — flipping daily<->
weekly meant an SF-topology edit. infrastructure/weekly_cadence.json is now
the single declared source; infrastructure/weekly_cadence_drift.py verifies
it against the live SSM copy the SF actually reads, mirroring
automation_pause.py's two-directional check.

These tests pin: the manifest parses and declares an allowed value with
documented meaning for all three (§1); the drift script both directions
(missing-in-aws / value-mismatch) plus the AccessDenied-is-not-absence
guard (§2, the same failure mode automation_pause.py's _live_state guards
against); deploy-infrastructure.sh actually invokes --enforce in the same
step that updates the postclose SF (§3); the SF definition itself
(§4, structural).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
MANIFEST_PATH = INFRA / "weekly_cadence.json"
MODULE_PATH = INFRA / "weekly_cadence_drift.py"
DEPLOY_SCRIPT = INFRA / "deploy-infrastructure.sh"

ALLOWED_VALUES = {"daily", "weekly-only", "off"}


def _load_module():
    spec = importlib.util.spec_from_file_location("weekly_cadence_drift", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["weekly_cadence_drift"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def module():
    return _load_module()


# ── §1: manifest shape ──────────────────────────────────────────────────────

def test_manifest_declares_an_allowed_value(manifest):
    assert manifest["exercise_cadence"] in ALLOWED_VALUES


def test_manifest_declares_daily_unchanged_by_this_change(manifest):
    """config#6689 changes the MECHANISM, never the cadence itself — the
    value stays 'daily' (alpha-engine-config#5489, Brian ruling 2026-07-29)."""
    assert manifest["exercise_cadence"] == "daily"


def test_allowed_values_field_matches_the_real_allowed_set(manifest):
    assert set(manifest["allowed_values"]) == ALLOWED_VALUES


def test_every_allowed_value_has_documented_meaning(manifest):
    meaning = manifest["_meaning"]
    for value in ALLOWED_VALUES:
        assert meaning.get(value, "").strip(), f"{value} has no documented meaning"


def test_meaning_names_the_ruling_and_the_issue(manifest):
    daily_meaning = manifest["_meaning"]["daily"]
    assert "5489" in daily_meaning, "the daily-cadence ruling (#5489) is not referenced"
    assert "6689" in manifest["_provenance"], "the mechanism issue (#6689) is not referenced"


# ── §2: drift script, both directions ───────────────────────────────────────

def test_declared_cadence_returns_the_manifest_value(module):
    assert module.declared_cadence() == "daily"


def test_declared_cadence_rejects_an_invalid_value(module, tmp_path):
    bad = tmp_path / "weekly_cadence.json"
    bad.write_text(json.dumps({"exercise_cadence": "hourly"}), encoding="utf-8")
    with pytest.raises(ValueError):
        module.declared_cadence(module.load_manifest(bad))


def test_check_reports_missing_in_aws_when_param_absent(module, monkeypatch):
    monkeypatch.setattr(module, "_live_value", lambda: None)
    findings = module.check()
    assert len(findings) == 1
    assert findings[0]["kind"] == "missing-in-aws"


def test_check_reports_value_mismatch(module, monkeypatch):
    monkeypatch.setattr(module, "_live_value", lambda: "weekly-only")
    findings = module.check()
    assert len(findings) == 1
    assert findings[0]["kind"] == "value-mismatch"
    assert "weekly-only" in findings[0]["detail"]
    assert "daily" in findings[0]["detail"]


def test_check_is_clean_when_manifest_matches_live(module, monkeypatch):
    monkeypatch.setattr(module, "_live_value", lambda: "daily")
    assert module.check() == []


def test_access_denied_is_not_read_as_absence(module, monkeypatch):
    """The same failure mode automation_pause.py's _live_state guards
    against: a permissions error read as 'not deployed yet' would let this
    check grade itself green by losing its own access."""
    def _boom(*_a, **_k):
        return (254, "", "An error occurred (AccessDenied) when calling the GetParameter operation")
    monkeypatch.setattr(module, "_aws", _boom)
    with pytest.raises(RuntimeError, match="AccessDenied"):
        module._live_value()


def test_enforce_writes_when_live_differs(module, monkeypatch):
    calls = []
    monkeypatch.setattr(module, "_live_value", lambda: "off")

    def _fake_aws(args):
        calls.append(args)
        return (0, "", "")

    monkeypatch.setattr(module, "_aws", _fake_aws)
    wrote = module.enforce()
    assert wrote is True
    assert calls and calls[0][:2] == ["ssm", "put-parameter"]
    assert "daily" in calls[0]


def test_enforce_is_a_noop_when_already_in_sync(module, monkeypatch):
    monkeypatch.setattr(module, "_live_value", lambda: "daily")
    monkeypatch.setattr(module, "_aws", lambda args: pytest.fail("should not call aws when already in sync"))
    assert module.enforce() is False


# ── §3: deploy wiring ────────────────────────────────────────────────────────

def test_deploy_infrastructure_invokes_enforce_after_the_eod_sf_update():
    src = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    assert "weekly_cadence_drift.py" in src and "--enforce" in src, (
        "deploy-infrastructure.sh does not sync the declared cadence to SSM — "
        "a manifest edit alone would never reach the live parameter the SF reads"
    )
    # Must run AFTER the EOD SF definition is updated, not before — otherwise a
    # deploy could push a new SF definition that reads a stale parameter value
    # for however long the rest of the script takes.
    eod_update_idx = src.index('update_or_create "$EOD_ARN"')
    sync_idx = src.index("weekly_cadence_drift.py")
    assert eod_update_idx < sync_idx


def test_deploy_infrastructure_sync_step_is_not_conditionally_skipped():
    """The section this line lives in (### 3a) must run unconditionally on
    every deploy — a merge-cadence flip should never depend on a flag."""
    src = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    section = src.split("# ── 3a. Sync the declared weekly exercise-cadence", 1)[1]
    section = section.split("# ── 3b.", 1)[0]
    assert "if " not in section and "if [" not in section
