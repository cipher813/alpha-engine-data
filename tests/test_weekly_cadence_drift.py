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

def test_manifest_declares_a_dated_cadence_history(manifest):
    """alpha-engine-config-I7175: exercise_cadence is a dated history, not a
    bare scalar, so a backward-looking reader can tell 'not declared yet'
    from 'declared and silent'."""
    history = manifest["exercise_cadence"]
    assert isinstance(history, list) and history
    for entry in history:
        assert entry["value"] in ALLOWED_VALUES
        # raises if malformed
        __import__("datetime").date.fromisoformat(entry["effective_from"])


def test_manifest_history_is_sorted_ascending_by_effective_from(manifest):
    dates = [e["effective_from"] for e in manifest["exercise_cadence"]]
    assert dates == sorted(dates)


def test_manifest_current_entry_is_a_declared_value(manifest):
    """The current entry must be one of the declared values, and carry the
    ruling that authorised it.

    This USED to assert the literal ``== "daily"``, under the name
    ``test_manifest_current_entry_is_daily_unchanged_by_this_change`` — correct
    for the change that introduced it (config#6689 altered the mechanism, not
    the cadence), but it outlived its purpose: it pinned the knob's POSITION in
    a test suite whose job is to check the knob WORKS. Six tests in this file
    hard-coded 'daily', so `weekly_cadence.json`'s stated promise — "flipping
    daily <-> weekly-only <-> off is a one-line diff" — was false; it was a
    one-line diff plus six test edits, and every legitimate cadence ruling had
    to fight its own guard. Brian's 2026-08-13 ruling (daily -> off) was the
    first to hit it.

    Asserting the property keeps the guard real: an undeclared or typo'd value
    still fails here, and `test_current_entry_records_its_ruling` below keeps
    the provenance requirement that made the old assertion feel load-bearing.
    """
    current = manifest["exercise_cadence"][-1]
    assert current["value"] in manifest["allowed_values"]


def test_current_entry_records_its_ruling(manifest):
    """A cadence change is a ruling, not tuning (sf-pipeline-policy §2.6 rule
    3), so the entry in force must say who decided it and why. This is the
    half of the old literal-pinning test that was genuinely load-bearing."""
    current = manifest["exercise_cadence"][-1]
    why = current.get("_why", "")
    assert len(why) > 80, "the current cadence entry must carry a substantive _why"
    assert "ruling" in why.lower() or "brian" in why.lower(), (
        "the current cadence entry must name the ruling that authorised it"
    )


def test_daily_entry_effective_from_precedes_the_ruling_date(manifest):
    """Measured live 2026-08-13 (alpha-engine-config-I7175): postclose FAILED
    before reaching LaunchWeeklyExerciseRun on 2026-07-27/07-28 — two real
    silent exercise slots — while the ruling recorded in #5489 is dated
    2026-07-29. effective_from must be <= 2026-07-27 or those two real
    silences reclassify as GATED_OFF, which is the opposite of what this
    issue exists to fix."""
    daily_entries = [e for e in manifest["exercise_cadence"] if e["value"] == "daily"]
    assert daily_entries
    assert daily_entries[0]["effective_from"] <= "2026-07-27"


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

def test_declared_cadence_returns_the_manifest_value(module, manifest):
    """Reads the manifest rather than a literal — which is what the test's own
    name always claimed. See test_manifest_current_entry_is_a_declared_value
    for why the literals were removed."""
    assert module.declared_cadence() == manifest["exercise_cadence"][-1]["value"]


def test_declared_cadence_rejects_an_invalid_value(module, tmp_path):
    bad = tmp_path / "weekly_cadence.json"
    bad.write_text(json.dumps({"exercise_cadence": "hourly"}), encoding="utf-8")
    with pytest.raises(ValueError):
        module.declared_cadence(module.load_manifest(bad))


def test_declared_cadence_resolves_the_entry_in_force_on_a_given_today(module):
    from datetime import date
    history = {
        "exercise_cadence": [
            {"value": "weekly-only", "effective_from": "2026-01-01"},
            {"value": "daily", "effective_from": "2026-07-27"},
        ]
    }
    assert module.declared_cadence(history, today=date(2026, 7, 20)) == "weekly-only"
    assert module.declared_cadence(history, today=date(2026, 7, 27)) == "daily"
    assert module.declared_cadence(history, today=date(2026, 8, 13)) == "daily"


def test_declared_cadence_raises_when_today_precedes_the_first_entry(module):
    from datetime import date
    history = {"exercise_cadence": [{"value": "daily", "effective_from": "2026-07-27"}]}
    with pytest.raises(ValueError):
        module.declared_cadence(history, today=date(2026, 1, 1))


def test_check_reports_missing_in_aws_when_param_absent(module, monkeypatch):
    monkeypatch.setattr(module, "_live_value", lambda: None)
    findings = module.check()
    assert len(findings) == 1
    assert findings[0]["kind"] == "missing-in-aws"


def test_check_reports_value_mismatch(module, monkeypatch, manifest):
    declared = manifest["exercise_cadence"][-1]["value"]
    other = next(v for v in manifest["allowed_values"] if v != declared)
    monkeypatch.setattr(module, "_live_value", lambda: other)
    findings = module.check()
    assert len(findings) == 1
    assert findings[0]["kind"] == "value-mismatch"
    assert other in findings[0]["detail"]
    assert declared in findings[0]["detail"]


def test_check_is_clean_when_manifest_matches_live(module, monkeypatch, manifest):
    declared = manifest["exercise_cadence"][-1]["value"]
    monkeypatch.setattr(module, "_live_value", lambda: declared)
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


def test_enforce_writes_when_live_differs(module, monkeypatch, manifest):
    declared = manifest["exercise_cadence"][-1]["value"]
    other = next(v for v in manifest["allowed_values"] if v != declared)
    calls = []
    monkeypatch.setattr(module, "_live_value", lambda: other)

    def _fake_aws(args):
        calls.append(args)
        return (0, "", "")

    monkeypatch.setattr(module, "_aws", _fake_aws)
    wrote = module.enforce()
    assert wrote is True
    assert calls and calls[0][:2] == ["ssm", "put-parameter"]
    assert declared in calls[0]


def test_enforce_is_a_noop_when_already_in_sync(module, monkeypatch, manifest):
    declared = manifest["exercise_cadence"][-1]["value"]
    monkeypatch.setattr(module, "_live_value", lambda: declared)
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
