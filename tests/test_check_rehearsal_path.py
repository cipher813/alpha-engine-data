"""Unit tests for scripts/check_rehearsal_path.py (alpha-engine-config-I6688).

Exercises every branch of weekly-sf-policy.md §7's accepted-forms decision:
no trailer at all, a valid/invalid `Rehearsal-path:` (mechanism keyword vs.
repo-relative test path vs. neither), both §7.3 exemption values plus a
bogus one, and a `Rehearsal-risk-acceptance:` paragraph both long enough and
too short to count as "the specific constraint" §7.2 route 3 requires.

Also asserts `weekly_critical_paths_touched` (the glob-matching helper) and
the module's own CLI entrypoint (reads `PR_BODY` from the environment,
never from a shell-interpolated body, since a PR body is attacker-controlled
text on `pull_request`).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_rehearsal_path.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_rehearsal_path", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_rehearsal_path"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load_module()


# ---------------------------------------------------------------------------
# evaluate() — no trailer at all
# ---------------------------------------------------------------------------


def test_no_trailers_fails(mod):
    result = mod.evaluate("This change hardens preflight. Needs a live Saturday to validate.", REPO_ROOT)
    assert result.ok is False
    assert "§7" in result.message or "accepted forms" in result.message.lower() or "Accepted forms" in result.message


def test_empty_body_fails(mod):
    result = mod.evaluate("", REPO_ROOT)
    assert result.ok is False


# ---------------------------------------------------------------------------
# Rehearsal-path: §7.1 mechanism keywords
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "run_weekly_offcycle.sh",
        "the Friday shell-run (infrastructure/run_weekly_offcycle.sh)",
        "canary-replay",
        "canary:replay label",
        "Replay canary (.github/workflows/canary-replay.yml)",
    ],
)
def test_rehearsal_path_mechanism_keyword_passes(mod, value):
    body = f"What & why.\n\nRehearsal-path: {value}\n\nVerified-when: ...\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is True


def test_rehearsal_path_existing_test_file_passes(mod):
    # This very test file exists in the checked-out tree.
    rel = "tests/test_check_rehearsal_path.py"
    body = f"Rehearsal-path: {rel}\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is True


def test_rehearsal_path_nonexistent_file_fails(mod):
    body = "Rehearsal-path: tests/test_does_not_exist_anywhere_260809.py\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is False


def test_rehearsal_path_escaping_repo_root_fails(mod):
    body = "Rehearsal-path: ../../etc/passwd\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is False


def test_rehearsal_path_prose_only_fails(mod):
    body = "Rehearsal-path: needs a Saturday, no rehearsal exists\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is False


# ---------------------------------------------------------------------------
# Rehearsal-exempt: §7.3, exhaustive two values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["structural", "complexity:ultra", "Structural", "COMPLEXITY:ULTRA"])
def test_rehearsal_exempt_valid_values_pass(mod, value):
    body = f"Rehearsal-exempt: {value}\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is True


def test_rehearsal_exempt_bogus_value_fails(mod):
    body = "Rehearsal-exempt: low-risk\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is False


# ---------------------------------------------------------------------------
# Rehearsal-risk-acceptance: §7.2 route 3 paragraph
# ---------------------------------------------------------------------------


def test_rehearsal_risk_acceptance_specific_constraint_passes(mod):
    body = (
        "Rehearsal-risk-acceptance: this touches the live IAM trust boundary "
        "for the weekly execution role; no sandbox account holds an "
        "equivalent role to rehearse the assume-role path against, so this "
        "is a genuine risk-acceptance decision rather than a deferral.\n"
    )
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is True


def test_rehearsal_risk_acceptance_multiline_paragraph_passes(mod):
    body = (
        "Rehearsal-risk-acceptance:\n"
        "The EventBridge rule change is irreversible once the current\n"
        "week's schedule fires; there is no dry-run mode for a live rule.\n"
        "\n"
        "Verified-when: manual review of the rule diff.\n"
    )
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is True


def test_rehearsal_risk_acceptance_placeholder_fails(mod):
    body = "Rehearsal-risk-acceptance: n/a\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is False


def test_rehearsal_risk_acceptance_empty_fails(mod):
    body = "Rehearsal-risk-acceptance:\n\nVerified-when: ...\n"
    result = mod.evaluate(body, REPO_ROOT)
    assert result.ok is False


# ---------------------------------------------------------------------------
# weekly_critical_paths_touched()
# ---------------------------------------------------------------------------


def test_weekly_critical_paths_touched_matches_expected(mod):
    changed = [
        "infrastructure/step_function.json",
        "infrastructure/step_function_offcycle.json",
        "infrastructure/spot_backtest.sh",
        "infrastructure/run_weekly_offcycle.sh",
        "scripts/weekly_sf_rerun.py",
        "README.md",
        ".github/workflows/canary-replay.yml",
    ]
    hits = mod.weekly_critical_paths_touched(changed)
    assert set(hits) == {
        "infrastructure/step_function.json",
        "infrastructure/step_function_offcycle.json",
        "infrastructure/spot_backtest.sh",
        "infrastructure/run_weekly_offcycle.sh",
        "scripts/weekly_sf_rerun.py",
    }


def test_weekly_critical_paths_touched_empty_when_no_match(mod):
    assert mod.weekly_critical_paths_touched(["README.md", "tests/test_foo.py"]) == []


# ---------------------------------------------------------------------------
# CLI entrypoint — reads PR_BODY from the environment, not argv/shell
# ---------------------------------------------------------------------------


def test_main_reads_pr_body_from_env_and_exits_nonzero_on_failure(mod, monkeypatch, capsys):
    monkeypatch.setenv("PR_BODY", "no trailers here")
    rc = mod.main(["check_rehearsal_path.py", str(REPO_ROOT)])
    assert rc == 1
    out = capsys.readouterr().out
    assert "Accepted forms" in out


def test_main_reads_pr_body_from_env_and_exits_zero_on_success(mod, monkeypatch, capsys):
    monkeypatch.setenv("PR_BODY", "Rehearsal-exempt: structural\n")
    rc = mod.main(["check_rehearsal_path.py", str(REPO_ROOT)])
    assert rc == 0


def test_main_defaults_repo_root_and_missing_pr_body(mod, monkeypatch):
    monkeypatch.delenv("PR_BODY", raising=False)
    rc = mod.main(["check_rehearsal_path.py"])
    assert rc == 1
