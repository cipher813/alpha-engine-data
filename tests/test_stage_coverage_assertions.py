"""Every weekly-SF stage this repo owns asserts its OWN declared output.

`alpha-engine-config-I7214`, `sf-pipeline-policy.md` §2.1. The assertion lives
in the stage's own launcher / handler, at the boundary where the fact becomes
knowable — NOT in a single end-of-run sweep, which learns of a miss ~3h late
and cannot attribute it to a stage still in flight.

The totality test below derives its denominator from the LIVE SF definition in
this repo rather than from a hand-written list: a test that enumerates what
exists is blind to where one is missing, and a hand-maintained stage list
drifts invisibly because the missing rows produce no signal.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
INFRA = REPO / "infrastructure"
SF_DEFINITION = INFRA / "step_function.json"

#: The CLI front door every bash launcher calls. One implementation, in krepis;
#: a per-repo copy would be the fork policy-shared-code forbids. It must be a
#: `krepis.*` module and not a `nousergon_lib.*` one: under runpy the latter is
#: a guard-less re-export shim that exits 0 SILENTLY without executing
#: (config#1646/#1649), which
#: tests/test_spot_data_weekly_ssm_transport.py::test_uses_lib_ssm_dispatcher_chokepoint
#: enforces — and which caught exactly this call site.
CLI_MODULE = "krepis.stage_coverage"


# ── Helpers ──────────────────────────────────────────────────────────────────


def _states(definition: dict) -> dict[str, dict]:
    """Flatten every state, including Parallel branches and Map iterators."""
    out: dict[str, dict] = {}

    def walk(states: dict) -> None:
        for name, body in states.items():
            out[name] = body
            for branch in body.get("Branches", []) or []:
                walk(branch["States"])
            if "Iterator" in body:
                walk(body["Iterator"]["States"])

    walk(definition["States"])
    return out


def _ssm_command_text(body: dict) -> str:
    params = (body.get("Parameters") or {}).get("Parameters") or {}
    return json.dumps(params)


def _launcher_stages_in_this_repo() -> dict[str, str]:
    """Return ``{sf_stage: launcher_script_name}`` for every weekly-SF Task
    state whose SSM command runs a script out of THIS repo's infrastructure/."""
    definition = json.loads(SF_DEFINITION.read_text())
    found: dict[str, str] = {}
    for name, body in _states(definition).items():
        if body.get("Type") != "Task" or "sendCommand" not in body.get("Resource", ""):
            continue
        text = _ssm_command_text(body)
        match = re.search(r"bash infrastructure/(spot_[a-z0-9_]+\.sh)", text)
        if match and (INFRA / match.group(1)).exists():
            found[name] = match.group(1)
    return found


def _script(name: str) -> str:
    return (INFRA / name).read_text()


# ── The denominator is derived, not enumerated ───────────────────────────────


def test_the_launcher_stage_set_is_discovered_from_the_live_definition() -> None:
    """Guards the guard: if this returns nothing, every totality test below
    passes vacuously — a detector that cannot fail."""
    stages = _launcher_stages_in_this_repo()
    assert stages, "no launcher-backed weekly stages discovered — the parser broke"
    # The four this repo is known to own. A NEW one appearing here is expected
    # to fail this assertion, which is the point: a stage added without an
    # assertion must break a build, not go quiet.
    assert set(stages) == {"MorningEnrich", "DataPhase1", "DataPhase2", "RAGIngestion"}


# ── Every launcher asserts, and asserts its OWN stage ────────────────────────


@pytest.mark.parametrize(
    ("stage", "script"),
    sorted(_launcher_stages_in_this_repo().items()),
)
def test_each_launcher_asserts_its_own_stage(stage: str, script: str) -> None:
    body = _script(script)
    assert CLI_MODULE in body, f"{script} never invokes {CLI_MODULE}"
    assert f"--stage {stage}" in body, (
        f"{script} backs SF state {stage} but does not assert under that name — "
        "a miss would be attributed to a stage that was working"
    )


@pytest.mark.parametrize(
    ("stage", "script"),
    sorted(_launcher_stages_in_this_repo().items()),
)
def test_each_launcher_passes_the_run_window(stage: str, script: str) -> None:
    """Without a window, a leftover from a previous cycle satisfies the probe
    while the consumer reads last week's belief."""
    body = _script(script)
    assert '--window-start "$_STAGE_WINDOW_START"' in body


def test_the_window_is_captured_before_the_workload_not_after() -> None:
    """A window taken after the write is trivially satisfied by it."""
    common = _script("_spot_common.sh")
    assert "_STAGE_WINDOW_START=" in common
    # In the one launcher that does not source _spot_common.sh, the capture
    # must precede the phase2 workload block.
    weekly = _script("spot_data_weekly.sh")
    assert weekly.index("_STAGE_WINDOW_START=") < weekly.index('RUN_MODE" = "phase2-only')


# ── The assertion may never fail the stage it observes ───────────────────────


@pytest.mark.parametrize(
    "script", sorted(set(_launcher_stages_in_this_repo().values()))
)
def test_the_assertion_cannot_fail_the_stage(script: str) -> None:
    """Observe mode. Every launcher runs under `set -euo pipefail`; an
    unguarded non-zero here would abort the stage — and a degraded summary
    routes the whole ~4h weekly run to a Fail state (config-I6891)."""
    for line in _script(script).splitlines():
        if CLI_MODULE in line and "assert --stage" in line:
            assert "||" in line, f"{script}: assertion is not failure-guarded"


@pytest.mark.parametrize(
    "script", sorted(set(_launcher_stages_in_this_repo().values()))
)
def test_the_guard_is_loud_not_a_bare_true(script: str) -> None:
    """`|| true` would make an unreachable assertion indistinguishable from a
    covered stage — the exact silence this mechanism exists to remove."""
    for line in _script(script).splitlines():
        if CLI_MODULE in line and "assert --stage" in line:
            assert "|| true" not in line
            assert "WARNING" in line and ">&2" in line


# ── Promotion to enforcing is a deliberate, reviewed diff ────────────────────


@pytest.mark.parametrize(
    "script", sorted(set(_launcher_stages_in_this_repo().values()))
)
def test_no_call_site_ships_enforcing(script: str) -> None:
    body = _script(script)
    for line in body.splitlines():
        if CLI_MODULE in line and "assert --stage" in line:
            assert "--enforce" not in line
    assert "STAGE_COVERAGE_ENFORCE" not in body


# ── The Friday shell run must not assert ─────────────────────────────────────


def test_preflight_only_paths_exit_before_the_assertion() -> None:
    """`--preflight-only` is a dry pass that writes nothing by design.
    Asserting on it would report a miss for every stage, every Friday — and a
    detector that cries wolf weekly is a detector that gets muted."""
    for script in ("spot_morning_enrich.sh", "spot_data_phase1.sh", "spot_rag_ingestion.sh"):
        body = _script(script)
        assertion_at = body.index(f"{CLI_MODULE} assert")
        preflight_exit = body.index('if [ "$PREFLIGHT_ONLY" = "1" ]')
        assert preflight_exit < assertion_at, (
            f"{script}: the preflight-only early exit must precede the assertion"
        )

    weekly = _script("spot_data_weekly.sh")
    phase2_assertion = weekly.index("assert --stage DataPhase2")
    preflight_exit = weekly.index(
        "Preflight-only run — heartbeat deliberately NOT emitted"
    )
    assert preflight_exit < phase2_assertion


# ── The multi-mode launcher derives its stage name ───────────────────────────


def test_the_multimode_launcher_asserts_only_the_phase2_stage() -> None:
    """`spot_data_weekly.sh` serves six RUN_MODEs and only `--phase2-only` is
    an SF-wired weekly stage. A file-level assertion would file every other
    mode's run under DataPhase2."""
    body = _script("spot_data_weekly.sh")
    assert body.count(f"{CLI_MODULE} assert") == 1
    block_start = body.index('if [ "$RUN_MODE" = "phase2-only" ]')
    assert body.index("assert --stage DataPhase2") > block_start


# ── The Lambda-backed stages this repo owns ──────────────────────────────────


def test_weekly_preflight_records_its_no_output_declaration() -> None:
    body = (INFRA / "lambdas" / "weekly-preflight" / "index.py").read_text()
    assert "from krepis.stage_coverage import assert_stage_coverage" in body
    assert '_assert_stage_coverage("WeeklyPreflight", started)' in body
    assert '"stage_coverage"' in body


def test_the_spot_dispatcher_derives_which_of_its_two_stages_ran() -> None:
    """One Lambda, two SF states. Hardcoding either name would file the
    relaunch's verdict under the dispatch's."""
    body = (
        INFRA / "lambdas" / "weekly-freshness-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"RelaunchWeeklyFreshnessSpot" if force_on_demand else "DispatchWeeklyFreshnessSpot"' in body


@pytest.mark.parametrize(
    "lambda_dir", ["weekly-preflight", "weekly-freshness-spot-dispatcher"]
)
def test_the_lambda_assertion_is_import_guarded_and_loud(lambda_dir: str) -> None:
    """The nousergon-lib pin may predate the module; an inert assertion must
    stay distinguishable from a covered stage, and must not change the
    handler's outcome."""
    body = (INFRA / "lambdas" / lambda_dir / "index.py").read_text()
    assert "except ImportError as exc:" in body
    assert "UNMEASURED" in body
    assert "except ImportError:\n        pass" not in body


@pytest.mark.parametrize(
    "lambda_dir", ["weekly-preflight", "weekly-freshness-spot-dispatcher"]
)
def test_the_lambda_already_carries_the_nousergon_lib_dependency(
    lambda_dir: str,
) -> None:
    """No new bundle: the assertion must not be the reason a Lambda grows a
    dependency 38 hours before a scheduled run."""
    reqs = (INFRA / "lambdas" / lambda_dir / "requirements.txt").read_text()
    assert "nousergon-lib" in reqs


# ── The end-of-run design is GONE ────────────────────────────────────────────


def test_no_stage_coverage_sf_state_remains() -> None:
    """The ruled rescope removes the convergence-point state entirely — no new
    SF state, no new Lambda action, no topology change."""
    definition = json.loads(SF_DEFINITION.read_text())
    names = set(_states(definition))
    assert not [n for n in names if n.startswith("StageCoverage")]
    assert "StageCoverageAssert" not in SF_DEFINITION.read_text()


def test_the_weekly_preflight_lambda_has_no_coverage_action_dispatch() -> None:
    """The second `action` on alpha-engine-weekly-preflight is gone with it."""
    body = (INFRA / "lambdas" / "weekly-preflight" / "index.py").read_text()
    assert "sf_stage_coverage" not in body
    assert 'event.get("action")' not in body


def test_the_end_of_run_module_is_gone() -> None:
    assert not (REPO / "sf_stage_coverage.py").exists()
