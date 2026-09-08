"""A rerun must not read its own auto-skipped output as last week's leftover.

``alpha-engine-config-I10194`` §3 / ``-I10173``. ``_STAGE_WINDOW_START`` in
``infrastructure/_spot_common.sh`` is captured as "now" at EVERY launcher
invocation, reruns included, and the stage-coverage assertion calls an
artifact older than it STALE — a leftover from a previous cycle. That is the
correct detector for a stage that always re-writes.

``weekly_collector.py``'s ``PhaseRegistry`` auto-skips any phase whose output
is already on S3 for the cycle's date. On a RERUN of a failed attempt the two
facts collide: the phases that already succeeded do not re-fetch, so this
cycle's own valid output predates this execution's window.

Measured 2026-09-08 on the 2026-09-04 cycle: the ``DataPhase1`` verdict
carried ``window_start: 2026-09-05T15:06:42Z`` while ``macro.json``,
``short_interest.json``, ``macro_history.parquet``,
``macro_release_calendar.parquet``, ``archive/fundamentals/2026-09-04.json``,
``universe_classification/latest.json`` and ``valuation_medians/latest.json``
were written 2026-09-05T09:48-10:30Z by the SAME cycle's earlier attempt, and
the SSM log shows every one of those collectors logging ``PHASE_SKIP ...
reason=auto_skip_marker_ok``.

**The scope limit is what this file exists to enforce.** Making every stage's
window track the cycle would delete the leftover detector for stages with no
auto-skip, turning a stage that STOPPED WRITING on a rerun into a false
COVERED. So the narrowing is a per-launcher declaration
(``_STAGE_WINDOW_TRACKS_CYCLE=1``) — and it is a CHECKED claim rather than a
hand-kept list of stage names: the truth is DERIVED here from
``weekly_collector.py``'s own source, by walking the dispatch function for
each collector mode a launcher runs and asking whether any ``_phase_collect``
call in it omits ``supports_auto_skip=False``. Flip a phase's auto-skip in the
collector and this file names the launcher that must change with it.
"""

from __future__ import annotations

import ast
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
INFRA = REPO / "infrastructure"
COMMON = INFRA / "_spot_common.sh"
#: The ONE definition of the window rule, sourced by BOTH _spot_common.sh and
#: the spot_data_weekly.sh monolith (which does not source _spot_common.sh).
WINDOW = INFRA / "_stage_window.sh"
COLLECTOR = REPO / "weekly_collector.py"

#: The declaration each launcher makes about its OWN workload.
FLAG = "_STAGE_WINDOW_TRACKS_CYCLE"

#: Collector CLI mode -> the ``run_weekly`` dispatch function it reaches.
#: Derived below from ``run_weekly``'s own body rather than hand-asserted, so a
#: renamed dispatch function fails here instead of silently dropping a mode.
_MODE_FLAGS = {
    "--morning-enrich": "morning_enrich",
    "--phase 1": "phase1",
    "--phase 2": "phase2",
}


def _collector_tree() -> ast.Module:
    return ast.parse(COLLECTOR.read_text())


def _dispatch_functions() -> dict[str, str]:
    """``{mode_key: dispatch function name}``, read from ``run_weekly``."""
    tree = _collector_tree()
    run_weekly = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "run_weekly"
    )
    source = ast.unparse(run_weekly)
    found: dict[str, str] = {}
    match = re.search(r'getattr\(args, [\'"]morning_enrich[\'"], False\):\s*\n?\s*return (\w+)\(', source)
    if match:
        found["morning_enrich"] = match.group(1)
    for phase in ("1", "2"):
        match = re.search(rf"phase == {phase}:\s*\n?\s*return (\w+)\(", source)
        if match:
            found[f"phase{phase}"] = match.group(1)
    return found


def _auto_skipping_phases(function_name: str) -> list[str]:
    """Phase names in ``function_name`` that CAN auto-skip.

    ``_phase_collect``'s ``supports_auto_skip`` defaults to ``True``; a call
    omitting the keyword is therefore auto-skip capable. ``_maybe_phase`` pins
    it ``False`` at its own definition, so those never count.
    """
    tree = _collector_tree()
    node = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == function_name
    )
    capable: list[str] = []
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        name = getattr(call.func, "id", None) or getattr(call.func, "attr", None)
        if name != "_phase_collect":
            continue
        if any(kw.arg == "supports_auto_skip" for kw in call.keywords):
            continue  # explicitly pinned — the collector says it always runs
        phase = (
            call.args[1].value
            if len(call.args) > 1 and isinstance(call.args[1], ast.Constant)
            else "?"
        )
        capable.append(str(phase))
    return capable


def _launchers() -> dict[Path, str]:
    return {path: path.read_text() for path in sorted(INFRA.glob("spot_*.sh"))}


def _collector_modes(text: str) -> set[str]:
    """Collector modes a launcher runs for real (dry-run/preflight excluded).

    A ``--dry-run`` or ``--preflight-only`` invocation writes nothing by
    design, so it can neither auto-skip nor be asserted against.
    """
    modes: set[str] = set()
    for line in text.splitlines():
        if "weekly_collector.py" not in line:
            continue
        if "--dry-run" in line or "--preflight-only" in line:
            continue
        for flag, key in _MODE_FLAGS.items():
            if flag in line:
                modes.add(key)
    return modes


def _asserting_launchers() -> dict[Path, str]:
    return {
        path: text
        for path, text in _launchers().items()
        if "krepis.stage_coverage assert" in text
    }


# ── The resolver itself ──────────────────────────────────────────────────────


def test_the_shared_resolver_exists_and_is_the_only_window_source() -> None:
    text = WINDOW.read_text()
    assert "resolve_stage_window_start()" in text, (
        "_spot_common.sh lost the shared window resolver — every launcher "
        "would silently return to recapturing 'now' on a rerun (I10194 §3)"
    )
    assert f'{FLAG}="${{{FLAG}:-}}"' in text, (
        f"{FLAG} must be declared EMPTY in _spot_common.sh: a non-empty "
        "default here makes every launcher's own assignment a silent no-op "
        "(the alpha-engine-config-I6922 swallow this file already carries)"
    )


def _resolver_body() -> str:
    """The text of ``resolve_stage_window_start`` alone, brace-delimited."""
    text = WINDOW.read_text()
    head = text.index("resolve_stage_window_start() {")
    tail = text.index("\n}\n", head)
    return text[head:tail]


def test_the_resolver_is_inert_for_a_stage_that_has_not_declared_the_flag() -> None:
    """The default path must be byte-identical to today's behaviour — the
    leftover-from-a-previous-cycle detector is correct for every stage that
    does not auto-skip, and a blanket change would delete it."""
    assert f'"${{{FLAG}:-}}" != "1"' in _resolver_body(), (
        "the resolver does not short-circuit to $_STAGE_WINDOW_START for an "
        "undeclared stage — this is the blanket change I10194 §3 forbids"
    )


def test_every_resolver_failure_path_degrades_toward_the_alarming_side() -> None:
    """A false STALE is a finding a human reads; a false COVERED is silence.
    Exactly ONE path may return a window read from S3; every other must return
    this execution's own captured start."""
    body = _resolver_body()
    # End-of-line anchored: `printf '%s' "$body" | python` further down is a
    # PIPE into the parser, not a value this function returns.
    returns = re.findall(r"printf '%s' \"\$(\w+)\"\s*$", body, re.MULTILINE)
    assert returns, "the resolver prints nothing"
    assert returns.count("prior") == 1, returns
    assert set(returns) == {"_STAGE_WINDOW_START", "prior"}, returns


def test_the_resolver_can_never_fail_the_stage_it_observes() -> None:
    """It runs inside a launcher under `set -e`, in observe mode. A non-zero
    return would make the observer able to kill what it observes — a new
    failure mode bolted onto the one it reports."""
    body = _resolver_body()
    assert "return 1" not in body and "return 2" not in body, body
    assert "exit " not in body


def test_the_resolver_never_swallows_a_read_failure_silently() -> None:
    body = _resolver_body()
    assert "could not read" in body and ">&2" in body, (
        "an unreadable prior verdict must be LOUD on stderr — a silent "
        "fallback is indistinguishable from a stage with no prior attempt"
    )
    assert "|| true" not in body


# ── The resolver actually EXECUTED, against a stubbed S3 ─────────────────────


def _run_resolver(
    tmp_path: Path,
    *,
    declared: bool,
    stage: str = "DataPhase1",
    run_date: str = "2026-09-04",
    aws_stdout: str = "",
    aws_stderr: str = "",
    aws_rc: int = 0,
) -> tuple[str, str]:
    """Source ``_spot_common.sh`` with a stub ``aws`` and run the resolver."""
    stub = tmp_path / "bin"
    stub.mkdir()
    aws = stub / "aws"
    aws.write_text(
        "#!/usr/bin/env bash\n"
        f"printf '%s' {shlex.quote(aws_stdout)}\n"
        f"printf '%s' {shlex.quote(aws_stderr)} >&2\n"
        f"exit {aws_rc}\n"
    )
    aws.chmod(0o755)

    script = f"""
set -euo pipefail
export PATH={shlex.quote(str(stub))}:$PATH
export LIB_PYTHON={shlex.quote(sys.executable)}
export _STAGE_WINDOW_START=2026-09-05T15:06:42Z
_SPOT_NAME=x
_SSM_SLUG=x
_PROCESS_NAME=x
MAX_RUNTIME_SECONDS=1
source {shlex.quote(str(COMMON))}
{FLAG}={'1' if declared else '0'}
resolve_stage_window_start {stage} {run_date}
"""
    proc = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout, proc.stderr


def test_an_undeclared_stage_gets_this_executions_window(tmp_path: Path) -> None:
    out, _ = _run_resolver(tmp_path, declared=False, aws_stdout='{"window_start": "OLD"}')
    assert out == "2026-09-05T15:06:42Z"


def test_a_declared_stage_reuses_the_cycles_first_attempt_window(tmp_path: Path) -> None:
    """The headline fix: a rerun asserts against the window of the attempt
    that actually wrote this cycle's output, not against its own start."""
    out, err = _run_resolver(
        tmp_path,
        declared=True,
        aws_stdout='{"stage": "DataPhase1", "window_start": "2026-09-05T09:40:00Z"}',
    )
    assert out == "2026-09-05T09:40:00Z"
    assert "reusing this CYCLE's first-attempt window" in err


def test_a_first_attempt_with_no_prior_verdict_keeps_its_own_window(tmp_path: Path) -> None:
    out, err = _run_resolver(
        tmp_path,
        declared=True,
        aws_stderr="An error occurred (404) when calling the HeadObject operation: Not Found",
        aws_rc=1,
    )
    assert out == "2026-09-05T15:06:42Z"
    assert "no prior verdict" in err


def test_a_real_read_failure_is_loud_and_degrades_to_the_alarming_side(tmp_path: Path) -> None:
    """AccessDenied is NOT 'no prior attempt'. Reporting it as one would make
    a permissions defect look like a clean first run — the detector-reports-
    its-own-harness-fault class this fleet keeps re-measuring."""
    out, err = _run_resolver(
        tmp_path,
        declared=True,
        aws_stderr="An error occurred (AccessDenied) when calling the GetObject operation",
        aws_rc=1,
    )
    assert out == "2026-09-05T15:06:42Z"
    assert "could not read" in err
    assert "AccessDenied" in err
    assert "no prior verdict" not in err


def test_an_unparseable_prior_verdict_degrades_loudly(tmp_path: Path) -> None:
    out, err = _run_resolver(tmp_path, declared=True, aws_stdout="{not json")
    assert out == "2026-09-05T15:06:42Z"
    assert "no usable window_start" in err


def test_a_prior_verdict_without_a_window_degrades_loudly(tmp_path: Path) -> None:
    """`window_start` is `None` in every UNMEASURED verdict krepis writes —
    reusing it would send an empty --window-start and silently disable the
    staleness half of the assertion."""
    out, err = _run_resolver(
        tmp_path, declared=True, aws_stdout='{"stage": "DataPhase1", "window_start": null}'
    )
    assert out == "2026-09-05T15:06:42Z"
    assert "no usable window_start" in err


def test_a_declared_stage_without_a_run_date_keeps_its_own_window(tmp_path: Path) -> None:
    out, err = _run_resolver(tmp_path, declared=True, run_date='""')
    assert out == "2026-09-05T15:06:42Z"
    assert "no run_date" in err


# ── The declaration is a CHECKED claim, not a hand-kept list ─────────────────


def test_the_collector_dispatch_functions_are_still_resolvable() -> None:
    """If this fails, every derivation below is vacuous — a renamed dispatch
    function must break loudly rather than quietly stop deriving anything."""
    dispatch = _dispatch_functions()
    assert set(dispatch) == {"morning_enrich", "phase1", "phase2"}, dispatch


@pytest.mark.parametrize("path", sorted(_asserting_launchers()))
def test_the_flag_matches_the_collector_source(path: Path) -> None:
    """The biconditional: a launcher declares ``_STAGE_WINDOW_TRACKS_CYCLE=1``
    if and only if some collector mode it actually runs has a phase that can
    auto-skip."""
    text = path.read_text()
    dispatch = _dispatch_functions()
    capable: dict[str, list[str]] = {}
    for mode in _collector_modes(text):
        phases = _auto_skipping_phases(dispatch[mode])
        if phases:
            capable[mode] = phases

    declared = re.search(rf"^{FLAG}=1$", text, re.MULTILINE) is not None
    if capable:
        assert declared, (
            f"{path.name} runs collector mode(s) with auto-skipping phases "
            f"{capable} but does not declare `{FLAG}=1` — a rerun of this "
            "stage will read its own auto-skip-preserved output as a "
            "previous cycle's leftover (alpha-engine-config-I10194 §3)"
        )
    else:
        assert not declared, (
            f"{path.name} declares `{FLAG}=1` but no collector mode it runs "
            "has an auto-skipping phase — reusing an earlier attempt's window "
            "here would turn a stage that STOPPED WRITING on a rerun into a "
            "false COVERED, which is the blanket change I10194 §3 forbids"
        )


def test_morning_enrich_is_deliberately_not_declared() -> None:
    """Named explicitly because it is the near-miss: it runs
    ``weekly_collector.py`` like DataPhase1 does, but every one of its phases
    goes through ``_maybe_phase``, which pins ``supports_auto_skip=False``. A
    fix scoped by "runs the collector" rather than "auto-skips" would have
    swept it in and deleted a working detector."""
    assert _auto_skipping_phases(_dispatch_functions()["morning_enrich"]) == []
    assert FLAG not in (INFRA / "spot_morning_enrich.sh").read_text()


def test_data_phase1_is_declared_and_uses_the_resolver() -> None:
    text = (INFRA / "spot_data_phase1.sh").read_text()
    assert re.search(rf"^{FLAG}=1$", text, re.MULTILINE)
    assert "resolve_stage_window_start DataPhase1" in text
    assert '--stage DataPhase1 --window-start "$_STAGE_WINDOW_START"' not in text, (
        "DataPhase1 still asserts against the RAW execution window"
    )


def test_data_phase2_is_declared_and_uses_the_resolver() -> None:
    """The CLASS, not the instance (`engagement-protocol` §5): I10194 §3
    measured the defect on DataPhase1, and `_run_phase2`'s `alternative`
    phase carries the same auto-skip property."""
    text = (INFRA / "spot_data_weekly.sh").read_text()
    assert re.search(rf"^{FLAG}=1$", text, re.MULTILINE)
    assert "resolve_stage_window_start DataPhase2" in text
    assert '--stage DataPhase2 --window-start "$_STAGE_WINDOW_START"' not in text


@pytest.mark.parametrize("path", sorted(_launchers()))
def test_no_launcher_uses_the_swallowable_assignment_form(path: Path) -> None:
    """``_STAGE_WINDOW_TRACKS_CYCLE="${_STAGE_WINDOW_TRACKS_CYCLE:-1}"`` in a
    launcher is the alpha-engine-config-I6922 no-op: ``_spot_common.sh``
    declares the parameter before the launcher runs, so ``:-`` would expand to
    nothing the moment that default stops being empty."""
    text = path.read_text()
    assert f'{FLAG}="${{{FLAG}:-' not in text, (
        f"{path.name}: use a BARE `{FLAG}=1`, not the `${{...:-}}` form"
    )


def test_the_window_rule_has_exactly_one_definition() -> None:
    """`spot_data_weekly.sh` does not source `_spot_common.sh` (it carries its
    own run_ssm/launch pair), so the naive fix was to paste the resolver into
    it — the fork `policy-shared-code` forbids, and the class this repo already
    paid for once as `alpha-engine-config-I6922`. Both sourcers reach the same
    file instead."""
    definers = [
        path.name
        for path, text in _launchers().items()
        if "resolve_stage_window_start() {" in text
    ]
    assert definers == [], f"a launcher redefines the resolver: {definers}"
    assert "resolve_stage_window_start() {" in WINDOW.read_text()
    assert "resolve_stage_window_start() {" not in COMMON.read_text()
    for sourcer in (COMMON, INFRA / "spot_data_weekly.sh"):
        assert "_stage_window.sh" in sourcer.read_text(), (
            f"{sourcer.name} calls the resolver but does not source its "
            "definition — `command not found` at runtime (I7338 class)"
        )
