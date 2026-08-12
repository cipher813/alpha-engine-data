"""The degraded terminal names what actually degraded (config-I6856).

`sf-pipeline-policy.md` §2.3's corollary requires the terminal failure
message to carry the actual error. The weekday and EOD terminals carried a
hardcoded ENUMERATION of the paths that could reach them instead, and the
enumeration drifted behind the paths:

    step_function_daily.json   5 states set $.degraded_summary, 2 were named
    step_function_eod.json     4 states set $.degraded_summary, 1 was named

Measured 2026-08-11. Preopen execution 021b85f7-4814-477e-9fe0-05c77f4296d6
terminated DEGRADED on `daily_scanner_fail_open` — the Scanner Lambda timed
out twice at 300s (config-I6855) — and the terminal asserted
"run_daemon_restart_failed and/or daily_data_spot_fail_open". Neither had
fired: RunDaemon succeeded and the data-spot path was never entered. The
message also directed the reader to a `PublishDataSpotFailureImmediate`
event "in execution history", a state absent on three of the five paths.

The truth was in `$.degraded_summary.reason` all along, and in the
completion marker the run wrote seconds earlier. Only the alert lied.

An enumeration that drifts is worse than boilerplate, because it reads as
specific: "one or more steps failed" sends nobody anywhere, while a wrong
state name sends the operator to the wrong place with confidence.

So these tests assert the DERIVATION, not the list. A test that merely
checked each `reason` literal appears in the terminal string would pass a
freshly-updated enumeration and re-drift the moment the next fail-open path
was added — reproducing the defect one commit later.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"

# Every definition with a DegradedRun terminal. step_function.json joined the
# set in alpha-engine-config-I6891, which brought the weekly pipeline to
# Option-A parity — before that it set degraded flags and terminated SUCCEEDED,
# so a run that did not really work counted as one that did.
_DEFS_WITH_DEGRADED_TERMINAL = (
    "step_function.json", "step_function_daily.json", "step_function_eod.json",
)

_ALL_DEFS = ("step_function.json", "step_function_daily.json", "step_function_eod.json")


def _load(name: str) -> dict:
    return json.loads((_INFRA / name).read_text(encoding="utf-8"))


def _degraded_setters(states: dict) -> dict[str, dict]:
    """States whose Parameters assert ``degraded: true``, by state name."""
    return {
        name: body["Parameters"]
        for name, body in states.items()
        if isinstance(body.get("Parameters"), dict) and body["Parameters"].get("degraded") is True
    }


def _degraded_terminals(states: dict) -> dict[str, dict]:
    return {
        name: body
        for name, body in states.items()
        if body.get("Type") == "Fail" and body.get("Error") == "DegradedRun"
    }


@pytest.mark.parametrize("definition", _DEFS_WITH_DEGRADED_TERMINAL)
def test_the_terminal_derives_its_cause_and_does_not_enumerate(definition: str) -> None:
    """`CausePath` off `$.degraded_summary`, never a static `Cause`.

    A static `Cause` cannot name a reason it was not written to know about,
    which is the whole defect.
    """
    states = _load(definition)["States"]
    for name, terminal in _degraded_terminals(states).items():
        assert "Cause" not in terminal, (
            f"{definition}::{name} carries a static Cause. It cannot name a "
            "degraded reason added after it was written — use CausePath."
        )
        cause_path = terminal.get("CausePath")
        assert cause_path, f"{definition}::{name} has neither Cause nor CausePath"
        assert "$.degraded_summary.reason" in cause_path, (
            f"{definition}::{name} must interpolate the actual reason, not describe it"
        )
        assert "States.JsonToString($.degraded_summary)" in cause_path, (
            f"{definition}::{name} must carry the full summary — the reason alone "
            "drops stage_error, which is where the underlying exception lives"
        )


@pytest.mark.parametrize("definition", _DEFS_WITH_DEGRADED_TERMINAL)
def test_no_degraded_reason_literal_is_hardcoded_into_the_terminal(definition: str) -> None:
    """The failure mode, stated directly.

    If any `reason` value appears verbatim in the terminal, an enumeration
    has grown back — and the next path added will not be in it.
    """
    doc = _load(definition)
    states = doc["States"]
    reasons = {p["reason"] for p in _degraded_setters(states).values() if "reason" in p}
    assert reasons, f"{definition} has no degraded setters — this test is not exercising anything"

    for name, terminal in _degraded_terminals(states).items():
        # CausePath only. The Comment legitimately quotes the 2026-08-11
        # wrong-cause string as the history of why this shape exists, and a
        # guard that cannot tell prose from behaviour would forbid recording
        # the defect it is guarding against.
        rendered = terminal["CausePath"]
        for reason in reasons:
            assert reason not in rendered, (
                f"{definition}::{name} hardcodes the reason {reason!r}. That is the "
                "enumeration this test exists to prevent: it is correct today and "
                "silently wrong the first time a fail-open path is added."
            )


@pytest.mark.parametrize("definition", _DEFS_WITH_DEGRADED_TERMINAL)
def test_every_degraded_setter_supplies_the_field_the_terminal_dereferences(definition: str) -> None:
    """`CausePath` dereferences `$.degraded_summary.reason` unguarded.

    That is a deliberate choice — a defensive fallback would let the
    guarantee lapse silently — so this test IS the guarantee. A setter
    landing without `reason` throws `States.Runtime` at the terminal, on a
    real degraded morning, which is the worst possible time to find out.
    """
    states = _load(definition)["States"]
    setters = _degraded_setters(states)
    assert setters, f"{definition} has no degraded setters"

    missing = sorted(name for name, params in setters.items() if not params.get("reason"))
    assert not missing, (
        f"{definition}: {missing} set degraded: true without a reason. The "
        "DegradedRun terminal interpolates $.degraded_summary.reason and will "
        "fail with States.Runtime on that path."
    )


@pytest.mark.parametrize("definition", _DEFS_WITH_DEGRADED_TERMINAL)
def test_every_degraded_setter_writes_the_summary_the_terminal_reads(definition: str) -> None:
    """`ResultPath: $.degraded_summary` — the field the Choice and terminal both read."""
    states = _load(definition)["States"]
    for name in _degraded_setters(states):
        assert states[name].get("ResultPath") == "$.degraded_summary", (
            f"{definition}::{name} sets degraded: true but does not write it to "
            "$.degraded_summary, so neither CheckDegradedOutcome nor the terminal sees it"
        )


# alpha-engine-config-I6891 CLOSED this gap: the weekly definition now carries
# CheckDegradedOutcome -> WriteCompletionMarkerDegraded -> DegradedRun, and
# every one of its eleven fail-open sites writes the $.degraded_summary the
# terminal derives its cause from. The exception map is deliberately kept as an
# EMPTY dict rather than deleted: a definition added later that degrades without
# a terminal must fail the assertion below rather than land in a file that no
# longer has an opinion about it.
_KNOWN_UNTERMINATED_DEGRADED: dict[str, frozenset[str]] = {}


@pytest.mark.parametrize("definition", _ALL_DEFS)
def test_every_definition_with_degraded_flags_has_a_derived_terminal(definition: str) -> None:
    """A definition that can degrade must have somewhere honest to end.

    §2.3: a degraded run must not read green to a machine consumer.
    """
    states = _load(definition)["States"]
    setters = frozenset(_degraded_setters(states))
    terminals = _degraded_terminals(states)
    if not setters:
        pytest.skip(f"{definition} has no degraded setters")

    known = _KNOWN_UNTERMINATED_DEGRADED.get(definition)
    if known is not None and not terminals:
        assert setters == known, (
            f"{definition}: the degraded-setter set moved while I6891 is open. "
            f"Expected {sorted(known)}, found {sorted(setters)}. A NEW state that "
            "degrades without a terminal is a new instance of the same defect, "
            "not covered by the existing exception."
        )
        pytest.xfail(f"{definition}: tracked by alpha-engine-config-I6891")

    assert terminals, (
        f"{definition} sets $.degraded_summary in {sorted(setters)} but has no "
        "DegradedRun terminal — a degraded run would end as a clean success"
    )
    assert definition not in _KNOWN_UNTERMINATED_DEGRADED, (
        f"{definition} now has a degraded terminal — remove it from "
        "_KNOWN_UNTERMINATED_DEGRADED and close alpha-engine-config-I6891"
    )


@pytest.mark.parametrize("definition", _DEFS_WITH_DEGRADED_TERMINAL)
def test_the_cause_expression_escapes_correctly_for_asl(definition: str) -> None:
    """No bare apostrophe inside the intrinsic's single-quoted literal.

    `States.Format('...')` delimits on `'`, so an unescaped apostrophe
    truncates the literal and the definition is rejected at deploy time —
    after review, in a place where the only signal is a red deploy job.
    Avoided by writing prose without apostrophes rather than by escaping,
    which is easier to verify than to get right.
    """
    states = _load(definition)["States"]
    for name, terminal in _degraded_terminals(states).items():
        expr = terminal["CausePath"]
        assert expr.startswith("States.Format('"), f"{definition}::{name}"
        literal = expr[len("States.Format('") :].rsplit("',", 1)[0]
        assert "'" not in literal, (
            f"{definition}::{name} has an apostrophe inside the States.Format literal — "
            "it will truncate the string and fail ASL validation"
        )


# ---------------------------------------------------------------------------
# The terminal can only name a cause that is still THERE when it runs.
# ---------------------------------------------------------------------------


def _paths_dereferenced(cause_path: str) -> set[str]:
    """Top-level state-data keys a `CausePath` expression reads."""
    import re

    return {m for m in re.findall(r"\$\.([A-Za-z_][A-Za-z0-9_]*)", cause_path)}


@pytest.mark.parametrize("definition", _ALL_DEFS)
def test_no_state_clobbers_the_summary_on_the_way_to_the_terminal(definition: str) -> None:
    """A `Task` with no `ResultPath` REPLACES the state input with its result.

    This is the defect the whole `CausePath` design rests on not having, and
    all three definitions shipped with it. `WriteCompletionMarkerDegraded` is
    an `s3:putObject` Task routing straight into `DegradedRun`; with the
    default `ResultPath` of `$`, the putObject result replaces the state data,
    so the terminal's `$.degraded_summary.reason` dereference cannot resolve
    and the execution fails with `States.Runtime` instead.

    `Error: DegradedRun` is a consumer contract — `sf-telegram-notifier`'s
    `_is_degraded_run()` matches that exact string to render DEGRADED rather
    than a crash-red FAILED — so the visible symptom is not a broken message.
    It is that a run which degraded honestly is reported as one that crashed,
    which is the same collapse §2.3 exists to prevent, arriving one state after
    everything §2.3 asked for was done correctly.

    Measured 2026-08-12 by static analysis of the three definitions;
    `states:ListExecutions` is denied to the laptop identity, so this is
    asserted structurally rather than confirmed against a live execution.
    """
    states = _load(definition)["States"]
    terminals = _degraded_terminals(states)
    if not terminals:
        pytest.skip(f"{definition} has no DegradedRun terminal")

    for terminal_name, terminal in terminals.items():
        needed = _paths_dereferenced(terminal["CausePath"])
        for name, body in states.items():
            if body.get("Next") != terminal_name:
                continue
            if body.get("Type") != "Task":
                continue  # a Pass/Choice with no ResultPath still carries its input through
            result_path = body.get("ResultPath")
            assert result_path is not None, (
                f"{definition}::{name} routes to {terminal_name} and declares no "
                f"ResultPath, so its result replaces the state input and the "
                f"terminal's dereference of {sorted(needed)} throws States.Runtime. "
                "Give it an explicit ResultPath naming a field the terminal does "
                "not read."
            )
            assert result_path != "$", f"{definition}::{name} ResultPath $ is the same clobber"
            assert result_path.lstrip("$.").split(".")[0] not in needed, (
                f"{definition}::{name} writes its result over {result_path}, which "
                f"the {terminal_name} terminal reads"
            )
