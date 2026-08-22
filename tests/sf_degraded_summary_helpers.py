"""Shared assertions for the weekly SF's degraded-summary chain (I6891).

Bringing `step_function.json` to Option-A parity put one extra `Pass` on every
fail-open path: the site's existing state still sets its family boolean (which
is what `CheckGateDegradedNotify` selects the completion notifier off), and a
sibling `Set<site>Summary` writes the `$.degraded_summary` that
`CheckDegradedOutcome` and the `DegradedRun` terminal read. A `Pass` has
exactly one `ResultPath`, and the two are different axes — family boolean picks
the email and must ACCUMULATE across families, summary carries the terminal
cause and is last-write-wins — so they cannot share a state.

Roughly two dozen tests across a dozen files pinned `setter["Next"] ==
"Publish<site>Degraded"`. Rewriting each by hand as three lines would have put
the same three lines in a dozen places, where the next topology change has to
find them all. This module is the one place they live, and it asserts MORE than
the line it replaced: the continuation is unchanged AND the summary is written
where the terminal reads it AND it carries the `reason` the terminal
dereferences unguarded.
"""
from __future__ import annotations


def summary_state_name(setter: str) -> str:
    """The sibling summary Pass for a degraded-flag setter.

    `Set*` setters keep their prefix (`SetMutexAcquireDegradedFlagSummary`);
    everything else gains one (`SetLibPinGateDegradedSummary`).
    """
    return f"{setter}Summary" if setter.startswith("Set") else f"Set{setter}Summary"


def assert_degraded_continuation(states: dict, setter: str, expected_next: str) -> str:
    """`setter` -> its summary Pass -> `expected_next`, unchanged.

    Returns the summary state name so a caller can make further assertions
    about it.
    """
    summary = summary_state_name(setter)
    assert states[setter]["Next"] == summary, (
        f"{setter} must route through {summary} (alpha-engine-config-I6891) — "
        f"found {states[setter].get('Next')!r}"
    )
    body = states[summary]
    assert body["Type"] == "Pass"
    assert body["ResultPath"] == "$.degraded_summary", (
        f"{summary} must write $.degraded_summary — neither CheckDegradedOutcome "
        "nor the DegradedRun terminal reads anywhere else"
    )
    params = body["Parameters"]
    assert params["degraded"] is True
    assert params.get("reason"), (
        f"{summary} sets degraded: true with no reason; the terminal "
        "dereferences $.degraded_summary.reason unguarded and would throw "
        "States.Runtime on this path"
    )
    assert "stage_error.$" not in params, (
        f"{summary} must not reference a per-stage error path: this site is "
        "reachable from BOTH a Catch (which writes it) and a Choice (which does "
        "not), so the dereference throws States.Runtime on the Choice path"
    )
    assert body["Next"] == expected_next, (
        f"{summary} must continue to {expected_next} — the fail-open "
        f"continuation is unchanged by I6891, only one state longer; found "
        f"{body.get('Next')!r}"
    )
    return summary


#: Every real-completion notifier's terminal chain. Before I6891 each of these
#: pointed straight at `WriteCompletionMarker`, whose body hardcodes
#: `"status":"SUCCEEDED"` and whose `End: true` made the execution SUCCEEDED —
#: so a run that had already announced its own degradation in the completion
#: email still told every machine consumer it worked.
COMPLETION_CHOKEPOINT = "CheckDegradedOutcome"


def assert_completion_notifier_chain(states: dict, notifier: str) -> None:
    """`notifier` -> CheckDegradedOutcome -> the honest marker for its outcome.

    Asserts the property the old `Next == "WriteCompletionMarker"` line stood
    for — a real-completion notifier converges on the marker rather than ending
    the execution itself — and adds the half that line could not express: WHICH
    marker, decided by `$.degraded_summary` rather than by which notifier ran.
    """
    assert "End" not in states[notifier], (
        f"{notifier} must route through {COMPLETION_CHOKEPOINT}, not End directly"
    )
    assert states[notifier]["Next"] == COMPLETION_CHOKEPOINT, (
        f"{notifier} must converge on {COMPLETION_CHOKEPOINT} "
        f"(alpha-engine-config-I6891) — found {states[notifier].get('Next')!r}"
    )
    choice = states[COMPLETION_CHOKEPOINT]
    assert choice["Type"] == "Choice"
    assert choice["Default"] == "WriteCompletionMarker", (
        "a run that never degraded takes the Default edge — absent flag means "
        "never degraded, which is the semantically exact reading"
    )
    (rule,) = choice["Choices"]
    guards = rule["And"]
    assert {g.get("IsPresent") for g in guards} == {True, None}, (
        "the $.degraded_summary.degraded dereference must be IsPresent-guarded: "
        "a fully green run never sets it, and an unguarded read throws "
        "States.Runtime at the last state of a successful weekly run"
    )
    assert all(g["Variable"] == "$.degraded_summary.degraded" for g in guards)
    assert rule["Next"] == "WriteCompletionMarkerDegraded"

    degraded_marker = states["WriteCompletionMarkerDegraded"]
    assert '"status":"DEGRADED"' in degraded_marker["Parameters"]["Body.$"]
    assert "States.JsonToString($.degraded_summary)" in degraded_marker["Parameters"]["Body.$"], (
        "the degraded marker must carry the summary — a marker reader that can "
        "see DEGRADED but not WHY has to go back to execution history"
    )
    assert degraded_marker["Next"] == "DegradedRun"
    assert states["DegradedRun"]["Type"] == "Fail"
    assert states["DegradedRun"]["Error"] == "DegradedRun", (
        "the error string is a consumer contract: sf-telegram-notifier keys its "
        "orange DEGRADED rendering off exactly this value, and any other string "
        "renders a degraded weekly run as an ordinary crash-red FAILED"
    )
    assert "Cause" not in states["DegradedRun"]

    clean_marker = states["WriteCompletionMarker"]
    assert '"status":"SUCCEEDED"' in clean_marker["Parameters"]["Body.$"]
    # alpha-engine-config-I8214: the clean marker no longer ENDS the execution;
    # it hands off to the observe-only stage-coverage sweep, which augments the
    # marker it just wrote with the cycle's real shape. The property the old
    # `End is True` line stood for — a clean run terminates SUCCEEDED, never
    # Fail — is asserted below over the whole tail, which is strictly more than
    # the line it replaced: an observe-only tail that could fail a completed run
    # would be the exact defect I8214's own comment forbids.
    assert "End" not in clean_marker
    assert clean_marker["Next"] == "WeeklyCoverageSweep"
    assert_observe_only_tail(states, "WeeklyCoverageSweep")


def assert_observe_only_tail(states: dict, entry: str) -> None:
    """Every path out of `entry` reaches a Succeed, and none reaches a Fail.

    An observe-only tail sits downstream of a pipeline's real success terminal.
    It may page, it may write, it may find nothing — but it may not turn a
    completed run into a failed one (sf-pipeline-policy §2.1 blast radius). A
    reachability walk rather than a hand-listed set of terminals, so a state
    added to the tail later is covered by this assertion existing.
    """
    seen: set[str] = set()
    stack = [entry]
    terminals: set[str] = set()
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        st = states[name]
        assert st["Type"] != "Fail", (
            f"{name} is reachable from the observe-only tail at {entry} and is a "
            "Fail state — a tail downstream of the success terminal must never "
            "fail a run that already completed"
        )
        nexts = []
        if "Next" in st:
            nexts.append(st["Next"])
        for choice in st.get("Choices", []) or []:
            nexts.append(choice["Next"])
        if st.get("Default"):
            nexts.append(st["Default"])
        for catch in st.get("Catch", []) or []:
            nexts.append(catch["Next"])
        if not nexts:
            assert st["Type"] == "Succeed" or st.get("End") is True, (
                f"{name} terminates the tail without being a Succeed"
            )
            terminals.add(name)
        stack.extend(nexts)
    assert terminals, f"no terminal reachable from {entry}"
