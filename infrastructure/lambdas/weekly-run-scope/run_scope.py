"""Derive the weekly pipeline's own run scope from its definition + execution.

Tracked as ``alpha-engine-config-I7620``.

WHY THIS EXISTS
---------------
Which stages the weekly pipeline actually ran has never been written down
anywhere a consumer could read it. The Director grades the week's numbers and
does not know which producers were switched off, so a stage deliberately
disabled by an operator flag is indistinguishable, on the rendered page, from a
stage that ran and failed. On 2026-08-14 that cost the whole card its
correctness attestation: ``skip_parity: true`` had been set on the EventBridge
target since 2026-08-13 by a recorded ruling, and the Director reported the
resulting absence as ``contamination: UNKNOWN — the producer never ran this
cycle``, then withheld its acting authority on the strength of it.

The fix that does NOT work is a hand-maintained registry of enabled stages.
This fleet already carries thirteen registries; each exists because its fact had
no machine-readable home. This fact has one — two, in fact, and they are already
authoritative:

* the **definition** says which stages exist and which flag gates each
  (``aws stepfunctions describe-state-machine``);
* the **execution record** says which branch every gate actually took
  (``aws stepfunctions get-execution-history``).

A YAML listing the same thing would be a copy that drifts the first time
somebody adds a stage and forgets. So the registry is DERIVED, every run, from
the two sources that cannot disagree with reality because they ARE reality.

WHAT IT PRODUCES
----------------
One row per gated stage, with a disposition drawn from a closed vocabulary of
four. Three is the number an operator thinks in; the fourth exists because a run
that dies at stage 3 leaves stages 4..40 in a state that is neither "disabled"
nor "ran and failed", and collapsing it into either is a lie the surface would
then repeat every week:

``DISABLED``
    The stage's own ``CheckSkipX`` Choice was entered and took the skip branch,
    or an ancestor gate did — ``disabled_by`` names the flag responsible. NOT
    graded. This is the state an operator creates on purpose.

``ENABLED_COMPLETED``
    Dispatched, entered, exited cleanly. Graded.

``ENABLED_FAILED``
    Dispatched and entered, but never exited cleanly. **Graded, and graded as a
    failure.** This row is the reason the whole module is written against
    dispatch rather than against success: if grading followed what succeeded, a
    stage could silently disable itself by crashing, which is precisely the
    class of defect this fleet keeps paying for.

``NOT_REACHED``
    The gate was never entered — the execution ended, or failed, upstream of it.
    Never read as disabled, never read as passing.

Nothing here calls AWS; ``index.py`` fetches the definition and the history and
hands them in. That split is deliberate: it lets the derivation be tested
against a REAL definition and a REAL execution history as fixtures, which is how
the rules below were established rather than assumed.
"""
from __future__ import annotations

from typing import Any, Iterable

SCHEMA = "run_scope-1.0.0"
SCHEMA_VERSION = 1

DISABLED = "DISABLED"
ENABLED_COMPLETED = "ENABLED_COMPLETED"
ENABLED_FAILED = "ENABLED_FAILED"
NOT_REACHED = "NOT_REACHED"

#: The closed vocabulary. A disposition outside it is a bug, not a new state.
DISPOSITIONS = frozenset(
    {DISABLED, ENABLED_COMPLETED, ENABLED_FAILED, NOT_REACHED}
)

#: Dispositions the Director grades. `DISABLED` and `NOT_REACHED` are excluded
#: for DIFFERENT reasons and must never be merged: the first is a decision, the
#: second is an absence of evidence.
GRADED_DISPOSITIONS = frozenset({ENABLED_COMPLETED, ENABLED_FAILED})

_GATE_PREFIX = "CheckSkip"


# ---------------------------------------------------------------------------
# 1. The definition half — what stages exist, and what gates each
# ---------------------------------------------------------------------------


def flatten_states(states: dict) -> dict[str, dict]:
    """Every state in the machine, including inside Parallel and Map.

    Nested states are addressed by bare name because Step Functions requires
    names to be unique across the whole definition, and the execution history
    reports them the same way.
    """
    out: dict[str, dict] = {}

    def walk(block: dict) -> None:
        for name, body in block.items():
            out[name] = body
            for branch in body.get("Branches", []) or []:
                walk(branch.get("States", {}))
            iterator = body.get("Iterator") or body.get("ItemProcessor")
            if isinstance(iterator, dict):
                walk(iterator.get("States", {}))

    walk(states)
    return out


def _skip_flag(choice: dict) -> str | None:
    """The ``skip_*`` input flag a CheckSkip Choice tests.

    The condition is written as ``And[{IsPresent}, {BooleanEquals: true}]`` on
    every one of these gates, so the flag is found by scanning for the first
    ``$.skip_*`` Variable at any nesting depth rather than by assuming a shape.
    """
    def scan(node: Any) -> str | None:
        if isinstance(node, dict):
            var = node.get("Variable")
            if isinstance(var, str) and var.startswith("$.skip_"):
                return var[len("$."):]
            for value in node.values():
                found = scan(value)
                if found:
                    return found
        elif isinstance(node, list):
            for item in node:
                found = scan(item)
                if found:
                    return found
        return None

    return scan(choice.get("Choices", []))


def derive_gates(definition: dict) -> dict[str, dict]:
    """Map every ``CheckSkipX`` Choice to the flag and the branches it selects.

    Returned per gate: the flag name, the state entered when the stage RUNS
    (``on_enabled``, the Choice's ``Default``) and the state entered when it is
    SKIPPED (``on_disabled``, the skip branch's ``Next``). Both come straight
    off the definition, so a gate renamed or re-pointed upstream is picked up
    without editing anything here.
    """
    states = flatten_states(definition.get("States", {}))
    gates: dict[str, dict] = {}
    for name, body in states.items():
        if not name.startswith(_GATE_PREFIX) or body.get("Type") != "Choice":
            continue
        flag = _skip_flag(body)
        choices = body.get("Choices") or []
        if not flag or not choices:
            # A CheckSkip-named Choice that tests something other than a skip
            # flag is not a scope gate. Recorded as unknown rather than guessed
            # at — a wrong gate mapping would mislabel a whole branch.
            continue
        gates[name] = {
            "flag": flag,
            "on_enabled": body.get("Default"),
            # EVERY choice target, not the first. `CheckSkipPredictorTraining`
            # declares two branches on the same flag (a skip marker and a
            # weights-freshness assertion) and reading only `Choices[0]` made a
            # real skip look like neither branch — silently NOT_REACHED.
            "on_disabled": sorted(
                {c.get("Next") for c in choices if isinstance(c.get("Next"), str)}
            ),
            "stage": name[len(_GATE_PREFIX):],
        }
    return gates


#: State types that are the RUN's terminal, never a stage's. They are shared by
#: every branch, so counting them inside a gate's governed set makes one
#: execution-level failure look like a failure of every stage that could reach
#: it — measured: the 2026-08-15 run's `DegradedRun` (Type: Fail) marked four
#: unrelated stages ENABLED_FAILED, one of which had already completed.
_TERMINAL_TYPES = frozenset({"Succeed", "Fail"})


def _successors(state: dict) -> Iterable[str]:
    """Forward edges only — `Catch` is deliberately NOT followed.

    Error handlers in this machine are shared infrastructure reached from many
    branches (`NormalizeFailureContext`, `HandleFailure`, `DegradedRun`).
    Following them merges every gate's governed set into one, which then blames
    whichever gate happens to be checked first for everything downstream.
    """
    for key in ("Next", "Default"):
        value = state.get(key)
        if isinstance(value, str):
            yield value
    for choice in state.get("Choices", []) or []:
        nxt = choice.get("Next")
        if isinstance(nxt, str):
            yield nxt


#: How far to look past a routing state for the real work state behind a gate.
#: The longest such run in this machine is CheckSkipEvalJudge -> ComputeEvalCadence
#: -> CheckMonthlyCadence -> EvalJudgeSubmit* (3). The bound exists so a
#: definition change that turns a gate's target into a long routing chain
#: degrades to "no work state found" rather than wandering the machine.
_WORK_LOOKAHEAD = 6

#: State types that do real work. A gate whose target is a Choice or a Pass is
#: routing to a GROUP, and the group's work state is a hop or two further on.
_WORK_TYPES = frozenset({"Task", "Parallel", "Map"})


def work_entry(definition: dict, entry: str | None) -> tuple[str | None, list[str]]:
    """The first real work state behind a gate's enabled branch.

    A bounded forward walk over `Default` / `Next` / first-choice edges — NOT a
    reachability or dominance analysis over the whole machine. Both of those
    were tried against the live definition and both are wrong here:

    * reachability, because the machine has retry loops (`MorningEnrichReissue`
      -> `MorningEnrich`, the poll waits), so "everything reachable from the
      evaluator branch" measured 132 states including states that run BEFORE it;
    * dominance, because `RouteAfterBootstrapSuccess` is a shared spot-relaunch
      hub with an edge back into the middle of several stage branches, so almost
      nothing in this machine is strictly dominated by its own gate.

    A short local walk needs neither property to be true. It answers only "which
    state does this gate switch on", which is all the disposition needs.
    """
    states = flatten_states(definition.get("States", {}))
    name = entry
    passed: list[str] = []
    for _ in range(_WORK_LOOKAHEAD):
        if not name or name not in states:
            return None, passed
        body = states[name]
        if body.get("Type") in _WORK_TYPES:
            return name, passed
        if name.startswith(_GATE_PREFIX) and body.get("Type") == "Choice":
            # A gate nested behind this one. Recorded so an outer gate that says
            # "run" over an inner gate that says "skip" is reported as DISABLED
            # by the inner flag, rather than as an outer stage that mysteriously
            # never started.
            passed.append(name)
        nxt = body.get("Default") or body.get("Next")
        if not nxt:
            choices = body.get("Choices") or []
            nxt = choices[0].get("Next") if choices else None
        name = nxt
    return None, passed


# ---------------------------------------------------------------------------
# 2. The execution half — what the run actually did
# ---------------------------------------------------------------------------

_ENTERED_SUFFIX = "StateEntered"
_EXITED_SUFFIX = "StateExited"


def entered_sequence(history: list[dict]) -> list[str]:
    """State names in the order the execution entered them."""
    seq: list[str] = []
    for event in history:
        if not event.get("type", "").endswith(_ENTERED_SUFFIX):
            continue
        details = event.get("stateEnteredEventDetails") or {}
        name = details.get("name")
        if name:
            seq.append(name)
    return seq


def exited_names(history: list[dict]) -> set[str]:
    out: set[str] = set()
    for event in history:
        if not event.get("type", "").endswith(_EXITED_SUFFIX):
            continue
        details = event.get("stateExitedEventDetails") or {}
        name = details.get("name")
        if name:
            out.add(name)
    return out


def gate_decisions(gates: dict[str, dict], history: list[dict]) -> dict[str, str]:
    """For each gate the run entered, whether it enabled or disabled its stage.

    Resolved by following the history's own ``previousEventId`` chain: the state
    entered immediately after a Choice is the event whose ``previousEventId`` is
    that Choice's exit event id.

    **Not by adjacency in the entered-order sequence.** Six of this machine's
    gates live inside `ResearchPredictorParallel`, and a Parallel interleaves
    events from concurrently-running branches — so "the next state name in the
    list" belongs to whichever branch happened to emit next. Measured against
    the real 2026-08-16 execution: adjacency read `CheckSkipScanner` as followed
    by `CheckSkipPredictorTraining` (a different branch entirely), which matched
    neither declared target and silently degraded six stages to NOT_REACHED. The
    event chain resolves the same gate to `CheckSkipRegimeSubstrate` — its skip
    branch — correctly.

    A gate the run never entered is absent from the result. That is a third fact,
    distinct from either branch, and it is kept distinct.
    """
    by_previous: dict[Any, list[dict]] = {}
    for event in history:
        by_previous.setdefault(event.get("previousEventId"), []).append(event)

    decisions: dict[str, str] = {}
    for event in history:
        if not event.get("type", "").endswith(_EXITED_SUFFIX):
            continue
        name = (event.get("stateExitedEventDetails") or {}).get("name")
        gate = gates.get(name)
        if gate is None or name in decisions:
            continue
        for following in by_previous.get(event.get("id"), []):
            if not following.get("type", "").endswith(_ENTERED_SUFFIX):
                continue
            entered_name = (following.get("stateEnteredEventDetails") or {}).get("name")
            if entered_name in gate["on_disabled"]:
                decisions[name] = DISABLED
            elif entered_name == gate["on_enabled"]:
                decisions[name] = "ENABLED"
            # Neither declared target means the execution left the Choice for
            # somewhere the definition does not describe — a definition edited
            # mid-flight. Left unrecorded, so it surfaces as NOT_REACHED rather
            # than as a confident wrong answer.
            break
    return decisions


# ---------------------------------------------------------------------------
# 3. Assembly — one row per gate, every row carrying its own provenance
# ---------------------------------------------------------------------------


def build_run_scope(
    definition: dict,
    history: list[dict],
    *,
    run_date: str,
    execution_arn: str = "",
    state_machine_arn: str = "",
    input_flags: dict | None = None,
) -> dict:
    """The run's own scope, derived. Never raises on a degenerate input.

    Every row records ``source`` — which of the two authorities decided it — so
    a reader can tell a disposition observed in the execution record from one
    inferred through an ancestor gate. A surface that renders scope without
    provenance is asserting knowledge it may not have.
    """
    gates = derive_gates(definition if isinstance(definition, dict) else {})
    history = history if isinstance(history, list) else []
    exited = exited_names(history)
    entered = set(entered_sequence(history))
    decisions = gate_decisions(gates, history)

    states = flatten_states(definition.get("States", {}))
    flags = input_flags if isinstance(input_flags, dict) else {}

    stages: dict[str, dict] = {}
    for name, gate in sorted(gates.items()):
        entry, nested = work_entry(definition, gate.get("on_enabled"))
        row: dict[str, Any] = {
            "gate": name,
            "flag": gate["flag"],
            "entry_state": entry,
            "entry_state_type": states.get(entry, {}).get("Type") if entry else None,
        }
        decision = decisions.get(name)
        if decision == DISABLED:
            row.update(
                disposition=DISABLED,
                disabled_by=gate["flag"],
                source="execution_history",
                reason=(
                    f"{name} was entered and took its skip branch — "
                    f"{gate['flag']} was true on this run."
                ),
            )
        elif decision == "ENABLED":
            if entry and entry in entered and entry not in exited:
                row.update(
                    disposition=ENABLED_FAILED,
                    source="execution_history",
                    reason=(
                        f"dispatched: {entry} was entered and never exited — the "
                        "stage did not complete."
                    ),
                )
            elif entry and entry in entered:
                row.update(
                    disposition=ENABLED_COMPLETED,
                    source="execution_history",
                    reason=f"dispatched: {entry} was entered and exited cleanly.",
                )
            else:
                inner = next(
                    (g for g in nested if decisions.get(g) == DISABLED), None
                )
                if inner:
                    # Outer gate said run, an inner gate said skip. The stage is
                    # off, and the flag worth naming is the INNER one — it is the
                    # one an operator would flip to turn the stage back on.
                    row.update(
                        disposition=DISABLED,
                        disabled_by=gates[inner]["flag"],
                        source="nested_gate",
                        reason=(
                            f"{name} took its default branch, but the nested gate "
                            f"{inner} skipped the work state via "
                            f"{gates[inner]['flag']}."
                        ),
                    )
                else:
                    # Neither a skip nor a completion — the run ended between the
                    # gate and the stage it switched on.
                    row.update(
                        disposition=ENABLED_FAILED,
                        source="execution_history",
                        reason=(
                            f"{name} took its default branch but "
                            f"{entry or 'its work state'} was never entered — the "
                            "execution ended between the gate and the stage."
                        ),
                    )
        else:
            # The gate was never entered. The run ended or branched away
            # upstream. Where the run's own input carried this stage's flag, say
            # so -- that is a FACT off the execution input, not an inference
            # over the state graph. Blame walked through the graph was tried and
            # got it wrong: the shared relaunch hub made containment
            # unresolvable, and the wrong parent flag is worse than none,
            # because the flag it names is not the flag to flip.
            flag_value = flags.get(gate["flag"])
            row.update(
                disposition=NOT_REACHED,
                source="execution_history",
                input_flag=flag_value,
                reason=(
                    f"{name} was never entered — the execution ended or branched "
                    "away upstream of it. An absence of evidence: never read as "
                    "disabled, never read as passing."
                    + (
                        f" (This run's input did carry {gate['flag']}="
                        f"{str(flag_value).lower()}.)"
                        if flag_value is not None else ""
                    )
                ),
            )
        stages[gate["stage"]] = row

    counts = {d: 0 for d in sorted(DISPOSITIONS)}
    for row in stages.values():
        counts[row["disposition"]] += 1
    graded = sorted(k for k, v in stages.items() if v["disposition"] in GRADED_DISPOSITIONS)

    return {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "run_date": run_date,
        "execution_arn": execution_arn,
        "state_machine_arn": state_machine_arn,
        "stages": stages,
        "graded_stages": graded,
        "counts": counts,
        "statement": _statement(counts, len(stages)),
    }


def _statement(counts: dict[str, int], total: int) -> str:
    """The one sentence a reader needs to size any verdict computed over this.

    Rendered beside the grade, never instead of it: "GREEN" over an unstated
    denominator is not a falsifiable claim, and every surface in this fleet that
    has ever gone quietly green did it by shrinking its own scope unannounced.
    """
    graded = counts[ENABLED_COMPLETED] + counts[ENABLED_FAILED]
    parts = [f"{graded} of {total} gated stages ran and are graded"]
    if counts[DISABLED]:
        parts.append(f"{counts[DISABLED]} disabled by operator flag")
    if counts[NOT_REACHED]:
        parts.append(f"{counts[NOT_REACHED]} never reached")
    if counts[ENABLED_FAILED]:
        parts.append(f"{counts[ENABLED_FAILED]} dispatched and did NOT complete")
    return "; ".join(parts) + "."


def graded_stage_names(block: Any) -> list[str]:
    """Consumer helper — the stages a grader may score this cycle.

    Expressed against the closed vocabulary rather than against truthiness, so
    a block from a future producer that grows a fifth disposition withholds it
    rather than silently grading it.
    """
    if not isinstance(block, dict):
        return []
    stages = block.get("stages")
    if not isinstance(stages, dict):
        return []
    return sorted(
        name for name, row in stages.items()
        if isinstance(row, dict) and row.get("disposition") in GRADED_DISPOSITIONS
    )
