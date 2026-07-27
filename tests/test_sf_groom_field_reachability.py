"""Static dataflow guard for the groom dispatch SF (groom-sweep-policy §2.2).

A `States.Runtime` raised while evaluating a state's `Parameters` is NOT catchable
by any `Catch` block — no runtime guard can exist for it. Correctness of parameter
references is therefore a *static* obligation, and this module is where it is met.

Origin (2026-07-27): three separate live-fatal defects, all of the same shape —
a JSONPath referencing a field that no predecessor on the reaching path produces:

  * `MapLaunches.ItemSelector.retryState` read `$.retry_count` / `$.max_retries` at
    the Map's *input* level, where neither exists. Fatal on entry to the Map.
  * `PrepRelaunch`, `SetForceOnDemand`, `NotifyRelaunch` and `GroomRetriesExhausted`
    read `$.groomPoll` / `$.groomLaunch` — retired along with the SSM poll loop by
    alpha-engine-config-I4333. The entire bounded-relaunch loop was dead.
  * `GroomRetriesExhausted` had two callers whose input shapes were disjoint, so it
    could not resolve on both.

The analysis walks each state graph from its entry point, propagating the set of
top-level field names available at each state, and asserts that every field a state
references is present on **every** path that reaches it. Only the first path segment
is modelled (`$.foo.bar.baz` -> `foo`), which is sufficient: every defect above was a
missing top-level field, and deeper modelling would require the Lambda's response
schema, which is not statically knowable here.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_SF_PATH = (
    Path(__file__).resolve().parent.parent
    / "infrastructure"
    / "step_function_groom.json"
)

# `$.foo` / `$.foo.bar` but never `$$.Map.Item.Value` (the context object, always
# available) and never a bare `$`.
_REF = re.compile(r"(?<!\$)\$\.([A-Za-z_][A-Za-z0-9_]*)")


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


def _refs(node) -> set[str]:
    """Every top-level field name referenced anywhere inside `node`."""
    found: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "Comment":
                continue
            found |= _refs(value)
    elif isinstance(node, list):
        for value in node:
            found |= _refs(value)
    elif isinstance(node, str):
        found |= set(_REF.findall(node))
    return found


def _param_keys(params: dict) -> set[str]:
    """The field names a Pass/Task `Parameters` block *produces* at the top level."""
    return {k[:-2] if k.endswith(".$") else k for k in params}


def _guarded_vars(rule: dict) -> set[str]:
    """Variables an `IsPresent` in this Choice rule makes safe to reference.

    A rule may legitimately test a field that does not exist on every path, provided
    the test is (or is guarded by) an `IsPresent` — that is the standard ASL idiom
    and does not raise. `CheckLaunchedCallback` depends on it.
    """
    safe: set[str] = set()
    if "IsPresent" in rule and "Variable" in rule:
        safe |= set(_REF.findall(rule["Variable"]))
    for key in ("And", "Or"):
        for sub in rule.get(key, []):
            safe |= _guarded_vars(sub)
    if "Not" in rule:
        safe |= _guarded_vars(rule["Not"])
    return safe


#: Keys whose contents are NOT references evaluated in this state's scope.
#: `ItemProcessor`/`Iterator` are a nested scope (checked separately, seeded by
#: `ItemSelector`); `ItemSelector` is evaluated at the Map's input and has its own
#: test; `Catch`/`Retry` describe routing; `ResultPath` is a write target.
_NOT_A_READ = ("ItemProcessor", "Iterator", "ItemSelector", "Catch", "Retry", "ResultPath")


def _state_refs(state: dict) -> set[str]:
    """Fields this state reads, excluding IsPresent-guarded Choice variables."""
    if state.get("Type") == "Choice":
        needed: set[str] = set()
        for rule in state.get("Choices", []):
            needed |= _refs(rule) - _guarded_vars(rule)
        return needed
    if state.get("Type") == "Fail":
        # `Error`/`Cause` are literal strings — only the *Path variants dereference.
        return _refs({k: state[k] for k in ("ErrorPath", "CausePath") if k in state})
    return _refs({k: v for k, v in state.items() if k not in _NOT_A_READ})


def _successors(name: str, state: dict) -> list[tuple[str, str | None]]:
    """(next_state, result_path_written_on_that_edge) for every outgoing edge."""
    out: list[tuple[str, str | None]] = []
    result_path = state.get("ResultPath")
    written = None
    if isinstance(result_path, str) and result_path.startswith("$."):
        written = result_path[2:].split(".")[0]

    if "Next" in state:
        # A Pass with Parameters and no ResultPath REPLACES the input entirely; that
        # is modelled in _walk, not here.
        out.append((state["Next"], written))
    for rule in state.get("Choices", []):
        if "Next" in rule:
            out.append((rule["Next"], None))
    if "Default" in state:
        out.append((state["Default"], None))
    for catch in state.get("Catch", []):
        caught = catch.get("ResultPath")
        cw = (
            caught[2:].split(".")[0]
            if isinstance(caught, str) and caught.startswith("$.")
            else None
        )
        out.append((catch["Next"], cw))
    return out


def _walk(states: dict, start: str, seed: set[str]) -> dict[str, set[str]]:
    """Fields guaranteed available at each reachable state (intersection of paths)."""
    available: dict[str, set[str]] = {}
    queue: list[tuple[str, frozenset[str]]] = [(start, frozenset(seed))]

    while queue:
        name, incoming = queue.pop()
        state = states[name]
        if name in available:
            merged = available[name] & incoming
            if merged == available[name]:
                continue  # no new constraint; stop
            available[name] = merged
        else:
            available[name] = set(incoming)

        current = set(available[name])
        params = state.get("Parameters")
        if state.get("Type") == "Pass" and params is not None and not state.get(
            "ResultPath"
        ):
            outgoing_base = _param_keys(params)  # Parameters replace the input
        elif state.get("Type") == "Pass" and params is not None:
            outgoing_base = current  # ResultPath merges the result back in
        else:
            outgoing_base = current

        for nxt, written in _successors(name, state):
            fields = set(outgoing_base)
            if written:
                fields.add(written)
            queue.append((nxt, frozenset(fields)))

    return available


def _check(states: dict, start: str, seed: set[str], scope: str) -> list[str]:
    available = _walk(states, start, seed)
    problems: list[str] = []
    for name, fields in sorted(available.items()):
        missing = _state_refs(states[name]) - fields
        if missing:
            problems.append(
                f"{scope}/{name} references {sorted(missing)} which no predecessor "
                f"produces on every reaching path (available: {sorted(fields)})"
            )
    return problems


def test_top_level_fields_are_all_producible(sf):
    """Every field the top-level states read is produced before they read it."""
    problems = _check(sf["States"], sf["StartAt"], {"schedInput", "decideMarker"}, "top")
    assert not problems, "\n".join(problems)


def test_map_item_processor_fields_are_all_producible(sf):
    """Every field a lane's states read is produced on EVERY path reaching them.

    This is the guard that would have caught the I4333 relaunch breakage: the
    timeout path reaches `PrepRelaunch` with no `groomPoll`, so a reference to it
    is a defect no matter how the success path looks.
    """
    map_state = sf["States"]["MapLaunches"]
    processor = map_state["ItemProcessor"]
    seed = _param_keys(map_state["ItemSelector"])
    problems = _check(processor["States"], processor["StartAt"], seed, "MapLaunches")
    assert not problems, "\n".join(problems)


def test_item_selector_only_reads_fields_present_at_map_input(sf):
    """`ItemSelector` is evaluated against the Map's INPUT, not the item.

    `retryState` read `$.retry_count` there — a field that only exists *inside* an
    iteration, because `ItemSelector` itself creates it. Fatal on entry to the Map.
    """
    available = _walk(sf["States"], sf["StartAt"], {"schedInput", "decideMarker"})
    at_map = available["MapLaunches"]
    referenced = _refs(sf["States"]["MapLaunches"]["ItemSelector"])
    missing = referenced - at_map
    assert not missing, (
        f"MapLaunches.ItemSelector references {sorted(missing)} at the Map's input "
        f"level, where only {sorted(at_map)} exist"
    )


# --------------------------------------------------------------------------- #
# Detector self-verification.
#
# A guard that passes on a healthy definition proves nothing on its own — the
# guard this one replaces was green for weeks while defending a live crash. These
# reintroduce each historical defect into an in-memory copy and assert the
# analysis rejects it, so the detector cannot silently stop detecting.
# --------------------------------------------------------------------------- #


def _lane_problems(doc: dict) -> list[str]:
    map_state = doc["States"]["MapLaunches"]
    processor = map_state["ItemProcessor"]
    return _check(
        processor["States"],
        processor["StartAt"],
        _param_keys(map_state["ItemSelector"]),
        "MapLaunches",
    )


def test_detector_rejects_item_selector_reading_iteration_fields(sf):
    """The `retryState` defect: ItemSelector reading fields it itself creates."""
    doc = json.loads(json.dumps(sf))
    doc["States"]["MapLaunches"]["ItemSelector"]["retryState"] = {
        "retry_count.$": "$.retry_count",
        "max_retries.$": "$.max_retries",
    }
    available = _walk(doc["States"], doc["StartAt"], {"schedInput", "decideMarker"})
    missing = _refs(doc["States"]["MapLaunches"]["ItemSelector"]) - available["MapLaunches"]
    assert missing == {"retry_count", "max_retries"}


def test_detector_rejects_a_reference_to_a_retired_producer(sf):
    """The `groomPoll` defect: a field whose producer was removed by I4333."""
    doc = json.loads(json.dumps(sf))
    doc["States"]["MapLaunches"]["ItemProcessor"]["States"]["PrepRelaunch"][
        "Parameters"
    ]["groomPoll.$"] = "$.groomPoll"
    assert any(
        "PrepRelaunch" in p and "groomPoll" in p for p in _lane_problems(doc)
    ), "reintroducing the groomPoll reference must be rejected"


def test_detector_rejects_one_terminal_shared_by_disjoint_paths(sf):
    """The merged-terminal defect: two callers whose input shapes do not intersect."""
    doc = json.loads(json.dumps(sf))
    states = doc["States"]["MapLaunches"]["ItemProcessor"]["States"]
    for rule in states["CheckLaunchedCallback"]["Choices"]:
        if rule.get("Next") == "GroomLaunchStateUnknown":
            rule["Next"] = "GroomRetriesExhausted"
    del states["GroomLaunchStateUnknown"]
    assert any(
        "GroomRetriesExhausted" in p and "timeoutError" in p
        for p in _lane_problems(doc)
    ), "collapsing the two terminals must be rejected"
