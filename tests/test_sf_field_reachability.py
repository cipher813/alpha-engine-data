"""Every field a state reads must exist on every path that reaches it — for
EVERY state-machine definition in this repo, not one of them.

alpha-engine-config#5950 (the ``States.Runtime``-masks-the-error class) and
alpha-engine-config#9077 (its fifth recurrence, 2026-08-28).

A ``States.Runtime`` raised while resolving a state's ``Parameters`` happens
before the task runs, so the state's own ``Catch: [States.ALL]`` never sees it
and a Map's ``ToleratedFailurePercentage`` never applies. One iteration's
payload defect killed the whole weekly pipeline on 2026-08-28. AWS's
``validate-state-machine-definition`` does not resolve JSONPath scope and the
file is valid JSON and valid ASL, so nothing else between the editor and
production rejects it.

The analysis that catches this already existed — in
``tests/test_sf_groom_field_reachability.py``, pinned to
``step_function_groom.json``. Run over ``step_function.json`` it would have made
#9077 a red check instead of a dead Saturday run. That is the fleet's
``fix-not-propagated-to-analogous-sites`` failure mode, and this module is the
propagation: the shared analyser lives in ``infrastructure/sf_reachability.py``
and every definition in ``infrastructure/step_function*.json`` is checked by
discovery, so a definition added later cannot quietly escape it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
sys.path.insert(0, str(_INFRA))

import sf_reachability as reach  # noqa: E402

#: Discovered, never enumerated — the whole point is that a new definition is
#: covered the day it lands rather than the day someone remembers to list it.
DEFINITIONS = sorted(_INFRA.glob("step_function*.json"))
CONTRACT_PATH = _INFRA / "sf_entry_contract.json"


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text())


def _entry(name: str) -> set[str]:
    contract = _contract()
    assert name in contract, (
        f"{name} has no entry contract in {CONTRACT_PATH.name}. Every definition "
        f"needs one: the fields EVERY declared caller supplies. Without it the "
        f"analysis cannot tell a caller-supplied field from a missing one."
    )
    return set(contract[name]["fields"])


def test_every_definition_has_an_entry_contract():
    contract = _contract()
    declared = {k for k in contract if not k.startswith("_")}
    found = {p.name for p in DEFINITIONS}
    assert declared == found, (
        f"entry contract and definitions disagree: "
        f"only in contract {sorted(declared - found)}, "
        f"only on disk {sorted(found - declared)}"
    )


@pytest.mark.parametrize("path", DEFINITIONS, ids=lambda p: p.name)
def test_all_referenced_fields_are_producible(path: Path):
    definition = json.loads(path.read_text())
    problems = reach.analyse(definition, _entry(path.name), path.name)
    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("path", DEFINITIONS, ids=lambda p: p.name)
def test_item_selectors_only_read_fields_present_at_the_map_input(path: Path):
    """An ``ItemSelector`` is evaluated at the Map's INPUT, not inside an iteration."""
    definition = json.loads(path.read_text())
    problems = reach.item_selector_problems(definition, _entry(path.name))
    assert not problems, "\n".join(problems)


# --------------------------------------------------------------------------- #
# Detector self-verification.
#
# A guard that passes on a healthy definition proves nothing: the guard #9077
# slipped past was green for six days while defending a live crash. These
# reintroduce real, dated defects into an in-memory copy and assert the analysis
# rejects each one, so the detector cannot silently stop detecting.
# --------------------------------------------------------------------------- #
def _weekly() -> dict:
    return json.loads((_INFRA / "step_function.json").read_text())


def test_detector_reproduces_the_9077_incident():
    """``ModelZooTrainMap``'s ItemSelector dropping ``run_date`` (2026-08-28)."""
    doc = _weekly()
    branch = doc["States"]["ResearchPredictorParallel"]["Branches"][1]
    del branch["States"]["ModelZooTrainMap"]["ItemSelector"]["run_date.$"]
    problems = reach.analyse(doc, _entry("step_function.json"), "weekly")
    assert any(
        "TrainSpecDispatch" in p and "run_date" in p for p in problems
    ), "removing the ItemSelector's run_date must be rejected"


def test_detector_rejects_an_item_selector_reading_iteration_fields():
    """An ItemSelector reading a field it itself creates is fatal on Map entry."""
    doc = _weekly()
    branch = doc["States"]["ResearchPredictorParallel"]["Branches"][1]
    branch["States"]["ModelZooTrainMap"]["ItemSelector"]["spec_id_echo.$"] = "$.spec_id"
    problems = reach.item_selector_problems(doc, _entry("step_function.json"))
    assert any("spec_id" in p for p in problems)


def test_detector_rejects_a_notifier_reading_an_unset_degradation_field():
    """The #5950 shape: a Choice edge reaching a notifier without its field."""
    doc = _weekly()
    doc["States"]["NotifyCompleteReportCardDegraded"]["Parameters"][
        "Subject.$"
    ] = "States.Format('{}', $.a_field_nothing_produces)"
    problems = reach.analyse(doc, _entry("step_function.json"), "weekly")
    assert any(
        "NotifyCompleteReportCardDegraded" in p and "a_field_nothing_produces" in p
        for p in problems
    )


def test_detector_rejects_removing_the_initialize_input_floor():
    """The floor in ``InitializeInput`` is what makes the notifier fields safe."""
    doc = _weekly()
    merged = doc["States"]["InitializeInput"]["Parameters"]["merged.$"]
    doc["States"]["InitializeInput"]["Parameters"]["merged.$"] = merged.replace(
        '"parity_error":{"status":"not_set"},', ""
    )
    problems = reach.analyse(doc, _entry("step_function.json"), "weekly")
    assert any("parity_error" in p for p in problems), (
        "dropping a floored field must be rejected — otherwise the floor can be "
        "deleted as dead weight by someone who cannot see what depends on it"
    )


def test_detector_rejects_the_eod_failure_reporter_regression():
    """EOD ``HandleFailure`` reading ``$.error`` on an edge that never sets it."""
    doc = json.loads((_INFRA / "step_function_eod.json").read_text())
    doc["States"]["MarketHoursGateChoice"]["Default"] = "HandleFailure"
    problems = reach.analyse(doc, _entry("step_function_eod.json"), "eod")
    assert any("HandleFailure" in p and "error" in p for p in problems)


# --------------------------------------------------------------------------- #
# False-positive guards. Each of these was a real false positive measured on a
# live definition on 2026-08-28; a guard that fires on a correct definition gets
# muted, and a muted guard is worse than no guard at all.
# --------------------------------------------------------------------------- #
def test_a_pass_result_literal_is_not_a_reference():
    """``Result`` is a literal value; a ``$.foo`` inside it is prose."""
    state = {
        "Type": "Pass",
        "Result": {"Cause": "the gate returned no verdict so $.market_hours_gate_error is unset"},
    }
    assert reach.state_refs(state) == set()


def test_a_quoted_intrinsic_argument_is_not_a_reference():
    """A single-quoted span inside an intrinsic is data, never a dereference."""
    state = {
        "Type": "Pass",
        "Parameters": {
            "merged.$": "States.JsonMerge(States.StringToJson('{\"error\":{\"detail\":\"no $.error was set\"}}'),$,false)"
        },
    }
    assert reach.state_refs(state) == set()


def test_a_result_selector_reads_the_task_result_not_the_payload():
    state = {"Type": "Task", "ResultSelector": {"Status.$": "$.Status"}}
    assert reach.state_refs(state) == set()


def test_json_merge_over_literals_produces_their_keys():
    expr = (
        "States.JsonMerge(States.StringToJson('{\"a\":1,\"b\":2}'),"
        "States.StringToJson('{\"c\":3}'),false)"
    )
    assert reach.produced_keys(expr, frozenset(), frozenset()) == {"a", "b", "c"}


def test_json_merge_with_the_current_payload_carries_it_forward():
    expr = "States.JsonMerge($,States.StringToJson('{\"c\":3}'),false)"
    assert reach.produced_keys(expr, frozenset({"a"}), frozenset()) == {"a", "c"}


def test_execution_input_contributes_only_the_entry_contract():
    expr = "States.JsonMerge(States.StringToJson('{\"a\":1}'),$$.Execution.Input,false)"
    assert reach.produced_keys(expr, frozenset(), frozenset({"pipeline_role"})) == {
        "a",
        "pipeline_role",
    }
