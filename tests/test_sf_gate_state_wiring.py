"""``ReportCard`` and ``Director`` carry the weekly run's correctness-gate state
(``alpha-engine-config-I7282``, ``sf-pipeline-policy.md`` §2.3a rule 3).

THE DEFECT THIS PINS
--------------------
Pre-fix the two surfaces that present the weekly run's numbers received::

    "ReportCard": {"Payload": {"date.$": "$.run_date",
                               "dry_run.$": "$.research_dry", "snapshot": true}}
    "Director":   {"Payload": {"date.$": "$.run_date",
                               "dry_run.$": "$.research_dry"}}

``$.gate_degraded``, the sibling degradation families and both pre-spend gate
probe payloads were all on the execution record by the time these states ran.
Neither Task passed any of them, so the Report Card and the Director advisory
rendered the week's numbers identically whether the correctness gates passed,
failed, or never ran. §2.3a rule 3: *a report card, dashboard or notification
rendering the run's numbers without saying whether the correctness check ran is
asserting a guarantee nobody established.*

Not hypothetical: ``PipelineContractGate`` reads
``private-docs/PIPELINE_CONTRACT.yaml`` from ``raw.githubusercontent.com``
unauthenticated and ``nousergon/alpha-engine-config`` is PRIVATE — the URL
returns HTTP 404 (measured live 2026-08-13, ``alpha-engine-config-I7281``). That
gate has never measured anything on any run.

THE HAZARD THIS ALSO PINS
-------------------------
``$.gate_degraded`` and its sibling families are **absent** on the clean path,
not ``false`` — they are written only by their own degraded ``Pass`` states. A
bare ``"gate_degraded.$": "$.gate_degraded"`` in a Task ``Payload`` therefore
raises ``States.Runtime`` **on the healthy path**: it would break exactly the
runs that are working, and only those. The fix seeds every referenced field in
``InitializeInput``'s ``States.JsonMerge`` floor — the convention that state
already uses for ``preflight_args`` / ``research_dry`` / ``regime_action`` — so
each field is present in BOTH polarities and every ``.$`` reference resolves on
every path.

``test_every_gate_state_path_resolves_against_the_initialize_input_floor`` is
the assertion that would have caught the naive fix, and it is the reason the
seeding is not optional.
"""
from __future__ import annotations

import hashlib
import json
import pathlib

import jsonschema
import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"
_SCHEMA_PATH = (
    pathlib.Path(__file__).parent.parent
    / "infrastructure" / "contracts" / "sf_gate_state.v1.schema.json"
)

#: The consumer repo (``crucible-evaluator``) carries a byte-identical copy at
#: ``grading/contracts/sf_gate_state.v1.schema.json`` and pins the SAME digest in
#: ``tests/test_pipeline_gates.py``. Neither repo's CI can read the other, so
#: this pin is what makes a one-sided edit fail loudly instead of silently
#: forking the contract.
_SCHEMA_SHA256 = "5f4c4a7736238103aa64d9cf989eddfd87840612a6872b266ce1e5578c2439b6"

#: The states whose payload must carry it — every surface presenting the run's
#: results (§2.3a rule 3). Adding a third reporting surface adds a row here.
_SURFACES = ("ReportCard", "Director")

#: The boolean degradation families, each seeded false at InitializeInput and set
#: true by exactly one Pass state.
_FAMILIES = (
    "gate_degraded",
    "health_check_degraded",
    "parity_degraded",
    "research_predictor_degraded",
)


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


@pytest.fixture(scope="module")
def floor(states) -> dict:
    """``InitializeInput``'s innermost defaults blob — the seeded floor every
    ``gate_state`` JSONPath must resolve against on the clean path."""
    merged = states["InitializeInput"]["Parameters"]["merged.$"]
    start = merged.index("States.StringToJson('") + len("States.StringToJson('")
    end = merged.index("')", start)
    return json.loads(merged[start:end])


# ---------------------------------------------------------------------------
# The payloads carry it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("surface", _SURFACES)
def test_surface_payload_carries_gate_state(states, surface):
    payload = states[surface]["Parameters"]["Payload"]
    assert "gate_state" in payload, (
        f"{surface} presents the weekly run's numbers and carries no gate "
        "verdict — sf-pipeline-policy.md §2.3a rule 3. A surface rendering the "
        "run's results without saying whether the correctness check ran is "
        "asserting a guarantee nobody established."
    )


@pytest.mark.parametrize("surface", _SURFACES)
def test_gate_state_carries_every_family_and_both_probes(states, surface):
    gate_state = states[surface]["Parameters"]["Payload"]["gate_state"]
    assert gate_state["schema_version"] == 1
    for family in _FAMILIES:
        assert gate_state.get(f"{family}.$") == f"${'.'}{family}", (
            f"{surface}'s gate_state must carry {family} — the notifier already "
            "branches on it, so the deliverable rendering the numbers is the one "
            "surface that cannot see it."
        )
    assert gate_state["lib_pin_drift.$"] == "$.libpin_drift_result.Payload"
    assert gate_state["pipeline_contract.$"] == "$.pipeline_contract_result.Payload"


def test_both_surfaces_send_an_identical_block(states):
    """One contract, one schema, one consumer implementation. Two surfaces that
    drift apart is two consumers, and the second one is the one nobody tests."""
    blocks = [states[s]["Parameters"]["Payload"]["gate_state"] for s in _SURFACES]
    assert blocks[0] == blocks[1]


# ---------------------------------------------------------------------------
# The absent-on-the-clean-path hazard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("family", _FAMILIES)
def test_families_are_seeded_false_not_absent(floor, family):
    assert floor.get(family) is False, (
        f"{family} must be seeded false in InitializeInput's floor. It is written "
        "only by its own degraded Pass state, so on the clean path it is ABSENT — "
        "and a bare $.{family} reference in a Task Payload raises States.Runtime "
        "on exactly the runs that are working. Seeding it also satisfies §2.3a's "
        "both-polarities requirement: a field that appears only on the bad path "
        "cannot be distinguished from a producer that broke."
    )


@pytest.mark.parametrize("probe", ["libpin_drift_result", "pipeline_contract_result"])
def test_probe_results_are_seeded_unknown_not_absent(floor, probe):
    seeded = floor.get(probe)
    assert isinstance(seeded, dict), (
        f"{probe} must be seeded in InitializeInput's floor: it is absent whenever "
        "the gate was skipped or its Task Catch fired, and UNKNOWN is the honest "
        "value for a gate that has not run yet."
    )
    payload = seeded.get("Payload")
    assert isinstance(payload, dict) and payload.get("status") == "UNKNOWN", (
        f"{probe}'s seed must read status=UNKNOWN — sf-pipeline-policy.md §2.3a "
        "rule 2: a missing verdict propagates as UNKNOWN, never as a pass."
    )
    # The seed must carry NO verdict key: an unmeasured gate that ships
    # has_drift/has_violation is the exact defect config-I7048/I7277 closed at
    # the producer, and re-minting it in the SF floor would undo both.
    assert "has_drift" not in payload and "has_violation" not in payload


@pytest.mark.parametrize("surface", _SURFACES)
def test_every_gate_state_path_resolves_against_the_initialize_input_floor(
    states, floor, surface,
):
    """THE regression this module exists for.

    Every ``.$`` reference inside ``gate_state`` must resolve against the state
    input a CLEAN run carries at this point — i.e. the InitializeInput floor,
    since no degraded Pass state ran to write any of the fields. A reference that
    does not resolve is a ``States.Runtime`` on the healthy path.
    """
    gate_state = states[surface]["Parameters"]["Payload"]["gate_state"]
    for key, path in gate_state.items():
        if not key.endswith(".$"):
            continue
        node = floor
        for segment in path.lstrip("$.").split("."):
            assert isinstance(node, dict) and segment in node, (
                f"{surface}.gate_state.{key} references {path}, which is NOT "
                "present on the clean path — InitializeInput's floor does not "
                f"seed {segment!r}. This raises States.Runtime on exactly the "
                "runs that are working."
            )
            node = node[segment]


def test_seeding_does_not_change_the_degraded_notifier_selection(states):
    """Every read of a seeded family is ``And(IsPresent, BooleanEquals true)``.

    Seeding them ``false`` makes ``IsPresent`` true on the clean path, so the
    selection is unchanged ONLY because every rule also compares the value. A
    bare ``IsPresent`` rule anywhere would start firing on every healthy run.
    """
    def walk(rule):
        for op in rule.get("And", []) or rule.get("Or", []):
            yield from walk(op)
        if "Variable" in rule:
            yield rule

    for name, st in states.items():
        if st.get("Type") != "Choice":
            continue
        for choice in st.get("Choices", []):
            leaves = list(walk(choice))
            for family in _FAMILIES:
                var = f"$.{family}"
                refs = [r for r in leaves if r.get("Variable") == var]
                if not refs:
                    continue
                assert any("BooleanEquals" in r for r in refs), (
                    f"{name} tests {var} for presence alone. Now that the field "
                    "is seeded false at InitializeInput it is ALWAYS present, so "
                    "this rule would fire on every clean run."
                )


# ---------------------------------------------------------------------------
# The versioned schema at the cross-repo boundary
# ---------------------------------------------------------------------------


def test_schema_digest_is_pinned_against_the_consumer_copy():
    digest = hashlib.sha256(_SCHEMA_PATH.read_bytes()).hexdigest()
    assert digest == _SCHEMA_SHA256, (
        "infrastructure/contracts/sf_gate_state.v1.schema.json changed. The "
        "consumer (crucible-evaluator grading/contracts/sf_gate_state.v1.schema"
        ".json) carries a byte-identical copy pinned to the same digest — update "
        "BOTH repos in one cross-repo change, or the contract has silently forked."
    )


def test_the_payload_the_sf_sends_on_a_clean_run_validates(states, floor):
    """Producer-side contract test: resolve the ReportCard payload's gate_state
    against the clean-path state input and validate the RESULT against the
    schema the consumer validates against."""
    gate_state = states["ReportCard"]["Parameters"]["Payload"]["gate_state"]
    resolved = {}
    for key, value in gate_state.items():
        if key.endswith(".$"):
            node = floor
            for segment in value.lstrip("$.").split("."):
                node = node[segment]
            resolved[key[:-2]] = node
        else:
            resolved[key] = value
    jsonschema.validate(resolved, json.loads(_SCHEMA_PATH.read_text()))
    # On the clean path the probe seeds are what a run that never reached the
    # gates carries — UNKNOWN, which is what the consumer must render.
    assert resolved["pipeline_contract"]["status"] == "UNKNOWN"
    assert resolved["gate_degraded"] is False


def test_a_measured_run_also_validates():
    """The other polarity: the real probe payloads, as crucible-predictor emits
    them after config-I7277."""
    measured = {
        "schema_version": 1,
        "gate_degraded": False,
        "health_check_degraded": False,
        "parity_degraded": False,
        "research_predictor_degraded": False,
        "lib_pin_drift": {"status": "MEASURED", "has_drift": False,
                          "parity_ok": True, "floor_ok": True, "offenders": []},
        "pipeline_contract": {"status": "MEASURED", "has_violation": False,
                              "violations": [], "boundary_count": 12},
    }
    jsonschema.validate(measured, json.loads(_SCHEMA_PATH.read_text()))
