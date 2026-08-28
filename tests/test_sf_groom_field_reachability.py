"""Groom-dispatch-SF regressions for the shared reachability analysis.

The analysis itself now lives in ``infrastructure/sf_reachability.py`` and runs
over EVERY definition via ``tests/test_sf_field_reachability.py`` — pinning it
here to one file is what let alpha-engine-config#9077 reach production from
another (alpha-engine-config#5950). What stays here is the part that is genuinely
groom-specific: the three dated, live-fatal defects this guard was built for on
2026-07-27 (alpha-engine-config#4333), reintroduced into an in-memory copy so the
detector cannot silently stop detecting them.

  * ``MapLaunches.ItemSelector.retryState`` read ``$.retry_count`` /
    ``$.max_retries`` at the Map's *input* level, where neither exists — the same
    shape as #9077, three weeks earlier and in a different file.
  * ``PrepRelaunch`` and friends read ``$.groomPoll`` / ``$.groomLaunch``, retired
    with the SSM poll loop by alpha-engine-config-I4333. The whole bounded-relaunch
    loop was dead.
  * ``GroomRetriesExhausted`` had two callers whose input shapes were disjoint, so
    it could not resolve on both.
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

_SF_PATH = _INFRA / "step_function_groom.json"
_ENTRY = set(
    json.loads((_INFRA / "sf_entry_contract.json").read_text())["step_function_groom.json"][
        "fields"
    ]
)


@pytest.fixture()
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


def _problems(doc: dict) -> list[str]:
    return reach.analyse(doc, _ENTRY, "groom")


def test_detector_rejects_item_selector_reading_iteration_fields(sf):
    """The ``retryState`` defect: an ItemSelector reading fields it itself creates."""
    sf["States"]["MapLaunches"]["ItemSelector"]["retryState"] = {
        "retry_count.$": "$.retry_count",
        "max_retries.$": "$.max_retries",
    }
    problems = reach.item_selector_problems(sf, _ENTRY)
    assert any("retry_count" in p and "max_retries" in p for p in problems), problems


def test_detector_rejects_a_reference_to_a_retired_producer(sf):
    """The ``groomPoll`` defect: a field whose producer was removed by I4333."""
    sf["States"]["MapLaunches"]["ItemProcessor"]["States"]["PrepRelaunch"]["Parameters"][
        "groomPoll.$"
    ] = "$.groomPoll"
    assert any(
        "PrepRelaunch" in p and "groomPoll" in p for p in _problems(sf)
    ), "reintroducing the groomPoll reference must be rejected"


def test_detector_rejects_one_terminal_shared_by_disjoint_paths(sf):
    """The merged-terminal defect: two callers whose input shapes do not intersect."""
    states = sf["States"]["MapLaunches"]["ItemProcessor"]["States"]
    for rule in states["CheckLaunchedCallback"]["Choices"]:
        if rule.get("Next") == "GroomLaunchStateUnknown":
            rule["Next"] = "GroomRetriesExhausted"
    del states["GroomLaunchStateUnknown"]
    assert any(
        "GroomRetriesExhausted" in p and "timeoutError" in p for p in _problems(sf)
    ), "collapsing the two terminals must be rejected"
