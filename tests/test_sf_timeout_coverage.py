"""Structural coverage for Step Functions execution timeouts (alpha-engine-config#6693).

Prior to this test, none of the three Step Functions definitions declared a
top-level ``TimeoutSeconds`` — a hung execution could run to the Step
Functions 1-year service maximum, invisibly, since status-keyed watchers only
key off a terminal (non-RUNNING) execution status. Several ``Task`` states in
the weekday and postclose definitions also lacked a per-state
``TimeoutSeconds``, leaving an individual stuck AWS SDK/Lambda/SSM call with
no bound narrower than the whole-execution one.

This test pins two invariants going forward:

  (a) every one of the three top-level SF definitions (``step_function.json``
      the Saturday/weekly SF, ``step_function_daily.json`` the weekday SF,
      ``step_function_eod.json`` the postclose/EOD SF) carries a top-level
      ``TimeoutSeconds``.
  (b) every ``Task``-type state in the weekday and EOD definitions (including
      states nested inside ``Parallel`` branches or ``Map`` iterators, though
      neither definition currently uses those) carries its own
      ``TimeoutSeconds``.

Per-task coverage for the weekly/Saturday SF (``step_function.json``) is
intentionally NOT asserted here — it is tracked separately (see
alpha-engine-config#6693's scope note) and this file must not silently start
enforcing a rule that repo's weekly-sf-policy hasn't yet mandated there.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"

_WEEKLY_PATH = _INFRA / "step_function.json"
_DAILY_PATH = _INFRA / "step_function_daily.json"
_EOD_PATH = _INFRA / "step_function_eod.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


def _walk_states(states: dict):
    """Yield (name, state) for every state, descending into Parallel branches
    and Map iterators/item-processors. Mirrors the walk() idiom already used
    by tests/test_weekly_sf_rerun.py."""
    for name, state in states.items():
        yield name, state
        if state.get("Type") == "Parallel":
            for branch in state.get("Branches", []):
                yield from _walk_states(branch["States"])
        if state.get("Type") == "Map":
            it = state.get("Iterator") or state.get("ItemProcessor") or {}
            yield from _walk_states(it.get("States", {}))


@pytest.mark.parametrize(
    "path",
    [_WEEKLY_PATH, _DAILY_PATH, _EOD_PATH],
    ids=["weekly", "daily", "eod"],
)
def test_definition_carries_top_level_timeout(path):
    doc = _load(path)
    assert "TimeoutSeconds" in doc, (
        f"{path.name}: no top-level TimeoutSeconds — a hung execution can run "
        "to the Step Functions 1-year service maximum invisibly "
        "(alpha-engine-config#6693)."
    )
    assert isinstance(doc["TimeoutSeconds"], int) and doc["TimeoutSeconds"] > 0


@pytest.mark.parametrize(
    "path",
    [_DAILY_PATH, _EOD_PATH],
    ids=["daily", "eod"],
)
def test_every_task_state_carries_timeout(path):
    doc = _load(path)
    missing = [
        name
        for name, state in _walk_states(doc["States"])
        if state.get("Type") == "Task" and "TimeoutSeconds" not in state
    ]
    assert not missing, (
        f"{path.name}: Task state(s) with no TimeoutSeconds: {sorted(missing)} "
        "(alpha-engine-config#6693) — a hung AWS SDK/Lambda/SSM call in one of "
        "these has no bound narrower than the whole-execution timeout."
    )


def test_daily_task_timeout_values_are_positive_ints():
    doc = _load(_DAILY_PATH)
    for name, state in _walk_states(doc["States"]):
        if state.get("Type") == "Task":
            ts = state["TimeoutSeconds"]
            assert isinstance(ts, int) and ts > 0, f"daily/{name}: TimeoutSeconds={ts!r}"


def test_eod_task_timeout_values_are_positive_ints():
    doc = _load(_EOD_PATH)
    for name, state in _walk_states(doc["States"]):
        if state.get("Type") == "Task":
            ts = state["TimeoutSeconds"]
            assert isinstance(ts, int) and ts > 0, f"eod/{name}: TimeoutSeconds={ts!r}"
