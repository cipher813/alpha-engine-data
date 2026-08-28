"""Every JsonPath a Map's ItemProcessor reads must be supplied by its ItemSelector.

alpha-engine-config-I9074. A Map's ItemProcessor runs against the ITEM payload,
not the Map's own input, so `$.foo` inside the processor resolves against
`ItemSelector` output alone. Referencing a field the selector does not pass is a
`States.Runtime` error raised at execution time -- not at deploy, not in CI, and
not by `aws stepfunctions validate-state-machine-definition`, which does not
resolve JsonPath scope.

Found live 2026-08-28: `ModelZooTrainMap`'s `ItemSelector` passed `spec_id`,
`ec2_instance_id` and `preflight_args`, while `TrainSpecDispatch` inside it read
`$.run_date` for its `EXECUTION_RUN_DATE` export. The Friday rehearsal
`friday-shell-2026-08-28-eod-2026-08-28-1787947216` FAILED with:

    The JsonPath argument for the field '$.run_date' could not be found in the
    input '{"ec2_instance_id":[...],"spec_id":"sota-directional-combine",
    "preflight_args":" --preflight-only"}'

`EXECUTION_RUN_DATE` entered `TrainSpecDispatch` in #1510 at 2026-08-22 10:49 PT
-- AFTER that morning's weekly run -- so the defect was latent for six days and
the 2026-08-29 scheduled run would have hit it deterministically.

The check is deliberately whole-definition rather than a pin on this one state:
the fleet's recorded failure class is `fix-not-propagated-to-analogous-sites`,
and a second Map added later must not be able to reintroduce this silently.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

DEFINITION = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"

# What counts as a READ of the item payload, and what does not. Three exclusions,
# each a real false positive measured against this definition on 2026-08-28:
#
#   * `$$.` is the CONTEXT object (`$$.Execution.Name`, `$$.Map.Item.Value`).
#     It never resolves against the item payload.
#   * A `ResultSelector` resolves against the TASK RESULT, not the item payload,
#     so `WaitTrainSpec`'s `{"Status.$": "$.Status"}` reads the SSM invocation's
#     own output.
#   * A `Catch` entry's `ResultPath` is a WRITE of the error into the payload,
#     not a read of it.
#
# Getting these wrong makes the check cry wolf on a correct definition, and a
# guard that fires on correct input is abandoned, which is worse than no guard.
_JSONPATH = re.compile(r"\$\.([A-Za-z_][A-Za-z0-9_]*)")
_CONTEXT = re.compile(r"\$\$\.[A-Za-z0-9_.\[\]]+")


def _iter_maps(states: dict, path: str = ""):
    for name, state in states.items():
        if not isinstance(state, dict):
            continue
        here = f"{path}/{name}"
        if state.get("Type") == "Map":
            yield here, name, state
        for key in ("Iterator", "ItemProcessor"):
            nested = state.get(key)
            if isinstance(nested, dict) and "States" in nested:
                yield from _iter_maps(nested["States"], here)
        for i, branch in enumerate(state.get("Branches", []) or []):
            yield from _iter_maps(branch["States"], f"{here}[{i}]")


def _supplied(state: dict) -> set:
    selector = state.get("ItemSelector") or state.get("Parameters") or {}
    return {key[:-2] if key.endswith(".$") else key for key in selector}


def _written_by_processor(processor: dict) -> set:
    """Fields a processor state writes into the item payload mid-flight."""
    written = set()
    for child in processor.get("States", {}).values():
        if not isinstance(child, dict):
            continue
        paths = [child.get("ResultPath")]
        paths += [c.get("ResultPath") for c in child.get("Catch", []) or []]
        paths += [r.get("ResultPath") for r in child.get("Retry", []) or []]
        for value in paths:
            if isinstance(value, str) and value.startswith("$."):
                written.add(value[2:].split(".")[0])
    return written


def _read_from_item(processor: dict) -> set:
    """JsonPaths the processor resolves against the ITEM payload."""
    names = set()
    for child in processor.get("States", {}).values():
        if not isinstance(child, dict):
            continue
        scoped = {k: v for k, v in child.items() if k != "ResultSelector"}
        blob = _CONTEXT.sub("", json.dumps(scoped))
        names |= set(_JSONPATH.findall(blob))
    return names


def test_every_map_item_processor_path_is_supplied_by_its_item_selector():
    definition = json.loads(DEFINITION.read_text())
    maps = list(_iter_maps(definition["States"]))
    assert maps, "no Map states found -- the walker is broken, not the definition"

    failures: list[str] = []
    for path, name, state in maps:
        processor = state.get("ItemProcessor") or state.get("Iterator") or {}
        available = _supplied(state) | _written_by_processor(processor)
        referenced = _read_from_item(processor)
        missing = sorted(referenced - available)
        if missing:
            failures.append(
                f"{path}: ItemProcessor reads {missing} which the ItemSelector "
                f"does not supply (supplies {sorted(_supplied(state))}). Those "
                f"resolve against the ITEM payload and will raise States.Runtime "
                f"at execution time."
            )

    assert not failures, "\n".join(failures)


def test_model_zoo_train_map_supplies_run_date():
    """The specific 2026-08-28 regression, pinned by name.

    The general check above is the guard; this one names the incident so a future
    reader can find it, and fails loudly if the selector is trimmed back.
    """
    definition = json.loads(DEFINITION.read_text())
    found = [s for _, name, s in _iter_maps(definition["States"]) if name == "ModelZooTrainMap"]
    assert len(found) == 1, "ModelZooTrainMap not found exactly once"
    selector = found[0]["ItemSelector"]
    assert selector.get("run_date.$") == "$.run_date", (
        "ModelZooTrainMap must pass run_date into the item payload -- "
        "TrainSpecDispatch exports it as EXECUTION_RUN_DATE. Removing it "
        "reproduces the 2026-08-28 Friday-rehearsal failure."
    )
