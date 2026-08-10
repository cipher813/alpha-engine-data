"""SF-level consumer contract for the per-pass pit_parity stats artifacts
(alpha-engine-config#6030, M0 rule: producer/consumer contract tests at
birth).

The producer half lives in crucible-backtester (contracts/
pit_stats_pass.schema.json + tests/test_pit_stats_artifact.py). This module
pins THIS repo's half: the weekly SF wires the pass-producing scripts and the
compare consumer exactly as contracts/pit_stats_pass.consumer.json declares,
so a rename of a script, state, or the S3 key template cannot drift the two
repos apart silently — the contract file is the single mirrored surface.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONTRACT = _REPO_ROOT / "contracts" / "pit_stats_pass.consumer.json"
_SF = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def contract() -> dict:
    return json.loads(_CONTRACT.read_text())


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF.read_text())


def _all_states(sf) -> dict:
    out = {}

    def walk(states):
        for name, state in states.items():
            out[name] = state
            if state.get("Type") == "Parallel":
                for b in state.get("Branches", []):
                    walk(b["States"])
            if state.get("Type") == "Map":
                it = state.get("Iterator") or state.get("ItemProcessor") or {}
                walk(it.get("States", {}))

    walk(sf["States"])
    return out


def test_contract_surface_pinned(contract):
    """The shared producer/consumer surface — must match crucible-backtester's
    analysis/pit_stats_artifact.py (PASS_SCHEMA + pass_artifact_key) verbatim.
    Change both repos in lockstep or neither."""
    assert contract["schema_id"] == "pit_stats_pass-1.0.0"
    assert contract["key_template"] == "parity/{run_date}/pit_stats_{pass}.json"
    assert contract["passes"] == ["lookahead", "walkforward"]
    assert contract["bucket"] == "alpha-engine-research"


def test_producer_states_wired_to_declared_scripts(contract, sf):
    states = _all_states(sf)
    for which, spec in contract["producers"].items():
        state = states.get(spec["sf_state"])
        assert state is not None, (
            f"{spec['sf_state']} missing from the weekly SF — the "
            f"{which} pass artifact would never be produced"
        )
        blob = json.dumps(state["Parameters"])
        script_name = spec["script"].split("/")[-1]
        assert script_name in blob, (
            f"{spec['sf_state']} no longer invokes {script_name} — the "
            f"{which} pass artifact's producer wiring drifted from "
            f"contracts/pit_stats_pass.consumer.json"
        )


def test_consumer_state_wired_to_declared_script(contract, sf):
    states = _all_states(sf)
    spec = contract["consumer"]
    state = states.get(spec["sf_state"])
    assert state is not None, "PitParityCompare missing — nothing consumes the pass artifacts"
    blob = json.dumps(state["Parameters"])
    assert spec["script"].split("/")[-1] in blob


def test_consumer_runs_after_all_producers(contract, sf):
    """The join must be OUTSIDE the Parallel that hosts the producers —
    a compare racing its own inputs would read partial artifacts."""
    states = sf["States"]
    assert contract["consumer"]["sf_state"] in states, (
        "PitParityCompare must be a top-level state (after the Parallel join)"
    )
    branch_state_names = {
        name
        for b in states["ParityParallel"]["Branches"]
        for name in b["States"]
    }
    for which, spec in contract["producers"].items():
        assert spec["sf_state"] in branch_state_names, (
            f"{spec['sf_state']} must live inside ParityParallel"
        )
    assert "PitParityCompare" not in branch_state_names, (
        "the compare must not run inside the Parallel it joins"
    )


def test_producer_scripts_exist_in_contract_only(contract):
    """The scripts live in crucible-backtester (a different repo) — this
    repo cannot verify their existence from a pure file read (the
    config#6684 constraint). What it CAN pin: every script named by the SF's
    parity-family states is declared in the contract file, so an SF edit
    that swaps a script must touch the contract too."""
    declared = {spec["script"].split("/")[-1] for spec in contract["producers"].values()}
    declared.add(contract["consumer"]["script"].split("/")[-1])
    assert declared == {
        "spot_pit_lookahead.sh",
        "spot_pit_walkforward.sh",
        "spot_parity_compare.sh",
    }


def test_replay_branch_is_not_a_pass_producer(sf, contract):
    """ParityReplay is deliberately NOT in the producers map: it consumes
    neither pass's output and produces parity_report.json, not a pit_stats
    artifact — bundling it with the passes was the §2.1 violation #6030
    removed. Pin that it stays out of the contract."""
    assert "replay" not in {p.lower() for p in contract["producers"]}
    states = _all_states(sf)
    blob = json.dumps(states["ParityReplay"]["Parameters"])
    assert "spot_parity_replay.sh" in blob
    assert "pit_stats" not in blob
