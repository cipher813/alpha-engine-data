"""The weekly launcher spot's capacity surface (alpha-engine-config-I7133).

`spot_dispatch.launch_with_fallback` rotates instance_type x subnet on a
capacity error, so the number of DISTINCT pools it can fall through is what
decides whether a capacity dip is survivable. The pool was 4 types of adjacent
generations in two families.

Measured 2026-08-12: 3 of 11 recent spot requests in this account died
`instance-terminated-no-capacity`, one of them mid-`DataPhase1` on the
scheduled weekly run (config-I7119).

config-I7119 makes a mid-run reclaim RECOVERABLE. This pins the properties
that make it RARER — recovery still costs a relaunch, a re-bootstrap and the
stage's runtime, so it is the floor rather than the goal.

These assert PROPERTIES, not a literal list: adding a type should not require
editing a test, but adding an arm64 or a 1-vCPU type must fail loudly.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_DISPATCHER = (
    Path(__file__).resolve().parents[1]
    / "infrastructure"
    / "lambdas"
    / "weekly-freshness-spot-dispatcher"
    / "index.py"
)

# x86_64, 2 vCPU, >= 4 GiB — the floor is c5.large, which already runs this
# workload successfully. arm64 families are EXCLUDED on purpose: the dispatcher
# pins an x86_64 AL2023 AMI, so an arm64 type fails the architecture check at
# launch. That failure is the reason this is a test and not a comment.
_X86_2VCPU_FAMILIES = {
    "c5", "c5a", "c5n", "c6i", "c6a", "c6in",
    "m5", "m5a", "m5n", "m6i", "m6a",
    "r5", "r5a", "r5n", "r6i", "r6a",
}
_ARM64_FAMILIES = {"c6g", "c7g", "m6g", "m7g", "r6g", "r7g", "t4g", "c8g", "m8g"}


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location(
        "weekly_freshness_spot_dispatcher", _DISPATCHER
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = m
    spec.loader.exec_module(m)
    return m


def _families(types: list[str]) -> set[str]:
    return {t.split(".", 1)[0] for t in types}


def test_the_pool_spans_enough_distinct_capacity_pools(mod):
    """Pool COUNT is the mechanism — one type per family is one pool."""
    types = mod.INSTANCE_TYPES
    assert len(types) >= 8, (
        f"only {len(types)} instance types; launch_with_fallback has too few "
        f"pools to rotate through on a capacity dip: {types}"
    )
    assert len(_families(types)) >= 5, (
        f"types cluster into too few families {sorted(_families(types))} — "
        f"capacity events correlate WITHIN a family, so 10 types across 2 "
        f"families is not 10 pools"
    )


def test_every_type_is_x86_64(mod):
    """The AMI is x86_64 AL2023; an arm64 type fails at launch, not at review."""
    arm = _families(mod.INSTANCE_TYPES) & _ARM64_FAMILIES
    assert not arm, (
        f"arm64 families {sorted(arm)} in the pool, but WEEKLY_SPOT_AMI_ID is "
        f"x86_64 — every rotation onto one of these fails the architecture check"
    )


def test_every_type_is_a_known_2vcpu_x86_family_at_or_above_the_floor(mod):
    """c5.large (2 vCPU / 4 GiB) is the measured floor — it already runs this
    workload. A smaller type would fail somewhere inside a multi-hour stage."""
    unknown = _families(mod.INSTANCE_TYPES) - _X86_2VCPU_FAMILIES
    assert not unknown, (
        f"unrecognised families {sorted(unknown)}; add them to "
        f"_X86_2VCPU_FAMILIES only after confirming x86_64 + >= c5.large specs"
    )
    sizes = {t.split(".", 1)[1] for t in mod.INSTANCE_TYPES}
    assert sizes == {"large"}, (
        f"mixed sizes {sorted(sizes)} — a rotation onto a smaller size changes "
        f"the workload's resources silently mid-pipeline"
    )


def test_the_subnets_span_multiple_azs(mod):
    """Type rotation is only half the surface; a single AZ re-enters the same
    physical capacity pool however many types are tried."""
    assert len(mod.SUBNETS) >= 3, f"too few subnets to rotate: {mod.SUBNETS}"


def test_the_pool_is_env_overridable(mod, monkeypatch):
    """The override is the operator's escape hatch during a capacity event —
    it must not have been hardcoded away while widening the default."""
    src = _DISPATCHER.read_text()
    assert 'os.environ.get(\n        "WEEKLY_SPOT_INSTANCE_TYPES"' in src or (
        '"WEEKLY_SPOT_INSTANCE_TYPES"' in src and "os.environ.get" in src
    )


def test_on_demand_fallback_is_still_reachable(mod):
    """Widening the pool must not become a REPLACEMENT for the on-demand
    escape: capacity can be exhausted across every pool at once."""
    src = _DISPATCHER.read_text()
    assert "force_on_demand" in src
    assert "launch_with_fallback" in src
