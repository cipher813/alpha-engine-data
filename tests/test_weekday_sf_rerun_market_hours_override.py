"""`weekday_sf_rerun.py` emits the market-hours override (config-I7807).

`sf-pipeline-policy` §2.5 sets the target — *mean operator actions to recover
from any single-stage failure = 1*. §3's standing rule says a failed preopen is
relaunched **while the NYSE session is open**, and §3's reconciliation clause
makes `market_hours_override` *"the normal instrument of this rule, not an
exception to it"*.

The helper did not emit it, so every in-session recovery — the case §2.5 exists
for — halted at `MarketHoursBoundary`, and the operator hand-wrote the input the
helper had just printed. Measured twice on 2026-08-19:
`operator-rerun-2026-08-19-151046` FAILED in 3s, then
`...-151134-override` SUCCEEDED — the same command, run twice, the second time
with a field added by hand. Again on 2026-08-20.

The probe deliberately calls the gate's OWN Lambda action rather than deciding
market hours locally: a second implementation could disagree with the state that
enforces the boundary, which would either attach an override to a run that does
not need one or withhold it from a run that does, on a holiday or an early
close.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "weekday_sf_rerun.py"


def _load():
    spec = importlib.util.spec_from_file_location("weekday_sf_rerun", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    # Registered BEFORE exec: the module defines dataclasses, and
    # `dataclasses._is_type` resolves the defining module out of `sys.modules`
    # — an unregistered one raises AttributeError at class-creation time.
    sys.modules["weekday_sf_rerun"] = mod
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


mod = _load()

_OPEN_GATE = {
    "is_market_hours": True,
    "now_et": "2026-08-20T11:15:00-04:00",
    "session_window_et": "09:30-16:00",
    "reason": "regular-session",
}


# ── the override's shape ─────────────────────────────────────────────────────


def test_it_carries_the_three_fields_the_sf_consumes():
    ov = mod.build_market_hours_override(
        source_arn="arn:aws:states:us-east-1:1:execution:ne-preopen-trading-pipeline:abc123",
        source_status="FAILED",
        authorized_by="brian@nousergon.ai",
        reason=None,
        expires_at="2026-08-20T20:00:00Z",
    )
    assert set(ov) == {"reason", "authorized_by", "expires_at"}
    assert ov["authorized_by"] == "brian@nousergon.ai"


def test_the_default_reason_names_the_source_and_the_standing_rule():
    """§3 wants the crossing auditable. A reason an operator has to retype every
    time is how it stops being."""
    ov = mod.build_market_hours_override(
        source_arn="arn:aws:states:us-east-1:1:execution:ne-preopen-trading-pipeline:abc123",
        source_status="FAILED",
        authorized_by="x",
        reason=None,
        expires_at="2026-08-20T20:00:00Z",
    )
    assert "abc123" in ov["reason"]
    assert "FAILED" in ov["reason"]
    assert "SFP-3-preopen-same-day-relaunch" in ov["reason"]


def test_an_explicit_reason_wins():
    ov = mod.build_market_hours_override(
        source_arn="arn:...:abc123", source_status="FAILED", authorized_by="x",
        reason="because I said so", expires_at="2026-08-20T20:00:00Z",
    )
    assert ov["reason"] == "because I said so"


# ── expiry comes from the gate, not a constant ───────────────────────────────


def test_expires_at_is_todays_close_in_utc():
    assert mod.session_close_utc(_OPEN_GATE) == "2026-08-20T20:00:00Z"


def test_an_early_close_shortens_the_override():
    """A hardcoded 20:00Z would outlive a 13:00 ET half-day close and leave a
    valid override sitting past the session it authorized."""
    early = dict(_OPEN_GATE, session_window_et="09:30-13:00")
    assert mod.session_close_utc(early) == "2026-08-20T17:00:00Z"


# ── it lands in the emitted input, and only when needed ──────────────────────


def _plan(**kw):
    return mod.RerunPlan(
        pipeline_key="daily",
        run_date="2026-08-20",
        run_date_provenance="test",
        original_input={"pipeline_role": "daily"},
        emitted_role="daily",
        **kw,
    )


def test_the_override_reaches_the_start_execution_input():
    ov = {"reason": "r", "authorized_by": "a", "expires_at": "2026-08-20T20:00:00Z"}
    assert _plan(market_hours_override=ov).rerun_input()["market_hours_override"] == ov


def test_a_plan_without_one_emits_no_key_at_all():
    """A run that does not need the override must not silently carry one — an
    override attached to an out-of-hours start is an unconsidered crossing
    wearing a deliberate one's clothes."""
    assert "market_hours_override" not in _plan().rerun_input()


# ── the boundary between "needs one" and "does not" ──────────────────────────


def test_only_the_preopen_pipeline_can_need_an_override():
    """The postclose pipeline runs after the close by construction."""
    class _P:
        sm_arn = "arn:aws:states:us-east-1:1:stateMachine:ne-postclose-trading-pipeline"

    class _Q:
        sm_arn = "arn:aws:states:us-east-1:1:stateMachine:ne-preopen-trading-pipeline"

    assert mod.pipeline_is_preopen(_P()) is False
    assert mod.pipeline_is_preopen(_Q()) is True


@pytest.mark.parametrize("window,expected", [("09:30-16:00", "2026-08-20T20:00:00Z"),
                                             ("09:30-16:15", "2026-08-20T20:15:00Z")])
def test_the_window_is_read_not_assumed(window, expected):
    assert mod.session_close_utc(dict(_OPEN_GATE, session_window_et=window)) == expected
