"""An absent vendor key fails the collector by NAME (alpha-engine-config-I7583).

`_pick(metrics, *keys, default=0.0)` returns 0.0 when the response carries none
of the requested keys, and nothing distinguished that from a genuine zero.
`capitalSpendingGrowth5Y` and `freeCashFlowTTM` do not exist in Finnhub's
`metric=all` response AT ALL, so `capex_growth_5y` and `fcf_yield` read as a
universe-wide 0.0 for the life of the integration — with this collector
reporting `ok` on every run. They were found by hand off a question in a chat
(alpha-engine-config-I7569), not by any check.

The collapse gate (`test_fundamentals_field_collapse_gate.py`) catches that
class by its SYMPTOM. This one catches it at the CAUSE, and the two are kept
deliberately, because each sees something the other cannot:

  cause-side  names the missing VENDOR KEYS, not the constant they produced,
              and still fires when an absent field's default happens to VARY.
  symptom-side catches every other route to a dead column — units saturation,
              a scaling divisor, a vendor pinning a field to a constant —
              without needing to know the cause.
"""

from __future__ import annotations

import pytest

from collectors.fundamentals import (
    _FIELD_SOURCE_KEYS,
    _SOURCE_ABSENCE_FAIL_SHARE,
    NEUTRAL,
    _absent_source_fields,
)


def _full_response() -> dict:
    """One vendor key present for every declared field."""
    return {keys[0]: 1.0 for keys in _FIELD_SOURCE_KEYS.values()}


class TestAbsenceDetection:
    def test_a_complete_response_has_nothing_absent(self):
        assert _absent_source_fields(_full_response()) == set()

    def test_the_two_keys_finnhub_never_exposed(self):
        """The measured instance. Neither `capitalSpendingGrowth5Y` nor
        `freeCashFlowTTM` exists in the response, so both fields' whole key
        lists are missing."""
        metrics = _full_response()
        for field in ("capex_growth_5y", "fcf_yield"):
            for key in _FIELD_SOURCE_KEYS[field]:
                metrics.pop(key, None)
        assert _absent_source_fields(metrics) == {"capex_growth_5y", "fcf_yield"}

    def test_a_fallback_key_still_counts_as_present(self):
        """Absence means NONE of the fallbacks landed — a field served by its
        second choice is covered, not missing."""
        metrics = _full_response()
        keys = _FIELD_SOURCE_KEYS["pe_ratio"]
        assert len(keys) > 1
        metrics.pop(keys[0])
        metrics[keys[1]] = 12.0
        assert "pe_ratio" not in _absent_source_fields(metrics)

    def test_an_explicit_null_is_absent_not_present(self):
        """Finnhub returns the key with a `null` value for fields it does not
        carry for a given ticker; `_pick` skips those, so the gate must too."""
        metrics = _full_response()
        for key in _FIELD_SOURCE_KEYS["roe"]:
            metrics[key] = None
        assert "roe" in _absent_source_fields(metrics)

    def test_an_empty_response_is_entirely_absent(self):
        assert _absent_source_fields({}) == set(_FIELD_SOURCE_KEYS)


class TestDeclarationIsTheSingleOwner:
    def test_every_declared_field_is_a_real_output_field(self):
        """The declaration must describe fields the collector actually emits —
        otherwise the gate guards names nothing reads."""
        unknown = set(_FIELD_SOURCE_KEYS) - set(NEUTRAL)
        assert not unknown, f"declared but not emitted: {sorted(unknown)}"

    def test_every_vendor_sourced_output_field_is_declared(self):
        """Guard-the-guard, and the property that actually matters: a field
        added to the collector without an entry here is a field the gate is
        blind to — exactly the state that let two dead columns ship."""
        undeclared = set(NEUTRAL) - set(_FIELD_SOURCE_KEYS)
        assert not undeclared, (
            f"emitted but not declared: {sorted(undeclared)} — add its vendor "
            "keys to _FIELD_SOURCE_KEYS or the absent-source gate cannot see it."
        )

    def test_the_picks_go_through_the_declaration(self):
        """One owner. If a call site re-inlines its key list the declaration can
        drift from what is actually read, and the gate silently checks the wrong
        keys."""
        from pathlib import Path

        src = (
            Path(__file__).resolve().parents[1] / "collectors" / "fundamentals.py"
        ).read_text(encoding="utf-8")
        body = src.split("def _fetch_single_ticker", 1)[1]
        # Every _pick call inside the fetch must unpack the declaration.
        for line in body.splitlines():
            if "_pick(metrics" in line:
                assert "_FIELD_SOURCE_KEYS[" in line or line.rstrip().endswith("_pick("), (
                    f"_pick call site does not read the declaration: {line.strip()}"
                )


class TestGateWiring:
    @staticmethod
    def _source() -> str:
        from pathlib import Path

        return (
            Path(__file__).resolve().parents[1] / "collectors" / "fundamentals.py"
        ).read_text(encoding="utf-8")

    def test_the_transport_key_never_reaches_a_consumer(self):
        """It rides on the per-ticker record only to get out of the fetch; if it
        survived it would break the `!= NEUTRAL` comparison, the value-range
        gate, and the S3 snapshot's shape."""
        src = self._source()
        assert "data.pop(_ABSENT_KEY, None)" in src

    def test_the_gate_returns_error_status(self):
        assert "Fundamentals absent-source gate" in self._source()

    def test_the_gate_runs_before_the_collapse_gate(self):
        src = self._source()
        assert src.index("Fundamentals absent-source gate") < src.index(
            "Fundamentals collapse gate"
        )

    def test_sub_threshold_absence_is_recorded_not_dropped(self):
        """A field drifting from 5% to 60% absent is a vendor deprecating a key.
        It should be visible before it crosses the threshold, not at the moment
        it halts a run."""
        assert "Fundamentals source-key coverage over" in self._source()

    def test_the_fail_share_leaves_room_for_real_sparsity(self):
        assert 0.5 < _SOURCE_ABSENCE_FAIL_SHARE < 1.0


@pytest.mark.parametrize("field", ["capex_growth_5y", "fcf_yield"])
def test_both_i7569_fields_would_have_been_caught_on_day_one(field):
    metrics = _full_response()
    for key in _FIELD_SOURCE_KEYS[field]:
        metrics.pop(key, None)
    assert field in _absent_source_fields(metrics)
