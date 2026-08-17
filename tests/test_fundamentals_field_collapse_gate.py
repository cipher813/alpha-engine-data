"""A field dead for the whole universe fails the collector (alpha-engine-config-I7583).

The per-ticker `ok_ratio` gate asks "did THIS TICKER return anything at all".
It cannot see a field that is dead for EVERY ticker: a ticker returning 13 of
14 fields counts as fully populated, so a field absent from the vendor response
for the whole universe passes it 903 times over.

That is how `capitalSpendingGrowth5Y` and `freeCashFlowTTM` — neither of which
exists in Finnhub's `metric=all` response — became a universe-wide 0.0 via
`_pick`'s `default=0.0`, with this collector reporting `ok` on every run for the
life of the Finnhub integration. It is also how the percent-point units bug
collapsed `gross_margin` and `roe` onto their clip ceilings for ~90% of the
universe. Both were fixed in alpha-engine-config-I7569 — by hand, after someone
asked a question. Nothing in the collector raised, and nothing would have raised
for the next one.

`_FUNDAMENTALS_FIELD_SPECS` cannot cover this: it validates values that `_clip`
has already bounded to its own bands, so it is structurally incapable of firing
on saturation.

These tests pin the gate that closes the class.
"""

from __future__ import annotations

import pytest

from collectors.fundamentals import (
    _FIELD_COLLAPSE_FAIL_SHARE,
    _FIELD_COLLAPSE_MIN_TICKERS,
    _FIELD_COLLAPSE_WARN_SHARE,
    _field_collapse_report,
)


def _universe(n: int, **overrides) -> list[dict]:
    """n records with a healthy spread, minus whatever the caller breaks."""
    records = []
    for i in range(n):
        rec = {
            "roe": 0.05 + i * 0.001,
            "gross_margin": 0.20 + i * 0.002,
            "fcf_yield": 0.01 + i * 0.0005,
        }
        rec.update({k: v for k, v in overrides.items()})
        records.append(rec)
    return records


class TestTheDefectsThatActuallyHappened:
    def test_an_absent_source_key_defaulting_to_zero_is_caught(self):
        """capitalSpendingGrowth5Y / freeCashFlowTTM: `_pick` returns 0.0 for
        every ticker because the vendor never exposes the key."""
        report = _field_collapse_report(_universe(903, capex_growth_5y=0.0))
        share, value = report["capex_growth_5y"]
        assert share == 1.0
        assert value == 0.0
        assert share >= _FIELD_COLLAPSE_FAIL_SHARE

    def test_clip_saturation_from_a_units_mismatch_is_caught(self):
        """Finnhub returns roeTTM=111.66 (percent points). Clipped as a
        fraction to [-1, 1], ~90% of the universe lands on 1.0 exactly."""
        records = _universe(900)
        for i, rec in enumerate(records):
            rec["roe"] = 1.0 if i < 830 else 0.4 + i * 0.0001
        share, value = _field_collapse_report(records)["roe"]
        assert value == 1.0
        assert _FIELD_COLLAPSE_WARN_SHARE <= share < _FIELD_COLLAPSE_FAIL_SHARE

    def test_a_healthy_field_is_not_flagged(self):
        share, _ = _field_collapse_report(_universe(903))["gross_margin"]
        assert share < _FIELD_COLLAPSE_WARN_SHARE


class TestPredicateEdges:
    def test_one_stray_ticker_cannot_mask_a_dead_field(self):
        """FAIL is 0.99, not 1.00, for exactly this reason."""
        records = _universe(903, fcf_yield=0.0)
        records[0]["fcf_yield"] = 0.037
        share, _ = _field_collapse_report(records)["fcf_yield"]
        assert share >= _FIELD_COLLAPSE_FAIL_SHARE

    def test_empty_input_reports_nothing_rather_than_dividing_by_zero(self):
        assert _field_collapse_report([]) == {}

    def test_thresholds_are_ordered(self):
        assert 0 < _FIELD_COLLAPSE_WARN_SHARE < _FIELD_COLLAPSE_FAIL_SHARE <= 1.0

    def test_a_field_absent_from_some_records_still_reports(self):
        """A field only some tickers carry must not crash the report — and its
        absence counts as a value, because `None` for most of the universe is
        the same defect wearing a different mask."""
        records = _universe(100)
        for rec in records[:99]:
            rec.pop("fcf_yield")
        share, value = _field_collapse_report(records)["fcf_yield"]
        assert value is None
        assert share >= _FIELD_COLLAPSE_FAIL_SHARE


class TestGateIsWiredIntoCollect:
    """Guard-the-guard: the predicate above is only worth anything if collect()
    actually consults it and actually refuses."""

    @staticmethod
    def _source() -> str:
        from pathlib import Path

        return (
            Path(__file__).resolve().parents[1] / "collectors" / "fundamentals.py"
        ).read_text(encoding="utf-8")

    def test_collect_calls_the_report(self):
        assert "_field_collapse_report(_real_records)" in self._source()

    def test_collect_returns_error_status_on_collapse(self):
        src = self._source()
        assert "Fundamentals collapse gate" in src
        assert '"status": "error"' in src

    def test_the_gate_runs_before_the_ok_ratio_gate(self):
        """ok_ratio cannot see this class, so a collapse must not be masked by
        an ok_ratio pass that returns first."""
        src = self._source()
        assert src.index("_field_collapse_report(_real_records)") < src.index(
            "if ok_ratio < _MIN_OK_RATIO:"
        )

    def test_neutral_records_are_excluded(self):
        """NEUTRAL rows are fetch failures already counted by ok_ratio; folding
        them in would let an outage manufacture a false collapse."""
        assert "if r != NEUTRAL" in self._source()

    def test_the_warn_band_does_not_halt(self):
        """alpha-engine-config-I7581: a new gate does not go straight to
        enforcing on a range whose real distribution has not been measured."""
        src = self._source()
        warn_at = src.index("Fundamentals field-collapse WARN")
        fail_at = src.index("Fundamentals collapse gate")
        between = src[warn_at:fail_at]
        assert '"status": "error"' not in between


@pytest.mark.parametrize("field", ["fcf_yield", "capex_growth_5y", "roe", "gross_margin"])
def test_every_field_i7569_had_to_fix_by_hand_is_now_machine_detectable(field):
    """The four fields alpha-engine-config-I7569 corrected manually. If this
    gate had existed, each would have failed the collector on its first run."""
    report = _field_collapse_report(_universe(903, **{field: 0.0}))
    share, _ = report[field]
    assert share >= _FIELD_COLLAPSE_FAIL_SHARE


class TestSampleSizeFloor:
    def test_the_floor_is_below_the_production_universe(self):
        """~900 tickers in production. The floor must never exempt a real run."""
        assert _FIELD_COLLAPSE_MIN_TICKERS < 500

    def test_the_floor_is_above_any_fixture(self):
        """Three identical fixture tickers must not read as a collapsed universe
        — that false positive broke six existing tests in this file's first
        draft, which is the evidence the floor is load-bearing rather than
        defensive."""
        assert _FIELD_COLLAPSE_MIN_TICKERS >= 20

    def test_a_below_floor_run_is_logged_not_silently_skipped(self):
        from pathlib import Path

        src = (
            Path(__file__).resolve().parents[1] / "collectors" / "fundamentals.py"
        ).read_text(encoding="utf-8")
        assert "field-collapse gate NOT APPLIED" in src
