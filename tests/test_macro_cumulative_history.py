"""alpha-engine-config-I10054 option (a) — cumulative ArcticDB history for the
un-adjusted (FRED-sourced) macro series only.

Measured origin (issue body, 2026-09-05):

  * ``collectors/prices.py`` refreshes the price cache as a rolling 10-year
    yfinance ``auto_adjust=True`` FULL REPLACE (append is unsafe for an
    adjusted series — the whole history is re-scaled on every corporate
    action).
  * ``collectors/fred_history.py`` fetches a trailing ``period_years`` window
    the same way.
  * ``builders/backfill.py`` writes each ArcticDB symbol wholesale
    (``lib.write``), so the head version loses its oldest week every Saturday.
    ``macro/HYOAS``'s 2023-05-09..2023-09-04 rows left the head version on
    2026-09-05 once I9287 unstuck its 3-year window.

FRED-sourced indices carry no split/dividend adjustment, so prepending the
rows we already hold is exact and seamless. Adjusted ETF/equity series stay
rolling until Crucible v2 owns the data plane.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import builders.backfill as _bf
from collectors.fred_history import FRED_HISTORY_MAP


def _series(first: str, n: int, start: float = 10.0, stop: float = 20.0) -> pd.Series:
    idx = pd.bdate_range(first, periods=n)
    s = pd.Series(np.linspace(start, stop, n), index=idx)
    s.index.name = "date"
    return s


def _frame(first: str, n: int, start: float = 10.0, stop: float = 20.0) -> pd.DataFrame:
    s = _series(first, n, start, stop)
    df = pd.DataFrame({"Close": s.values}, index=s.index)
    df.index.name = "date"
    return df


class _FakeLib:
    """Minimal ArcticDB library stand-in (mirrors the I9256 guard tests)."""

    def __init__(self, data: dict[str, pd.DataFrame] | None = None):
        self.data = dict(data or {})
        self.writes: list[tuple[str, int]] = []

    def has_symbol(self, symbol):
        return symbol in self.data

    def list_symbols(self):
        return list(self.data)

    def read(self, symbol, **kwargs):
        if symbol not in self.data:
            raise KeyError(symbol)
        return type("R", (), {"data": self.data[symbol]})()

    def tail(self, symbol, n=1, **kwargs):
        if symbol not in self.data:
            raise KeyError(symbol)
        return type("R", (), {"data": self.data[symbol].tail(n)})()

    def write(self, symbol, df, **kwargs):
        self.data[symbol] = df
        self.writes.append((symbol, len(df)))


# ── the declared registry ────────────────────────────────────────────────────


def test_cumulative_registry_is_exactly_the_fred_sourced_set():
    """The retention ruling is a DECLARED set, and it must stay identical to
    the single declared FRED source map (``FRED_HISTORY_MAP``, merged as the
    one source by alpha-engine-config-I9286).

    This is the guard that makes the registry maintainable: adding a FRED
    series to the collector without ruling on its ArcticDB retention fails
    here rather than silently landing on the rolling default. It is asserted,
    never derived — a set derived from the map at import time could never
    fail, and 'FRED-sourced implies un-adjusted' is a judgement about the
    data, not a property of the dict.
    """
    assert _bf.CUMULATIVE_MACRO_SYMBOLS == frozenset(FRED_HISTORY_MAP), (
        "CUMULATIVE_MACRO_SYMBOLS and FRED_HISTORY_MAP have drifted. A new "
        "FRED series needs an explicit retention ruling in "
        "builders/backfill.py::CUMULATIVE_MACRO_SYMBOLS (and a row in "
        "features/SCHEMA.md §2b) — see alpha-engine-config-I10054."
    )


def test_the_adjusted_macro_symbols_are_not_declared_cumulative():
    """The yfinance ``auto_adjust`` families must stay rolling: splicing an
    old row onto a re-adjusted series is a silent mid-series seam."""
    for symbol in ("SPY", "GLD", "USO", "XLK", "XLF", "SMH", "IGV"):
        assert symbol not in _bf.CUMULATIVE_MACRO_SYMBOLS


def test_registry_membership_is_not_inferable_from_the_symbol_name():
    """Guards against a future 'looks like an index' heuristic replacing the
    declaration: SPY and VIX are indistinguishable by shape, and TWO/BAA10Y
    look nothing alike."""
    assert "VIX" in _bf.CUMULATIVE_MACRO_SYMBOLS
    assert "VIX3M" in _bf.CUMULATIVE_MACRO_SYMBOLS
    assert "TWO" in _bf.CUMULATIVE_MACRO_SYMBOLS
    assert "BAA10Y" in _bf.CUMULATIVE_MACRO_SYMBOLS


# ── the prepend at the write boundary ────────────────────────────────────────


def test_prepend_happens_for_a_declared_symbol():
    """The measured HYOAS shape: a 3y FRED window that slid ~85 sessions.
    The written head is the union, starting where ArcticDB already started."""
    lib = _FakeLib({"HYOAS": _frame("2023-05-09", 786)})
    planned = _frame("2023-09-05", 783)

    _bf._write_macro_series_no_shrink(lib, "HYOAS", planned)

    written = lib.data["HYOAS"]
    assert written.index[0] == pd.Timestamp("2023-05-09")
    assert written.index[-1] == planned.index[-1]
    assert written.index.is_monotonic_increasing
    assert not written.index.has_duplicates
    # Union: every existing date older than the planned window, plus all of it.
    older = _frame("2023-05-09", 786).loc[lambda d: d.index < planned.index[0]]
    assert len(written) == len(older) + len(planned)


def test_prepend_does_not_happen_for_an_adjusted_symbol():
    """SPY is not declared cumulative — the rolling window is written as-is,
    because ``auto_adjust`` re-scales its whole history on every action."""
    lib = _FakeLib({"SPY": _frame("2016-08-29", 2514)})
    planned = _frame("2016-09-06", 2514)

    _bf._write_macro_series_no_shrink(lib, "SPY", planned)

    assert lib.writes == [("SPY", 2514)]
    assert lib.data["SPY"].index[0] == planned.index[0]


def test_overlapping_dates_take_the_planned_value():
    """The fresh window is authoritative wherever the two overlap — a FRED
    restatement of a recent print must not be shadowed by the stored copy."""
    existing = _frame("2024-01-01", 200, start=1.0, stop=1.0)
    planned = _frame("2024-04-01", 160, start=9.0, stop=9.0)
    lib = _FakeLib({"VIX": existing})

    _bf._write_macro_series_no_shrink(lib, "VIX", planned)

    written = lib.data["VIX"]
    overlap = written.index.intersection(planned.index)
    assert len(overlap) == len(planned)
    assert (written.loc[overlap, "Close"] == 9.0).all()
    # And the pre-window rows are the stored ones, untouched.
    assert (written.loc[written.index < planned.index[0], "Close"] == 1.0).all()


def test_prepend_is_idempotent_on_a_second_run():
    """A second rebuild from the same source window reproduces the same head
    exactly — the union of (union, planned) is the union."""
    lib = _FakeLib({"TNX": _frame("2016-01-04", 2500)})
    planned = _frame("2016-03-01", 2600)

    _bf._write_macro_series_no_shrink(lib, "TNX", planned)
    first_write = lib.data["TNX"].copy()

    _bf._write_macro_series_no_shrink(lib, "TNX", planned)
    second_write = lib.data["TNX"]

    pd.testing.assert_frame_equal(first_write, second_write)
    assert len(lib.writes) == 2


def test_prepend_is_a_noop_when_the_window_did_not_slide(caplog):
    """No older rows to restore → nothing prepended, nothing logged."""
    lib = _FakeLib({"IRX": _frame("2016-08-19", 2513)})
    planned = _frame("2016-08-19", 2514)

    with caplog.at_level("INFO"):
        _bf._write_macro_series_no_shrink(lib, "IRX", planned)

    assert lib.writes == [("IRX", 2514)]
    assert not any("MACRO_HISTORY_PREPENDED" in r.getMessage() for r in caplog.records)


def test_prepend_emits_a_reconstructible_log_record(caplog):
    """``MACRO_HISTORY_PREPENDED symbol=… rows=… first=…`` — the prepend must
    be reconstructible from durable logs alone."""
    lib = _FakeLib({"BAA10Y": _frame("1986-01-02", 500)})
    planned = _frame("1986-06-02", 500)

    with caplog.at_level("INFO"):
        _bf._write_macro_series_no_shrink(lib, "BAA10Y", planned)

    prepends = [r for r in caplog.records if "MACRO_HISTORY_PREPENDED" in r.getMessage()]
    assert len(prepends) == 1
    message = prepends[0].getMessage()
    assert "symbol=BAA10Y" in message
    assert "rows=" in message
    assert "first=1986-01-02" in message


def test_prepend_handles_a_tz_aware_stored_index():
    """A stored index carrying a timezone must not defeat the date comparison
    or produce a mixed-tz written index."""
    existing = _frame("2024-01-01", 200)
    existing.index = existing.index.tz_localize("UTC")
    lib = _FakeLib({"VIX3M": existing})
    planned = _frame("2024-04-01", 160)

    _bf._write_macro_series_no_shrink(lib, "VIX3M", planned)

    written = lib.data["VIX3M"]
    assert written.index.tz is None
    assert written.index.is_monotonic_increasing
    assert written.index[0] == pd.Timestamp("2024-01-01")


# ── the guard, made consistent with the new behaviour ────────────────────────


def test_a_slide_on_a_declared_cumulative_symbol_is_refused_when_unprepended(monkeypatch):
    """A cumulative series never slides. If the prepend is bypassed or skipped
    for any reason, the written frame still carries the slide — and that is a
    REFUSAL, not a warning: publishing a truncated head under a cumulative
    contract is the exact loss I10054 closes."""
    monkeypatch.setattr(
        _bf, "_cumulative_prepend", lambda symbol, existing, planned: (planned, 0, None)
    )
    lib = _FakeLib({"HYOAS": _frame("2023-05-09", 786)})
    planned = _frame("2023-09-05", 783)

    with pytest.raises(RuntimeError, match="declared\\s+CUMULATIVE"):
        _bf._write_macro_series_no_shrink(lib, "HYOAS", planned)

    assert lib.writes == [], "a refused write must not reach the library"


def test_a_column_mismatch_skips_the_prepend_loudly_and_then_refuses(caplog):
    """A macro schema change under an existing symbol must not fabricate NaN
    columns. The prepend is skipped with a greppable finding, and the
    cumulative invariant then refuses the (now truncating) write."""
    existing = _frame("2023-05-09", 786).rename(columns={"Close": "value"})
    lib = _FakeLib({"HYOAS": existing})
    planned = _frame("2023-09-05", 783)

    with caplog.at_level("WARNING"):
        with pytest.raises(RuntimeError, match="declared\\s+CUMULATIVE"):
            _bf._write_macro_series_no_shrink(lib, "HYOAS", planned)

    assert any(
        "MACRO_HISTORY_PREPEND_SKIPPED" in r.getMessage() and "column_mismatch" in r.getMessage()
        for r in caplog.records
    )


def test_the_source_truncation_detector_still_fires_on_a_cumulative_symbol():
    """Detection blindness outranks the defect it hides: the prepend closes
    the LOSS, but a source answering 16 rows where it answered 2509 last week
    is still a producer defect that must page (I9256) — it must not be
    silently repaired into looking healthy."""
    lib = _FakeLib({"VIX3M": _frame("2016-08-19", 2509)})
    planned = _frame("2016-08-19", 2509).tail(16)

    with pytest.raises(RuntimeError, match="would TRUNCATE"):
        _bf._write_macro_series_no_shrink(lib, "VIX3M", planned)

    assert lib.writes == []


def test_a_first_write_of_a_cumulative_symbol_is_not_a_violation():
    """Nothing established to be cumulative against."""
    lib = _FakeLib({})
    _bf._write_macro_series_no_shrink(lib, "TWO", _frame("2016-08-19", 2514))
    assert lib.writes == [("TWO", 2514)]


# ── the preflight's wording stays honest ─────────────────────────────────────


def test_history_regression_slide_wording_differs_by_retention(caplog):
    existing_rows, existing_first = 786, pd.Timestamp("2023-05-09")
    planned = _frame("2023-09-05", 783)

    with caplog.at_level("WARNING"):
        assert _bf._history_regression(
            "macro.HYOAS", planned, existing_rows, existing_first, cumulative=True,
        ) is None
    assert any("retained by the cumulative prepend" in r.getMessage() for r in caplog.records)

    caplog.clear()
    with caplog.at_level("WARNING"):
        assert _bf._history_regression(
            "macro.GLD", planned, existing_rows, existing_first, cumulative=False,
        ) is None
    assert any("gone from ArcticDB's head version" in r.getMessage() for r in caplog.records)
