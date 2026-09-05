"""Regression tests for alpha-engine-config-I9256 — a macro series that loses
its history must RAISE, never be written.

Measured origin (2026-08-29, ArcticDB version history for ``macro/VIX3M``):

    ver=353 2026-08-22 09:10:46  rows=2509 first=2016-08-19 last=2026-08-21
    ver=354 2026-08-22 10:42:40  rows=  16 first=2026-07-31 last=2026-08-21   <-- backfill
    ver=365 2026-08-29 09:10:52  rows=  21 first=2026-07-31 last=2026-08-28
    ver=366 2026-08-29 10:48:39  rows=  16 first=2026-08-07 last=2026-08-28   <-- backfill

Both truncating writes kept the LAST date identical, so ``_assert_no_arctic_regression``
(which compared last dates only) passed clean. These tests fail against the
pre-fix code.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import builders.backfill as _bf


def _series(first: str, n: int) -> pd.Series:
    idx = pd.bdate_range(first, periods=n)
    return pd.Series(np.linspace(10.0, 20.0, n), index=idx)


def _frame(first: str, n: int) -> pd.DataFrame:
    s = _series(first, n)
    df = pd.DataFrame({"Close": s.values}, index=s.index)
    df.index.name = "date"
    return df


class _FakeLib:
    """Minimal ArcticDB library stand-in."""

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


# ── the write boundary ───────────────────────────────────────────────────────

def test_write_boundary_refuses_the_measured_vix3m_truncation():
    """2509 rows -> 16 rows with the SAME last date must raise."""
    existing = _frame("2016-08-19", 2509)
    lib = _FakeLib({"VIX3M": existing})
    planned = existing.tail(16)

    with pytest.raises(RuntimeError, match="would TRUNCATE"):
        _bf._write_macro_series_no_shrink(lib, "VIX3M", planned)

    assert lib.writes == [], "a refused write must not reach the library"
    assert len(lib.data["VIX3M"]) == 2509


# ── rolling-window slide vs truncation (2026-09-05 weekly SF failure) ─────────
#
# The price cache is BY DESIGN a rolling 10-year, fully re-adjusted window
# (collectors/prices.py: "full replace, not append", yfinance auto_adjust), so
# its FIRST date advances at every refresh. The first live Saturday after the
# I9256 guard landed (2026-09-05 10:48 UTC) it refused all 17 macro symbols:
#
#     macro.SPY: planned_first=2016-09-06 > existing_first=2016-08-29
#     macro.HYOAS: planned_first=2023-09-05 > existing_first=2023-05-09
#
# A window that slides forward at both ends is not truncation. Truncation is a
# start that jumps forward by more than any rebuild cadence can explain, or a
# row loss the slide does not account for.

def test_write_boundary_allows_a_one_week_rolling_window_slide():
    """The measured 2026-09-05 SPY shape: same length, start one week later."""
    lib = _FakeLib({"SPY": _frame("2016-08-29", 2514)})
    planned = _frame("2016-09-06", 2514)
    _bf._write_macro_series_no_shrink(lib, "SPY", planned)
    assert lib.writes == [("SPY", 2514)]


def test_write_boundary_allows_a_slide_after_a_source_freeze(caplog):
    """The measured 2026-09-05 HYOAS shape: a 3y FRED window that had been
    frozen for four months (I9287) and then advanced ~85 sessions at once.
    Allowed, but LOUD — a start moving more than a few weeks means a freeze
    released or rebuilds were missed.

    HYOAS is a DECLARED-CUMULATIVE symbol (alpha-engine-config-I10054), so the
    rows the window slid past are restored by the prepend rather than lost:
    the SOURCE still slid (the detector must still say so — that is the I9287
    signal) but ArcticDB's head keeps its 2023-05-09 start."""
    lib = _FakeLib({"HYOAS": _frame("2023-05-09", 786)})
    planned = _frame("2023-09-05", 783)
    with caplog.at_level("INFO"):
        _bf._write_macro_series_no_shrink(lib, "HYOAS", planned)
    assert any("MACRO_WINDOW_SLIDE" in r.message for r in caplog.records)
    # The written frame is the union, not the source window.
    (written_symbol, written_rows), = lib.writes
    assert written_symbol == "HYOAS"
    assert written_rows > 783
    assert lib.data["HYOAS"].index[0] == pd.Timestamp("2023-05-09")


def test_write_boundary_slide_after_a_freeze_is_lossy_on_an_adjusted_symbol(caplog):
    """The same shape on an ADJUSTED symbol (auto_adjust ETF) stays rolling:
    written as-is, and the finding says the history really is gone."""
    lib = _FakeLib({"GLD": _frame("2023-05-09", 786)})
    planned = _frame("2023-09-05", 783)
    with caplog.at_level("WARNING"):
        _bf._write_macro_series_no_shrink(lib, "GLD", planned)
    assert lib.writes == [("GLD", 783)]
    assert any(
        "MACRO_WINDOW_SLIDE" in r.message
        and "gone from ArcticDB's head version" in r.getMessage()
        for r in caplog.records
    )


def test_write_boundary_allows_a_missed_rebuild_week():
    """Two weeks of daily_append rows on top of the last rebuild, then a
    rebuild from a window that slid two weeks: net loss of ~10 rows is the
    slide, not a truncation."""
    lib = _FakeLib({"GLD": _frame("2016-08-15", 2524)})
    planned = _frame("2016-08-29", 2514)
    _bf._write_macro_series_no_shrink(lib, "GLD", planned)
    assert lib.writes == [("GLD", 2514)]


def test_write_boundary_refuses_a_start_that_jumps_past_the_slide_allowance():
    """Same row count, start two years later: a frequency/coverage change,
    not a rolling window — refused."""
    lib = _FakeLib({"VIX3M": _frame("2016-08-19", 2514)})
    planned = _frame("2018-08-20", 2514)
    with pytest.raises(RuntimeError, match="history start moved forward"):
        _bf._write_macro_series_no_shrink(lib, "VIX3M", planned)
    assert lib.writes == []


def test_write_boundary_refuses_a_row_loss_the_slide_does_not_explain():
    """Start unchanged, 30 rows gone from the body/tail — truncation."""
    lib = _FakeLib({"USO": _frame("2016-08-19", 2514)})
    planned = _frame("2016-08-19", 2484)
    with pytest.raises(RuntimeError, match="planned_rows=2484 < existing_rows=2514"):
        _bf._write_macro_series_no_shrink(lib, "USO", planned)


def test_write_boundary_allows_a_normal_append():
    lib = _FakeLib({"SPY": _frame("2016-08-19", 2513)})
    _bf._write_macro_series_no_shrink(lib, "SPY", _frame("2016-08-19", 2514))
    assert lib.writes == [("SPY", 2514)]


def test_write_boundary_allows_a_first_write():
    lib = _FakeLib({})
    _bf._write_macro_series_no_shrink(lib, "NEWSYM", _frame("2026-07-23", 26))
    assert lib.writes == [("NEWSYM", 26)]


def test_write_boundary_tolerates_a_small_restatement():
    """A vendor dropping a couple of bad prints is not a truncation."""
    lib = _FakeLib({"GLD": _frame("2016-08-19", 2514)})
    _bf._write_macro_series_no_shrink(lib, "GLD", _frame("2016-08-19", 2512))
    assert lib.writes == [("GLD", 2512)]


def test_write_boundary_raises_when_the_existing_symbol_is_unreadable():
    """An unreadable existing symbol must not be treated as absent."""

    class _Broken(_FakeLib):
        def read(self, symbol, **kwargs):
            raise RuntimeError("s3 timeout")

    lib = _Broken({"VIX3M": _frame("2016-08-19", 2509)})
    with pytest.raises(RuntimeError, match="could not read existing ArcticDB symbol"):
        _bf._write_macro_series_no_shrink(lib, "VIX3M", _frame("2026-08-07", 16))


# ── the backfill preflight ───────────────────────────────────────────────────

def test_preflight_catches_a_truncation_that_keeps_the_last_date(monkeypatch):
    """The exact 2026-08-22 shape: last date unchanged, 2509 -> 16 rows."""
    existing = _frame("2016-08-19", 2509)
    macro_lib = _FakeLib({"VIX3M": existing})
    universe_lib = _FakeLib({})

    monkeypatch.setattr(_bf, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_bf, "get_universe_lib", lambda *a, **k: universe_lib)

    planned_macro = {"VIX3M": existing["Close"].tail(16)}

    with pytest.raises(RuntimeError, match=r"planned_rows=16 < existing_rows=2509"):
        _bf._assert_no_arctic_regression(
            "alpha-engine-research", planned_macro, {}, "2026-08-22",
        )


def test_preflight_passes_a_healthy_macro_frame(monkeypatch):
    # Current through the reference date — alpha-engine-config-I9287's
    # staleness check would otherwise fail this "healthy" fixture too.
    existing = _series("2016-08-19", 2615)
    existing_frame = pd.DataFrame({"Close": existing.values}, index=existing.index)
    existing_frame.index.name = "date"
    macro_lib = _FakeLib({"SPY": existing_frame})
    universe_lib = _FakeLib({})
    monkeypatch.setattr(_bf, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_bf, "get_universe_lib", lambda *a, **k: universe_lib)

    planned_macro = {"SPY": _series("2016-08-19", 2616)}
    _bf._assert_no_arctic_regression(
        "alpha-engine-research", planned_macro, {}, "2026-08-29",
    )


# ── staleness regression (alpha-engine-config-I9287) ─────────────────────────
# ``macro/HYOAS`` recorded continuous weekly + daily ArcticDB versions right
# through 2026-08-29 while every version's last row stayed 2026-05-07 — row
# count and first date never moved, so the shrink/first-date checks above
# passed it clean every single week. Only the max-date lag reveals it.

def test_write_boundary_refuses_the_measured_hyoas_staleness():
    """786 rows, never shrinking, last row frozen at 2026-05-07 while the
    reference date is 2026-08-29 — ~75 trading days stale."""
    lib = _FakeLib({"HYOAS": _frame("2023-05-09", 786)})
    # Same row count and start, but the payload never advanced.
    frozen = _frame("2023-05-09", 786)

    with pytest.raises(RuntimeError, match="rewriting a FROZEN upstream value"):
        _bf._write_macro_series_no_shrink(lib, "HYOAS", frozen, reference_date="2026-08-29")

    assert lib.writes == [], "a refused write must not reach the library"


def test_write_boundary_tolerates_ordinary_publication_lag():
    """A few trading days behind (FRED publication lag) must NOT raise."""
    lib = _FakeLib({"SPY": _frame("2016-08-19", 2610)})
    planned = _frame("2016-08-19", 2612)  # 2 rows fresher, still within tolerance

    _bf._write_macro_series_no_shrink(lib, "SPY", planned, reference_date="2026-08-29")
    assert lib.writes == [("SPY", 2612)]


def test_write_boundary_skips_staleness_check_with_no_reference_date():
    """``reference_date=None`` (no run_date known to the caller) must not raise —
    it has no basis to judge staleness, so it defers to the shrink check alone."""
    lib = _FakeLib({"HYOAS": _frame("2023-05-09", 786)})
    frozen = _frame("2023-05-09", 786)
    _bf._write_macro_series_no_shrink(lib, "HYOAS", frozen)  # no reference_date
    assert lib.writes == [("HYOAS", 786)]


def test_preflight_catches_the_measured_hyoas_staleness(monkeypatch):
    macro_lib = _FakeLib({"HYOAS": _frame("2023-05-09", 786)})
    universe_lib = _FakeLib({})
    monkeypatch.setattr(_bf, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_bf, "get_universe_lib", lambda *a, **k: universe_lib)

    planned_macro = {"HYOAS": _series("2023-05-09", 786)}
    with pytest.raises(RuntimeError, match="trading days behind"):
        _bf._assert_no_arctic_regression(
            "alpha-engine-research", planned_macro, {}, "2026-08-29",
        )


# ── first-write visibility (alpha-engine-config-I9289) ───────────────────────

def test_first_write_of_a_short_new_symbol_is_logged_not_blocked(caplog):
    """The measured sub-sector-ETF shape: 26 rows next to SPY's ~2514 must
    WRITE (never blocked — a short-lived listing is legitimate) but must be
    LOUD about it."""
    lib = _FakeLib({"SPY": _frame("2016-08-19", 2514)})
    planned = _frame("2026-07-23", 26)

    with caplog.at_level("WARNING"):
        _bf._write_macro_series_no_shrink(lib, "SMH", planned, reference_date="2026-08-29")

    assert lib.writes == [("SMH", 26)], "a short first write must still succeed"
    assert any(
        "MACRO_NEW_SYMBOL_SHORT_HISTORY" in rec.message and "SMH" in rec.message
        for rec in caplog.records
    ), "a short first write must log a loud, greppable finding"


def test_first_write_of_a_healthy_new_symbol_does_not_warn(caplog):
    """A first write comparable in length to SPY, and current, should not be
    flagged."""
    lib = _FakeLib({"SPY": _frame("2016-08-19", 2514)})
    planned = _frame("2017-01-27", 2500)  # ends 2026-08-27 — close to reference

    with caplog.at_level("WARNING"):
        _bf._write_macro_series_no_shrink(lib, "NEWETF", planned, reference_date="2026-08-29")

    assert not any("MACRO_NEW_SYMBOL_SHORT_HISTORY" in rec.message for rec in caplog.records)
