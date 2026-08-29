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


def test_write_boundary_refuses_a_forward_moving_history_start():
    """Same row count, but the series start slid forward a week — still a loss."""
    lib = _FakeLib({"VIX3M": _frame("2026-07-31", 16)})
    planned = _frame("2026-08-07", 16)

    with pytest.raises(RuntimeError, match="history start moved forward"):
        _bf._write_macro_series_no_shrink(lib, "VIX3M", planned)


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
