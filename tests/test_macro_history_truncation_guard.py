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
    existing = _frame("2016-08-19", 2513)
    macro_lib = _FakeLib({"SPY": existing})
    universe_lib = _FakeLib({})
    monkeypatch.setattr(_bf, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(_bf, "get_universe_lib", lambda *a, **k: universe_lib)

    planned_macro = {"SPY": _series("2016-08-19", 2514)}
    _bf._assert_no_arctic_regression(
        "alpha-engine-research", planned_macro, {}, "2026-08-29",
    )
