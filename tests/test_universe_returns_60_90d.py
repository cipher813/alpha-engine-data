"""Tests for the W3.1 (L4469) 60d/90d universe_returns horizon columns.

``return_60d``/``return_90d``/``log_return_60d``/``log_return_90d`` (and the
sibling ``return_30d``) were DROPPED 2026-08-22 (alpha-engine-config-I8185):
measured 0-of-2.14M non-null fleet-wide (``return_30d`` frozen at eval_date
2026-03-30), grep-verified with no consumer across `crucible-*` /
`nousergon-data`. Root cause: `_get_existing_dates` gated re-processing on
5d/21d completeness only, so a date already 21d-complete was never revisited
when its later 30d/60d/90d windows closed — see that function's docstring.

This test now asserts the DROPPED state: the four columns (plus
`return_30d`) are absent from a freshly created schema, and a row dict that
happens to carry those keys (an older caller) is silently ignored rather than
inserted. The SPY-relative/beat/log-spy siblings
(`spy_return_60d`/`spy_return_90d`/`beat_spy_60d`/`beat_spy_90d`/
`log_spy_return_60d`/`log_spy_return_90d`, and `spy_return_30d`/
`beat_spy_30d`) were NOT in scope for I8185 and are still written.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.universe_returns import _ensure_table, _insert_rows

# Dropped 2026-08-22 (alpha-engine-config-I8185) — must never reappear in the
# schema or be written by the producer.
_DROPPED_COLS = [
    "return_30d", "return_60d", "return_90d",
    "log_return_60d", "log_return_90d",
]

# Siblings that stayed in scope and must still round-trip.
_SURVIVING_W3_1_COLS = [
    "spy_return_60d", "spy_return_90d",
    "beat_spy_60d", "beat_spy_90d",
    "log_spy_return_60d", "log_spy_return_90d",
]


def _db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    _ensure_table(path)
    return path


def test_dropped_columns_absent_from_schema():
    path = _db()
    try:
        conn = sqlite3.connect(path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
        conn.close()
        for c in _DROPPED_COLS:
            assert c not in cols, f"{c} should have been dropped from universe_returns schema (I8185)"
    finally:
        os.remove(path)


def test_surviving_60_90d_columns_present_in_schema():
    path = _db()
    try:
        conn = sqlite3.connect(path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
        conn.close()
        for c in _SURVIVING_W3_1_COLS:
            assert c in cols, f"{c} missing from universe_returns schema"
    finally:
        os.remove(path)


def test_60_90d_round_trip_without_dropped_columns():
    path = _db()
    try:
        row = {
            "ticker": "AAPL", "eval_date": "2024-01-02", "sector": "Tech",
            "close_price": 100.0,
            "return_5d": 0.01, "return_10d": 0.02, "return_21d": 0.03,
            "spy_return_5d": 0.005, "spy_return_10d": 0.01, "spy_return_21d": 0.015, "spy_return_30d": 0.02,
            "beat_spy_5d": 1, "beat_spy_10d": 1, "beat_spy_21d": 1, "beat_spy_30d": 1,
            "log_return_21d": 0.0295, "log_spy_return_21d": 0.0149,
            "spy_return_60d": 0.05, "spy_return_90d": 0.07,
            "beat_spy_60d": 1, "beat_spy_90d": 1,
            "log_spy_return_60d": 0.0488, "log_spy_return_90d": 0.0677,
            "sector_etf": "XLK", "sector_etf_return_5d": 0.006, "beat_sector_5d": 1,
        }
        assert _insert_rows(path, [row]) == 1
        conn = sqlite3.connect(path)
        got = conn.execute(
            "SELECT spy_return_60d, spy_return_90d, beat_spy_60d, log_spy_return_90d, log_spy_return_60d "
            "FROM universe_returns WHERE ticker='AAPL'"
        ).fetchone()
        conn.close()
        assert got == (0.05, 0.07, 1, 0.0677, 0.0488)
    finally:
        os.remove(path)


def test_row_carrying_dropped_keys_still_inserts_and_ignores_them():
    # An older caller / test fixture that still builds return_60d etc. into
    # the row dict must not break _insert_rows — the INSERT statement simply
    # no longer references those keys.
    path = _db()
    try:
        row = {
            "ticker": "MSFT", "eval_date": "2024-01-02", "sector": "Tech",
            "close_price": 200.0,
            "return_5d": 0.01, "return_10d": None, "return_21d": None,
            "return_30d": 0.04, "return_60d": 0.08, "return_90d": 0.12,
            "log_return_60d": 0.077, "log_return_90d": 0.1133,
            "spy_return_5d": 0.005, "spy_return_10d": None, "spy_return_21d": None, "spy_return_30d": None,
            "beat_spy_5d": 1, "beat_spy_10d": None, "beat_spy_21d": None, "beat_spy_30d": None,
            "log_return_21d": None, "log_spy_return_21d": None,
            "sector_etf": "XLK", "sector_etf_return_5d": None, "beat_sector_5d": None,
        }
        assert _insert_rows(path, [row]) == 1
        conn = sqlite3.connect(path)
        got = conn.execute(
            "SELECT spy_return_60d, log_spy_return_90d FROM universe_returns WHERE ticker='MSFT'"
        ).fetchone()
        conn.close()
        assert got == (None, None)
    finally:
        os.remove(path)
