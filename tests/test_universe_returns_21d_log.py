"""universe_returns 21d arithmetic + log-domain column behavior.

Covers the schema migration, idempotency, forward-window gating for the new
21d horizon, and the log-vs-arithmetic relationship between log_return_21d
and return_21d. The polygon API call surface is exercised via a fake client.
"""
from __future__ import annotations

import math
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from collectors.universe_returns import (
    _NEW_COLUMNS_21D,
    _build_rows_for_date,
    _ensure_table,
    _get_existing_dates,
    _insert_rows,
    _log_return,
)


# -- Schema migration ---------------------------------------------------------


class TestSchemaMigration:
    def test_fresh_db_creates_all_21d_columns(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _ensure_table(db)
            with sqlite3.connect(db) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
            for col, _ in _NEW_COLUMNS_21D:
                assert col in cols, f"missing column {col} on fresh DB"

    def test_legacy_db_migration_adds_21d_columns(self):
        """A pre-PR-A DB has no 21d columns; migration adds them in place."""
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            with sqlite3.connect(db) as conn:
                conn.execute(
                    "CREATE TABLE universe_returns ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "ticker TEXT NOT NULL, "
                    "eval_date TEXT NOT NULL, "
                    "return_5d REAL, "
                    "UNIQUE(ticker, eval_date))"
                )
                conn.commit()

            _ensure_table(db)

            with sqlite3.connect(db) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
            for col, _ in _NEW_COLUMNS_21D:
                assert col in cols

    def test_migration_idempotent(self):
        """Calling _ensure_table twice does not raise (column-exists path)."""
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _ensure_table(db)
            _ensure_table(db)  # should be a no-op
            with sqlite3.connect(db) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()}
            for col, _ in _NEW_COLUMNS_21D:
                assert col in cols


# -- Existing-dates gating: 21d aware -----------------------------------------


class TestExistingDates21dGating:
    def _seed(self, db: str, eval_date: str, return_5d: float | None, return_21d: float | None):
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO universe_returns (ticker, eval_date, return_5d, return_21d) "
                "VALUES (?, ?, ?, ?)",
                ("AAPL", eval_date, return_5d, return_21d),
            )
            conn.commit()

    def test_complete_row_treated_as_existing(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _ensure_table(db)
            self._seed(db, "2026-03-01", return_5d=0.01, return_21d=0.02)
            existing = _get_existing_dates(db, today=date(2026, 5, 9))
            assert "2026-03-01" in existing

    def test_stale_null_21d_with_closed_window_excluded(self):
        """fwd_21d closed but column NULL → re-enqueue for backfill."""
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _ensure_table(db)
            self._seed(db, "2026-03-01", return_5d=0.01, return_21d=None)
            existing = _get_existing_dates(db, today=date(2026, 5, 9))
            assert "2026-03-01" not in existing

    def test_null_21d_with_unclosed_window_treated_as_existing(self):
        """Recent rows where 21d window has not yet closed are skipped."""
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _ensure_table(db)
            self._seed(db, "2026-05-05", return_5d=0.01, return_21d=None)
            existing = _get_existing_dates(db, today=date(2026, 5, 9))
            assert "2026-05-05" in existing

    def test_missing_5d_excluded(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _ensure_table(db)
            self._seed(db, "2026-03-01", return_5d=None, return_21d=None)
            existing = _get_existing_dates(db, today=date(2026, 5, 9))
            assert "2026-03-01" not in existing


# -- Row build: 21d gating + polygon coverage ---------------------------------


def _fake_polygon(prices_by_date: dict[str, dict[str, dict[str, float]]]) -> MagicMock:
    """Return a polygon-shaped client with .get_grouped_daily(date_str) -> bars."""
    client = MagicMock()
    client.get_grouped_daily.side_effect = lambda d: prices_by_date.get(d, {})
    return client


class TestRowBuild21d:
    def test_log_arithmetic_relationship(self):
        """log_return_21d ≈ log(1 + return_21d) within float tolerance."""
        # close goes 100 → 102 over 21 trading days
        prices = {
            "2026-03-02": {"AAPL": {"close": 100.0}, "SPY": {"close": 400.0}},
            "2026-03-09": {"AAPL": {"close": 100.5}, "SPY": {"close": 400.5}},  # +5d
            "2026-03-16": {"AAPL": {"close": 101.0}, "SPY": {"close": 401.0}},  # +10d
            "2026-03-31": {"AAPL": {"close": 102.0}, "SPY": {"close": 408.0}},  # +21d
            "2026-04-14": {"AAPL": {"close": 103.0}, "SPY": {"close": 410.0}},  # +30d
        }

        # Patch date.today() inside the module to a date well past +30d
        import collectors.universe_returns as ur

        original_today = date.today
        try:
            class _StubDate(date):
                @classmethod
                def today(cls):
                    return cls(2026, 5, 9)
            ur.date = _StubDate
            rows = _build_rows_for_date(
                "2026-03-02",
                _fake_polygon(prices),
                sector_map=None,
            )
        finally:
            ur.date = date

        aapl_row = next(r for r in rows if r["ticker"] == "AAPL")
        assert aapl_row["return_21d"] == pytest.approx(0.02, abs=1e-4)
        assert aapl_row["log_return_21d"] is not None
        # log(1 + 0.02) ≈ 0.0198026
        assert aapl_row["log_return_21d"] == pytest.approx(math.log(1.02), abs=1e-5)
        # Expected relationship: log_return_21d ≈ log(1 + return_21d)
        assert aapl_row["log_return_21d"] == pytest.approx(
            math.log(1.0 + aapl_row["return_21d"]), abs=1e-3
        )

    def test_21d_gated_to_null_when_window_unclosed(self):
        """If today is too soon after eval_date, return_21d/log_return_21d are NULL."""
        prices = {
            "2026-04-30": {"AAPL": {"close": 100.0}, "SPY": {"close": 400.0}},
            "2026-05-07": {"AAPL": {"close": 101.0}, "SPY": {"close": 402.0}},  # +5d
        }

        import collectors.universe_returns as ur

        try:
            class _StubDate(date):
                @classmethod
                def today(cls):
                    return cls(2026, 5, 9)
            ur.date = _StubDate
            rows = _build_rows_for_date(
                "2026-04-30",
                _fake_polygon(prices),
                sector_map=None,
            )
        finally:
            ur.date = date

        aapl_row = next(r for r in rows if r["ticker"] == "AAPL")
        assert aapl_row["return_5d"] is not None  # 5d window has closed
        assert aapl_row["return_21d"] is None  # 21d not yet
        assert aapl_row["log_return_21d"] is None
        assert aapl_row["spy_return_21d"] is None

    def test_polygon_called_for_t_plus_21d(self):
        """The collector must fetch grouped-daily for the t+21d trading day."""
        prices = {
            "2026-03-02": {"AAPL": {"close": 100.0}, "SPY": {"close": 400.0}},
            "2026-03-09": {"AAPL": {"close": 101.0}, "SPY": {"close": 401.0}},
            "2026-03-16": {"AAPL": {"close": 102.0}, "SPY": {"close": 402.0}},
            "2026-03-31": {"AAPL": {"close": 103.0}, "SPY": {"close": 403.0}},
            "2026-04-14": {"AAPL": {"close": 104.0}, "SPY": {"close": 404.0}},
        }
        client = _fake_polygon(prices)

        import collectors.universe_returns as ur

        try:
            class _StubDate(date):
                @classmethod
                def today(cls):
                    return cls(2026, 5, 9)
            ur.date = _StubDate
            _build_rows_for_date("2026-03-02", client, sector_map=None)
        finally:
            ur.date = date

        called = {c.args[0] for c in client.get_grouped_daily.call_args_list}
        assert "2026-03-31" in called, f"missing t+21d call; called={called}"


# -- Insert path: round-trip new columns --------------------------------------


class TestInsertNewColumns:
    def test_round_trip_21d_log_columns(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "research.db")
            _ensure_table(db)

            row = {
                "ticker": "AAPL",
                "eval_date": "2026-03-02",
                "sector": "Technology",
                "close_price": 100.0,
                "return_5d": 0.005,
                "return_10d": 0.010,
                "return_21d": 0.020,
                "return_30d": 0.030,
                "spy_return_5d": 0.001,
                "spy_return_10d": 0.002,
                "spy_return_21d": 0.012,
                "spy_return_30d": 0.018,
                "beat_spy_5d": 1,
                "beat_spy_10d": 1,
                "beat_spy_21d": 1,
                "beat_spy_30d": 1,
                "log_return_21d": math.log(1.02),
                "log_spy_return_21d": math.log(1.012),
                "sector_etf": "XLK",
                "sector_etf_return_5d": 0.003,
                "beat_sector_5d": 1,
            }
            _insert_rows(db, [row])

            with sqlite3.connect(db) as conn:
                got = conn.execute(
                    "SELECT return_21d, log_return_21d, spy_return_21d, "
                    "log_spy_return_21d, beat_spy_21d "
                    "FROM universe_returns WHERE ticker='AAPL' AND eval_date='2026-03-02'"
                ).fetchone()

            assert got[0] == pytest.approx(0.020)
            assert got[1] == pytest.approx(math.log(1.02))
            assert got[2] == pytest.approx(0.012)
            assert got[3] == pytest.approx(math.log(1.012))
            assert got[4] == 1


# -- Helper edge cases --------------------------------------------------------


class TestLogReturnHelper:
    def test_zero_or_negative_price_returns_none(self):
        assert _log_return(0.0, 100.0) is None
        assert _log_return(100.0, 0.0) is None
        assert _log_return(-1.0, 100.0) is None

    def test_none_returns_none(self):
        assert _log_return(None, 100.0) is None
        assert _log_return(100.0, None) is None

    def test_unchanged_price_returns_zero(self):
        assert _log_return(100.0, 100.0) == pytest.approx(0.0)
