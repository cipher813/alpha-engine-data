"""Tests for score_performance calibrator-v1 context seeding + backfill.

Covers the 2026-05-10 producer-side fix: `_seed_score_performance` was
inserting only `(symbol, score_date, score, price_on_date)` and leaving
the 5 canonical context columns (quant_score, qual_score, conviction,
sector_modifier, market_regime) NULL. Saturday 2026-05-09's evaluator
tripped on this when weight_optimizer's downstream lookup expected those
columns post research migration #12.

These tests pin two contracts:
  - Initial INSERT carries all 5 canonical context fields, sourced from
    the same signals.json payload that drives the BUY filter.
  - `_backfill_score_context` repairs legacy rows that were seeded
    before the producer learned to write them. UPDATE-WHERE-NULL means
    re-runs converge to a no-op once every row has at least one source.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from collectors.signal_returns import (
    _backfill_score_context,
    _ensure_score_performance_schema,
    _extract_signal_context,
    _seed_score_performance,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db(tmp_path: Path) -> str:
    db = tmp_path / "research.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE score_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                score_date TEXT NOT NULL,
                score REAL,
                price_on_date REAL,
                UNIQUE(symbol, score_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE universe_returns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                eval_date TEXT NOT NULL,
                close_price REAL,
                UNIQUE(ticker, eval_date)
            )
            """
        )
        conn.commit()
    return str(db)


def _signals_payload() -> dict:
    """Representative signals.json with sub_scores + context populated."""
    return {
        "date": "2026-05-01",
        "market_regime": "bull",
        "sector_modifiers": {"Technology": 1.05, "Healthcare": 0.95},
        "signals": {
            "AAPL": {
                "score": 78.0, "rating": "BUY", "sector": "Technology",
                "quant_score": 80.0, "qual_score": 72.0, "conviction": "rising",
            },
            "MSFT": {
                "score": 70.0, "rating": "BUY", "sector": "Technology",
                "quant_score": 68.0, "qual_score": 71.0, "conviction": "stable",
            },
            "JNJ": {
                "score": 65.0, "rating": "HOLD", "sector": "Healthcare",
                # HOLD — should be skipped by BUY filter
                "quant_score": 60.0, "qual_score": 65.0, "conviction": "stable",
            },
            "PFE": {
                "score": 76.0, "rating": "BUY", "sector": "Healthcare",
                "quant_score": 75.0, "qual_score": 78.0, "conviction": "declining",
            },
        },
    }


def _mock_s3_for(payload: dict) -> MagicMock:
    body = MagicMock()
    body.read.return_value = json.dumps(payload).encode()
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": body}
    # Single date in the listing
    page = {"CommonPrefixes": [{"Prefix": "signals/2026-05-01/"}]}
    paginator = MagicMock()
    paginator.paginate.return_value = [page]
    s3.get_paginator.return_value = paginator
    return s3


def _seed_universe_close(db: str, ticker: str, eval_date: str, close: float) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO universe_returns (ticker, eval_date, close_price) VALUES (?, ?, ?)",
            (ticker, eval_date, close),
        )
        conn.commit()


# ── _extract_signal_context ───────────────────────────────────────────────────


class TestExtractSignalContext:

    def test_resolves_all_five_fields(self):
        payload = _signals_payload()
        ctx = _extract_signal_context(payload, "AAPL")
        assert ctx == {
            "quant_score": 80.0,
            "qual_score": 72.0,
            "conviction": "rising",
            "sector_modifier": 1.05,
            "market_regime": "bull",
        }

    def test_unknown_ticker_returns_all_none(self):
        ctx = _extract_signal_context(_signals_payload(), "NVDA")
        assert ctx == {
            "quant_score": None, "qual_score": None, "conviction": None,
            "sector_modifier": None, "market_regime": "bull",  # market_regime is payload-level
        }

    def test_missing_sector_modifier_when_sector_absent(self):
        payload = {
            "market_regime": "neutral",
            "sector_modifiers": {"Technology": 1.10},
            "signals": {
                "AAPL": {"quant_score": 70, "qual_score": 60},  # no sector
            },
        }
        ctx = _extract_signal_context(payload, "AAPL")
        assert ctx["sector_modifier"] is None
        assert ctx["market_regime"] == "neutral"


# ── _seed_score_performance — initial INSERT carries canonical context ──────


class TestSeedScorePerformanceCanonicalInsert:

    def test_buy_rows_get_canonical_context_on_insert(self, tmp_db):
        _seed_universe_close(tmp_db, "AAPL", "2026-05-01", 205.50)
        _seed_universe_close(tmp_db, "MSFT", "2026-05-01", 430.25)
        _seed_universe_close(tmp_db, "PFE", "2026-05-01", 28.10)

        s3 = _mock_s3_for(_signals_payload())
        out = _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["status"] == "ok"
        assert out["rows_written"] == 3  # AAPL + MSFT + PFE; JNJ is HOLD

        with sqlite3.connect(tmp_db) as conn:
            rows = conn.execute(
                "SELECT symbol, quant_score, qual_score, conviction, "
                "sector_modifier, market_regime FROM score_performance "
                "ORDER BY symbol"
            ).fetchall()
        by_sym = {r[0]: r[1:] for r in rows}
        assert by_sym["AAPL"] == (80.0, 72.0, "rising", 1.05, "bull")
        assert by_sym["MSFT"] == (68.0, 71.0, "stable", 1.05, "bull")
        assert by_sym["PFE"] == (75.0, 78.0, "declining", 0.95, "bull")

    def test_skips_hold_rated_rows(self, tmp_db):
        _seed_universe_close(tmp_db, "JNJ", "2026-05-01", 152.00)
        s3 = _mock_s3_for(_signals_payload())
        _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        with sqlite3.connect(tmp_db) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM score_performance WHERE symbol='JNJ'"
            ).fetchone()[0] == 0

    def test_existing_rows_are_not_reseeded(self, tmp_db):
        """INSERT OR IGNORE means a re-run doesn't overwrite. Canonical
        context backfill is a separate step (_backfill_score_context)."""
        _seed_universe_close(tmp_db, "AAPL", "2026-05-01", 205.50)
        with sqlite3.connect(tmp_db) as conn:
            conn.execute(
                "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50)"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        out = _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        # Pre-existing row filtered by `existing` set; seeder reports 0 written.
        assert out["rows_written"] == 0

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score, qual_score FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        # Still NULL — that's what _backfill_score_context exists to repair.
        assert row == (None, None)


# ── _backfill_score_context — UPDATE-WHERE-NULL repair for legacy rows ──────


class TestBackfillScoreContext:

    def test_repairs_legacy_null_rows(self, tmp_db):
        """Rows seeded before the canonical-context fix should pick up
        all 5 fields on backfill."""
        with sqlite3.connect(tmp_db) as conn:
            # Add the canonical columns (mirrors prior schema-ensure run)
            _ensure_score_performance_schema(conn)
            conn.execute(
                "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50)"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        out = _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["status"] == "ok"
        assert out["rows_written"] == 1

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score, qual_score, conviction, "
                "sector_modifier, market_regime "
                "FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        assert row == (80.0, 72.0, "rising", 1.05, "bull")

    def test_rerun_is_noop_once_populated(self, tmp_db):
        """Repeat invocations after backfill should converge to 0 updates."""
        _seed_universe_close(tmp_db, "AAPL", "2026-05-01", 205.50)
        s3 = _mock_s3_for(_signals_payload())
        _seed_score_performance(s3, "bucket", tmp_db, "signals", dry_run=False)
        out = _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=False)
        assert out["rows_written"] == 0
        assert "no NULL context rows" in (out.get("note") or "")

    def test_dry_run_does_not_persist(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            conn.execute(
                "INSERT INTO score_performance (symbol, score_date, score, price_on_date) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50)"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        out = _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=True)
        assert out["rows_written"] == 1

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        assert row[0] is None  # not actually written

    def test_partial_null_only_fills_missing(self, tmp_db):
        """A row that already has quant_score should keep it; only NULL
        fields get backfilled."""
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            conn.execute(
                "INSERT INTO score_performance "
                "(symbol, score_date, score, price_on_date, quant_score, conviction) "
                "VALUES ('AAPL', '2026-05-01', 78.0, 205.50, 99.0, 'manual')"
            )
            conn.commit()

        s3 = _mock_s3_for(_signals_payload())
        _backfill_score_context(s3, "bucket", tmp_db, "signals", dry_run=False)

        with sqlite3.connect(tmp_db) as conn:
            row = conn.execute(
                "SELECT quant_score, qual_score, conviction, "
                "sector_modifier, market_regime "
                "FROM score_performance WHERE symbol='AAPL'"
            ).fetchone()
        # Pre-existing values preserved; NULLs filled.
        assert row == (99.0, 72.0, "manual", 1.05, "bull")


# ── Schema-ensure mirrors migration #12 ──────────────────────────────────────


class TestEnsureScorePerformanceSchema:

    def test_adds_canonical_columns_idempotently(self, tmp_db):
        with sqlite3.connect(tmp_db) as conn:
            _ensure_score_performance_schema(conn)
            _ensure_score_performance_schema(conn)  # second call is a no-op
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(score_performance)"
            ).fetchall()}

        for canonical_col in ("quant_score", "qual_score", "conviction",
                              "sector_modifier", "market_regime"):
            assert canonical_col in cols, f"missing {canonical_col}"
