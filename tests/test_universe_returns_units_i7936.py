"""alpha-engine-config-I7936 — producer contract for universe_returns.

Two columns named ``return_5d`` exist in research.db with OPPOSITE conventions.
This file pins the convention THIS producer writes, and pins the exclusion of
the test securities that produced the anchor observation on that issue.

Measured on live ``s3://alpha-engine-research/research.db`` (2026-08-21):

* ``universe_returns.return_5d`` -- DECIMAL FRACTION. 2,012,661 non-null rows,
  eval_date 2025-12-08..2026-08-10. median 0.0004, p99 0.3023, min -0.9944,
  max 6686.1759.
* ``score_performance.return_5d`` -- 2dp PERCENT POINTS. 533 non-null rows,
  median -0.02, range [-20.42, 22.70]. The same quantity in
  ``score_performance_outcomes`` (horizon_days=5) sits at [-0.2042, 0.2270]:
  exactly 100x.
"""

from __future__ import annotations

import pytest

from collectors.universe_returns import (
    _SKIP_TICKERS,
    _TEST_SECURITIES,
    _build_rows_for_date,
    _pct_return,
)


# ── The declared convention ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (100.0, 105.0, 0.05),      # +5% is 0.05, NOT 5.0
        (100.0, 95.0, -0.05),
        (100.0, 100.0, 0.0),
        (19.44, 129998.70, 6686.175925925926),  # ZWZZT, see below
    ],
)
def test_pct_return_is_a_decimal_fraction(start, end, expected):
    assert _pct_return(start, end) == pytest.approx(expected)


def test_a_plausible_universe_has_a_median_absolute_return_in_thousandths():
    """The discriminating statistic between the two conventions. The live
    column's median |r| is 0.0004; the percent-point sibling's is 0.02 -- and
    a column whose median |5-day move| is a whole percent POINT read as a
    fraction would be claiming the typical stock moves 100% a week."""
    live_median_abs = 0.0004
    assert live_median_abs < 0.5


# ── Test securities are not companies ───────────────────────────────────────


def test_nasdaq_test_securities_are_skipped():
    for t in ("ZWZZT", "ZVZZT", "ZJZZT", "ZXZZT", "ZBZX", "ZTEST", "ZEXIT", "ZIEXT"):
        assert t in _TEST_SECURITIES, t
        assert t in _SKIP_TICKERS, t


def test_the_6686_anchor_is_a_test_security_not_a_units_error():
    """ZWZZT closed at 19.44 on 2026-03-30 and 129,998.70 five sessions later.
    The arithmetic is right; the security is not real. This is the number on
    alpha-engine-config-I7936, and it is NOT evidence of a percent/decimal
    mix-up."""
    assert round(_pct_return(19.44, 129998.70), 4) == 6686.1759


def test_build_rows_excludes_test_securities(monkeypatch):
    """End to end through the row builder: a test security present in the
    grouped-daily response must not reach a row."""
    import collectors.universe_returns as ur

    prices = {
        "AAPL": {"close": 100.0},
        "SPY": {"close": 500.0},
        "ZWZZT": {"close": 19.44},
        "ZVZZT": {"close": 10.0},
    }
    fwd = {
        "AAPL": {"close": 102.0},
        "SPY": {"close": 505.0},
        "ZWZZT": {"close": 129998.70},
        "ZVZZT": {"close": 40.0},
    }
    monkeypatch.setattr(
        ur, "_grouped_daily_or_empty",
        lambda client, d, today=None: prices if d == "2026-03-30" else fwd,
    )
    rows = _build_rows_for_date("2026-03-30", object(), sector_map=None)
    tickers = {r["ticker"] for r in rows}
    assert "AAPL" in tickers
    assert not (tickers & _TEST_SECURITIES), tickers & _TEST_SECURITIES
    assert "SPY" not in tickers
    for r in rows:
        if r["return_5d"] is not None:
            assert abs(r["return_5d"]) < 5.0, r


# ── The historical rows are healed in-region, with no operator step ──────────


def test_ensure_table_purges_stored_test_securities(tmp_path):
    """A standing invariant, not a one-shot migration: every collection run
    re-asserts it, so a restored backup or a newly-listed test symbol heals on
    the next weekly pass."""
    import sqlite3

    from collectors.universe_returns import _ensure_table

    db = tmp_path / "research.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE universe_returns (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "ticker TEXT NOT NULL, eval_date TEXT NOT NULL, return_5d REAL, "
        "UNIQUE(ticker, eval_date))"
    )
    for t, r in (("AAPL", 0.012), ("MSFT", -0.004), ("ZWZZT", 6686.1759),
                 ("ZVZZT", 3.4501), ("ZBZX", 0.0121)):
        conn.execute(
            "INSERT INTO universe_returns (ticker, eval_date, return_5d) VALUES (?,?,?)",
            (t, "2026-03-30", r),
        )
    conn.commit()
    conn.close()

    _ensure_table(str(db))

    conn = sqlite3.connect(db)
    remaining = {r[0] for r in conn.execute("SELECT ticker FROM universe_returns")}
    mx = conn.execute("SELECT MAX(return_5d) FROM universe_returns").fetchone()[0]
    conn.close()
    assert remaining == {"AAPL", "MSFT"}
    assert mx == pytest.approx(0.012)


def test_purge_is_idempotent_and_silent_on_a_clean_table(tmp_path, caplog):
    import sqlite3

    from collectors.universe_returns import _ensure_table

    db = tmp_path / "research.db"
    _ensure_table(str(db))
    with caplog.at_level("WARNING"):
        _ensure_table(str(db))
    assert not [r for r in caplog.records if "purged" in r.getMessage()]
