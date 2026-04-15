"""Regression: builders/daily_append.py must use ArcticDB update(), not append().

2026-04-15: 904/909 tickers in the production universe library had
duplicate date rows when read back from ArcticDB. Root cause traced to
daily_append calling `lib.append()` which does not dedupe — a race or
re-run would produce same-date duplicates. Fix: swap to `lib.update()`
which is idempotent for same-date rows (input overlap replaces existing).

This test locks the semantic. A future revert to `append()` on any of
the three write sites (universe, macro, sector ETF) would reintroduce
the accumulation bug and should fail loudly here.
"""

from pathlib import Path


_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


def test_universe_lib_uses_update_not_append():
    src = _source()
    assert "universe_lib.update(ticker, today_row)" in src, (
        "daily_append must call universe_lib.update() — append() accumulates "
        "duplicate same-date rows and caused the 2026-04-15 retrain outage."
    )
    assert "universe_lib.append(ticker, today_row)" not in src, (
        "Found universe_lib.append() — this is the duplicate-row bug. "
        "Use update() instead."
    )


def test_macro_lib_uses_update_not_append():
    src = _source()
    # Both key-path and sym-path (sector ETFs) must use update.
    assert "macro_lib.update(key, new_row)" in src
    assert "macro_lib.update(sym, new_row)" in src
    assert "macro_lib.append(key," not in src
    assert "macro_lib.append(sym," not in src
