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


def test_macro_missing_keys_raise():
    """Missing macro key from today's closes must hard-fail, not silently skip.

    Regression for 2026-04-15: daily_append returned status='ok' despite
    macro/SPY going 5 days stale because closes.get('SPY') returned None
    and the old code silently skipped the update. Pipeline claimed success,
    inference preflight caught the staleness only after the fact.
    """
    src = _source()
    # Must have the missing-keys tracker + raise
    assert "macro_missing_from_closes" in src
    assert "Macro/sector-ETF keys missing" in src


def test_macro_verification_readback_present():
    """After update(), the code must verify today landed in macro_lib."""
    src = _source()
    assert "verification_failures" in src
    assert "Macro update verification failed" in src
    assert "macro_updated + sector_updated" in src


def test_sector_etfs_iterate_explicit_list():
    """Sector ETF iteration must use explicit list (so missing keys surface)."""
    src = _source()
    # Old code: `for sym in closes: if sym.startswith("XL")` — missing keys
    # silently don't iterate. New code: explicit list.
    assert 'sector_etfs = ["XLB"' in src or 'sector_etfs = [\n' in src


def test_short_history_writes_ohlcv_not_skipped():
    """Short-history tickers (new listings, spinoffs) must get an OHLCV-only
    row written, never silently skipped.

    Regression for 2026-04-21 SNDK incident: the 2026 WDC flash-memory
    spinoff re-listed SNDK with ~44 rows of history. daily_append's
    `len(hist) < MIN_ROWS_FOR_FEATURES` branch silently n_skip++'d without
    writing any row. EOD reconcile then hard-failed on every held
    short-history ticker because authoritative close was missing from
    ArcticDB. New listings are a normal market event (20-40 S&P
    constituent changes/year; every spinoff creates one). They are a
    first-class supported state.

    The fix writes OHLCV + NaN-for-every-feature-column when below the
    warmup threshold, logs loudly with a structured `short-history
    ticker=X rows=N` message, and increments a dedicated ``n_partial``
    counter (not ``n_skip``, not ``n_err`` — short history is neither).
    """
    src = _source()

    # Loud warning with structured key=val tags so coverage gaps surface.
    assert "short-history ticker=" in src, (
        "short-history branch must log `short-history ticker=X rows=N` — "
        "silent fallback is forbidden (feedback_no_silent_fails)."
    )

    # Write path must exist — ticker gets OHLCV, not a skip.
    assert "n_partial" in src, (
        "short-history path must track a dedicated n_partial counter, "
        "distinct from n_skip (legitimate skips) and n_err (read errors)."
    )

    # Skip-only pattern (the bug) must be gone: the old `if len(hist) <
    # MIN_ROWS_FOR_FEATURES: n_skip += 1; continue` with no write.
    # Check the short-history branch reaches universe_lib.update().
    lines = src.splitlines()
    for i, line in enumerate(lines):
        if "len(hist) < MIN_ROWS_FOR_FEATURES" in line:
            window = "\n".join(lines[i:i + 60])
            assert "universe_lib.update(ticker" in window, (
                "short-history branch must reach universe_lib.update() — "
                "writing OHLCV-only is the whole point of the fix."
            )
            assert "n_partial" in window, (
                "short-history branch must increment n_partial."
            )
            break
    else:
        raise AssertionError("short-history branch not found in daily_append.py")


def test_short_history_matches_stored_dtype():
    """Short-history branch must astype every column to ``hist.dtypes[col]``
    — never hardcode a dtype.

    Regression for 2026-04-21 shipping of PR #76: the short-history
    branch hardcoded ``Volume → int64``. ArcticDB rejects updates whose
    column dtypes don't match the existing version. Stored Volume dtype
    varies across tickers (some int64, some float64, depending on when
    they were first backfilled) — SOLS, ULS, and one other short-history
    ticker all failed the update with a FLOAT64/INT64 mismatch.

    The only correct approach is to match the stored dtype per-column
    via ``hist.dtypes[col]``, which is authoritative by construction.
    """
    src = _source()

    # The per-column astype loop must reference hist.dtypes so every
    # column matches the ticker's current storage schema.
    assert "astype(hist.dtypes[col])" in src, (
        "short-history branch must astype(hist.dtypes[col]) to match "
        "the stored schema — hardcoded dtypes cause ArcticDB to reject "
        "updates when stored dtype differs (SOLS/ULS 2026-04-21)."
    )

    # Hardcoded Volume casts are forbidden — they were the original bug.
    assert 'astype("int64")' not in src, (
        "hardcoded astype(\"int64\") — almost certainly the Volume-dtype "
        "regression. Use hist.dtypes[col] instead."
    )


def test_no_skip_guard_on_existing_today_row():
    """daily_append must NOT skip tickers whose history already contains today_ts.

    Regression for 2026-04-18: a `if today_ts in hist.index: skip` guard
    defeated the idempotency guarantee that update() provides. Symptom was
    discovered during the 2026-04-17 incident recovery — the poisoned
    morning run had already written T-1 data under index=T, and a re-run
    with correct polygon data couldn't overwrite because every ticker
    tripped the skip guard.

    update() is explicitly chosen (see the comment at the update call site)
    BECAUSE it replaces same-date rows. The guard was redundant at best,
    actively harmful at worst. This test locks the removal so a future
    well-intentioned refactor doesn't re-introduce it.
    """
    src = _source()
    # Must not have the exact skip pattern. Allow comments that document
    # why the guard was removed (they reference today_ts in hist.index).
    # The test looks for the executable pattern: an `if today_ts in hist.index`
    # immediately followed by `n_skip += 1` in the next 2 lines.
    lines = src.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue  # skip comments
        if "today_ts in hist.index" in stripped and stripped.startswith("if "):
            # Check if this is followed by `n_skip += 1 ... continue` (the
            # skip pattern). If so, the guard was reintroduced.
            following = "\n".join(lines[i:i+4])
            assert "n_skip" not in following, (
                f"Found skip-on-existing-today guard at line {i+1}. Remove it — "
                "update() already handles same-date idempotency. See "
                "2026-04-17 label-bug incident."
            )
