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


def test_unified_path_no_min_rows_skip():
    """daily_append must not BYPASS ``compute_features`` for short-history
    tickers. A branch that ENRICHES the warmup context (e.g. the
    parquet-warmup path added 2026-04-22) and then falls through to the
    shared compute_features / update() is allowed — what's forbidden is
    a branch that sets ``n_skip`` / writes an OHLCV-only row and
    ``continue``s past compute_features.

    History: the Phase 2 fix (2026-04-21) removed the implicit
    ``df.dropna(subset=FEATURES)`` in ``compute_features`` and the
    short-history-only write branch. Every ticker runs through the same
    feature pipeline and writes whatever's computable. The parquet-warmup
    branch added 2026-04-22 preserves that invariant — it unions the
    weekly 10y parquet into the warmup frame but still hands the result
    to compute_features.

    A regression that re-introduces the SKIP / OHLCV-only-and-continue
    branch would undo first-class short-history support and resurrect
    the 2026-04-21 SNDK executor crash (NaN ``atr_14_pct`` from a ticker
    that WAS supposed to get one because ATR-14 only needs ≥14 rows).
    """
    src = _source()
    lines = src.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if "MIN_ROWS_FOR_FEATURES" not in stripped:
            continue
        if not stripped.startswith("if "):
            continue
        # Found an `if ... MIN_ROWS_FOR_FEATURES ...:` branch. Look at the
        # next ~6 lines. If that block increments n_skip / n_partial /
        # writes to universe_lib AND then `continue`s — it's the forbidden
        # bypass pattern. If it reassigns the warmup source and falls
        # through — it's the allowed parquet-warmup pattern.
        window = "\n".join(lines[i:i + 30])
        bypass_signals = (
            "n_skip += 1" in window
            or "n_partial += 1" in window
            or "universe_lib.update(" in window
        )
        if bypass_signals and "continue" in window:
            raise AssertionError(
                f"Line {i+1}: re-introduced the `MIN_ROWS_FOR_FEATURES` "
                f"SKIP branch. Any branch gated on MIN_ROWS_FOR_FEATURES "
                f"must enrich warmup context and fall through to the "
                f"shared compute_features / update() path — it must not "
                f"`continue` past compute_features. See the 2026-04-22 "
                f"parquet-warmup design."
            )


def test_partial_features_are_loudly_logged():
    """Every row written with ≥1 NaN feature must emit a structured
    ``partial-features ticker=X rows=N nan=M/... features=[...]`` log.

    Silent partial coverage is forbidden per feedback_no_silent_fails.
    The log message shape (key=value tags + feature list) is what lets
    us grep production logs for coverage drift.
    """
    src = _source()
    assert "partial-features ticker=" in src, (
        "daily_append must log `partial-features ticker=X rows=N` when a "
        "row is written with NaN features — silent fallback is forbidden."
    )
    assert "n_partial" in src, (
        "dedicated n_partial counter required, distinct from n_skip "
        "(legitimate skips) and n_err (read errors)."
    )


def test_counters_increment_after_successful_write():
    """n_ok and n_partial must be incremented AFTER universe_lib.update()
    so an exception rolls the iteration back cleanly into n_err.

    Locks the 2026-04-21 rewrite where counters were hoisted post-write
    to prevent double-counting when update() throws.
    """
    src = _source()
    # The update() call site + increments must appear in that order.
    # Find the update call for universe_lib.update(ticker, today_row) —
    # the committed increment happens on the same side.
    lines = src.splitlines()
    update_idx = None
    for i, line in enumerate(lines):
        if "universe_lib.update(ticker, today_row)" in line:
            update_idx = i
            break
    assert update_idx is not None, (
        "universe_lib.update(ticker, today_row) call site not found"
    )
    window_after = "\n".join(lines[update_idx:update_idx + 20])
    assert "n_partial += 1" in window_after and "n_ok += 1" in window_after, (
        "n_ok / n_partial must be incremented AFTER the update() call, "
        "not before. Increments before write cause miscount on exception."
    )


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
