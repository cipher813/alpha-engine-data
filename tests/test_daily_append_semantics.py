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
    """Per-ticker writes must use ArcticDB's `update_batch` (idempotent,
    replaces same-date rows) for the steady-state append-at-head case
    and `write_batch` for the rare backfill case. Direct `lib.append()`
    must never appear — it accumulates duplicate same-date rows and
    caused the 2026-04-15 retrain outage.

    History: PR #100 (2026-04-27) wrapped per-ticker `_write_row_backfill_safe`
    calls in a ThreadPoolExecutor for parallelism. PR #152 (2026-05-05)
    measured that pool achieving zero parallelism in production
    (1535s wall ≈ serialized rate). PR replaced it with `update_batch`,
    which ArcticDB documents as parallel via its native C++ thread pool.
    `_write_row_backfill_safe` is still the per-symbol helper for
    macro_lib (small N=7-11) and is the canonical reference for the
    update-vs-write decision logic.
    """
    src = _source()
    assert "universe_lib.update_batch(" in src, (
        "Per-ticker writes must fan out via universe_lib.update_batch — "
        "ArcticDB's native C++ thread pool is the only thing that achieves "
        "actual parallelism (Python ThreadPoolExecutor + lib.update did not, "
        "per 2026-05-05 MorningEnrich incident)."
    )
    # Idempotent semantics still required — UpdatePayload + update_batch
    # use the same update() semantics as the per-symbol path (replaces
    # same-date rows rather than accumulating duplicates). The ban on
    # raw lib.append() still holds for both universe and macro paths.
    assert "lib.update(symbol, new_row)" in src, (
        "_write_row_backfill_safe (still used for macro_lib) must call "
        "lib.update() in the append branch — append() accumulates "
        "duplicate same-date rows (2026-04-15 retrain outage)."
    )
    assert "universe_lib.append(" not in src, (
        "Found universe_lib.append() — duplicate-row bug. Use update_batch "
        "for the bulk path or _write_row_backfill_safe for individual symbols."
    )


def test_macro_lib_uses_update_not_append():
    """Macro + sector ETF writes must also route through
    _write_row_backfill_safe so historical macro backfills work."""
    src = _source()
    # Both key-path and sym-path (sector ETFs) must use the helper.
    assert "_write_row_backfill_safe(macro_lib, key, new_row)" in src
    assert "_write_row_backfill_safe(macro_lib, sym, new_row)" in src
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
    """n_ok and n_partial must be incremented AFTER the per-ticker write
    so a failed write rolls the iteration back cleanly into n_err.

    Locks the 2026-04-21 rewrite where counters were hoisted post-write
    to prevent double-counting when the write throws. PR #152 (2026-05-05)
    replaced the ThreadPoolExecutor + closure pattern with `update_batch`,
    so the aggregation loop now iterates `update_batch` results and
    routes each result to n_ok / n_partial / n_err based on whether
    ArcticDB returned a `DataError` (write-side failure) or a
    `VersionedItem` (success). Same semantic guarantee, different shape.
    """
    import re
    src = _source()
    # Find the bulk-write call site (universe_lib.update_batch).
    write_match = re.search(
        r"universe_lib\.update_batch\s*\(",
        src,
    )
    assert write_match is not None, (
        "universe_lib.update_batch call site not found — required for "
        "the parallel ArcticDB write path."
    )
    after_write = src[write_match.end():]
    # Counters must increment AFTER the bulk-write site in source order
    # (i.e. the post-write aggregation loop drives them, not the payload
    # build loop). Increments before the write would miscount on failure.
    assert "n_partial += 1" in after_write and "n_ok += 1" in after_write, (
        "n_ok / n_partial must be incremented AFTER update_batch, not "
        "before. Increments in the payload-build loop would miscount on "
        "ArcticDB-side write failures."
    )
    assert "n_err += 1" in after_write, (
        "Aggregation must increment n_err when ArcticDB returns DataError "
        "for a payload — write-side failures must not silently inflate n_ok."
    )
    # The DataError type-check is the new "exception path" — ArcticDB
    # batch APIs return DataError instead of raising, so the type-check
    # is what routes failures to n_err.
    assert "isinstance(result, DataError)" in after_write or "DataError" in after_write, (
        "Aggregation must detect DataError returned by update_batch / "
        "write_batch (ArcticDB batch APIs return errors per-payload "
        "rather than raising)."
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


def test_no_unconditional_skip_guard_on_existing_today_row():
    """daily_append must NOT *unconditionally* skip tickers whose history
    already contains today_ts.

    Regression for 2026-04-18: an unconditional ``if today_ts in
    hist.index: skip`` guard defeated the idempotency guarantee that
    update() provides. Symptom surfaced during the 2026-04-17 incident
    recovery — the poisoned morning run had written T-1 data under
    index=T, and a re-run with correct polygon data couldn't overwrite
    because every ticker tripped the skip guard.

    The 2026-05-01 follow-up introduced an opt-in gate
    (``skip_if_exists`` parameter) so EOD post-market re-runs don't
    redundantly rewrite all 904 tickers via the slow lib.write backfill
    path (see test_daily_append_skip_if_exists.py for that contract).
    The opt-in form ``if skip_if_exists and today_ts in hist.index:``
    is allowed; an unconditional ``if today_ts in hist.index:`` is not.
    """
    src = _source()
    lines = src.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if "today_ts in hist.index" not in stripped:
            continue
        if not stripped.startswith("if "):
            continue
        # Allow the explicit opt-in gate: caller has to pass skip_if_exists=True.
        if "skip_if_exists" in stripped:
            continue
        # Bare ``if today_ts in hist.index:`` followed by skip is the
        # forbidden pattern — the 2026-04-17 polygon-relabel bug recurs
        # if a future PR reintroduces it without gating.
        following = "\n".join(lines[i:i+4])
        assert "n_skip" not in following, (
            f"Found UNCONDITIONAL skip-on-existing-today guard at line "
            f"{i+1}. Gate it behind ``skip_if_exists`` (see the 2026-05-01 "
            f"design note in daily_append.py) or remove it."
        )
