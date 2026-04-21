"""Regression: builders/promote_ohlcv_only_schema.py must rewrite
symbols via ``lib.write()`` (not ``lib.update()``) so the stored schema
is replaced with the full OHLCV + FEATURE set.

Context:
    PR #76 introduced short-history OHLCV-only rows before the unified
    path landed in PR #78. Two symbols in the 2026-04-21 post-#78
    daily_append run hit an ArcticDB column-mismatch (``update()``
    enforces schema match). This migration promotes their stored
    schema; the tests below lock its invariants so a regression to
    ``update()`` or a silent skip on empty results fails loudly.
"""

from pathlib import Path


_SCRIPT = Path(__file__).parent.parent / "builders" / "promote_ohlcv_only_schema.py"


def _source() -> str:
    return _SCRIPT.read_text()


def test_script_exists():
    assert _SCRIPT.exists(), f"promote_ohlcv_only_schema.py missing at {_SCRIPT}"


def test_promotion_uses_write_not_update():
    """Schema promotion MUST call ``lib.write()`` — ``update()`` would
    fail with the same column-mismatch that motivated this script.

    A regression that swaps back to ``update()`` would silently no-op
    every promotion while reporting success.
    """
    src = _source()
    assert "universe_lib.write(ticker, out)" in src, (
        "Schema promotion must call universe_lib.write(ticker, out). "
        "update() enforces schema match and cannot widen OHLCV-only to "
        "OHLCV+FEATURE — that's the whole point of this migration."
    )
    # update() has no legitimate use here — forbid it in the write path.
    assert "universe_lib.update(ticker" not in src, (
        "Found universe_lib.update() — regression of the column-mismatch "
        "bug the script exists to fix."
    )


def test_candidate_detection_scans_every_feature():
    """``_needs_promotion`` must flag a symbol when ANY feature column
    is missing, not just some heuristic subset.

    Any partial-schema symbol will fail the next daily_append update(),
    so the detector must be exhaustive.
    """
    src = _source()
    # The check walks FEATURES and returns True on any missing column.
    assert "any(f not in df.columns for f in FEATURES)" in src, (
        "_needs_promotion must check every f in FEATURES against "
        "df.columns. Partial subsets miss real mismatches."
    )


def test_empty_compute_features_is_an_error_not_a_skip():
    """If compute_features returns empty for a candidate symbol,
    something upstream is broken — report as error, not silent skip.

    Silent skips on migration are especially dangerous: user thinks the
    symbol was handled, next daily_append run errors anyway.
    """
    src = _source()
    assert 'compute_features empty' in src, (
        "Empty compute_features result must surface as an explicit error "
        "reason, per feedback_no_silent_fails."
    )


def test_dry_run_does_not_write():
    """--dry-run must never call lib.write(). The mode exists exactly to
    let a reviewer see the promotion plan before committing."""
    src = _source()
    lines = src.splitlines()
    # Find the write() line and confirm it sits inside an `if not dry_run:`
    # block (string-level check — we can't execute without ArcticDB).
    for i, line in enumerate(lines):
        if "universe_lib.write(ticker, out)" in line:
            prior = "\n".join(lines[max(0, i - 6):i])
            assert "if not dry_run" in prior, (
                f"Line {i+1}: universe_lib.write() call not guarded by "
                f"`if not dry_run:` — dry-run would still write."
            )
            break
    else:
        raise AssertionError("universe_lib.write() call site not found")


def test_partial_coverage_is_logged_loudly():
    """Symbols promoted with NaN features (short history) must emit a
    structured ``partial-features ticker=X nan=N/total features=[...]``
    log, matching daily_append's convention."""
    src = _source()
    assert "partial-features ticker=" in src, (
        "Missing the structured partial-features log — silent partial "
        "coverage on migration is forbidden."
    )
