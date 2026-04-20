"""
builders/migrate_universe_vwap.py — One-time ArcticDB universe schema migration.

Why this exists
---------------
``builders/daily_append.py`` (2026-04-17) added ``VWAP`` to ``OHLCV_COLS``
so polygon grouped-daily's ``vw`` field lands in ArcticDB as a first-class
column. The universe library uses static-schema storage, so every
per-ticker ``update()`` must match the stored schema exactly. Historical
rows were written before VWAP was added, so the stored schema does not
include VWAP — every post-2026-04-17 append fails with a field-descriptor
mismatch (``n_ok=0 n_err=899`` on 2026-04-20 PostMarketData).

This script backfills a NaN VWAP column into every symbol in the universe
library, reordering columns so VWAP appears right after Volume (matching
``OHLCV_COLS`` order in ``daily_append.py``). After migration, daily_append
succeeds against the new schema and every subsequent day works without
further intervention.

Idempotent: symbols that already have a VWAP column are skipped.

Hard-fail: per ``feedback_hard_fail_until_stable``, this raises on any
per-symbol failure rather than reporting partial success. Run it once,
verify ``n_err=0`` at the end, then proceed with daily_append.

Usage
-----
    python -m builders.migrate_universe_vwap             # migrate all
    python -m builders.migrate_universe_vwap --dry-run   # report only
"""

from __future__ import annotations

import argparse
import logging
import sys

import numpy as np

from builders.daily_append import OHLCV_COLS
from store.arctic_store import get_universe_lib

log = logging.getLogger(__name__)


def migrate(dry_run: bool = False) -> tuple[int, int, int]:
    """Backfill VWAP column into every universe symbol.

    Returns (n_migrated, n_skipped_already_has_vwap, n_err).
    """
    universe = get_universe_lib()
    symbols = universe.list_symbols()
    log.info("Universe library has %d symbols", len(symbols))

    n_migrated = 0
    n_skipped = 0
    errors: list[tuple[str, str]] = []

    for sym in sorted(symbols):
        try:
            df = universe.read(sym).data
            if "VWAP" in df.columns:
                n_skipped += 1
                continue

            # Build the new column order: OHLCV_COLS first (with VWAP right
            # after Volume), then every other existing column in its current
            # order. This matches how daily_append constructs its write
            # (OHLCV_COLS + FEATURES).
            df_out = df.copy()
            df_out["VWAP"] = np.nan

            leading = [c for c in OHLCV_COLS if c in df_out.columns]
            trailing = [c for c in df_out.columns if c not in leading]
            df_out = df_out[leading + trailing]

            if dry_run:
                log.info(
                    "[dry-run] %s: would add VWAP, %d rows, %d cols → %d cols",
                    sym, len(df_out), len(df.columns), len(df_out.columns),
                )
                n_migrated += 1
                continue

            # write() replaces the symbol with the new schema. ArcticDB
            # versioning preserves the prior version, so this is recoverable.
            universe.write(sym, df_out)

            # Read-back verification: prove the new schema landed before
            # counting this symbol as migrated. Silent-write-failure caught
            # us on the 2026-04-14 RAG Phase 4/5 incident; apply the same
            # defensive check here.
            verify = universe.read(sym).data
            if "VWAP" not in verify.columns:
                raise RuntimeError(
                    f"post-write verify: VWAP column absent after write({sym!r})"
                )

            n_migrated += 1
            if n_migrated % 50 == 0:
                log.info("progress: migrated %d symbols", n_migrated)

        except Exception as exc:
            log.error("FAILED %s: %s", sym, exc)
            errors.append((sym, str(exc)))

    if errors:
        raise RuntimeError(
            f"migrate_universe_vwap: {len(errors)} symbol(s) failed "
            f"(migrated={n_migrated} skipped={n_skipped}). "
            f"First 10 failures: {errors[:10]}"
        )

    return n_migrated, n_skipped, 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill VWAP column into ArcticDB universe symbols"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    log.info("migrate_universe_vwap: start (dry_run=%s)", args.dry_run)
    n_migrated, n_skipped, n_err = migrate(dry_run=args.dry_run)
    log.info(
        "migrate_universe_vwap: DONE migrated=%d skipped_has_vwap=%d err=%d",
        n_migrated, n_skipped, n_err,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
