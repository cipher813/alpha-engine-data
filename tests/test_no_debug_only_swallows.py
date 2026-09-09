"""Class guard: no `except Exception` handler in this repo's source
directories may swallow a failure into `logger.debug(...)` (or `log.debug`)
or a bare `pass`, with the root logger at INFO on every entrypoint — that
means the record is emitted **nowhere** (alpha-engine-config-I10031, the
originating incident in `crucible-executor`).

This repo is the fleet's PRODUCER — it owns the ArcticDB price universe,
macro indicators, the engineered feature store, corporate actions, RAG
ingestion, and per-ticker alternative data (see `AGENTS.md`). Its own
standing rule is stricter than the fleet default: "the fleet's fail-loud
default has no graceful-degrade carve-out on any writer here. A collector
that swallows and returns partial data is a silent corruption of every
consumer, not a degraded run." Every entry in
`.debug-swallow-allowlist.yaml` was triaged against that rule specifically —
see the file header there.

The AST detector itself (the class this test enforces, and the exact same
`_is_debug_only_or_pass` shape) lives in `nousergon_lib.testing.debug_swallow_guard`
— lifted out of `crucible-executor/tests/test_no_debug_only_swallows.py`
(`crucible-executor-PR547`) on second adoption per `policy-shared-code`
(`alpha-engine-config-I10226` measured the same class present in five
sibling repos). This file is a thin call-site: scan this repo's source
directories, diff against the repo-local allowlist, done.

A new site with no allowlist entry fails the build (`_UNCOVERED` case). An
allowlist entry whose `expires` has passed fails loudly — re-justify or
remove, never silently re-grandfather (`_EXPIRED` case). An entry that no
longer matches anything ALSO fails, so the allowance cannot quietly widen
after the site it covered is fixed or moves (`_STALE` case) — mirrors
`.provider-linkage-allowlist.yaml` / `nousergon-lib/scripts/
provider_linkage_guard.py` (alpha-engine-config-I9295).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nousergon_lib.testing.debug_swallow_guard import (
    check_against_allowlist,
    check_allowlist_entries_self_contained,
    find_debug_only_swallows,
    load_allowlist,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ALLOWLIST_PATH = _REPO_ROOT / ".debug-swallow-allowlist.yaml"

# Every top-level source directory in this repo, mirroring OVERVIEW.md's
# module map (`collectors/` fetch, `builders/` assemble, `features/`
# compute, `rag/` embeds, `corporate_actions/` adjusts, plus the loose
# top-level modules — `dates.py`, `emailer.py`, `polygon_client.py`,
# `preflight.py`, `sf_preflight.py`, `weekly_collector.py`). Each entry is
# scanned non-recursively (the repo root itself included, for the loose
# top-level `*.py` files), matching `find_debug_only_swallows`'s
# `executor/*.py` shape from the original detector — a multi-package repo
# passes each package directory separately.
_SOURCE_DIRS = (
    "",
    "builders",
    "collectors",
    "contracts",
    "corporate_actions",
    "data",
    "features",
    "infrastructure",
    "lambda",
    "migrations",
    "rag",
    "scripts",
)


def _all_swallow_sites() -> dict[str, set[int]]:
    merged: dict[str, set[int]] = {}
    for rel_dir in _SOURCE_DIRS:
        source_dir = (_REPO_ROOT / rel_dir) if rel_dir else _REPO_ROOT
        for path, lines in find_debug_only_swallows(source_dir, repo_root=_REPO_ROOT).items():
            if lines:
                merged.setdefault(path, set()).update(lines)
    return merged


def test_no_new_debug_only_swallows_outside_allowlist():
    """Every debug-only-or-pass `except Exception` swallow in this repo's
    source directories is either fixed (raised, or recorded at
    WARNING/ERROR+) or has a non-expired, matching entry in
    `.debug-swallow-allowlist.yaml`."""
    live_sites = _all_swallow_sites()
    allowlist = load_allowlist(_ALLOWLIST_PATH)
    failures = check_against_allowlist(live_sites, allowlist)
    assert not failures, "\n".join(failures)


def test_allowlist_entries_are_self_contained():
    """Every entry names a reason, an expiry, and a tracking issue — a
    swallow with no named recording surface is not a swallow, it is a
    deletion (alpha-engine-config-I10031 deliverable 2)."""
    allowlist = load_allowlist(_ALLOWLIST_PATH)
    failures = check_allowlist_entries_self_contained(allowlist)
    assert not failures, "\n".join(failures)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
