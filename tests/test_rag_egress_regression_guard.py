"""Egress regression guard — CI-enforced mechanical check (config#2958 deliverable 3).

The 2026-07-16 Neon quota exhaustion's proximate cause was a single SQL
fragment (``SELECT c.embedding FROM rag.chunks``) running a full-corpus
vector read amplified by per-PR canary replays. The fix for that specific
site is centroid pushdown (``AVG(c.embedding)``, server-side) — but prose
docstring conventions don't prevent the same class of unbounded embedding
read from being re-introduced in a new module or a refactor.

This test closes the enforcement gap: it mechanically scans every Python file
under ``rag/`` for SQL fragments (lines containing both a SQL keyword and
``.embedding``) and fails if any embedding-column reference lacks a safe
wrapper — aggregators (AVG, VAR_SAMP, COUNT, ...), the pgvector ``<=>``
cosine-distance operator, or ``IS [NOT] NULL`` in a WHERE clause.

A line that ships a raw 512-dim vector across the wire to a single caller
(even one consumer) could, if replayed across every canary push the way the
``synchronize`` trigger amplified the old query, re-exhaust the quota. This
test makes that pattern structurally impossible to reintroduce without
failing CI.

The whitelist is deliberately narrow — false positives (a line that looks
dangerous but isn't) should be *suppressed by adding it to the list*, not
by widening the regex. The list is checked at runtime: a safe line that
was dropped from the source (refactored away) yields a CI failure so the
stale entry is removed immediately rather than silently rotting.

See nousergon-lib's ``tests/test_rag_egress_regression_guard.py`` for the
identical guard on the library/retrieval side. The two guards cover the
SQL sites in the library (retrieval path) and the data pipelines (ingest
+ batch-read path) respectively.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path


def _rag_source_files() -> list[Path]:
    """Every .py file under ``rag/`` (pipelines + preflight + __init__)."""
    rag_root = Path(__file__).resolve().parents[1] / "rag"
    if not rag_root.is_dir():
        raise AssertionError(f"rag source directory not found: {rag_root}")
    return sorted(p for p in rag_root.rglob("*.py") if p.name != "__pycache__")


# ── Egress-guard core ────────────────────────────────────────────────────────


_SQL_KEYWORDS_RE = re.compile(
    r"\b(SELECT|FROM|JOIN|WHERE|ORDER\s+BY|GROUP\s+BY|HAVING|INSERT|UPDATE|DELETE|LIMIT)\b",
    re.IGNORECASE,
)

_EMBEDDING_COLUMN_RE = re.compile(r"\b\w+\.embedding\b")

_SAFE_WRAPPER_RE = re.compile(
    r"\b(AVG|VAR_SAMP|VAR_POP|STDDEV_SAMP|STDDEV_POP|SUM|COUNT|MIN|MAX)\s*\([^)]*\.embedding|"
    r"\.embedding\s*<=>|"
    r"<=>\s*[^)]+\)|"
    r"\.embedding\s+IS\s+(NOT\s+)?NULL"
)


def _offending_lines(file_path: Path) -> list[tuple[int, str]]:
    """Return (lineno, stripped_line) for every unsafe ``.embedding`` SQL
    reference in *file_path*. An empty result means the file is clean."""
    source = file_path.read_text(encoding="utf-8")
    lines = source.splitlines()
    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        tree = None

    docstring_lines: set[int] = set()
    if tree is not None:
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)):
                doc = ast.get_docstring(node)
                if doc is not None:
                    start = node.body[0].lineno
                    end = node.body[0].end_lineno  # type: ignore[attr-defined]
                    docstring_lines.update(range(start, end + 1))

    offending: list[tuple[int, str]] = []
    for i, line in enumerate(lines, start=1):
        if i in docstring_lines:
            continue
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if not _SQL_KEYWORDS_RE.search(stripped):
            continue
        if not _EMBEDDING_COLUMN_RE.search(stripped):
            continue
        if _SAFE_WRAPPER_RE.search(stripped):
            continue
        offending.append((i, stripped))
    return offending


# ── Known-acceptable lines — suppressed by exact line (checked at runtime) ───
# Each entry is (file_path_relative_to_cwd, lineno, canonical_line_text).
# When a line is genuinely safe but trips the regex, add it here INSTEAD of
# loosening the regex — and when the source line is removed or refactored,
# this test will fail on the stale entry, forcing it out of the list.
#
# Rationale for each entry is recorded as a comment. Entries without a comment
# are a CI-policing gap — don't add one without writing *why* it's safe.
_KNOWN_SAFE: list[tuple[str, int, str]] = [
    # No entries needed as of 2026-07-24 — every ``.embedding`` reference
    # under rag/ is either inside a safe wrapper (AVG / <=> / IS NULL),
    # inside a comment or docstring, or an import of the ``embeddings``
    # submodule from nousergon-lib. The regex correctly passes them all.
    # If a future change adds a line that trips this guard but is genuinely
    # safe, add it here and explain *in the comment* what makes it safe.
]


# ── Test entry points ────────────────────────────────────────────────────────


def test_no_unbounded_embedding_selects_in_rag():
    """Fails CI on any SQL fragment that selects ``.embedding`` without a
    safe wrapper (aggregator, ``<=>``, or ``IS [NOT] NULL``)."""
    files = _rag_source_files()
    assert files, "no .py files found under rag/ — check the guard is running from the correct directory"

    all_offending: dict[str, list[tuple[int, str]]] = {}
    known_safe_index = {
        (rel_path, lineno): (lineno, text)
        for rel_path, lineno, text in _KNOWN_SAFE
    }

    for file_path in files:
        rel = str(file_path.relative_to(file_path.parents[1]))  # e.g. "rag/pipelines/filing_change_detection.py"
        offending = _offending_lines(file_path)
        filtered = []
        for lineno, text in offending:
            key = (rel, lineno)
            if key in known_safe_index:
                expected_lineno, expected_text = known_safe_index[key]
                if lineno == expected_lineno and text == expected_text:
                    continue
            filtered.append((lineno, text))
        if filtered:
            all_offending[rel] = filtered

    if not all_offending:
        return

    lines = []
    for rel in sorted(all_offending):
        lines.append(f"\n  {rel}:")
        for lineno, text in all_offending[rel]:
            lines.append(f"    L{lineno}: {text[:120]}")
    msg = (
        "Unbounded embedding-column references found in RAG SQL.\n"
        "Each line references ``table.embedding`` inside a SQL fragment "
        "without wrapping it in AVG(), VAR_SAMP(), the ``<=>`` operator, "
        "or ``IS [NOT] NULL`` — the three patterns proven not to ship raw "
        "512-dim vectors to the client.\n"
        "The 2026-07-16 Neon quota exhaustion was this exact pattern "
        "(``SELECT c.embedding FROM rag.chunks``) amplified by per-PR "
        "canary replays.\n"
        "\n"
        "Fix: wrap the column in AVG() / <=> / IS NULL, or if this is a "
        "genuinely safe reference, add an entry to ``_KNOWN_SAFE`` in "
        "tests/test_rag_egress_regression_guard.py WITH a comment explaining "
        "why it's safe."
        + "".join(lines)
    )
    raise AssertionError(msg)


def test_known_safe_list_is_current():
    """Every entry in ``_KNOWN_SAFE`` must correspond to a real source line
    (same file + line number + text) — stale entries fail the test so the
    list doesn't silently rot across refactors."""
    files = _rag_source_files()
    all_lines: dict[str, dict[int, str]] = {}
    for file_path in files:
        rel = str(file_path.relative_to(file_path.parents[1]))
        all_lines[rel] = {
            i: line.rstrip("\n")
            for i, line in enumerate(file_path.read_text(encoding="utf-8").splitlines(), start=1)
        }

    stale = []
    for rel_path, lineno, expected_text in _KNOWN_SAFE:
        actual = all_lines.get(rel_path, {}).get(lineno)
        if actual is None:
            stale.append(f"  {rel_path}:{lineno} — FILE or LINE no longer exists at this position")
        elif actual != expected_text:
            stale.append(
                f"  {rel_path}:{lineno} — TEXT diverged\n"
                f"    expected: {expected_text[:100]}\n"
                f"    actual:   {actual[:100]}"
            )
    if stale:
        raise AssertionError(
            f"{len(stale)} stale entry(s) in _KNOWN_SAFE — remove them:\n"
            + "\n".join(stale)
        )
