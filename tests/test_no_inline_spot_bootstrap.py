"""No shell file in this repo may bootstrap a spot instance inline.

## Why

``infrastructure/_spot_common.sh`` and ``crucible-predictor``'s file of the
same name were never forked from each other — both were written by applying
ARCHITECTURE.md §111, which named a shape and no implementation. They diverged,
and over 2026-08-10/11 the weekly Step Function failed three consecutive times,
each on a different defect inside that one script, each fixed twice
(``nousergon-data#1294``/``#1296``, then ``crucible-predictor#461``/``#462``
16-23 hours later). ``krepis.spot_bootstrap`` ended the duplication;
``nousergon-data#1388`` cut ``bootstrap_spot()`` over, and
``alpha-engine-config-I7372`` cut over the last shell copy in this repo,
``infrastructure/spot_data_weekly.sh``.

This test is what stops a fourth copy. Deriving matters more than the
assertion: the detector is behavioural and names no file, so a bootstrap that
reappears under a new filename, in a new directory, or with every function
renamed is still caught. ``crucible-backtester``'s fork renamed every function
it shared with its twin and was invisible to the ``grep -rl
ec2-spot-watchdog`` search that ``alpha-engine-config-I6922`` prescribed —
a search keyed to one marker cannot see a fork whose divergence is the absence
of that marker.

## The three assertions, and why one is not enough

1. ``scan_for_inline_bootstraps`` — the fleet detector, owned by ``krepis`` so
   that tightening it tightens every repo at once.
2. The same signature table applied PER REGION — a repo-local backstop for a
   MEASURED hole in (1): ``scan_for_inline_bootstraps`` clears a whole FILE
   on a single ``-m krepis.spot_bootstrap`` match, before evaluating any
   signature, so a file that delegates in one function and hand-rolls a
   bootstrap in another scans clean. Filed as ``alpha-engine-config-I7378``
   (the fix belongs in krepis: clear per rendered region, not per file). Until
   that lands, this repo's own guard is what covers the delegate-plus-heredoc
   shape, which is the shape both surviving forks are one edit away from.
3. No silent interpreter fallback anywhere under ``infrastructure/`` — see
   below. This one trips no bootstrap signature at all, so neither (1) nor (2)
   would ever see it.

## The silent fallback (assertion 3)

    command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3

``requirements.txt`` is resolved against 3.12. Falling back to the AMI's
python3 resolves different wheels and reports nothing: the first symptom is an
``ImportError`` or a subtly different numpy several steps and one process
later. It survived ``#1388`` in ``_spot_common.sh``'s ``install_deps()`` — the
step where the interpreter decides which wheels get installed — and in all
four per-stage launchers, because that PR collapsed only the bootstrap.
Removing it from a bootstrap is not the fix; every step that selects an
interpreter must resolve strictly or exit non-zero.
"""

from __future__ import annotations

import re
from pathlib import Path

from krepis.spot_bootstrap import (
    BOOTSTRAP_SIGNATURES,
    MIN_CATEGORIES,
    scan_for_inline_bootstraps,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

#: Same subdirectories ``scan_for_inline_bootstraps`` walks by default, so
#: assertions 2 and 3 cover exactly the surface assertion 1 does.
_SUBDIRS = ("infrastructure", "scripts", "bin")
_SUFFIXES = (".sh", ".bash")

#: A ``PYTHON``-ish variable assigned from a 3.12 probe that falls through to a
#: bare ``python3``. Written against the ACT (a conditional selection ending in
#: ``python3``), not against the exact line that shipped, so reintroducing it
#: with a different variable name, spacing, or probe order still trips.
_SILENT_FALLBACK = re.compile(
    r"command\s+-v\s+python3\.\d+[^\n]*\|\|\s*\w*(?:PY|PYTHON)\w*\s*=\s*python3\b",
    re.I,
)

#: The delegation to the shared renderer, matched against the INVOCATION and
#: never a mention — the same discipline ``krepis._DELEGATES`` uses, because
#: every cut-over file now carries comments NAMING the module. Unlike krepis's,
#: this one deletes the matching LINES and keeps scanning the rest, which is
#: the whole point of assertion 2.
_DELEGATE_LINE = re.compile(r"-m\s+krepis\.spot_bootstrap\b")

#: The shell heredoc opener a hand-rolled remote script arrives in.
_HEREDOC_OPEN = re.compile(r"<<-?\s*['\"]?(\w+)['\"]?\s*$", re.M)


def _shell_files() -> list[Path]:
    files: list[Path] = []
    for subdir in _SUBDIRS:
        base = _REPO_ROOT / subdir
        if not base.is_dir():
            continue
        files.extend(
            p for p in sorted(base.rglob("*")) if p.is_file() and p.suffix in _SUFFIXES
        )
    return files


def _strip_comments(text: str) -> str:
    """Drop whole-line shell comments before classifying.

    Both directions matter, and this mirrors ``krepis``'s own stripping. A file
    EXPLAINING a defect it has already fixed — which every file this PR touches
    now does — must not read as carrying it. And a comment must not be able to
    clear a file either, which is why only whole-line comments go.
    """
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )


def test_no_shell_file_bootstraps_a_spot_inline():
    """Assertion 1 — the fleet detector, derived from the tree."""
    findings = scan_for_inline_bootstraps(_REPO_ROOT)
    assert not findings, (
        "inline spot bootstrap(s) found — these must render through "
        "krepis.spot_bootstrap (alpha-engine-config-I6922 / I7372):\n  "
        + "\n  ".join(str(f) for f in findings)
    )


def _regions(code: str) -> "list[tuple[str, str]]":
    """Split a shell file into independently-classifiable regions.

    ``(label, text)`` for each heredoc body, plus one region for the remaining
    launcher-side code with every delegating line removed. Regions are the
    unit because a bootstrap is a REGION-sized act: a file that renders one
    step and hand-rolls another contains both, and a file-level verdict can
    only report the first one it meets.
    """
    lines = code.splitlines()
    regions: list[tuple[str, str]] = []
    rest: list[str] = []
    i = 0
    while i < len(lines):
        m = _HEREDOC_OPEN.search(lines[i])
        if not m:
            if not _DELEGATE_LINE.search(lines[i]):
                rest.append(lines[i])
            i += 1
            continue
        delim = m.group(1)
        body: list[str] = []
        i += 1
        while i < len(lines) and lines[i].strip() != delim:
            body.append(lines[i])
            i += 1
        regions.append((f"heredoc <<{delim}", "\n".join(body)))
        i += 1
    regions.append(("launcher-side code", "\n".join(rest)))
    return regions


def test_no_region_of_any_shell_file_bootstraps_a_spot():
    """Assertion 2 — the repo-local backstop for the file-level clearing bug.

    ``scan_for_inline_bootstraps`` returns early on the FIRST delegate match in
    a file, before evaluating a single signature, so a file that renders in one
    function and hand-rolls a bootstrap in another is reported clean
    (measured; filed as ``alpha-engine-config-I7378`` — the fix belongs in
    krepis, which owns the detector). This applies the SAME signature table and
    the SAME two-category threshold, but per region and with delegating lines
    removed rather than treated as absolution. Reusing ``BOOTSTRAP_SIGNATURES``
    rather than restating it keeps a fleet-wide tightening from stopping at
    this repo's door.
    """
    violations: list[str] = []
    for path in _shell_files():
        code = _strip_comments(path.read_text(encoding="utf-8"))
        for label, region in _regions(code):
            hits = tuple(
                name
                for name, (_, pat) in sorted(BOOTSTRAP_SIGNATURES.items())
                if pat.search(region)
            )
            categories = {BOOTSTRAP_SIGNATURES[name][0] for name in hits}
            if len(categories) >= MIN_CATEGORIES:
                violations.append(
                    f"{path.relative_to(_REPO_ROOT)} [{label}]: {', '.join(hits)}"
                )
    assert not violations, (
        "a region of a shell file bootstraps a spot without going through "
        "krepis.spot_bootstrap — a delegate call elsewhere in the same file "
        "does not clear it:\n  " + "\n  ".join(violations)
    )


def test_no_silent_interpreter_fallback():
    """Assertion 3 — the defect no bootstrap signature can see."""
    violations: list[str] = []
    for path in _shell_files():
        for line in _strip_comments(path.read_text(encoding="utf-8")).splitlines():
            if _SILENT_FALLBACK.search(line):
                violations.append(f"{path.relative_to(_REPO_ROOT)}: {line.strip()}")
    assert not violations, (
        "silent interpreter fallback — resolve python3.12 strictly and exit "
        "non-zero when it is absent; requirements.txt is compiled against "
        "3.12 and the AMI's python3 resolves different wheels "
        "(alpha-engine-config-I7372):\n  " + "\n  ".join(violations)
    )
