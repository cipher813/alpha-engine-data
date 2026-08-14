"""No Lambda dispatcher in this repo may render a spot bootstrap inline.

## Why this exists separately from the shell guard

``krepis.spot_bootstrap.scan_for_inline_bootstraps`` — the fleet detector, and
what ``tests/test_no_inline_spot_bootstrap.py`` runs — only walks ``.sh`` and
``.bash``. Every finding it produced across the five spot-launching repos was
therefore a shell file, and the sweep that produced the cutover list
(alpha-engine-config-I7372) could not see that ``nousergon-data`` also
bootstraps spots from **Python**: five ``infrastructure/lambdas/*/index.py``
dispatchers built the same script as an f-string, carrying the same silent
interpreter fallback

    command -v python3.12 >/dev/null && PYTHON_BIN=python3.12 || PYTHON_BIN=python3

and, in three of them, a hand-written ``systemd-run --on-active`` timer with no
``ec2-spot-watchdog`` unit beside it. A detector that cannot read the language
a defect is written in reports the defect's absence.

## What this asserts, and how it is derived

Every string literal in every ``index.py`` under ``infrastructure/lambdas/`` is
classified with ``krepis.spot_bootstrap.BOOTSTRAP_SIGNATURES`` — the SAME
signature table and the SAME ``MIN_CATEGORIES`` threshold the shell scanner
uses, so a fleet-wide tightening of the definition reaches Python too instead
of stopping at this repo's door. Nothing here names a handler, a function or a
variable: a dispatcher that renders a bootstrap under a new name, in a new
directory, or assembled by a differently-named helper still trips.

**Per string literal, not per file.** Two reasons:

1. It is what makes the check honest about composition. A cut-over dispatcher
   legitimately keeps a prelude and a tail of its own (a private-repo clone the
   renderer cannot express, a venv build, a workload). Those are separate
   regions and each is judged on its own, so ``fail() { … shutdown -h now; }``
   in a prelude and a ``git clone`` in a tail are two single-category regions,
   not one two-category bootstrap.
2. The file-level alternative is a measured failure mode.
   ``scan_for_inline_bootstraps`` clears a whole FILE on its first
   ``-m krepis.spot_bootstrap`` match, before evaluating a single signature —
   so a file that renders in one place and hand-rolls in another scans clean
   (``alpha-engine-config-I7378``). This guard is written without that bug from
   the start: importing the renderer earns a file nothing.
"""

from __future__ import annotations

import ast
from pathlib import Path

from krepis.spot_bootstrap import BOOTSTRAP_SIGNATURES, MIN_CATEGORIES

_LAMBDAS = Path(__file__).resolve().parents[1] / "infrastructure" / "lambdas"

#: Dispatchers this repo does NOT own the cutover of, by directory name.
#: Exhaustive, and each entry states who owns it and when it clears — an
#: allowlist without an owner is where the next fork hides.
_NOT_OURS_THIS_ROUND = {
    # ── Overseer plane (alpha-engine-config-I7374). A concurrent agent owns
    # the substrate these five share; cutting them over from here would
    # conflict with that work mid-flight. Same defect class, different owner.
    "alert-drain-dispatcher",
    "canary-replay-dispatcher",
    "ci-watch-dispatcher",
    "scheduled-groom-dispatcher",
    "sf-watch-spot-dispatcher",
    # ── thinktank-spot-dispatcher is a PRELUDE, not a bootstrap: it execs
    # crucible-research's own `thinktank_spot_bootstrap.sh`, which lives in
    # that repo and is that repo's cutover to make. Removing it from here
    # would move the fork, not end it.
    "thinktank-spot-dispatcher",
}


def _string_regions(source: str) -> "list[tuple[int, str]]":
    """``(lineno, text)`` for every string literal in ``source``.

    f-strings are flattened to their literal parts joined by newlines: an
    interpolated value cannot be known statically, and every signature this
    classifies is a literal command anyway (``dnf install``, ``git clone``,
    ``systemd-run --on-active``, ``shutdown -h now``). Joining on newlines
    rather than the empty string keeps two adjacent literal fragments from
    fusing into a line that neither of them contains.
    """
    regions: list[tuple[int, str]] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            regions.append((node.lineno, node.value))
        elif isinstance(node, ast.JoinedStr):
            parts = [
                v.value
                for v in node.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)
            ]
            regions.append((node.lineno, "\n".join(parts)))
    return regions


def _findings(path: Path) -> "list[str]":
    out: list[str] = []
    for lineno, region in _string_regions(path.read_text(encoding="utf-8")):
        hits = tuple(
            name
            for name, (_, pat) in sorted(BOOTSTRAP_SIGNATURES.items())
            if pat.search(region)
        )
        categories = {BOOTSTRAP_SIGNATURES[name][0] for name in hits}
        if len(categories) >= MIN_CATEGORIES:
            out.append(f"{path.parent.name}/index.py:{lineno}: {', '.join(hits)}")
    return out


def _handlers() -> "list[Path]":
    return sorted(_LAMBDAS.glob("*/index.py"))


def test_no_lambda_handler_renders_a_bootstrap_inline():
    violations: list[str] = []
    for path in _handlers():
        if path.parent.name in _NOT_OURS_THIS_ROUND:
            continue
        violations.extend(_findings(path))
    assert not violations, (
        "Lambda handler(s) building a spot bootstrap inline — render through "
        "krepis.spot_bootstrap.render_bootstrap() instead "
        "(alpha-engine-config-I7372):\n  " + "\n  ".join(violations)
    )


def test_the_allowlist_names_only_directories_that_exist():
    """An allowlist entry that no longer matches anything is a silent hole.

    A dispatcher renamed while exempt would be exempt under its old name
    forever and unguarded under its new one, which is exactly how an
    exemption list stops being a list of exceptions and becomes a blind spot.
    """
    present = {p.parent.name for p in _handlers()}
    stale = sorted(_NOT_OURS_THIS_ROUND - present)
    assert not stale, (
        "allowlisted dispatcher(s) no longer exist — drop the entry or fix the "
        f"name: {stale}"
    )


def test_every_allowlisted_dispatcher_still_has_the_defect():
    """And an entry that is CLEAN must be removed, not left standing.

    This is the half an allowlist normally omits. Once the Overseer-plane arc
    (alpha-engine-config-I7374) lands, these entries stop being true, and an
    exemption nobody revisits is indistinguishable from coverage. This test
    fails the moment one of them is cut over, which is the reminder.
    """
    clean = sorted(
        name
        for name in _NOT_OURS_THIS_ROUND
        if (_LAMBDAS / name / "index.py").is_file()
        and not _findings(_LAMBDAS / name / "index.py")
    )
    assert not clean, (
        "allowlisted dispatcher(s) no longer bootstrap inline — remove them "
        f"from _NOT_OURS_THIS_ROUND so they are guarded from now on: {clean}"
    )
