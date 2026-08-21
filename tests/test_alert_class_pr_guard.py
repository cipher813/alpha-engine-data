"""Unit tests for ``infrastructure/overseer/alert_class_pr_guard.py``.

`alpha-engine-config-I7896` deliverable 2. Every test builds a real throwaway
git repo — the guard's whole method is a base-vs-head scan across a detached
worktree, so a test that mocked git away would exercise none of it.

The cases that matter, and why each is here rather than being obvious:

* **The negative control.** A PR that introduces an unregistered source must go
  RED. A guard nobody has watched fail is unproven, so this is asserted on the
  exit code AND on the output naming the target file and emitting a row.
* **Pre-existing drift must NOT fail an unrelated PR.** That is the distinction
  that keeps a chokepoint from being routed around on day one.
* **A removed row must fail in the registry repo's own PR.** The reverse
  direction of the same defect, and invisible to any file-path diff.
* **The pending-companion-PR tolerance, both ways.** Granted when an open PR in
  the registry repo carries the row (this is what stops the `I7860` deadlock
  from recurring), and UNMEASURED — never clean — when the lookup cannot run.
* **The severity vocabulary gotcha.** `publish_observe_alert`'s default is the
  literal `"WARN"`; nothing in `playbooks.yaml` matches `warn`.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

_OVERSEER = Path(__file__).resolve().parent.parent / "infrastructure" / "overseer"
sys.path.insert(0, str(_OVERSEER))
import alert_class_pr_guard as g  # noqa: E402
import alert_class_registry_drift as d  # noqa: E402


# ── helpers ─────────────────────────────────────────────────────────────────


def _git(repo: Path, *args: str) -> str:
    out = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True, timeout=60
    )
    return out.stdout


def _init_repo(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "t@t")
    _git(root, "config", "user.name", "t")


def _commit(root: Path, message: str) -> str:
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "--allow-empty", "-m", message)
    return _git(root, "rev-parse", "HEAD").strip()


def _playbooks(path: Path, sources: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "alert_classes": [
                    {
                        "class": g.class_name_for(s),
                        "source": s,
                        "severities": ["warning"],
                        "intake": "bus",
                        "response": "drain-queue",
                    }
                    for s in sources
                ]
                or [
                    {
                        "class": "placeholder_row",
                        "source": "placeholder:row",
                        "severities": ["warning"],
                        "intake": "bus",
                        "response": "drain-queue",
                    }
                ]
            }
        )
    )


def _emitter(path: Path, source: str, severity: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sev = f', severity="{severity}"' if severity else ""
    path.write_text(
        "from observe_alerts import publish_observe_alert\n\n\n"
        f'def go():\n    publish_observe_alert("m", source="{source}"{sev})\n'
    )


@pytest.fixture
def fleet(tmp_path: Path) -> dict:
    """A registry checkout and an emitter checkout, both real git repos."""
    registry = tmp_path / "nousergon-data"
    emitter = tmp_path / "crucible-research"
    _init_repo(registry)
    _init_repo(emitter)
    _playbooks(registry / g.PLAYBOOKS_REL, ["already:registered"])
    _commit(registry, "registry")
    _emitter(emitter / "lambda" / "existing.py", "already:registered")
    base = _commit(emitter, "base")
    return {
        "registry": registry,
        "emitter": emitter,
        "playbooks": registry / g.PLAYBOOKS_REL,
        "base": base,
    }


def _run(argv: list[str]) -> int:
    return g.main(argv)


# ── the negative control ────────────────────────────────────────────────────


def test_pr_adding_an_unregistered_source_fails(fleet, capsys):
    """THE negative control. A guard never observed failing is unproven."""
    _emitter(fleet["emitter"] / "lambda" / "new.py", "newthing:corpus_stats")
    _commit(fleet["emitter"], "add emitter")

    rc = _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ])
    err = capsys.readouterr().err
    assert rc == 1
    assert "newthing:corpus_stats" in err
    # Actionable without reading source: the exact file, a paste-ready row,
    # and the merge order. This is the bar the I7860 guard set.
    assert "infrastructure/overseer/playbooks.yaml" in err
    assert "source: newthing:corpus_stats" in err
    assert "class: newthing_corpus_stats" in err
    assert "MERGE ORDER" in err


def test_the_emitted_row_validates_against_the_live_schema(fleet, capsys):
    """A row that is 'paste-ready' but schema-invalid is not paste-ready."""
    import json

    _emitter(fleet["emitter"] / "lambda" / "new.py", "newthing:corpus_stats")
    _commit(fleet["emitter"], "add emitter")
    _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ])
    err = capsys.readouterr().err
    block = err[err.index("  - class:"):]
    row = yaml.safe_load(block.split("\n\n")[0])[0]

    schema = json.loads((_OVERSEER / "playbooks.schema.json").read_text())
    item = schema["properties"]["alert_classes"]["items"]
    assert set(item["required"]) <= set(row)
    assert not set(row) - set(item["properties"])
    assert row["severities"][0] in item["properties"]["severities"]["items"]["enum"]
    assert row["intake"] in item["properties"]["intake"]["enum"]


def test_a_clean_pr_passes(fleet):
    _emitter(fleet["emitter"] / "lambda" / "other.py", "already:registered")
    _commit(fleet["emitter"], "another registered emitter")
    assert _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ]) == 0


def test_pre_existing_drift_does_not_fail_an_unrelated_pr(fleet, capsys):
    """The whole reason this is a DELTA guard and not the fleet sweep."""
    _emitter(fleet["emitter"] / "lambda" / "old_gap.py", "ancient:gap")
    base = _commit(fleet["emitter"], "pre-existing drift")
    (fleet["emitter"] / "README.md").write_text("unrelated\n")
    _commit(fleet["emitter"], "unrelated change")

    rc = _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", base, "--head", "HEAD", "--no-pending-tolerance",
    ])
    assert rc == 0
    assert "ancient:gap" in capsys.readouterr().out  # named, but not gated


# ── the registry repo's own PRs ─────────────────────────────────────────────


def test_removing_a_row_fails_the_registry_repos_own_pr(fleet, capsys):
    """The reverse direction. A file-path diff sees a one-line YAML edit."""
    _emitter(fleet["registry"] / "collectors" / "emit.py", "data:collector_gap")
    _playbooks(fleet["playbooks"], ["already:registered", "data:collector_gap"])
    base = _commit(fleet["registry"], "emitter + row together")

    _playbooks(fleet["playbooks"], ["already:registered"])  # row removed
    _commit(fleet["registry"], "drop the row")

    rc = _run([
        "--repo", "nousergon-data", "--repo-root", str(fleet["registry"]),
        "--base", base, "--head", "HEAD", "--no-pending-tolerance",
    ])
    assert rc == 1
    assert "data:collector_gap" in capsys.readouterr().err


def test_registry_repo_may_add_emitter_and_row_in_one_commit(fleet):
    """No companion PR is needed when both live in the same repo — so the
    registry repo's own PRs can never be blocked by this guard on a change it
    is able to make itself. That is half of why the cycle cannot form."""
    _emitter(fleet["registry"] / "collectors" / "emit.py", "data:same_commit")
    _playbooks(fleet["playbooks"], ["already:registered", "data:same_commit"])
    _commit(fleet["registry"], "emitter + row")
    assert _run([
        "--repo", "nousergon-data", "--repo-root", str(fleet["registry"]),
        "--base", "HEAD~1", "--head", "HEAD", "--no-pending-tolerance",
    ]) == 0


# ── the companion-PR tolerance ──────────────────────────────────────────────


def test_open_companion_pr_carrying_the_row_satisfies_the_guard(fleet, capsys, monkeypatch):
    """The `I7860` deadlock, prevented. The emitter PR goes green the moment
    the row PR EXISTS — it does not have to merge first."""
    _emitter(fleet["emitter"] / "lambda" / "new.py", "newthing:corpus_stats")
    _commit(fleet["emitter"], "add emitter")

    monkeypatch.setattr(
        g, "pending_registry_patterns",
        lambda repo: ([("newthing_corpus_stats", "newthing:corpus_stats", False)],
                      {"newthing:corpus_stats": 1476}),
    )
    rc = _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD",
    ])
    out = capsys.readouterr().out
    assert rc == 0
    assert "TOLERATED-PENDING" in out
    assert "#1476" in out  # the reader is told WHICH PR must still merge


def test_an_unavailable_tolerance_is_unmeasured_never_clean(fleet, capsys, monkeypatch):
    """The `I7860` tolerance failed live by logging an HTTPError and then
    reading 'unknown' as 'not pending'. Here an unreadable tolerance fails the
    guard, and says so in those words."""
    _emitter(fleet["emitter"] / "lambda" / "new.py", "newthing:corpus_stats")
    _commit(fleet["emitter"], "add emitter")

    def _boom(repo):
        raise g.GuardError("REST (HTTPError: 500) and gh (exit 1)")

    monkeypatch.setattr(g, "pending_registry_patterns", _boom)
    rc = _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD",
    ])
    err = capsys.readouterr().err
    assert rc == 1
    assert "UNMEASURED" in err
    assert "newthing:corpus_stats" in err  # still fully actionable


def test_tolerance_refuses_to_truncate_a_long_open_pr_list(monkeypatch):
    monkeypatch.setattr(g, "_api_get", lambda path: [{"number": n, "head": {"sha": "x"}}
                                                     for n in range(g._MAX_OPEN_PRS)])
    with pytest.raises(g.GuardError, match="truncated"):
        g.pending_registry_patterns("nousergon/nousergon-data")


# ── cannot-look never equals clean ──────────────────────────────────────────


@pytest.mark.parametrize("mutate,expect", [
    (lambda f: ["--repo-root", "/nonexistent/path"], "not a git checkout"),
    (lambda f: ["--playbooks", "/nonexistent/playbooks.yaml"], "not readable"),
])
def test_unreadable_substrate_fails_as_unmeasured(fleet, capsys, mutate, expect):
    argv = [
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ]
    override = mutate(fleet)
    argv[argv.index(override[0]) + 1] = override[1]
    assert _run(argv) == 1
    assert expect in capsys.readouterr().err


def test_an_empty_registry_is_unmeasured_not_universal_drift(fleet, capsys):
    fleet["playbooks"].write_text("alert_classes: []\n")
    assert _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ]) == 1
    assert "declares no alert_classes" in capsys.readouterr().err


def test_a_cli_invocation_with_no_source_is_reported_as_unfixable_by_a_row(fleet, capsys):
    p = fleet["emitter"] / "infrastructure" / "box.sh"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text('#!/bin/bash\npython3 -m krepis.alerts publish --message "hi"\n')
    _commit(fleet["emitter"], "sourceless CLI invocation")
    assert _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ]) == 1
    err = capsys.readouterr().err
    assert "--source" in err
    assert "No registry row can ever cover it" in err


# ── the severity vocabulary gotcha ──────────────────────────────────────────


@pytest.mark.parametrize("literal,expected", [
    ("WARN", "warning"), ("warn", "warning"), ("WARNING", "warning"),
    ("ERROR", "error"), ("critical", "critical"), ("FATAL", "critical"),
    ("INFO", "info"), ("alarm", "alarm"), ("not-a-severity", "dynamic"),
])
def test_call_site_severity_is_normalized_to_the_registry_vocabulary(literal, expected):
    assert d.normalize_severity(literal) == expected
    assert expected in d.SEVERITY_VOCAB


def test_publish_observe_alert_default_becomes_warning_not_warn(fleet, capsys):
    """The measured gotcha: the wrapper's default parameter value is the string
    `"WARN"`; no row in playbooks.yaml uses `warn`, 35 use `warning`. A row
    written from the call-site literal would register a severity that matches
    nothing and fails the schema's closed enum."""
    _emitter(fleet["emitter"] / "lambda" / "new.py", "newthing:corpus_stats")  # no severity=
    _commit(fleet["emitter"], "add emitter")
    _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ])
    err = capsys.readouterr().err
    assert "severities: [warning]" in err
    assert "[warn]" not in err
    assert "[WARN]" not in err


def test_a_non_literal_severity_becomes_dynamic(fleet, capsys):
    p = fleet["emitter"] / "lambda" / "new.py"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        "from observe_alerts import publish_observe_alert\n\n\n"
        'def go(lvl):\n    publish_observe_alert("m", source="dyn:thing", severity=lvl)\n'
    )
    _commit(fleet["emitter"], "dynamic severity")
    _run([
        "--repo", "crucible-research", "--repo-root", str(fleet["emitter"]),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ])
    assert "severities: [dynamic]" in capsys.readouterr().err


def test_class_name_is_always_schema_legal():
    import re
    for source in ["a:b", "alpha-engine/executor/main.py", "x", "A::B__C",
                   "z" * 200, "weekly-sf/stage::fn"]:
        assert re.fullmatch(r"^[a-z0-9_]{3,60}$", g.class_name_for(source)), source

# NOTE ON THE FORK THIS MOVE CREATED. `scan_exclusions.py` stayed in
# alpha-engine-config with the three other scanners that import it, so
# `alert_class_registry_drift.EXCLUDED_DIR_NAMES` is a second copy of that
# list. The test pinning the two together lives in alpha-engine-config
# (`scripts/test_alert_class_sibling_contract.py`, run by
# `alert-class-sibling-contract.yml` at PR time) — that is the only repo where
# BOTH checkouts are present, so it is the only place the assertion can HARD
# FAIL rather than skip. A permanently-skipped test here would be a component
# emitting nothing, rendered green.


# ── the scanner must not scan itself ────────────────────────────────────────


def test_the_scanner_and_guard_modules_are_excluded_from_the_scan(fleet):
    """Caught by this guard on its OWN first CI run (nousergon-data-PR1479,
    run 32436244797): it reported `...`, `X` and `<missing --source>` as live
    emitters, all of them ILLUSTRATIONS in these two modules' docstrings. In
    alpha-engine-config the scanner was never in the scanned set; here it is.
    """
    repo = fleet["registry"]
    for rel in sorted(d.SELF_EXCLUDED_RELPATHS):
        dst = repo / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text((_OVERSEER / Path(rel).name).read_text(encoding="utf-8"))
    _commit(repo, "the scanner and guard themselves")

    assert _run([
        "--repo", "nousergon-data", "--repo-root", str(repo),
        "--base", "HEAD~1", "--head", "HEAD", "--no-pending-tolerance",
    ]) == 0


def test_the_self_exclusion_is_by_exact_path_not_by_filename(fleet, capsys):
    """A rule keyed on the basename — or worse, on 'mentions krepis.alerts' —
    would blind the scanner to a real emitter somewhere else in the tree. Only
    those two exact relative paths are exempt."""
    repo = fleet["emitter"]
    impostor = repo / "lambda" / "alert_class_pr_guard.py"
    _emitter(impostor, "impostor:not_exempt")
    _commit(repo, "same filename, different path")

    assert _run([
        "--repo", "crucible-research", "--repo-root", str(repo),
        "--playbooks", str(fleet["playbooks"]),
        "--base", fleet["base"], "--head", "HEAD", "--no-pending-tolerance",
    ]) == 1
    assert "impostor:not_exempt" in capsys.readouterr().err


def test_the_self_exclusion_names_files_that_actually_exist():
    """A path that stops resolving is an exemption nobody notices went stale."""
    for rel in d.SELF_EXCLUDED_RELPATHS:
        assert (_OVERSEER.parent.parent / rel).is_file(), rel
