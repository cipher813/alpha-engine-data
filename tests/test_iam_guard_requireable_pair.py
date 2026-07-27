"""The IAM guard and its no-op companion must stay a matched pair.

nousergon-data-I1078. `iam-policy-change-guard.yml` is path-filtered to
`infrastructure/lambdas/*/iam-policy.json`, so making it a REQUIRED status
check directly would block every PR that does not touch IAM — GitHub waits
forever for a status that never reports.

The fix is a companion workflow triggering on the exact complement
(`paths-ignore`) with the SAME workflow name and job id, so exactly one of the
pair runs per PR and the shared context always reports.

That only holds while the two lists stay in lockstep. If they drift:

  - guard `paths` ⊄ noop `paths-ignore`  ->  BOTH run on some PRs (duplicate
    context, ambiguous required status)
  - noop `paths-ignore` ⊄ guard `paths`  ->  NEITHER runs on some PRs, and a
    required context that never reports blocks the merge permanently

Either way the required check becomes unreliable, which is worse than not
requiring it — an unenforceable guard reads as protection that is not there.
That is exactly what happened before: the guard was advisory-only, reported
FAIL on nousergon-data-PR1077, and the PR merged anyway.
"""

from __future__ import annotations

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"
GUARD = WORKFLOWS / "iam-policy-change-guard.yml"
NOOP = WORKFLOWS / "iam-policy-change-guard-noop.yml"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(doc: dict) -> dict:
    # PyYAML parses the bare key `on:` as the boolean True (YAML 1.1).
    return doc.get("on", doc.get(True)) or {}


def test_both_workflows_exist():
    assert GUARD.is_file(), "the IAM policy-change guard is missing"
    assert NOOP.is_file(), (
        "the guard's no-op companion is missing — without it the guard's context "
        "cannot be required without deadlocking non-IAM PRs"
    )


def test_workflow_names_match():
    """Branch protection keys off the reported context, which derives from the
    workflow name + job id. Divergent names produce two distinct contexts."""
    assert _load(GUARD)["name"] == _load(NOOP)["name"]


def test_job_ids_match_and_are_specific():
    guard_jobs = list(_load(GUARD)["jobs"])
    noop_jobs = list(_load(NOOP)["jobs"])
    assert guard_jobs == noop_jobs, (
        f"job ids diverged: guard={guard_jobs} noop={noop_jobs} — the pair would "
        f"report two different check contexts"
    )
    assert guard_jobs == ["iam-policy-change-guard"], (
        "the job id IS the required branch-protection context; keep it specific "
        "so a future workflow's generic job name cannot collide with a required "
        "status"
    )


def test_path_filters_are_exact_complements():
    guard_paths = _triggers(_load(GUARD))["pull_request"]["paths"]
    noop_ignores = _triggers(_load(NOOP))["pull_request"]["paths-ignore"]
    assert guard_paths == noop_ignores, (
        f"guard paths {guard_paths} and noop paths-ignore {noop_ignores} must be "
        f"identical. Drift means some PR gets BOTH checks (ambiguous) or NEITHER "
        f"(required context never reports -> permanent merge block)."
    )


def test_both_react_to_label_events():
    """The guard's escape hatch is a `gate:operator` label, so both halves must
    re-run on label changes — otherwise adding the label after the initial run
    leaves the failing status in place with no way to clear it."""
    for path in (GUARD, NOOP):
        types = _triggers(_load(path))["pull_request"].get("types") or []
        assert "labeled" in types and "unlabeled" in types, (
            f"{path.name} must trigger on labeled/unlabeled so the gate:operator "
            f"escape hatch can actually clear the check"
        )
