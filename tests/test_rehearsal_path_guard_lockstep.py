"""Lockstep guard for alpha-engine-config-I6688 deliverable 2: the weekly-SF-
critical path list must be identical across three places, or the check that
requires a Rehearsal-path trailer and the canary that auto-arms on those
same paths silently drift apart — exactly the shape weekly-sf-policy.md
§7.4 documents happening to the policy text itself (two sections, same
requirement, different vocabulary, different exemption sets).

The three places:

  1. ``scripts/check_rehearsal_path.py``'s ``WEEKLY_CRITICAL_GLOBS`` — the
     canonical list.
  2. ``.github/workflows/rehearsal-path-guard.yml``'s own ``weekly_critical``
     paths-filter (decides whether the trailer check even runs).
  3. The weekly-critical subset of ``.github/workflows/canary-replay.yml``'s
     ``weekly_critical`` paths-filter (decides whether the canary auto-arms
     — this filter also carries its own pre-existing entries unrelated to
     I6688, e.g. ``rag/pipelines/**``, so this test asserts a SUBSET
     relationship there, not equality).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_rehearsal_path.py"
GUARD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "rehearsal-path-guard.yml"
CANARY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "canary-replay.yml"


def _load_check_module():
    spec = importlib.util.spec_from_file_location("check_rehearsal_path", CHECK_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_rehearsal_path"] = module
    spec.loader.exec_module(module)
    return module


def _weekly_critical_filter_globs(workflow_path: Path, job_id: str) -> list[str]:
    """Extract the `weekly_critical` filter list embedded as a YAML string
    inside a `dorny/paths-filter` step's `with.filters` block."""
    definition = yaml.safe_load(workflow_path.read_text())
    steps = definition["jobs"][job_id]["steps"]
    filter_step = next(s for s in steps if s.get("id") == "filter")
    filters_block = filter_step["with"]["filters"]
    parsed = yaml.safe_load(filters_block)
    return list(parsed["weekly_critical"])


def test_check_script_globs_match_guard_workflow_filter():
    canonical = list(_load_check_module().WEEKLY_CRITICAL_GLOBS)
    guard_globs = _weekly_critical_filter_globs(GUARD_WORKFLOW, "label")
    assert guard_globs == canonical, (
        "rehearsal-path-guard.yml's paths-filter has drifted from "
        "scripts/check_rehearsal_path.py's WEEKLY_CRITICAL_GLOBS — "
        f"guard={guard_globs!r} canonical={canonical!r}"
    )


def test_canary_replay_filter_is_superset_of_weekly_critical_globs():
    canonical = set(_load_check_module().WEEKLY_CRITICAL_GLOBS)
    canary_globs = set(_weekly_critical_filter_globs(CANARY_WORKFLOW, "label"))
    missing = canonical - canary_globs
    assert not missing, (
        "canary-replay.yml's paths-filter is missing weekly-critical paths "
        f"that rehearsal-path-guard.yml enforces: {missing!r} — the canary "
        "will not auto-arm on a PR that the trailer check gates."
    )
