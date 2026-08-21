"""Lockstep guard for alpha-engine-config-I7798's two-job restructure of
sf-definition-validate.yml: the infra path list now appears twice inside the
same workflow file — once in the `label` job's dorny/paths-filter (decides
whether the `sf-definition-validate` job runs on a `pull_request` event) and
once in the `push` trigger's own `paths:` filter (decides whether a direct
push to `main` re-runs validation).

The two lists are INTENTIONALLY not identical: `push`'s list omits this
workflow file itself — a direct push that only changes the workflow file
changes no SF definition, so re-validating on push is pointless — while the
`label` filter includes it (a PR that edits the workflow deserves its own
job to actually run). This test asserts the `push` list is a SUBSET of the
`label` filter's list, with exactly that one documented exclusion, so any
OTHER drift between the two (an infra path added to one and not the other)
fails loudly instead of silently.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "sf-definition-validate.yml"

WORKFLOW_FILE_PATH = ".github/workflows/sf-definition-validate.yml"


def _label_filter_globs() -> list[str]:
    definition = yaml.safe_load(WORKFLOW.read_text())
    steps = definition["jobs"]["label"]["steps"]
    filter_step = next(s for s in steps if s.get("id") == "filter")
    filters_block = filter_step["with"]["filters"]
    parsed = yaml.safe_load(filters_block)
    return list(parsed["infra_changed"])


def _push_paths() -> list[str]:
    definition = yaml.safe_load(WORKFLOW.read_text())
    return list(definition[True]["push"]["paths"])  # YAML parses bare `on:` as True


def test_push_paths_are_subset_of_label_filter_minus_workflow_file():
    label_globs = set(_label_filter_globs())
    push_globs = set(_push_paths())

    assert WORKFLOW_FILE_PATH in label_globs, (
        "label job's infra_changed filter no longer includes the workflow "
        "file itself — a PR editing this workflow would silently stop "
        "running its own validate job"
    )
    assert WORKFLOW_FILE_PATH not in push_globs, (
        "push trigger's paths list now includes the workflow file itself — "
        "update this test's documented exclusion if that's deliberate"
    )

    expected_push_globs = label_globs - {WORKFLOW_FILE_PATH}
    assert push_globs == expected_push_globs, (
        "sf-definition-validate.yml's push.paths has drifted from the "
        "label job's infra_changed filter (minus the workflow file) — "
        f"push={push_globs!r} expected={expected_push_globs!r}"
    )


def test_sf_definition_validate_job_name_unchanged():
    definition = yaml.safe_load(WORKFLOW.read_text())
    job = definition["jobs"]["sf-definition-validate"]
    assert job["name"] == "sf-definition-validate", (
        "the job NAME must stay exactly 'sf-definition-validate' — a "
        "rename breaks the future required-check promotion (alpha-engine-"
        "config-I7798)"
    )
