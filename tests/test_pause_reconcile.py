#!/usr/bin/env python3
"""The pause manifest, the observability registry and live AWS must agree.

`automation_pause.py` reconciles the manifest against live AWS **for the names
the manifest happens to list** — its coverage is hand-listed, which is
`observability-policy.md` §2.2's defect one level up. And nothing reconciled the
registry's `lifecycle: disabled` declarations, which are hand-copied assertions
about a file in another repository, against the manifest they quote.
`pause_reconcile.py` closes both (alpha-engine-config-I7118).

Every finding class below is exercised by INDUCING the condition, per §9's
commissioning standard — a detector nobody has watched fail is not a detector.
The three registers are injected, so the whole comparison runs with no AWS and
no network; the live wiring is asserted separately (`test_ci_runs_the_reconcile`).
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
MODULE_PATH = INFRA / "pause_reconcile.py"
MANIFEST_PATH = INFRA / "automation_pause.json"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "sf-arn-drift-check.yml"


def _load():
    spec = importlib.util.spec_from_file_location("pause_reconcile", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pause_reconcile"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load()


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _row(cid: str, lifecycle: str = "in-service", **kw) -> dict:
    row = {"component_id": cid, "lifecycle": lifecycle, "substrate": "eventbridge"}
    row.update(kw)
    return row


def _manifest(paused: dict | None = None, kept: dict | None = None) -> dict:
    return {
        "ruling": {"by": "Brian", "date": "2026-08-07", "statement": "x"},
        "not_paused": kept or {},
        "pending": {},
        "paused": {"events_rules": paused or {}, "scheduler_schedules": {}},
    }


def _reconcile(mod, *, manifest, rows, triggers, targets=None, sf=None, ran=None):
    return mod.reconcile(
        manifest=manifest, rows=rows, triggers=triggers,
        targets_of=(lambda t: (targets or {}).get(t["name"], [])),
        sf_invoked=sf or set(),
        invocations_of=(lambda cid: (ran or {}).get(cid, 0)),
    )


# ── the finding classes, each induced ────────────────────────────────────────

def test_undeclared_dark_fires_when_no_register_accounts_for_a_dark_trigger(mod):
    """A trigger off with nobody's decision behind it is §8.3's forbidden collapse."""
    findings = _reconcile(
        mod, manifest=_manifest(), rows={"other": _row("other")},
        triggers=[{"surface": "events", "name": "ghost", "state": "DISABLED"}],
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == [("undeclared-dark", "ghost")]


def test_a_dark_trigger_is_declared_by_EITHER_register(mod):
    """The union is the declaration surface — the manifest OR the registry.

    Measured 2026-08-12: eight live rules (`ae-executor-start`, `ae-predictor-*`,
    `alpha-research-daily`, …) are DISABLED and named nowhere in the manifest,
    and all eight already carry `lifecycle: deprecated` in the registry. A
    reconciler that read only the manifest would have demanded eight new manifest
    entries, back-dating a 2026-08-07 ruling over decisions that predate it.
    """
    dark = [{"surface": "events", "name": "legacy", "state": "DISABLED"}]
    by_registry = _reconcile(
        mod, manifest=_manifest(), rows={"legacy": _row("legacy", "deprecated")},
        triggers=dark)
    by_manifest = _reconcile(
        mod, manifest=_manifest(paused={"legacy": "ruled off"}), rows={}, triggers=dark)
    assert by_registry == [] and by_manifest == []


def test_running_while_declared_off_fires_on_a_live_trigger_behind_a_disabled_row(mod):
    """I7118 Finding 2, induced: the component runs again, the row still says off."""
    findings = _reconcile(
        mod, manifest=_manifest(), rows={"probe": _row("probe", "disabled")},
        triggers=[{"surface": "events", "name": "probe", "state": "ENABLED"}],
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("running-while-declared-off", "probe")]


def test_orphaned_lifecycle_declaration_fires_when_the_quoted_entry_is_gone(mod):
    """The row quotes automation_pause.json; the manifest no longer names it."""
    findings = _reconcile(
        mod, manifest=_manifest(),
        rows={"tap": _row("tap", "disabled",
                          lifecycle_reason="paused per automation_pause.json")},
        triggers=[{"surface": "events", "name": "tap", "state": "DISABLED"}],
    )
    kinds = {f["kind"] for f in findings}
    assert "orphaned-lifecycle-declaration" in kinds, findings


def test_orphan_is_silent_while_the_entry_still_exists(mod):
    findings = _reconcile(
        mod, manifest=_manifest(paused={"tap": "ruled off 2026-08-07"}),
        rows={"tap": _row("tap", "disabled",
                          lifecycle_reason="paused per automation_pause.json")},
        triggers=[{"surface": "events", "name": "tap", "state": "DISABLED"}],
    )
    assert findings == []


def test_undeclared_enabled_fires_on_scheduled_work_no_register_names(mod):
    """Found live: `alpha-research-openrouter-shadow-weekly`, ENABLED under the
    pause, in neither block and carrying no registry row."""
    findings = _reconcile(
        mod, manifest=_manifest(), rows={},
        triggers=[{"surface": "events", "name": "stowaway", "state": "ENABLED"}],
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("undeclared-enabled", "stowaway")]


def test_dark_and_undeclared_component_fires_for_a_target_behind_dark_triggers(mod):
    """alpha-engine-config-I7117's rule, derived rather than hand-listed."""
    findings = _reconcile(
        mod, manifest=_manifest(paused={"tick": "ruled off"}),
        rows={"tick": _row("tick", "deprecated"),
              "worker": _row("worker", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        targets={"tick": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 0},
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("dark-and-undeclared-component", "worker")]


# ── the two guards that keep the detector from writing false decisions ───────

def test_a_step_functions_stage_is_not_reported_dark(mod):
    """Measured 2026-08-12: `alpha-engine-predictor-inference` and
    `alpha-engine-research-runner` have only DEPRECATED rules of their own and
    are invoked by the KEPT weekly and preopen pipelines on every run. Without
    the Step Functions register this reports both as dark, and acting on that
    declares two live Crucible stages deliberately off."""
    args = dict(
        manifest=_manifest(paused={"tick": "ruled off"}),
        rows={"tick": _row("tick", "deprecated"),
              "stage": _row("stage", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        targets={"tick": ["arn:aws:lambda:us-east-1:1:function:stage"]},
        ran={"stage": 0},
    )
    assert _reconcile(mod, sf=set(), **args), "the guard cannot be shown to bind"
    assert _reconcile(mod, sf={"stage"}, **args) == []


def test_invocations_behind_a_declared_off_row_are_not_a_finding(mod):
    """The ruling says "manual invocation only", so a paused component that ran
    by hand is the ruling WORKING. Measured: `alpha-engine-predictor-health-check`
    shows 10 post-ruling invocations at irregular single-call hours with its own
    rule correctly DISABLED — firing on that pages on compliance."""
    findings = _reconcile(
        mod, manifest=_manifest(paused={"tick": "ruled off"}),
        rows={"tick": _row("tick", "deprecated"),
              "worker": _row("worker", "disabled", substrate="lambda")},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        targets={"tick": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 250},
    )
    assert findings == []


def test_the_invocation_window_opens_after_the_ruling_not_14_days_back(mod, manifest):
    """A flat trailing window counts PRE-ruling invocations as evidence against a
    post-ruling declaration. Measured 2026-08-12: that produced eleven
    `running-while-declared-off` findings of which ten were pre-pause traffic —
    `alpha-engine-crypto-balances` scored 853 and has not run once since the
    pause. The bound is read from the manifest, never written as a literal."""
    start = mod.window_start(manifest, days=365)
    ruled = datetime.datetime.fromisoformat(
        manifest["ruling"]["date"]).replace(tzinfo=datetime.timezone.utc)
    assert start > ruled, "a 365-day window must still not reach before the ruling"


# ── the never-mutate boundary, and the wiring ────────────────────────────────

def test_the_module_cannot_write_anything(mod):
    """I7118: a job that edits lifecycle declarations autonomously would be
    inferring a decision from a file diff, which §8.3 forbids."""
    source = MODULE_PATH.read_text(encoding="utf-8")
    for forbidden in ("enable-rule", "disable-rule", "update-schedule",
                      "put-rule", "delete-rule", "write_text", "s3 cp"):
        assert forbidden not in source, (
            f"pause_reconcile.py contains {forbidden!r} — it is a detector, and a "
            "detector that can act can act on a wrong reading")


def test_an_empty_registry_raises_rather_than_reporting_everything_undeclared(mod, tmp_path):
    """Grading against an empty registry would report the whole account as
    undeclared — a check that fails LOUD beats one that invents 195 findings."""
    with pytest.raises(RuntimeError, match="empty"):
        mod.load_registry(tmp_path)


def test_ci_runs_the_reconcile(mod):
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "pause_reconcile.py" in text, (
        "the reconciler is not wired into the daily out-of-band sweep; a detector "
        "that runs nowhere is the gap it was built to close"
    )
    step = text[text.index("pause_reconcile.py") - 600:text.index("pause_reconcile.py") + 200]
    assert "if:" not in step.split("- name:")[-1], (
        "the reconcile step carries an `if:` — the same reachability regression "
        "test_ci_runs_the_pause_check exists to prevent"
    )


def test_the_check_publishes_its_own_console_row(mod):
    """§2.2 — a detector that reports nowhere is unobserved."""
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "--publish" in text, "the reconcile step does not write its console row"
    assert mod.CADENCE_MINUTES == 1440, (
        "the row's cadence must match the sweep that writes it, or the console's "
        "staleness threshold is derived from a cadence nothing meets"
    )


def test_non_service_lifecycles_match_the_registry(mod):
    """The three-state set is duplicated from another repo's
    `observability_registry.py::LIFECYCLE_NEEDS_REASON`. If a fourth is added
    there, a row could declare itself off in a way this module reads as
    in-service, and the reconciler would silently under-report."""
    assert mod.NON_SERVICE_LIFECYCLES == frozenset(
        {"disabled", "deprecated", "retired"})
