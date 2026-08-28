"""Tests for ``infrastructure/overseer/alert_class_response_gap.py`` — the
overseer-policy.md invariant 4 detector added by alpha-engine-config-I8991
("closed loop or tracked gap ... there is no third state").

Pins:
  1. The REAL registry has no un-grandfathered gap (find_new_gaps == []).
  2. The grandfather list has no stale entries (every listed class still
     exists and still lacks both fields).
  3. ``metron_deploy_drift`` specifically carries
     ``migration_issue: alpha-engine-config-I8991``.
  4. The detector actually detects: a synthetic pageable class with neither
     field, not in the grandfather list, IS flagged — the "demonstrated
     failing" requirement for a self-detecting check. Without this test, a
     regression in ``is_pageable``/``find_new_gaps`` (e.g. a typo'd severity
     set) could silently stop catching real gaps and nothing would notice.
  5. A grandfathered class that gains a disposition field but is left in the
     grandfather list is caught as stale (proves the anti-drift half works,
     not just the detection half).
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infrastructure" / "overseer"))
import alert_class_response_gap as g  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
PLAYBOOKS_PATH = REPO_ROOT / "infrastructure" / "overseer" / "playbooks.yaml"
REAL_ALERT_CLASSES = yaml.safe_load(PLAYBOOKS_PATH.read_text())["alert_classes"]


# ── real registry ──


def test_real_registry_has_no_new_gaps():
    """Every pageable class either has a disposition field or is a tracked,
    named grandfather entry (alpha-engine-config-I8996). A non-empty result
    means a class was added or edited without a ruling."""
    assert g.find_new_gaps(REAL_ALERT_CLASSES) == []


def test_real_registry_grandfather_list_has_no_stale_entries():
    """Every grandfathered class name still exists in the registry and still
    lacks both fields. A class fixed in place (field added) but left on the
    list, or a renamed/removed class still named, is stale bookkeeping that
    hides real progress and must be trimmed in the fixing PR."""
    assert g.find_stale_grandfather_entries(REAL_ALERT_CLASSES) == []


def test_metron_deploy_drift_carries_migration_issue():
    by_name = {c["class"]: c for c in REAL_ALERT_CLASSES}
    assert "metron_deploy_drift" in by_name
    row = by_name["metron_deploy_drift"]
    assert row.get("migration_issue") == "alpha-engine-config-I8991"
    assert "operator_reason" not in row  # exactly one of the two fields


def test_all_43_swept_classes_are_grandfathered_and_only_those():
    """Pins the exact I8991 sweep set so a future edit to GRANDFATHERED_CLASSES
    is a visible diff against a known baseline, not silent growth or shrink."""
    swept = {
        "alpha_engine_preopen_deploy_readiness_probe", "backtester_champion_promotion",
        "backtester_cost_anomaly", "backtester_live_key_reconciliation",
        "backtester_stance_drift", "boot_pull", "box_health", "canary_replay_liveness",
        "check_systemd_unit_drift", "data_constituents_drift", "data_phase_marker_sweep",
        "data_sf_definition_drift", "data_stage_output_sweep",
        "deploy_notification_backtester_concordance",
        "deploy_notification_backtester_counterfactual",
        "deploy_notification_backtester_health", "deploy_notification_changelog",
        "deploy_notification_data_plane", "deploy_notification_predictor",
        "deploy_notification_research", "deploy_on_merge", "eod_snapshot_existence_check",
        "executor_turnover_tripwire", "executor_zero_entries_alarm",
        "groom_lifecycle_bus_events", "llm_egress_proxy_sli", "llm_router_reload_health",
        "metron_deploy", "metron_reconciliation", "overseer_dispatch_escalation",
        "pipeline_watchdog_stuck_sf", "predictor_inference", "predictor_model_zoo",
        "predictor_predictions_write", "reboot_if_needed",
        "research_archive_writer_quarantine", "research_cut_promotion_alerts",
        "research_daemon_down", "research_eval_rolling_mean_producer_failure",
        "research_signals_envelope_rejected", "research_weekly_ledger_alerts",
        "router_canary", "weekly_sf_recovery_metric",
    }
    assert g.GRANDFATHERED_CLASSES == swept
    assert len(swept) == 43


# ── synthetic: proves the detector actually detects ──

_UNGRANDFATHERED_GAP_CLASS = {
    "class": "synthetic_new_pageable_class_no_disposition",
    "source": "synthetic-test-source",
    "severities": ["error"],
    "intake": "bus",
    "response": "drain-queue",
}


def test_detects_a_new_pageable_class_with_no_disposition_field():
    """A class shaped exactly like the real gaps found by the I8991 sweep —
    pageable severity, drain-queue response, no operator_reason/migration_issue
    — and NOT on the grandfather list, is flagged. This demonstrates the check
    failing when a class's field is missing/removed, per I8991's task 4."""
    assert _UNGRANDFATHERED_GAP_CLASS["class"] not in g.GRANDFATHERED_CLASSES
    gaps = g.find_new_gaps([_UNGRANDFATHERED_GAP_CLASS])
    assert gaps == ["synthetic_new_pageable_class_no_disposition"]


def test_removing_the_disposition_field_from_metron_deploy_drift_is_detected():
    """Directly demonstrates the 'field removed' scenario I8991 task 4 names:
    take the real metron_deploy_drift row, strip migration_issue, and confirm
    the detector flags it (it is not grandfathered, so nothing else hides
    the regression)."""
    by_name = {c["class"]: c for c in REAL_ALERT_CLASSES}
    row = dict(by_name["metron_deploy_drift"])
    assert "metron_deploy_drift" not in g.GRANDFATHERED_CLASSES
    row.pop("migration_issue", None)
    row.pop("operator_reason", None)
    assert g.find_new_gaps([row]) == ["metron_deploy_drift"]


def test_adding_a_disposition_field_removes_the_gap():
    fixed = dict(_UNGRANDFATHERED_GAP_CLASS)
    fixed["operator_reason"] = "Declared human-only for this synthetic test row."
    assert g.find_new_gaps([fixed]) == []


def test_response_playbook_is_not_pageable_in_this_sense():
    """A class already routed to an automated T2 agent (response:playbook:*)
    is not in the third state — it has a closed loop, just not via the
    operator_reason/migration_issue mechanism."""
    routed = {
        "class": "already_routed_class",
        "source": "x", "severities": ["error"], "intake": "bus",
        "response": "playbook:alert-drain",
    }
    assert g.find_new_gaps([routed]) == []


def test_warning_only_severity_is_not_pageable():
    """A class whose severities never reach error/critical/dynamic never
    pushes per alerting.py's severity-is-the-push-switch contract, so it is
    not in the third state even with no disposition field."""
    warn_only = {
        "class": "warning_only_class",
        "source": "x", "severities": ["warning"], "intake": "bus",
        "response": "drain-queue",
    }
    assert g.find_new_gaps([warn_only]) == []


def test_stale_grandfather_entry_detected_when_field_added_but_not_trimmed():
    """Proves the anti-drift half: a grandfathered class that gains a
    disposition field but is left on the list is caught, so a fixing PR
    that forgets to trim the list does not silently understate progress."""
    fixed_but_still_listed = {
        "class": "backtester_stance_drift",  # a real grandfathered class
        "source": "x", "severities": ["error"], "intake": "bus",
        "response": "drain-queue",
        "operator_reason": "Now declared page-only.",
    }
    stale = g.find_stale_grandfather_entries([fixed_but_still_listed])
    assert "backtester_stance_drift" in stale


def test_main_exits_nonzero_when_registry_has_a_gap(tmp_path):
    playbooks = tmp_path / "playbooks.yaml"
    playbooks.write_text(yaml.dump({
        "schema_version": 1,
        "playbooks": {},
        "alert_classes": [_UNGRANDFATHERED_GAP_CLASS],
    }))
    rc = g.main(["--playbooks", str(playbooks)])
    assert rc == 1


def test_main_exits_zero_when_registry_is_clean(tmp_path):
    """A registry containing every grandfathered class UNCHANGED (still no
    field — nothing here claims they're fixed) plus one new class that DOES
    carry a disposition field exits 0: no new gap, and nothing stale because
    every grandfathered row is present and still matches its listing."""
    playbooks = tmp_path / "playbooks.yaml"
    clean = dict(_UNGRANDFATHERED_GAP_CLASS)
    clean["migration_issue"] = "alpha-engine-config-I1"
    grandfathered_rows = [
        {
            "class": name, "source": "x", "severities": ["error"],
            "intake": "bus", "response": "drain-queue",
        }
        for name in g.GRANDFATHERED_CLASSES
    ]
    playbooks.write_text(yaml.dump({
        "schema_version": 1,
        "playbooks": {},
        "alert_classes": grandfathered_rows + [clean],
    }))
    rc = g.main(["--playbooks", str(playbooks)])
    assert rc == 0
