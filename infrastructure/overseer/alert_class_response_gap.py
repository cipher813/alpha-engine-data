#!/usr/bin/env python3
"""
Overseer invariant-4 detector: a class in ``alert_classes:`` (playbooks.yaml)
that can terminate in a page and carries neither ``operator_reason`` nor
``migration_issue`` (alpha-engine-config-I8991, overseer-policy.md invariant
4 — "closed loop or tracked gap ... there is no third state").

This is a DIFFERENT axis from the existing schema-level requirement in
``playbooks.schema.json`` (``intake: none`` rows must carry one of the two
fields). That rule only covers drain-blind classes — ones the Overseer never
sees at all. Most classes ARE seen (``intake: bus``/``cw-alarm``) and are
routed to the alert-drain's T0-T3 tiers (``response: drain-queue``) or
declared operator-only (``response: operator``); either can still terminate
in an unactioned page, because ``severity`` is the push switch
(``api/services/alerting.py``: error/critical push the phone, everything
else is silent-in-channel) and neither ``response`` value guarantees a
disposition beyond "a human sees it."

A class is PAGEABLE here when:
  - its ``severities`` set intersects {error, critical, dynamic} (dynamic
    classes choose their severity at emit time and MAY reach error/critical,
    so they are treated as pageable rather than assumed safe), AND
  - its ``response`` is ``drain-queue`` or ``operator`` — NOT
    ``playbook:<name>``, which is already routed to an automated T2 agent
    and so is not in the third state this detector is for.

``GRANDFATHERED_CLASSES`` covers the 43 pre-existing rows found by the
alpha-engine-config-I8991 sweep — enforcing this immediately, unconditionally
would redden the fleet's CI on a backlog nobody has triaged yet. Each
grandfathered class needs a per-row ruling (page-only vs. tracked migration),
tracked as `alpha-engine-config-I8996`. A NEWLY ADDED pageable class with
neither field is NOT grandfathered and fails this check immediately — that is
what stops the gap silently reopening every time a class is added.

Usage:
  python infrastructure/overseer/alert_class_response_gap.py \\
    --playbooks infrastructure/overseer/playbooks.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

# The 43 rows found pageable-with-no-field by the alpha-engine-config-I8991
# sweep (2026-08-27), tracked for per-class disposition in
# alpha-engine-config-I8996. Remove an entry here in the SAME PR that gives
# its row an operator_reason or migration_issue — leaving it here after the
# row is fixed is caught by test_grandfather_list_has_no_stale_entries.
GRANDFATHERED_CLASSES: frozenset[str] = frozenset({
    "alpha_engine_preopen_deploy_readiness_probe",
    "backtester_champion_promotion",
    "backtester_cost_anomaly",
    "backtester_live_key_reconciliation",
    "backtester_stance_drift",
    "boot_pull",
    "box_health",
    "canary_replay_liveness",
    "check_systemd_unit_drift",
    "data_constituents_drift",
    "data_phase_marker_sweep",
    "data_sf_definition_drift",
    "data_stage_output_sweep",
    "deploy_notification_backtester_concordance",
    "deploy_notification_backtester_counterfactual",
    "deploy_notification_backtester_health",
    "deploy_notification_changelog",
    "deploy_notification_data_plane",
    "deploy_notification_predictor",
    "deploy_notification_research",
    "deploy_on_merge",
    "eod_snapshot_existence_check",
    "executor_turnover_tripwire",
    "executor_zero_entries_alarm",
    "groom_lifecycle_bus_events",
    "llm_egress_proxy_sli",
    "llm_router_reload_health",
    "metron_deploy",
    "metron_reconciliation",
    "overseer_dispatch_escalation",
    "pipeline_watchdog_stuck_sf",
    "predictor_inference",
    "predictor_model_zoo",
    "predictor_predictions_write",
    "reboot_if_needed",
    "research_archive_writer_quarantine",
    "research_cut_promotion_alerts",
    "research_daemon_down",
    "research_eval_rolling_mean_producer_failure",
    "research_signals_envelope_rejected",
    "research_weekly_ledger_alerts",
    "router_canary",
    "weekly_sf_recovery_metric",
})

_PAGEABLE_SEVERITIES = frozenset({"error", "critical", "dynamic"})
_PAGEABLE_RESPONSES = frozenset({"drain-queue", "operator"})


def is_pageable(alert_class: dict) -> bool:
    """True when this row's severities/response combination can reach a
    human page with no automated disposition guaranteed."""
    severities = set(alert_class.get("severities", []))
    response = alert_class.get("response", "")
    return bool(severities & _PAGEABLE_SEVERITIES) and response in _PAGEABLE_RESPONSES


def has_disposition_field(alert_class: dict) -> bool:
    return "operator_reason" in alert_class or "migration_issue" in alert_class


def find_new_gaps(alert_classes: list[dict]) -> list[str]:
    """Classes that are pageable, lack both fields, and are NOT grandfathered
    — i.e. a class added or edited since the I8991 sweep without a
    disposition. Non-empty means invariant 4 has regressed."""
    return sorted(
        c["class"]
        for c in alert_classes
        if is_pageable(c)
        and not has_disposition_field(c)
        and c["class"] not in GRANDFATHERED_CLASSES
    )


def find_stale_grandfather_entries(alert_classes: list[dict]) -> list[str]:
    """Grandfathered class names that either no longer exist in the registry
    or now carry a disposition field — both mean the grandfather list is
    stale and must be trimmed in the same PR that fixed the row."""
    by_name = {c["class"]: c for c in alert_classes}
    stale = []
    for name in sorted(GRANDFATHERED_CLASSES):
        cls = by_name.get(name)
        if cls is None or has_disposition_field(cls):
            stale.append(name)
    return stale


def _load_alert_classes(playbooks_path: Path) -> list[dict]:
    data = yaml.safe_load(playbooks_path.read_text())
    return data.get("alert_classes", [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Detect alert_classes rows in the third state overseer-policy.md "
        "invariant 4 forbids: pageable, with neither operator_reason nor migration_issue."
    )
    parser.add_argument(
        "--playbooks",
        type=Path,
        default=Path(__file__).resolve().parent / "playbooks.yaml",
        help="Path to playbooks.yaml (default: sibling file).",
    )
    args = parser.parse_args(argv)

    alert_classes = _load_alert_classes(args.playbooks)

    new_gaps = find_new_gaps(alert_classes)
    stale = find_stale_grandfather_entries(alert_classes)

    ok = True
    if new_gaps:
        ok = False
        print(
            "overseer-policy.md invariant 4 violated — pageable alert_classes row(s) "
            "with neither operator_reason nor migration_issue (alpha-engine-config-I8991):",
            file=sys.stderr,
        )
        for name in new_gaps:
            print(f"  - {name}", file=sys.stderr)
    if stale:
        ok = False
        print(
            "GRANDFATHERED_CLASSES has stale entries (row fixed or removed but still "
            "listed) — trim these from alert_class_response_gap.py:",
            file=sys.stderr,
        )
        for name in stale:
            print(f"  - {name}", file=sys.stderr)

    if ok:
        print(f"alert_class_response_gap: OK ({len(alert_classes)} classes checked)")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
