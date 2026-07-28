"""Shared flow-doctor Telegram routing for alpha-engine-data Lambdas (config#1742)."""

from __future__ import annotations

import contextlib
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Sequence

from nousergon_lib.telegram import send_message

logger = logging.getLogger(__name__)

_FLOW_DOCTOR_BY_NAME: dict[str, Any | None] = {}
_INIT_ATTEMPTED: set[str] = set()

# Deterministic backstop (config#2208): callers that thread `owner_repo`
# through `context` (e.g. freshness-monitor) get this chokepoint refused
# regardless of test-suite stubbing state, so fixture data can never page a
# real Telegram channel even if a test forgets to mock this module. This is
# a belt on top of — not a substitute for — hermetic test stubbing: it only
# fires for the `owner_repo` values test fixtures actually use, and only
# when a caller passes `owner_repo` in `context` at all.
TEST_NAMESPACE_OWNER_REPOS = frozenset({"ae-test", "alpha-engine-test"})


def reset_flow_doctor_cache() -> None:
    """Test hook — clear lazy-init state between handler invocations."""
    _FLOW_DOCTOR_BY_NAME.clear()
    _INIT_ATTEMPTED.clear()


def build_flow_doctor_config(
    flow_name: str,
    topics: Sequence[Any],
    *,
    db_basename: str,
    repo: str = "nousergon/nousergon-data",
    notifier_overrides: Optional[Dict[str, Any]] = None,
) -> dict:
    """Build a flow-doctor config for ``topics``.

    ``notifier_overrides`` shallow-merges into EVERY notifier dict the fleet
    spec produced — the escape hatch for a flow whose delivery posture differs
    from its topic's canonical default. The one live use is the groom
    CYCLE-level lifecycle flow: ``FleetTelegramTopic.GROOM``'s canonical spec is
    ``notify_on=("info",)`` + ``disable_notification=True``, i.e. per-box
    lifecycle pings land in ``#groom`` silently and anything above ``info`` is
    dropped outright. That is the right default for ~18 per-box pings/day, and
    the WRONG one for the 2 cycle-level pings/trigger the operator asked to
    actually receive (2026-07-28 ruling).

    This is a parameter rather than a second hand-written spec because
    ``groom_flow_doctor_notify.py`` already open-codes exactly this override on
    the box rail; two divergent copies of "how does groom traffic reach
    Telegram" is the §2.3 one-owner-per-config-fact defect. Callers that pass
    nothing are byte-identical to before.
    """
    from nousergon_lib.flow_doctor_fleet import fleet_telegram_notifier_dicts

    notify = fleet_telegram_notifier_dicts(topics)
    if notifier_overrides:
        notify = [{**spec, **notifier_overrides} for spec in notify]

    return {
        "flow_name": flow_name,
        "repo": repo,
        "owner": "@brianmcmahon",
        "notify": notify,
        # DynamoDB, not local SQLite — dedup cooldowns must survive across
        # separate Lambda invocations (a fresh cold start gets an empty /tmp
        # every time, so SQLite there can never dedup cross-invocation;
        # config#2418, mirrors the data-collector flow-doctor.yaml fix in
        # #790/I2417). Table provisioned out-of-band (PAY_PER_REQUEST);
        # runtime role only needs CRUD, not CreateTable — see
        # infrastructure/iam/alpha-engine-data-role.json and each fleet
        # Lambda's own iam-policy.json FlowDoctorDedupStore statement.
        "store": {"type": "dynamodb", "table_name": "flow-doctor-store", "region": "us-east-1"},
        "dedup_cooldown_minutes": 1,
        "rate_limits": {"max_alerts_per_day": 100},
    }


def get_flow_doctor(
    flow_name: str,
    topics: Sequence[Any],
    *,
    db_basename: str,
    notifier_overrides: Optional[Dict[str, Any]] = None,
) -> Any | None:
    # NOTE: the cache is keyed on flow_name alone, so a caller applying
    # notifier_overrides MUST use a distinct flow_name (the groom cycle flow
    # uses "backlog-groom-cycle" against the per-box rail's
    # "backlog-groom-lifecycle"). Sharing a flow_name across differing
    # overrides would silently serve whichever posture initialized first.
    if flow_name in _FLOW_DOCTOR_BY_NAME:
        return _FLOW_DOCTOR_BY_NAME[flow_name]
    if flow_name in _INIT_ATTEMPTED:
        return None
    _INIT_ATTEMPTED.add(flow_name)
    if os.environ.get("FLOW_DOCTOR_ENABLED", "1") != "1":
        _FLOW_DOCTOR_BY_NAME[flow_name] = None
        return None
    try:
        import yaml
        from nousergon_lib.logging import get_flow_doctor, setup_logging

        cfg = build_flow_doctor_config(
            flow_name, topics, db_basename=db_basename,
            notifier_overrides=notifier_overrides,
        )
        path = Path(f"/tmp/flow_doctor_{db_basename}.yaml")
        path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
        setup_logging(flow_name, flow_doctor_yaml=str(path))
        fd = get_flow_doctor()
        _FLOW_DOCTOR_BY_NAME[flow_name] = fd
        return fd
    except Exception as exc:  # noqa: BLE001 — fall back to send_message
        logger.warning("flow-doctor init failed for %s: %s", flow_name, exc)
        _FLOW_DOCTOR_BY_NAME[flow_name] = None
        return None


def topic_telegram_notifier(fd: Any, topic: Any) -> Any | None:
    from flow_doctor.notify.telegram import TelegramNotifier
    from nousergon_lib.flow_doctor_fleet import fleet_telegram_thread_id_env

    want = os.environ.get(fleet_telegram_thread_id_env(topic))
    for notifier in fd._notifiers:
        if not isinstance(notifier, TelegramNotifier):
            continue
        thread_id = getattr(notifier, "message_thread_id", None)
        if thread_id is not None and str(thread_id) == str(want):
            return notifier
    return None


@contextlib.contextmanager
def _event_source_override(source: Optional[str]) -> Iterator[None]:
    """Attribute the Overseer intake bus event this call produces to ``source``.

    Every Telegram delivery path this function can take — ``fd.notify_event``
    (via ``flow_doctor.notify.telegram.TelegramNotifier.send``),
    ``notifier.send_raw`` (the ``silent_topic`` branch), and the bare
    ``send_message`` fallback — funnels through ``krepis.telegram.send_message``
    at the bottom, which has no ``source`` parameter at all: it always calls
    ``krepis.fleet_events.emit_alert_event`` with no explicit source, so
    attribution resolves via ``krepis.fleet_events._resolve_source`` — explicit
    arg (never supplied on this path) > ``KREPIS_EVENT_SOURCE`` env > the
    Lambda's own runtime ``AWS_LAMBDA_FUNCTION_NAME`` identity.

    ``KREPIS_EVENT_SOURCE`` is krepis's own documented override for exactly
    this case, so this sets it for the duration of the call rather than
    reaching into krepis/flow-doctor (separate repos) to thread a ``source``
    kwarg through every intermediate layer. Restores the prior value
    (including "unset") afterward so a warm Lambda container never leaks one
    invocation's source into the next. config-I3513 — before this fix, every
    caller here silently fell back to Lambda runtime identity, which matches
    NO row in ``playbooks.yaml``'s ``alert_classes`` for most callers
    (confirmed live: 7 of 10 freshness-monitor intake events unclassified).
    """
    if source is None:
        yield
        return
    prior = os.environ.get("KREPIS_EVENT_SOURCE")
    os.environ["KREPIS_EVENT_SOURCE"] = source
    try:
        yield
    finally:
        if prior is None:
            os.environ.pop("KREPIS_EVENT_SOURCE", None)
        else:
            os.environ["KREPIS_EVENT_SOURCE"] = prior


def notify_via_flow_doctor(
    text: str,
    *,
    silent: bool,
    severity: str,
    dedup_key: str,
    flow_name: str,
    topics: Sequence[Any],
    db_basename: str,
    source: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    silent_topic: Any | None = None,
    notifier_overrides: Optional[Dict[str, Any]] = None,
) -> bool:
    """Route ``text`` through flow-doctor forum topics; fallback to ``send_message``.
    Args:
        text: Notification body text.
        silent: If True, send with notifications suppressed (Telegram silent).
        severity: One of ``critical``, ``error``, ``warning``, ``info``.
        dedup_key: Stable dedup key for cross-invocation cooldown.
        flow_name: Flow doctor flow name.
        topics: Telegram topic routing.
        db_basename: Flow doctor store basename.
        source: Explicit event source override. When set, ``KREPIS_EVENT_SOURCE``
            is temporarily scoped so ``emit_alert_event``'s ``_resolve_source``
            picks it up instead of falling through to ``AWS_LAMBDA_FUNCTION_NAME``.
            If omitted, the caller's Lambda function name is used (the default
            krepis behavior).
        context: Arbitrary key-value metadata.
        silent_topic: Optional topic for silent notifications when ``silent`` is True.
        notifier_overrides: Per-flow delivery-posture override merged into every
            notifier dict — see ``build_flow_doctor_config``. Requires a
            flow_name unique to that posture (the config cache is keyed on it).
    """
    owner_repo = (context or {}).get("owner_repo")
    if owner_repo in TEST_NAMESPACE_OWNER_REPOS:
        logger.warning(
            "notify_via_flow_doctor: refusing to dispatch — owner_repo=%r is a "
            "test-fixture namespace (config#2208 deterministic backstop)",
            owner_repo,
        )
        return False
    with _event_source_override(source):
        fd = get_flow_doctor(
            flow_name, topics, db_basename=db_basename,
            notifier_overrides=notifier_overrides,
        )
        if fd is None:
            return send_message(text, disable_notification=silent)

        subject = text.replace("*", "").strip()

        if silent and silent_topic is not None:
            notifier = topic_telegram_notifier(fd, silent_topic)
            if notifier is not None:
                return notifier.send_raw(text, disable_notification=True) is not None

        report_id = fd.notify_event(
            subject,
            body=None,
            severity=severity,
            context=context or {},
            dedup_key=dedup_key,
        )
        return report_id is not None
