"""alpha-engine-overseer-backstop-responder — bounded, deterministic, non-agentic
recovery responder between the dumb SNS backstop and the human page.

Subscribes DIRECTLY to the SNS backstop topic (``alpha-engine-alarm-backstop``),
gathers fleet state, attempts ONE bounded recovery per alarm per cooldown window,
and forwards an enhanced page to Telegram. Designed per alpha-engine-config-I4480
(G9 of the 2026-07-27 conformance audit).

INVARIANT (overseer-policy §4.3): the backstop must stay dumb — no agent, no
queue, no bus dependency, nothing that can fail non-obviously. This Lambda:
  - Has NO LLM call, NO queue read/write, NO EventBridge dependency.
  - Invokes other Lambdas synchronously (Lambda→Lambda is a deterministic,
    auditable call, not a bus/queue dependency).
  - Uses urllib only for Telegram (zero pip deps, same as backstop-telegram-notifier).
  - Cooldown state in S3 (no DynamoDB — S3 is the simplest possible persistence).

AUTHORITY: T1 (named deterministic remediation, per overseer-policy §6). The
recovery actions are a HARDCODED, REVIEWED list — never improvised at run time:
  1. Re-invoke the liveness probe (read-only, always safe).
  2. Re-dispatch a failed playbook once via the router (for intake-age alarm).

COOLDOWN: one recovery attempt per alarm per window (default 60 min). Second
occurrence within the window escalates — no retry, human needed now.

Design:
  - urllib only — zero pip dependencies, zero 3rd-party packages
  - Bot token read from SSM at EVERY invoke (no cached credentials)
  - SNS → Lambda direct subscription (no EventBridge, no DLQ)
  - All external calls are caught and reported — never crash the handler
  - The email leg (SNS→email subscription) is unaffected — this is an ADDITIONAL
    independent channel
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

# ── SSM paths ─────────────────────────────────────────────────────────────────
SSM_BOT_TOKEN_PATH = os.environ.get(
    "SSM_BOT_TOKEN_PATH", "/alpha-engine/TELEGRAM_BOT_TOKEN"
)
SSM_CHAT_ID_PATH = os.environ.get(
    "SSM_CHAT_ID_PATH", "/alpha-engine/TELEGRAM_CHAT_ID"
)
SSM_THREAD_ID_PATH = os.environ.get(
    "SSM_THREAD_ID_PATH",
    "/alpha-engine/FLOW_DOCTOR_TELEGRAM_THREAD_CRITICAL",
)

TELEGRAM_API_BASE = "https://api.telegram.org/bot"

# ── Cooldown ───────────────────────────────────────────────────────────────────
COOLDOWN_MINUTES = int(os.environ.get("COOLDOWN_MINUTES", "60"))
COOLDOWN_BUCKET = os.environ.get("COOLDOWN_BUCKET", "alpha-engine-research")
COOLDOWN_PREFIX = os.environ.get(
    "COOLDOWN_PREFIX", "consolidated/overseer_backstop/cooldown"
)

# ── Recovery targets ──────────────────────────────────────────────────────────
LIVENESS_PROBE_FUNCTION = os.environ.get(
    "LIVENESS_PROBE_FUNCTION", "alpha-engine-overseer-liveness-probe"
)
OVERSEER_DISPATCHER_FUNCTION = os.environ.get(
    "OVERSEER_DISPATCHER_FUNCTION", "alpha-engine-overseer-dispatcher"
)

# ── Queue names for state reporting ───────────────────────────────────────────
INTAKE_QUEUE_NAME = os.environ.get(
    "INTAKE_QUEUE_NAME", "nousergon-overseer-intake"
)
INTAKE_DLQ_NAME = os.environ.get(
    "INTAKE_DLQ_NAME", "nousergon-overseer-intake-dlq"
)

# ── S3 paths for ledger/liveness state ────────────────────────────────────────
WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
DISPATCH_LEDGER_PREFIX = os.environ.get(
    "DISPATCH_LEDGER_PREFIX", "overseer/dispatch_ledger"
)
DRAIN_LEDGER_PREFIX = os.environ.get(
    "DRAIN_LEDGER_PREFIX", "overseer/drain_ledger"
)
LIVENESS_STATE_KEY = os.environ.get(
    "LIVENESS_STATE_KEY", "consolidated/overseer_liveness/alerted.json"
)

# ── Alarm→recovery mapping ────────────────────────────────────────────────────
# Hardcoded, reviewed action list. Keys are alarm-name substrings; values are
# lists of recovery actions to attempt. Order matters — actions run sequentially,
# each on failure of the prior. An empty list means "report state only."
ALARM_RECOVERY_MAP = {
    "overseer-intake-age": ["invoke_liveness_probe", "redispatch_alert_drain"],
    "overseer-liveness-probe": ["invoke_liveness_probe"],
    "overseer-intake-dlq": ["invoke_liveness_probe", "redispatch_alert_drain"],
}
# Default recovery for any backstop alarm not explicitly mapped:
DEFAULT_RECOVERY_ACTIONS = ["invoke_liveness_probe"]


# ═══════════════════════════════════════════════════════════════════════════════
# SSM helpers (mirrors backstop-telegram-notifier)
# ═══════════════════════════════════════════════════════════════════════════════

def _ssm_parameter(path: str) -> str:
    """Read a plaintext SSM parameter. Raises on any error (fail-loud)."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=path, WithDecryption=True)
    return resp["Parameter"]["Value"]


# ═══════════════════════════════════════════════════════════════════════════════
# Telegram (mirrors backstop-telegram-notifier — urllib only, zero pip deps)
# ═══════════════════════════════════════════════════════════════════════════════

def _send_telegram(
    bot_token: str,
    chat_id: str,
    text: str,
    thread_id: str | None = None,
) -> dict | None:
    """Send ``text`` to the given Telegram chat/thread via the Bot API.

    Returns the JSON response on success, or None on any non-fatal error.
    Uses raw urllib — zero pip dependencies.
    """
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if thread_id:
        payload["message_thread_id"] = int(thread_id)

    data = json.dumps(payload).encode("utf-8")
    url = f"{TELEGRAM_API_BASE}{bot_token}/sendMessage"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read()
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read()
            if isinstance(body, bytes):
                body = body.decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            body = str(exc)
        logger.warning("Telegram API HTTP %s: %s", exc.code, body)
        return None
    except urllib.error.URLError as exc:
        logger.warning("Telegram API connection failed: %s", exc.reason)
        return None
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("Telegram send unexpected error: %s", exc)
        return None


def _escape_markdown(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters."""
    special = r"_*[]()~`>#+-=|{}.!"
    for ch in special:
        text = text.replace(ch, f"\\{ch}")
    return text


# ═══════════════════════════════════════════════════════════════════════════════
# Cooldown (S3-based — no DynamoDB dependency)
# ═══════════════════════════════════════════════════════════════════════════════

def _alarm_slug(alarm_name: str) -> str:
    """Deterministic S3-safe slug from an alarm name."""
    return hashlib.sha256(alarm_name.encode()).hexdigest()[:16]


def _cooldown_key(alarm_name: str) -> str:
    return f"{COOLDOWN_PREFIX}/{_alarm_slug(alarm_name)}.json"


def _load_cooldown(s3, alarm_name: str) -> dict | None:
    """Load the cooldown state for ``alarm_name``, or None if no prior firing."""
    key = _cooldown_key(alarm_name)
    try:
        obj = s3.get_object(Bucket=COOLDOWN_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", ""}:
            logger.warning("Cooldown read error for %s: %s", alarm_name, exc)
        return None


def _save_cooldown(s3, alarm_name: str, state: dict) -> None:
    """Persist cooldown state. Best-effort — failure only risks a duplicate
    recovery next firing, never a missed alarm."""
    key = _cooldown_key(alarm_name)
    try:
        s3.put_object(
            Bucket=COOLDOWN_BUCKET,
            Key=key,
            Body=json.dumps(state, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Cooldown write failed for %s: %s", alarm_name, exc)


def _within_cooldown(last_fired_at_str: str | None, now: datetime) -> bool:
    """True iff the last firing was within the cooldown window."""
    if not last_fired_at_str:
        return False
    try:
        last = datetime.fromisoformat(last_fired_at_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False
    return (now - last) < timedelta(minutes=COOLDOWN_MINUTES)


# ═══════════════════════════════════════════════════════════════════════════════
# Recovery actions (hardcoded, reviewed list — T1 authority)
# ═══════════════════════════════════════════════════════════════════════════════

def _invoke_liveness_probe() -> dict:
    """Synchronously invoke the liveness probe. Read-only, always safe.
    Returns the probe's verdict: problems list, kill_switches, checks_run,
    checks_failed, clean, alerted."""
    try:
        lam = boto3.client("lambda", region_name=REGION)
        resp = lam.invoke(
            FunctionName=LIVENESS_PROBE_FUNCTION,
            InvocationType="RequestResponse",
            Payload=b"{}",
        )
        body = json.loads(resp["Payload"].read())
        if resp.get("FunctionError"):
            return {
                "action": "invoke_liveness_probe",
                "ok": False,
                "error": f"probe function error: {str(body)[:500]}",
            }
        return {
            "action": "invoke_liveness_probe",
            "ok": True,
            "verdict": body,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Liveness probe invoke failed: %s", exc)
        return {
            "action": "invoke_liveness_probe",
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _redispatch_alert_drain() -> dict:
    """Re-dispatch the alert-drain playbook via the overseer-dispatcher router.
    Synchronous — waits for the router's verdict. The router handles its own
    escalation on failure."""
    try:
        lam = boto3.client("lambda", region_name=REGION)
        resp = lam.invoke(
            FunctionName=OVERSEER_DISPATCHER_FUNCTION,
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "playbook": "alert-drain",
                "payload": {
                    "trigger": "backstop-responder",
                    "run_mode": "backstop_recovery",
                },
            }).encode("utf-8"),
        )
        body = json.loads(resp["Payload"].read())
        if resp.get("FunctionError"):
            return {
                "action": "redispatch_alert_drain",
                "ok": False,
                "error": f"dispatcher function error: {str(body)[:500]}",
            }
        return {
            "action": "redispatch_alert_drain",
            "ok": True,
            "verdict": body,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Alert-drain redispatch failed: %s", exc)
        return {
            "action": "redispatch_alert_drain",
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


RECOVERY_ACTIONS = {
    "invoke_liveness_probe": _invoke_liveness_probe,
    "redispatch_alert_drain": _redispatch_alert_drain,
}


def _recovery_actions_for_alarm(alarm_name: str) -> list[str]:
    """Determine which recovery actions apply to this alarm."""
    for pattern, actions in ALARM_RECOVERY_MAP.items():
        if pattern in alarm_name:
            return actions
    return DEFAULT_RECOVERY_ACTIONS


def _attempt_recovery(alarm_name: str) -> list[dict]:
    """Run the hardcoded recovery actions for ``alarm_name``. Each action is
    attempted once; the result (ok + verdict/error) is recorded. Actions are
    independent — one failure does not block the next."""
    action_names = _recovery_actions_for_alarm(alarm_name)
    results: list[dict] = []
    for name in action_names:
        fn = RECOVERY_ACTIONS.get(name)
        if fn is None:
            logger.warning("Unknown recovery action %r for alarm %s", name, alarm_name)
            results.append({"action": name, "ok": False, "error": "unknown_action"})
            continue
        logger.info("Backstop recovery: attempting %s for alarm %s", name, alarm_name)
        result = fn()
        results.append(result)
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# State gathering (decision-shaped reporting)
# ═══════════════════════════════════════════════════════════════════════════════

def _queue_metrics(queue_name: str) -> dict:
    """Read ApproximateNumberOfMessages + ApproximateAgeOfOldestMessage for a
    queue. Best-effort — a failure returns an error dict, never raises."""
    try:
        sqs = boto3.client("sqs", region_name=REGION)
        url_resp = sqs.get_queue_url(QueueName=queue_name)
        queue_url = url_resp["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateAgeOfOldestMessage",
            ],
        ).get("Attributes", {})
        age_sec = int(attrs.get("ApproximateAgeOfOldestMessage", 0))
        depth = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        age_min = age_sec // 60
        return {
            "queue": queue_name,
            "depth": depth,
            "in_flight": in_flight,
            "oldest_message_age_minutes": age_min,
            "ok": True,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Queue metrics failed for %s: %s", queue_name, exc)
        return {
            "queue": queue_name,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _last_ledger_entry(prefix: str) -> dict:
    """Find the most recent ledger object under ``prefix`` in the watch bucket.
    Returns the key, last_modified, and a parsed timestamp if available.
    Best-effort — a failure returns an error dict, never raises."""
    try:
        s3 = boto3.client("s3", region_name=REGION)
        # List today's prefix first, then fall back to yesterday
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        for date_str in (today, yesterday):
            date_prefix = f"{prefix}/{date_str}/"
            resp = s3.list_objects_v2(
                Bucket=WATCH_BUCKET, Prefix=date_prefix, MaxKeys=5
            )
            objects = sorted(
                (obj for obj in resp.get("Contents", []) if obj["Key"].endswith(".json")),
                key=lambda o: o["LastModified"],
                reverse=True,
            )
            if objects:
                newest = objects[0]
                # Try to read run_start from the ledger entry
                run_start = None
                try:
                    body = s3.get_object(Bucket=WATCH_BUCKET, Key=newest["Key"])["Body"].read()
                    record = json.loads(body)
                    run_start = record.get("run_start") or record.get("started_at")
                except Exception:  # noqa: BLE001
                    pass
                return {
                    "prefix": prefix,
                    "key": newest["Key"],
                    "last_modified": newest["LastModified"].isoformat(),
                    "run_start": run_start,
                    "ok": True,
                }
        return {"prefix": prefix, "key": None, "ok": True, "note": "no ledger entries found"}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ledger read failed for %s: %s", prefix, exc)
        return {"prefix": prefix, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _last_probe_state() -> dict:
    """Read the liveness probe's own state object (last fingerprint + timestamp).
    Best-effort — a failure returns an error dict, never raises."""
    try:
        s3 = boto3.client("s3", region_name=REGION)
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=LIVENESS_STATE_KEY)
        state = json.loads(obj["Body"].read())
        return {
            "ok": True,
            "fingerprint": state.get("fingerprint"),
            "updated_at": state.get("updated_at"),
            "healthy": state.get("fingerprint") is None,
        }
    except Exception as exc:  # noqa: BLE001
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code in {"NoSuchKey", "404"}:
            return {"ok": True, "healthy": True, "note": "no prior probe state (never alerted)"}
        logger.warning("Probe state read failed: %s", exc)
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _gather_state() -> dict:
    """Gather fleet state for the enhanced page. Every sub-call is best-effort
    — one failure does not block the rest. Returns a dict of state sections."""
    state: dict = {}

    # Queue metrics
    state["intake_queue"] = _queue_metrics(INTAKE_QUEUE_NAME)
    state["intake_dlq"] = _queue_metrics(INTAKE_DLQ_NAME)

    # Last ledger entries per routed playbook
    state["dispatch_ledger"] = _last_ledger_entry(DISPATCH_LEDGER_PREFIX)
    state["drain_ledger"] = _last_ledger_entry(DRAIN_LEDGER_PREFIX)

    # Last probe state
    state["probe_state"] = _last_probe_state()

    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Message formatting
# ═══════════════════════════════════════════════════════════════════════════════

def _format_duration_minutes(total_minutes: int) -> str:
    """Human-readable duration from minutes."""
    if total_minutes < 60:
        return f"{total_minutes}m"
    hours = total_minutes // 60
    mins = total_minutes % 60
    if mins == 0:
        return f"{hours}h"
    return f"{hours}h{mins}m"


def _format_recovery_section(results: list[dict]) -> str:
    """Format recovery action results for Telegram."""
    if not results:
        return ""
    lines = ["", "*Recovery attempt:*"]
    for r in results:
        action = r.get("action", "unknown")
        ok = r.get("ok", False)
        if ok:
            verdict = r.get("verdict", {})
            if action == "invoke_liveness_probe":
                problems = verdict.get("problems", [])
                clean = verdict.get("clean", False)
                ks = verdict.get("kill_switches", {})
                checks_run = verdict.get("checks_run", 0)
                checks_failed = verdict.get("checks_failed", 0)
                if clean:
                    lines.append(f"  ✅ Liveness probe: all {checks_run} checks clean")
                else:
                    lines.append(f"  ⚠️ Liveness probe: {len(problems)} problem(s), "
                                 f"{checks_run} checks ({checks_failed} failed to run)")
                if ks:
                    ks_str = ", ".join(f"{k}={v}" for k, v in sorted(ks.items()))
                    lines.append(f"  🔘 Kill-switches: {_escape_markdown(ks_str)}")
            elif action == "redispatch_alert_drain":
                v = verdict
                routed = v.get("routed", False)
                launched = v.get("verdict", {}).get("launched", False)
                reason = v.get("verdict", {}).get("reason", v.get("reason", "?"))
                if routed and launched:
                    lines.append(f"  ✅ Alert-drain re-dispatched: launched (reason: {_escape_markdown(str(reason))})")
                elif routed:
                    lines.append(f"  ⚠️ Alert-drain re-dispatched: NOT launched (reason: {_escape_markdown(str(reason))})")
                else:
                    lines.append(f"  ❌ Alert-drain dispatch failed: {_escape_markdown(str(reason))}")
            else:
                lines.append(f"  ✅ {_escape_markdown(action)}: ok")
        else:
            error = r.get("error", "unknown error")
            lines.append(f"  ❌ {_escape_markdown(action)}: {_escape_markdown(str(error)[:200])}")
    return "\n".join(lines)


def _format_state_section(state: dict) -> str:
    """Format gathered fleet state for Telegram."""
    lines: list[str] = []

    # Intake queue
    iq = state.get("intake_queue", {})
    if iq.get("ok"):
        depth = iq.get("depth", 0)
        in_flight = iq.get("in_flight", 0)
        age = iq.get("oldest_message_age_minutes", 0)
        lines.append(f"📥 *Intake queue:* {depth} visible, {in_flight} in-flight, "
                     f"oldest {_format_duration_minutes(age)}")
    else:
        lines.append(f"📥 *Intake queue:* UNREADABLE ({iq.get('error', '?')})")

    # DLQ
    dlq = state.get("intake_dlq", {})
    if dlq.get("ok"):
        dlq_depth = dlq.get("depth", 0)
        if dlq_depth > 0:
            lines.append(f"🗑 *DLQ:* {dlq_depth} messages")
    elif not dlq.get("ok"):
        pass  # DLQ error is non-critical; don't clutter the page

    # Dispatch ledger
    dl = state.get("dispatch_ledger", {})
    if dl.get("ok") and dl.get("key"):
        ts = dl.get("run_start") or dl.get("last_modified", "?")
        lines.append(f"📒 *Last dispatch:* {_escape_markdown(str(ts)[:19])}")
    elif dl.get("ok"):
        lines.append("📒 *Last dispatch:* none found")

    # Drain ledger
    dr = state.get("drain_ledger", {})
    if dr.get("ok") and dr.get("key"):
        ts = dr.get("run_start") or dr.get("last_modified", "?")
        lines.append(f"📒 *Last drain:* {_escape_markdown(str(ts)[:19])}")
    elif dr.get("ok"):
        lines.append("📒 *Last drain:* none found")

    # Probe state
    ps = state.get("probe_state", {})
    if ps.get("ok"):
        if ps.get("healthy"):
            lines.append("🟢 *Probe state:* healthy (no standing problems)")
        else:
            updated = ps.get("updated_at", "?")
            lines.append(f"🔴 *Probe state:* PROBLEMS since {_escape_markdown(str(updated)[:19])}")
    else:
        lines.append(f"⚠️ *Probe state:* UNREADABLE ({ps.get('error', '?')})")

    return "\n".join(lines)


def _format_escalation_note(is_escalation: bool) -> str:
    """The escalation note for a second-occurrence page."""
    if is_escalation:
        return (
            "\n⚠️ *ESCALATED:* second firing within the cooldown window — "
            "recovery already attempted, human needed now\\."
        )
    return ""


def _format_alarm_header(alarm: dict, is_escalation: bool) -> str:
    """Build the alarm header line."""
    alarm_name = alarm.get("AlarmName", "unknown")
    new_state = alarm.get("NewStateValue", "?")
    old_state = alarm.get("OldStateValue", "?")

    emoji = {"ALARM": "🔴", "OK": "🟢", "INSUFFICIENT_DATA": "⚪"}
    e = emoji.get(new_state, "🔔")

    if is_escalation:
        return f"🚨 {e} *BACKSTOP ESCALATION: {_escape_markdown(alarm_name)}*"
    if new_state == "ALARM":
        return f"{e} *BACKSTOP: {_escape_markdown(alarm_name)}*"
    return f"{e} *BACKSTOP RESOLVED: {_escape_markdown(alarm_name)}*"


def _format_page(
    alarm: dict,
    state: dict,
    recovery_results: list[dict],
    is_escalation: bool,
    cooldown_info: str,
) -> str:
    """Build the full enhanced Telegram page."""
    alarm_name = alarm.get("AlarmName", "unknown")
    reason = alarm.get("NewStateReason", "")
    region_alarm = alarm.get("Region", REGION)

    lines = [
        _format_alarm_header(alarm, is_escalation),
        f"State: {alarm.get('OldStateValue', '?')} → *{alarm.get('NewStateValue', '?')}*",
    ]

    if reason:
        escaped = _escape_markdown(reason)
        if len(escaped) > 400:
            escaped = escaped[:397] + "..."
        lines.append(f"Reason: {escaped}")

    # Trigger metric
    trigger = alarm.get("Trigger", {})
    metric = trigger.get("MetricName", "")
    if metric:
        dims = trigger.get("Dimensions", [])
        dim_str = " ".join(
            f"`{d['value']}`" for d in dims
            if isinstance(d, dict) and d.get("value")
        )
        namespace = trigger.get("Namespace", "")
        lines.append(f"Metric: {_escape_markdown(namespace)}/{_escape_markdown(metric)} {dim_str}")

    # Fleet state
    lines.append("")
    lines.append("*Fleet state:*")
    lines.append(_format_state_section(state))

    # Recovery
    recovery_text = _format_recovery_section(recovery_results)
    if recovery_text:
        lines.append(recovery_text)
    else:
        lines.append("")
        lines.append("_No recovery actions configured for this alarm\\._")

    # Escalation note
    esc = _format_escalation_note(is_escalation)
    if esc:
        lines.append(esc)

    # Cooldown info
    lines.append(cooldown_info)

    # Console link
    encoded_name = alarm_name.replace(" ", "+")
    console_url = (
        f"https://{region_alarm}.console.aws.amazon.com/cloudwatch/home"
        f"?region={region_alarm}#alarmsV2:alarm/{encoded_name}"
    )
    lines.append(f"[AWS Console]({console_url})")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# Handler
# ═══════════════════════════════════════════════════════════════════════════════

def handler(event: dict, context) -> dict:  # noqa: ARG001
    """SNS-triggered handler: for every backstop alarm, gather state, attempt
    ONE bounded recovery (if within cooldown rules), and forward an enhanced
    page to Telegram.

    Accepts the SNS event shape (``Records[].Sns``). Processes each alarm
    independently with its own cooldown tracking.

    Returns a summary dict with per-record outcomes. Errors are logged and
    returned — never raised — so a transient failure does not cause SNS redrive.
    """
    records = event.get("Records", [])
    if not records:
        records = [{"Sns": {"Message": json.dumps(event)}}]

    # Read Telegram secrets at invoke time (no cached credentials).
    try:
        bot_token = _ssm_parameter(SSM_BOT_TOKEN_PATH)
        chat_id = _ssm_parameter(SSM_CHAT_ID_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to read SSM parameters: %s", exc)
        return {"status": "error", "reason": f"SSM read failed: {exc}", "sent": 0}

    thread_id: str | None = None
    try:
        thread_id = _ssm_parameter(SSM_THREAD_ID_PATH)
    except Exception:  # noqa: S110, BLE001
        pass

    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION)
    results: list[dict] = []
    sent_count = 0

    for record in records:
        sns_info = record.get("Sns", {})
        message_str = str(sns_info.get("Message", "{}"))
        message_id = str(sns_info.get("MessageId", "?"))

        try:
            alarm = json.loads(message_str)
        except (ValueError, TypeError) as exc:
            logger.warning("Non-JSON SNS message %s: %s", message_id, exc)
            results.append({
                "message_id": message_id, "status": "skipped", "reason": "non-json"
            })
            continue

        if "AlarmName" not in alarm:
            logger.info("SNS message %s is not a CloudWatch alarm — skipping", message_id)
            results.append({
                "message_id": message_id, "status": "skipped", "reason": "not-an-alarm"
            })
            continue

        alarm_name = alarm["AlarmName"]
        new_state = alarm.get("NewStateValue", "")

        # Only act on ALARM state. OK/INSUFFICIENT_DATA are informational —
        # we forward them but skip recovery.
        is_alarm = new_state == "ALARM"

        # ── Cooldown check ──
        prior = _load_cooldown(s3, alarm_name)
        in_window = _within_cooldown(
            prior.get("last_fired_at") if prior else None, now
        )
        already_attempted = prior.get("recovery_attempted", False) if prior else False
        is_escalation = is_alarm and in_window and already_attempted
        should_recover = is_alarm and not (in_window and already_attempted)

        # ── State gathering (always for ALARM; skip for OK to keep it fast) ──
        recovery_results: list[dict] = []
        state: dict = {}
        if is_alarm:
            state = _gather_state()

        # ── Recovery attempt ──
        if should_recover:
            recovery_results = _attempt_recovery(alarm_name)
            any_ok = any(r.get("ok") for r in recovery_results)
            _save_cooldown(s3, alarm_name, {
                "alarm_name": alarm_name,
                "last_fired_at": now.isoformat(),
                "recovery_attempted": True,
                "recovery_results": recovery_results,
                "occurrence": (prior.get("occurrence", 0) + 1) if prior else 1,
                "any_recovery_ok": any_ok,
            })
        elif is_alarm and is_escalation:
            # Second occurrence — escalate, no retry
            _save_cooldown(s3, alarm_name, {
                "alarm_name": alarm_name,
                "last_fired_at": now.isoformat(),
                "recovery_attempted": True,
                "recovery_results": prior.get("recovery_results", []) if prior else [],
                "occurrence": (prior.get("occurrence", 0) + 1) if prior else 1,
                "escalated": True,
            })
        elif is_alarm:
            # In cooldown but no prior recovery? Defensive: attempt recovery.
            logger.warning(
                "Alarm %s in cooldown with no prior recovery — attempting recovery now",
                alarm_name,
            )
            recovery_results = _attempt_recovery(alarm_name)
            _save_cooldown(s3, alarm_name, {
                "alarm_name": alarm_name,
                "last_fired_at": now.isoformat(),
                "recovery_attempted": True,
                "recovery_results": recovery_results,
                "occurrence": (prior.get("occurrence", 0) + 1) if prior else 1,
            })
        elif not is_alarm:
            # OK/INSUFFICIENT_DATA — clear cooldown so next ALARM is a fresh start
            _save_cooldown(s3, alarm_name, {
                "alarm_name": alarm_name,
                "last_fired_at": now.isoformat(),
                "last_state": new_state,
                "recovery_attempted": False,
                "occurrence": 0,
                "resolved": True,
            })

        # ── Cooldown info line ──
        if is_escalation:
            cooldown_info = (
                f"\\⏱ _Escalation: {COOLDOWN_MINUTES}m cooldown, "
                f"occurrence #{prior.get('occurrence', 0) + 1 if prior else 1}_"
            )
        elif should_recover and recovery_results:
            cooldown_info = (
                f"\\⏱ _Recovery attempted \\(cooldown: {COOLDOWN_MINUTES}m, "
                f"occurrence #{(prior.get('occurrence', 0) + 1) if prior else 1}\\)_"
            )
        elif not is_alarm:
            cooldown_info = "\\✅ _Alarm resolved, cooldown reset_"
        else:
            cooldown_info = f"\\⏱ _Within {COOLDOWN_MINUTES}m cooldown \\(recovery already attempted\\)_"

        # ── Build and send page ──
        text = _format_page(alarm, state, recovery_results, is_escalation, cooldown_info)
        resp = _send_telegram(bot_token, chat_id, text, thread_id)
        if resp and resp.get("ok"):
            sent_count += 1
            results.append({
                "message_id": message_id,
                "alarm_name": alarm_name,
                "status": "sent",
                "recovery_attempted": bool(recovery_results),
                "escalated": is_escalation,
            })
        else:
            err_desc = (resp or {}).get("description", "send failed")
            logger.warning("Telegram send failed for %s: %s", message_id, err_desc)
            results.append({
                "message_id": message_id,
                "alarm_name": alarm_name,
                "status": "failed",
                "error": err_desc,
            })

    logger.info("backstop-responder: %d/%d sent", sent_count, len(results))
    return {
        "status": "ok",
        "sent": sent_count,
        "total": len(results),
        "results": results,
    }
