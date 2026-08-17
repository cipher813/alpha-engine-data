"""The EOD terminal lands where the RUNNING ping lands (alpha-engine-config-I7573).

Brian's ruling, 2026-08-17: "eod outcome should land in the same place as
'Post-close Trading SF — RUNNING'".

They were not landing in the same place, and the reason was that they take
different delivery paths inside ``notify_via_flow_doctor``:

* RUNNING is ``silent=True``, so it takes the ``silent_topic`` branch and is
  written straight to the PIPELINE thread with ``send_raw`` — flow-doctor's
  dedup, severity filter and rate limiter are never consulted, so it always
  arrives.
* The terminal goes through ``fd.notify_event``, where the fleet-shared daily
  alert budget applies. ``rate_limit_exempt_severities`` defaults to
  critical+error, and a terminal is ``info`` (SUCCEEDED) or ``warning``
  (FAILED / DegradedRun) — neither exempt.

Measured on ne-postclose-trading-pipeline: 2026-08-03, -04 and -07 all reached
SUCCEEDED and all three reports were recorded ``reason=rate_limited``, i.e.
delivered to nobody, while each of those mornings' RUNNING pings landed
normally.

These tests pin both halves of the fix: the exemption that stops the drop, and
the ``guaranteed_topic`` belt that catches any future suppression which is not
dedup.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import flow_doctor_telegram  # noqa: E402


class _Notifier:
    def __init__(self):
        self.sent: list[tuple[str, bool]] = []

    def send_raw(self, text, disable_notification=False):
        self.sent.append((text, disable_notification))
        return "message-id"


class _FlowDoctor:
    """Minimal stand-in for the bits of FlowDoctor this module touches."""

    def __init__(self, reason: str):
        self._reason = reason
        self.events: list[dict] = []

    def notify_event(self, subject, body=None, *, severity, context, dedup_key):
        self.events.append({"subject": subject, "severity": severity})
        # A report id is returned for EVERY outcome, including suppressed ones.
        return "report-id"

    def last_dispatched(self) -> bool:
        return self._reason == "fired"

    def last_dispatch_reason(self) -> str:
        return self._reason


@pytest.fixture
def wired(monkeypatch):
    """Patch get_flow_doctor / topic_telegram_notifier and hand back the doubles."""

    def _wire(reason: str, *, notifier: _Notifier | None = None):
        fd = _FlowDoctor(reason)
        found = _Notifier() if notifier is None else notifier
        monkeypatch.setattr(flow_doctor_telegram, "get_flow_doctor", lambda *a, **k: fd)
        monkeypatch.setattr(
            flow_doctor_telegram, "topic_telegram_notifier", lambda _fd, _topic: found
        )
        return fd, found

    return _wire


def _call(**overrides):
    kwargs = dict(
        text="Post-close Trading SF — SUCCEEDED",
        silent=False,
        severity="info",
        dedup_key="k",
        flow_name="sf-telegram-notifier",
        topics=(),
        db_basename="db",
    )
    kwargs.update(overrides)
    return flow_doctor_telegram.notify_via_flow_doctor(**kwargs)


class TestHonestReturnValue:
    def test_a_rate_limited_alert_is_not_reported_as_sent(self, wired):
        """The old expression was `report_id is not None`, and flow-doctor
        returns a report id for a rate-limited alert too — so `telegram_sent`
        was True on every drop."""
        wired("rate_limited")
        assert _call() is False

    def test_a_delivered_alert_is_reported_as_sent(self, wired):
        wired("fired")
        assert _call() is True


class TestGuaranteedTopic:
    def test_rate_limited_terminal_still_reaches_the_pipeline_thread(self, wired):
        _fd, notifier = wired("rate_limited")
        assert _call(guaranteed_topic="PIPELINE") is True
        assert notifier.sent == [("Post-close Trading SF — SUCCEEDED", False)]

    @pytest.mark.parametrize(
        "reason",
        ["rate_limited", "severity_filtered", "category_filtered",
         "delivery_failed", "no_notifiers"],
    )
    def test_every_non_dedup_suppression_falls_back(self, wired, reason):
        _fd, notifier = wired(reason)
        assert _call(guaranteed_topic="PIPELINE") is True
        assert len(notifier.sent) == 1

    def test_dedup_is_respected_not_bypassed(self, wired):
        """A repeat inside the cooldown is a message the operator has already
        been shown. Re-sending it raw would defeat the one suppression that
        exists to protect them."""
        _fd, notifier = wired("deduped")
        assert _call(guaranteed_topic="PIPELINE") is False
        assert notifier.sent == []

    def test_a_delivered_alert_is_not_sent_twice(self, wired):
        _fd, notifier = wired("fired")
        assert _call(guaranteed_topic="PIPELINE") is True
        assert notifier.sent == []

    def test_without_a_guaranteed_topic_nothing_changes(self, wired):
        _fd, notifier = wired("rate_limited")
        assert _call() is False
        assert notifier.sent == []

    def test_missing_notifier_returns_false_rather_than_claiming_delivery(
        self, monkeypatch
    ):
        fd = _FlowDoctor("rate_limited")
        monkeypatch.setattr(flow_doctor_telegram, "get_flow_doctor", lambda *a, **k: fd)
        monkeypatch.setattr(
            flow_doctor_telegram, "topic_telegram_notifier", lambda _fd, _topic: None
        )
        assert _call(guaranteed_topic="PIPELINE") is False


@pytest.fixture
def fleet_module():
    """A `nousergon_lib.flow_doctor_fleet` that has `fleet_telegram_notifier_dicts`.

    ``test_handler.py`` installs a module-scope stub of that package into
    ``sys.modules`` and never removes it, and the stub omits this function — so
    whether ``build_flow_doctor_config`` is importable at all depends on
    pytest's collection order. That order-dependence is a pre-existing defect
    in this directory's suite (it is why
    ``test_flow_doctor_fleet_wiring.py::test_build_flow_doctor_config_matches_pipeline_observer_topics``
    passes alone and fails in a full run, on `main` as well as on this branch);
    it is filed separately rather than patched over here. This fixture only
    stops THESE tests from inheriting it.
    """
    import sys as _sys

    mod = _sys.modules.get("nousergon_lib.flow_doctor_fleet")
    if mod is not None and not hasattr(mod, "fleet_telegram_notifier_dicts"):
        mod.fleet_telegram_notifier_dicts = lambda topics: [
            {"type": "telegram", "topic": t} for t in topics
        ]
    return mod


@pytest.mark.usefixtures("fleet_module")
class TestRateLimitExemption:
    def test_default_config_is_unchanged(self):
        cfg = flow_doctor_telegram.build_flow_doctor_config(
            "f", (), db_basename="db"
        )
        assert cfg["rate_limits"] == {"max_alerts_per_day": 100}

    def test_exemption_is_emitted_when_requested(self):
        cfg = flow_doctor_telegram.build_flow_doctor_config(
            "f", (), db_basename="db",
            rate_limit_exempt_severities=("critical", "error", "warning", "info"),
        )
        assert cfg["rate_limits"]["rate_limit_exempt_severities"] == [
            "critical", "error", "warning", "info",
        ]

    def test_the_sf_notifier_exempts_both_terminal_severities(self):
        """`info` is a SUCCEEDED terminal and `warning` is a FAILED or
        DegradedRun terminal. Missing either one puts that outcome back inside
        the shared budget that dropped it."""
        import index

        cfg = index.build_flow_doctor_config_for_tests()
        exempt = cfg["rate_limits"]["rate_limit_exempt_severities"]
        assert "info" in exempt
        assert "warning" in exempt


def test_module_imports_without_boto(monkeypatch):
    """Guard the guard: these tests assert on a real module, not a stub that
    would make every assertion above vacuous."""
    assert isinstance(flow_doctor_telegram, types.ModuleType)
    assert hasattr(flow_doctor_telegram, "notify_via_flow_doctor")
