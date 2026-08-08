"""A prepaid provider account must page BEFORE it reaches zero.

Root incident — alpha-engine-config-I6613, 2026-08-05..08. The DeepSeek account
all autonomous lanes route to as primary ran out of credit. Every lane returned
``402 Insufficient Balance`` for three days, through a live three-day trading
outage, while sf-watch dispatched a diagnose agent each morning that died on the
same 402 within ninety seconds and filed an issue whose entire content was "the
diagnoser crashed". Detection fired, produced nothing, and the outage ran.

**The balance was already being collected.** ``credits_remaining`` for
OpenRouter, ``total_balance`` for DeepSeek — both fetched every run and both
buried in a ``detail`` blob no alert reads. That is `principles.md` §2.7 in its
pure form: a component emitting nothing is not healthy, it is unobserved, and a
number written to an artifact nobody alerts on is emitting nothing.

What these tests pin is therefore not "can we read a balance" — we always
could — but that the balance now produces a **page, before zero**, and that the
predicate refuses to fire on ignorance.
"""

from __future__ import annotations

import importlib
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Stub nousergon_lib + flow_doctor_telegram before importing index ─────────
# Mirrors test_handler.py exactly: both are git-only / bundled deps this suite
# does not require installed, and the notify path must be a no-op by default so
# no test can reach the live ops-health channel with fixture data.
_ng = types.ModuleType("nousergon_lib")
_ng_fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")


class _FleetTelegramTopic:
    CRITICAL = "CRITICAL"
    OPS_HEALTH = "OPS_HEALTH"


_ng_fleet.FleetTelegramTopic = _FleetTelegramTopic
_ng.flow_doctor_fleet = _ng_fleet
sys.modules.setdefault("nousergon_lib", _ng)
sys.modules.setdefault("nousergon_lib.flow_doctor_fleet", _ng_fleet)

_fdt = types.ModuleType("flow_doctor_telegram")
_fdt.notify_via_flow_doctor = lambda *a, **k: True  # type: ignore[attr-defined]
sys.modules["flow_doctor_telegram"] = _fdt

from _shared.hermetic_import_guard import (  # noqa: E402
    assert_hermetic_imports_satisfied,
)

assert_hermetic_imports_satisfied(__file__)

import index  # noqa: E402


@pytest.fixture(autouse=True)
def _reload():
    """Each test gets a clean module — several read env-driven module
    constants that other tests monkeypatch."""
    importlib.reload(index)
    yield


def _mw(elapsed_frac: float = 0.5, days_in_month: int = 30) -> dict:
    total = days_in_month * 86400.0
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    return {
        "period": "2026-08", "start": start, "total_seconds": total,
        "elapsed_frac": elapsed_frac,
        "now": start,
    }


# ── The runway arithmetic ───────────────────────────────────────────────────


def test_days_to_zero_is_computed_from_observed_burn():
    """15 days into a 30-day month, $15 spent ⇒ $1/day. $10 left ⇒ 10 days."""
    assert index._days_to_zero(10.0, 15.0, _mw(0.5)) == 10.0


def test_days_to_zero_scales_with_the_observation_window():
    """The same balance and spend measured over HALF the window is twice the
    burn and therefore half the runway. Getting this backwards would report a
    comfortable margin on an account about to die."""
    fast = index._days_to_zero(10.0, 15.0, _mw(0.5), observed_frac=0.25)
    assert fast == 5.0


@pytest.mark.parametrize(
    "balance,mtd,frac,why",
    [
        (None, 15.0, 0.5, "no balance — the provider bills in arrears"),
        (10.0, None, 0.5, "no measured spend"),
        (10.0, 0.0, 0.5, "burn indistinguishable from zero"),
        (10.0, 15.0, 0.001, "observation window too short to extrapolate"),
    ],
)
def test_days_to_zero_returns_unknown_rather_than_a_guess(balance, mtd, frac, why):
    """None is UNKNOWN. Every one of these could be rendered as a large number
    or as infinity, and both would read as 'plenty of runway' on the surface a
    human scans."""
    assert index._days_to_zero(balance, mtd, _mw(frac)) is None, why


def test_a_zero_balance_reports_no_runway_rather_than_a_negative():
    """An account already at or below zero is an outage in progress, not a
    projection. A negative days_to_zero would sort oddly and read as a
    forecast."""
    assert index._days_to_zero(-4.20, 15.0, _mw(0.5)) == 0.0


def test_stamp_promotes_the_balance_to_a_first_class_field():
    row = index._row("deepseek", "DeepSeek")
    row["mtd_cost_usd"] = 15.0
    index._stamp_funded_balance(row, _mw(0.5), 10.0)
    assert row["funded_balance_usd"] == 10.0
    assert row["days_to_zero"] == 10.0


def test_rows_default_to_unknown_not_absent():
    """Every row carries both keys. An arrears-billed provider reports null,
    which the console must render as 'n/a' — a MISSING key is indistinguishable
    from a healthy one to a template that uses `.get`."""
    row = index._row("anthropic_api", "Anthropic API")
    assert row["funded_balance_usd"] is None
    assert row["days_to_zero"] is None


# ── The predicate ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "row,expected,why",
    [
        ({"funded_balance_usd": 0.0, "days_to_zero": None}, True, "at zero"),
        ({"funded_balance_usd": -4.2, "days_to_zero": 0.0}, True, "already negative"),
        ({"funded_balance_usd": 3.0, "days_to_zero": 2.0}, True, "inside the window"),
        ({"funded_balance_usd": 300.0, "days_to_zero": 90.0}, False, "comfortable"),
        ({"funded_balance_usd": None, "days_to_zero": None}, False, "arrears billing"),
        ({"funded_balance_usd": 300.0, "days_to_zero": None}, False, "burn unmeasurable"),
    ],
)
def test_is_depleting(row, expected, why):
    assert index._is_depleting(row) is expected, why


def test_the_threshold_is_days_not_dollars(monkeypatch):
    """A dollar floor means something different at $0.30/month and $30/month.
    'How long have I got' is the question that transfers across providers."""
    row = {"funded_balance_usd": 50.0, "days_to_zero": 10.0}
    assert index._is_depleting(row) is False          # default 7 days
    monkeypatch.setattr(index, "BALANCE_ALERT_DAYS", 14.0)
    assert index._is_depleting(row) is True


def test_a_402_is_the_state_this_must_fire_before():
    """The incident's own end state: balance exhausted, every routed lane
    returning 402. If the predicate only fired here it would be a post-mortem,
    not a monitor — so the runway case above is the load-bearing one and this
    is the backstop."""
    assert index._is_depleting({"funded_balance_usd": 0.0, "days_to_zero": 0.0})


# ── The rising-edge pass ────────────────────────────────────────────────────


class _FakeS3:
    def __init__(self, state=None):
        self.state = state or {}
        self.saved = []

    def get_object(self, **_kw):
        raise RuntimeError("no prior state")

    def put_object(self, **kw):
        self.saved.append(kw)


def _run(monkeypatch, rows, prior=None):
    sent = []
    monkeypatch.setattr(index, "_load_alert_state",
                        lambda _s3, _p: {"providers": prior or {}})
    monkeypatch.setattr(index, "_save_alert_state",
                        lambda _s3, _p, breached: sent.append(("state", breached)))
    monkeypatch.setattr(index, "_alert_balance_depletion",
                        lambda row, period: sent.append(("alert", row["key"])) or True)
    result = index.run_balance_depletion_alerts(_FakeS3(), "2026-08", rows)
    return result, sent


def test_alerts_once_on_the_rising_edge(monkeypatch):
    row = {"key": "deepseek", "label": "DeepSeek", "funded_balance_usd": 1.0,
           "days_to_zero": 2.0, "mtd_cost_usd": 15.0}
    result, sent = _run(monkeypatch, [row])
    assert result["alerted"] == ["deepseek"]
    assert ("alert", "deepseek") in sent


def test_stays_quiet_on_a_sustained_breach(monkeypatch):
    """Three days of the same 402 produced three identical issues last time.
    Sustained means already-known."""
    row = {"key": "deepseek", "label": "DeepSeek", "funded_balance_usd": 1.0,
           "days_to_zero": 2.0, "mtd_cost_usd": 15.0}
    result, sent = _run(monkeypatch, [row], prior={"balance:deepseek": True})
    assert result["alerted"] == []
    assert not [s for s in sent if s[0] == "alert"]


def test_re_arms_after_a_top_up(monkeypatch):
    """A second depletion in the same month must alert again — the account
    being topped up is exactly when the flag has to clear."""
    healthy = {"key": "deepseek", "label": "DeepSeek", "funded_balance_usd": 500.0,
               "days_to_zero": 120.0, "mtd_cost_usd": 15.0}
    _result, sent = _run(monkeypatch, [healthy], prior={"balance:deepseek": True})
    saved = [s[1] for s in sent if s[0] == "state"][0]
    assert saved["balance:deepseek"] is False


def test_state_namespace_does_not_collide_with_the_over_budget_flag(monkeypatch):
    """Over budget and out of money are different questions about the same
    provider, and a provider can be both. Sharing one flag would let either
    suppress the other."""
    row = {"key": "deepseek", "label": "DeepSeek", "funded_balance_usd": 1.0,
           "days_to_zero": 2.0, "mtd_cost_usd": 15.0}
    # The over-budget pass has already flagged this provider under its bare key.
    result, sent = _run(monkeypatch, [row], prior={"deepseek": True})
    assert result["alerted"] == ["deepseek"], (
        "the over-budget flag suppressed the depletion alert — they must use "
        "separate state namespaces"
    )
    saved = [s[1] for s in sent if s[0] == "state"][0]
    assert saved["deepseek"] is True, "the over-budget flag must survive this pass"
    assert saved["balance:deepseek"] is True


def test_the_pass_never_raises(monkeypatch):
    """Notification layered after a successful rollup write — a bug here must
    not cost the rollup, mirroring run_over_budget_alerts."""
    monkeypatch.setattr(index, "_load_alert_state",
                        mock.Mock(side_effect=RuntimeError("boom")))
    result = index.run_balance_depletion_alerts(_FakeS3(), "2026-08", [])
    assert result["alerted"] == []
    assert "boom" in result["error"]


def test_the_alert_is_critical_not_warning():
    """An over-budget month is a number to look at. An unfunded account is
    every routed lane down at once, and the lanes cannot report it — they die
    on the 402 before they can."""
    captured = {}

    def _notify(text, **kw):
        captured.update(kw)
        captured["text"] = text
        return True

    with mock.patch.object(index, "notify_via_flow_doctor", _notify):
        index._alert_balance_depletion(
            {"key": "deepseek", "label": "DeepSeek", "funded_balance_usd": 0.0,
             "days_to_zero": 0.0, "mtd_cost_usd": 15.0}, "2026-08")
    assert captured["severity"] == "critical"
    assert "already unfunded" in captured["text"]
