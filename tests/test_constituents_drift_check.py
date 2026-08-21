"""Tests for the Friday-Preflight Wikipedia-vs-ArcticDB constituents
drift check (5/23-SF P0 (g))."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_check_drift_no_drift_returns_ok():
    """When Wikipedia ⊆ ArcticDB universe (modulo skip-list), status=ok."""
    from validators.constituents_drift_check import check_drift

    # Wikipedia returns a small known set; ArcticDB has the same + extras.
    fake_fetch = MagicMock(return_value=(
        ["AAPL", "MSFT", "NVDA"],  # tickers
        {}, {}, {},                   # sector_map, sector_etf_map, sub_industry_map
        2, 1,                        # sp500_count, sp400_count
    ))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL", "MSFT", "NVDA", "GOOG"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == []


def test_check_drift_missing_tickers_detected():
    """The canonical 5/23 scenario: Wikipedia lists BNY/P/SN, ArcticDB
    doesn't — drift_detected + missing list populated."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(
        ["AAPL", "MSFT", "BNY", "P", "SN"],
        {}, {}, {},
        3, 2,
    ))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL", "MSFT"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "drift_detected"
    assert set(result["missing_from_arctic"]) == {"BNY", "P", "SN"}
    assert result["within_threshold"] is False


def test_check_drift_under_threshold_passes():
    """max_stragglers tolerance: 1 missing with cap=2 → status=ok."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "BNY"], {}, {}, {}, 1, 1))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False, max_stragglers=2)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == ["BNY"]
    assert result["within_threshold"] is True


def test_check_drift_skip_list_excluded_from_diff():
    """SPY (in _SKIP_TICKERS) doesn't fire drift even if Wikipedia
    lists it but ArcticDB lacks the SKIP_TICKERS entry — _SKIP_TICKERS
    is stripped from BOTH sides of the comparison."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "SPY", "VIX"], {}, {}, {}, 1, 0))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == []


def test_check_drift_sector_etf_excluded():
    """XLK / XLF / XL* prefixes excluded from drift comparison."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "XLK", "XLF"], {}, {}, {}, 1, 0))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"


def test_check_drift_wikipedia_fetch_failure_returns_error():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(side_effect=Exception("Wikipedia 503"))
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch):
        result = check_drift(alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "wikipedia_fetch"


def test_check_drift_arctic_failure_returns_error():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL"], {}, {}, {}, 1, 0))
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               side_effect=Exception("ArcticDB unreachable")):
        result = check_drift(alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "arctic_list"


def test_main_exit_code_ok():
    from validators.constituents_drift_check import main

    fake_fetch = MagicMock(return_value=(["AAPL"], {}, {}, {}, 1, 0))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        rc = main(["--no-alert"])
    assert rc == 0


def test_main_exit_code_drift_detected():
    from validators.constituents_drift_check import main

    fake_fetch = MagicMock(return_value=(["AAPL", "BNY"], {}, {}, {}, 1, 1))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        rc = main(["--no-alert"])
    assert rc == 1


# ---------------------------------------------------------------------------
# Alert message content (alpha-engine-config-I8094).
#
# These assert on the emitted STRING, which is unusual for this repo and
# deliberate. The 2026-08-21 SUI/VMRK page was correct in every field except
# its prose: it named Wikipedia as the membership source (moved to SSGA in
# I2812) and told the reader Research preflight was about to fail on a gate
# that cannot fail on this input. Both facts were only ever asserted in an
# f-string, so nothing caught either one drifting. The message is the
# product here — a detector nobody can act on has not detected anything.
# ---------------------------------------------------------------------------


def _capture_alert_message(*, tickers, arctic, sp500=1, sp400=1):
    """Run check_drift with alerting ON and return the published message."""
    from unittest.mock import MagicMock, patch

    from validators import constituents_drift_check as mod

    fake_fetch = MagicMock(return_value=(tickers, {}, {}, {}, sp500, sp400))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = arctic

    published = {}

    class _Leg:
        ok = True

    class _Result:
        sns = _Leg()
        telegram = _Leg()
        any_ok = True

    def _publish(message, **kwargs):
        published["message"] = message
        published["kwargs"] = kwargs
        return _Result()

    fake_alerts = MagicMock()
    fake_alerts.publish = _publish

    with patch.object(mod, "_fetch_constituents", fake_fetch), \
         patch.object(mod, "_open_universe_lib", return_value=fake_lib), \
         patch.dict("sys.modules", {"nousergon_lib.alerts": fake_alerts}), \
         patch("nousergon_lib.alerts", fake_alerts, create=True):
        mod.check_drift(alert=True)

    return published.get("message", "")


def test_alert_names_ssga_not_wikipedia_as_membership_source():
    """Membership ground truth moved to SSGA SPY/MDY holdings in I2812.

    An alert naming Wikipedia sends the reader to audit a system that has
    not been the source of truth since 2026-07.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "SSGA" in msg
    assert "Wikipedia-listed" not in msg


def test_alert_does_not_claim_research_preflight_will_fail():
    """`ResearchPreflight` checks macro.SPY freshness only.

    It cannot fail on ticker completeness, so the alert must not say it
    will. Guards the exact phrasing that produced the 2026-08-21 page.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "will likely fail at Research preflight" not in msg


def test_alert_states_the_margin_against_the_reachable_ceiling():
    """The reachable gate is fetch_price_data's 5% error-rate ceiling.

    A reader deciding whether to act tonight needs the observed fraction
    and the ceiling, not an adjective.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "price_fetcher.py::fetch_price_data" in msg
    assert "5%" in msg


def test_alert_says_under_ceiling_when_drift_is_a_small_fraction():
    """1 of 3 is over 5%; 1 of 40 is under. The clause must follow the
    arithmetic, not the mere existence of drift."""
    small = _capture_alert_message(
        tickers=[f"T{i}" for i in range(40)] + ["SUI"],
        arctic=[f"T{i}" for i in range(40)],
    )
    assert "UNDER the ceiling" in small
    assert "OVER the ceiling" not in small


def test_alert_says_over_ceiling_when_drift_exceeds_it():
    big = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI", "VMRK"], arctic=["AAPL"],
    )
    assert "OVER the ceiling" in big
    assert "UNDER the ceiling" not in big


def test_alert_cites_the_live_tracker_not_archived_roadmap_lines():
    """`ROADMAP P0 (g)` and `L1316` are both closed, archived work.

    Pointing triage at two tombstones costs a session every time.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "I8094" in msg
    assert "ROADMAP P0 (g)" not in msg
    assert "L1316" not in msg
