"""Tests for the polygon per-ticker fallback path.

Locks the 2026-05-02 incident invariant: when polygon's bulk grouped-daily
endpoint returns an inconsistent ticker subset (observed: two calls 4h
apart returned 913-ticker subsets that differed by 8 real S&P 500/400
names), MorningEnrich must NOT hard-fail the SF on the missing-from-closes
threshold check. Instead, the per-ticker /aggs/ticker endpoint covers the
gap — same polygon source, no silent yfinance fallback.

Two layers tested:

1. ``PolygonClient.get_single_day_bar`` — happy path, no-data, 403.
2. ``_fetch_polygon_closes_per_ticker`` — recovers what it can, logs what
   it can't, never raises (caller's coverage gate is the load-bearing
   hard-fail).

The integration ``_fetch_polygon_closes`` test verifies that a partial
grouped response triggers the fallback and the final ticker count
includes both grouped-recovered and per-ticker-recovered tickers.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from polygon_client import PolygonClient, PolygonForbiddenError


def _make_client() -> PolygonClient:
    return PolygonClient(api_key="test-key", calls_per_min=5)


def _bar(close: float = 100.0) -> dict:
    return {
        "results": [
            {"o": 99.0, "h": 101.0, "l": 98.0, "c": close, "v": 1_000_000, "vw": 99.5, "t": 0}
        ],
        "resultsCount": 1,
    }


# ── PolygonClient.get_single_day_bar ──────────────────────────────────────────


def test_single_day_bar_returns_dict_on_results():
    client = _make_client()
    with patch.object(client, "_get", return_value=_bar(close=200.0)) as mock_get:
        bar = client.get_single_day_bar("AAPL", "2026-05-01")

    assert bar is not None
    assert bar == {
        "open": 99.0, "high": 101.0, "low": 98.0,
        "close": 200.0, "volume": 1_000_000, "vwap": 99.5,
    }
    mock_get.assert_called_once()
    called_path = mock_get.call_args[0][0]
    assert "/v2/aggs/ticker/AAPL/range/1/day/2026-05-01/2026-05-01" == called_path


def test_single_day_bar_returns_none_when_results_empty():
    client = _make_client()
    with patch.object(client, "_get", return_value={"results": [], "resultsCount": 0}):
        bar = client.get_single_day_bar("DELISTED", "2026-05-01")
    assert bar is None


def test_single_day_bar_returns_none_when_results_missing():
    client = _make_client()
    with patch.object(client, "_get", return_value={"resultsCount": 0}):
        bar = client.get_single_day_bar("DELISTED", "2026-05-01")
    assert bar is None


def test_single_day_bar_returns_none_on_403():
    """Free-tier 403 (same-day endpoint) is treated as no-data, not an error.
    The caller's coverage gate decides whether to hard-fail."""
    client = _make_client()
    with patch.object(
        client, "_get",
        side_effect=PolygonForbiddenError("Polygon 403: free tier same-day"),
    ):
        bar = client.get_single_day_bar("AAPL", "2026-05-02")
    assert bar is None


def test_single_day_bar_handles_missing_vwap():
    """Older / illiquid bars may lack vw — must not KeyError."""
    client = _make_client()
    payload = {"results": [{"o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 100, "t": 0}]}
    with patch.object(client, "_get", return_value=payload):
        bar = client.get_single_day_bar("ILIQUID", "2026-05-01")
    assert bar is not None
    assert bar["vwap"] is None


# ── _fetch_polygon_closes_per_ticker ──────────────────────────────────────────


def test_per_ticker_fallback_recovers_missing_tickers():
    """Happy path: 3 tickers requested, 2 have data on per-ticker endpoint,
    1 returns None. Records get the 2; recovered count is 2."""
    from collectors.daily_closes import _fetch_polygon_closes_per_ticker

    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_single_day_bar.side_effect = [
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100, "vwap": 1.2},
        None,
        {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 5000, "vwap": 10.2},
    ]

    with patch("polygon_client.polygon_client", return_value=fake_client):
        recovered = _fetch_polygon_closes_per_ticker(
            ["ASGN", "DELISTED", "HOLX"], "2026-05-01", records
        )

    assert recovered == 2
    assert {r["ticker"] for r in records} == {"ASGN", "HOLX"}
    asgn = next(r for r in records if r["ticker"] == "ASGN")
    assert asgn["Close"] == 1.5
    assert asgn["VWAP"] == 1.2
    assert asgn["Adj_Close"] == 1.5  # Adj_Close mirrors Close per polygon contract


def test_per_ticker_fallback_strips_caret_prefix():
    """Index tickers (^TNX, ^VIX) carry a caret prefix in the request list
    but polygon's API + downstream consumers use the bare symbol. Per-ticker
    fallback must apply the same lstrip the grouped path does."""
    from collectors.daily_closes import _fetch_polygon_closes_per_ticker

    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_single_day_bar.return_value = {
        "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100, "vwap": 1.2,
    }

    with patch("polygon_client.polygon_client", return_value=fake_client):
        recovered = _fetch_polygon_closes_per_ticker(["^VIX"], "2026-05-01", records)

    assert recovered == 1
    fake_client.get_single_day_bar.assert_called_once_with("VIX", "2026-05-01")
    assert records[0]["ticker"] == "VIX"


def test_per_ticker_fallback_swallows_per_call_exceptions():
    """A single polygon hiccup on one ticker must not break the whole
    fallback loop. Each ticker is independent; we log + continue."""
    from collectors.daily_closes import _fetch_polygon_closes_per_ticker

    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_single_day_bar.side_effect = [
        ConnectionError("transient"),
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100, "vwap": 1.2},
    ]

    with patch("polygon_client.polygon_client", return_value=fake_client):
        recovered = _fetch_polygon_closes_per_ticker(
            ["FLAKY", "OK"], "2026-05-01", records
        )

    assert recovered == 1
    assert records[0]["ticker"] == "OK"


# ── _fetch_polygon_closes integration ─────────────────────────────────────────


def test_fetch_polygon_closes_invokes_per_ticker_for_grouped_misses():
    """The 2026-05-02 incident scenario: grouped returns AAPL but not HOLX.
    Per-ticker fallback fires for HOLX, recovers it, polygon_count = 2."""
    from collectors.daily_closes import _fetch_polygon_closes

    records: list[dict] = []
    fake_client = MagicMock()
    # grouped returns AAPL only
    fake_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 5_000_000, "vwap": 99.5},
    }
    # per-ticker fallback returns HOLX
    fake_client.get_single_day_bar.return_value = {
        "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
        "volume": 100_000, "vwap": 50.2,
    }

    with patch("polygon_client.polygon_client", return_value=fake_client):
        polygon_count = _fetch_polygon_closes(
            ["AAPL", "HOLX"], "2026-05-01", records, source="polygon_only",
        )

    assert polygon_count == 2
    assert {r["ticker"] for r in records} == {"AAPL", "HOLX"}
    fake_client.get_single_day_bar.assert_called_once_with("HOLX", "2026-05-01")


def test_fetch_polygon_closes_skips_per_ticker_for_fred_indices():
    """FRED-handled indices (^TNX/^IRX/^VIX/^VIX3M) are not stocks and have
    no polygon coverage. Per-ticker fallback must skip them so we don't waste
    rate-limit slots on calls that are guaranteed to return nothing."""
    from collectors.daily_closes import _fetch_polygon_closes

    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 5_000_000, "vwap": 99.5},
    }

    with patch("polygon_client.polygon_client", return_value=fake_client):
        _fetch_polygon_closes(
            ["AAPL", "^TNX", "^IRX", "^VIX"], "2026-05-01", records, source="polygon_only",
        )

    fake_client.get_single_day_bar.assert_not_called()


def test_fetch_polygon_closes_no_per_ticker_when_grouped_complete():
    """No grouped misses → no per-ticker calls → no rate-limit cost.
    Locks the cost-quiet path so the fallback only fires when needed."""
    from collectors.daily_closes import _fetch_polygon_closes

    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 5_000_000, "vwap": 99.5},
        "HOLX": {"open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
                 "volume": 100_000, "vwap": 50.2},
    }

    with patch("polygon_client.polygon_client", return_value=fake_client):
        polygon_count = _fetch_polygon_closes(
            ["AAPL", "HOLX"], "2026-05-01", records, source="polygon_only",
        )

    assert polygon_count == 2
    fake_client.get_single_day_bar.assert_not_called()
