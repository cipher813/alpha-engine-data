"""Tests for polygon_client.PolygonClient.

Focus: response caching on get_grouped_daily, which dedup's calendar-date
repeats across overlapping eval_date windows in universe_returns and cuts
the free-tier 5 calls/min rate-limit tax by ~3.5x on backfill runs.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from polygon_client import PolygonClient


def _make_client() -> PolygonClient:
    return PolygonClient(api_key="test-key", calls_per_min=5)


def _fake_response(tickers: list[tuple[str, float]]) -> dict:
    return {
        "results": [
            {"T": t, "o": 1.0, "h": 2.0, "l": 0.5, "c": close, "v": 1000, "vw": 1.5}
            for t, close in tickers
        ],
        "resultsCount": len(tickers),
    }


def test_grouped_daily_caches_identical_dates():
    client = _make_client()
    with patch.object(client, "_get", return_value=_fake_response([("AAPL", 200.0)])) as mock_get:
        first = client.get_grouped_daily("2026-01-05")
        second = client.get_grouped_daily("2026-01-05")
    assert mock_get.call_count == 1
    assert first == second
    assert first["AAPL"]["close"] == 200.0


def test_grouped_daily_distinct_dates_hit_api():
    client = _make_client()
    responses = [
        _fake_response([("AAPL", 200.0)]),
        _fake_response([("AAPL", 201.0)]),
    ]
    with patch.object(client, "_get", side_effect=responses) as mock_get:
        a = client.get_grouped_daily("2026-01-05")
        b = client.get_grouped_daily("2026-01-06")
    assert mock_get.call_count == 2
    assert a["AAPL"]["close"] == 200.0
    assert b["AAPL"]["close"] == 201.0


def test_grouped_daily_caches_empty_response():
    """Non-trading days return empty dicts — cache them too (same URL, same answer)."""
    client = _make_client()
    with patch.object(client, "_get", return_value={"results": [], "resultsCount": 0}) as mock_get:
        first = client.get_grouped_daily("2026-01-03")  # Saturday
        second = client.get_grouped_daily("2026-01-03")
    assert mock_get.call_count == 1
    assert first == {}
    assert second == {}


def test_cache_is_per_instance():
    c1 = _make_client()
    c2 = _make_client()
    with patch.object(c1, "_get", return_value=_fake_response([("AAPL", 200.0)])) as m1:
        c1.get_grouped_daily("2026-01-05")
    with patch.object(c2, "_get", return_value=_fake_response([("AAPL", 201.0)])) as m2:
        c2.get_grouped_daily("2026-01-05")
    assert m1.call_count == 1
    assert m2.call_count == 1
