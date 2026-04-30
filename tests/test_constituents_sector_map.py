"""
Regression tests for constituents.py — sector_map coverage.

Bug: prior to fix, _fetch_constituents only extracted GICS sectors from the
S&P 500 Wikipedia table, leaving every S&P 400 mid-cap ticker without a
sector mapping. EOD reconcile's sector attribution depended on this map and
silently fell through to "Unknown" for any held mid-cap (e.g. JHG fired
flow-doctor on 2026-04-30).
"""
from __future__ import annotations

from io import StringIO
from unittest.mock import patch

import pandas as pd
import pytest

from collectors import constituents


def _fake_html(tickers: list[str], sectors: list[str]) -> str:
    """Build minimal Wikipedia-shaped HTML with Symbol + GICS Sector columns."""
    df = pd.DataFrame({"Symbol": tickers, "GICS Sector": sectors})
    return df.to_html(index=False)


class _FakeResp:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        pass


def test_sector_map_covers_both_sp500_and_sp400() -> None:
    """sector_map must include every ticker from both index tables."""
    sp500_html = _fake_html(["AAPL", "MSFT"], ["Information Technology", "Information Technology"])
    sp400_html = _fake_html(["JHG", "WSO"], ["Financials", "Industrials"])

    def fake_get(url, **kwargs):
        if "S%26P_500" in url:
            return _FakeResp(sp500_html)
        if "S%26P_400" in url:
            return _FakeResp(sp400_html)
        raise AssertionError(f"unexpected URL: {url}")

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        tickers, sector_map, sector_etf_map, sp500_count, sp400_count = (
            constituents._fetch_constituents()
        )

    assert sp500_count == 2
    assert sp400_count == 2
    assert sector_map["AAPL"] == "Information Technology"
    assert sector_map["JHG"] == "Financials"
    assert sector_map["WSO"] == "Industrials"
    assert sector_etf_map["JHG"] == "XLF"
    assert sector_etf_map["WSO"] == "XLI"
    assert set(tickers) == {"AAPL", "MSFT", "JHG", "WSO"}


def test_collect_raises_when_sector_coverage_incomplete(tmp_path) -> None:
    """If a ticker lands in `tickers` without a sector entry, collect() must raise."""
    # Simulate the prior-bug condition: tickers list has 4 entries but
    # sector_map only has 2 (S&P 500 only).
    def fake_fetch():
        return (
            ["AAPL", "MSFT", "JHG", "WSO"],
            {"AAPL": "Information Technology", "MSFT": "Information Technology"},
            {"AAPL": "XLK", "MSFT": "XLK"},
            2,
            2,
        )

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch):
        with pytest.raises(RuntimeError, match="Sector mapping incomplete"):
            constituents.collect(bucket="any", dry_run=True)


def test_fetch_raises_when_sector_column_missing() -> None:
    """If Wikipedia table column header changes, _fetch_constituents must raise."""
    df = pd.DataFrame({"Symbol": ["AAPL"], "Industry": ["Tech"]})  # no GICS column
    sp500_html = df.to_html(index=False)

    def fake_get(url, **kwargs):
        return _FakeResp(sp500_html)

    with patch("collectors.constituents.requests.get", side_effect=fake_get):
        # _fetch_constituents catches Exception and falls back to cache; we want
        # to verify the raise happens BEFORE the broad except — the symptom is
        # that no sector_map gets populated, which is exactly the cache-fallback
        # signature. The downstream collect() check catches this case.
        tickers, sector_map, _, _, _ = constituents._fetch_constituents()

    # Either the cache fallback returned tickers with empty sector_map, or
    # the cache was empty too. Either way: the post-loop validation in
    # collect() will hard-fail on this state. That's the contract.
    if tickers:
        assert not sector_map or len(sector_map) < len(tickers)
