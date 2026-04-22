"""Tests for the analyst sub-collector in ``collectors/alternative.py``.

Contract as of 2026-04-22:

  * Finnhub ``/stock/recommendation`` drives ``rating`` + ``num_analysts``.
  * yfinance ``Ticker.info`` drives ``target_price`` (Finnhub's
    ``/stock/price-target`` and FMP's ``price-target-consensus`` are both
    paid-tier).
  * Failures on either provider must degrade loudly (WARN) but never
    raise — ``_fetch_analyst`` is called per-ticker and a provider outage
    on one ticker must not poison the whole Phase 2 batch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from collectors import alternative


def _finnhub_recommendation_stub(bullish=8, bearish=1, hold=2):
    return [
        {
            "strongBuy": max(bullish - 2, 0),
            "buy": min(bullish, 2),
            "hold": hold,
            "sell": min(bearish, 1),
            "strongSell": max(bearish - 1, 0),
            "period": "2026-04-01",
            "symbol": "AAPL",
        }
    ]


def test_target_price_from_yfinance():
    """When yfinance exposes targetMeanPrice, it must land in target_price."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.return_value.info = {
        "targetMeanPrice": 215.5,
        "numberOfAnalystOpinions": 42,
    }

    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("AAPL")

    assert out["target_price"] == 215.5
    # Finnhub returned num_analysts → yfinance must NOT clobber it
    assert out["num_analysts"] == 11  # 8 bullish + 2 hold + 1 bearish stub totals


def test_num_analysts_backfilled_from_yfinance_when_finnhub_empty():
    """If Finnhub returns nothing, yfinance's count populates num_analysts."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.return_value.info = {
        "targetMeanPrice": 300.0,
        "numberOfAnalystOpinions": 25,
    }

    # Finnhub returns empty list → no rating / no num_analysts from Finnhub.
    # ``_finnhub_get`` is called twice (recommendation, earnings), both empty.
    with patch.object(alternative, "_finnhub_get", return_value=[]), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("NEWCO")

    assert out["target_price"] == 300.0
    assert out["num_analysts"] == 25
    assert out["rating"] is None  # no Finnhub data → no rating classification


def test_yfinance_failure_degrades_loudly_without_raising():
    """yfinance raising inside _fetch_analyst must not bubble up."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.side_effect = RuntimeError("yfinance IP block")

    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("AAPL")

    # Finnhub path still populated rating + num_analysts
    assert out["rating"] in ("Buy", "Hold", "Sell")
    assert out["num_analysts"] == 11
    # yfinance failed → target_price stays None (degraded but observable via WARN)
    assert out["target_price"] is None


def test_missing_target_mean_price_leaves_target_price_none():
    """yfinance sometimes returns info without targetMeanPrice (new/illiquid names)."""
    mock_yf_module = MagicMock()
    mock_yf_module.Ticker.return_value.info = {
        "numberOfAnalystOpinions": 3,
        # no targetMeanPrice
    }

    with patch.object(alternative, "_finnhub_get", return_value=_finnhub_recommendation_stub()), \
         patch.dict("sys.modules", {"yfinance": mock_yf_module}):
        out = alternative._fetch_analyst("THINLY_COVERED")

    assert out["target_price"] is None
    # Finnhub still authoritative on num_analysts
    assert out["num_analysts"] == 11
