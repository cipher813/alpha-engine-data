"""
Polygon.io market data client with rate limiting and dividend adjustment.

Free tier: 5 API calls/min, ~2 years historical depth, EOD data only.
Index tickers (VIX/TNX/IRX) are not available on free tier.

Used by collectors/universe_returns.py for grouped-daily price fetches.

Usage:
    from polygon_client import PolygonClient, polygon_client

    # Singleton (reads POLYGON_API_KEY from env):
    client = polygon_client()
    bars = client.get_daily_bars("AAPL", "2025-01-01", "2026-03-28")

    # All US stocks for a single date:
    prices = client.get_grouped_daily("2026-03-28")
    # -> {"AAPL": {"open": 253.9, "high": 255.5, ...}, ...}
"""

from __future__ import annotations

import logging
import os
import time
from collections import deque
from datetime import date, datetime, timedelta

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.polygon.io"
_MAX_BARS_PER_REQUEST = 50_000  # polygon limit param max


class PolygonRateLimitError(Exception):
    """Raised when rate limit is exhausted and caller should backoff."""


class PolygonClient:
    """Rate-limited polygon.io REST client with dividend adjustment."""

    def __init__(self, api_key: str | None = None, calls_per_min: int = 5):
        self._api_key = api_key or os.environ.get("POLYGON_API_KEY", "")
        if not self._api_key:
            raise ValueError("POLYGON_API_KEY not set")
        self._calls_per_min = calls_per_min
        self._call_times: deque[float] = deque()
        self._session = requests.Session()
        self._session.params = {"apiKey": self._api_key}  # type: ignore[assignment]

    # -- Rate limiter --------------------------------------------------------

    def _wait_for_slot(self) -> None:
        """Block until a rate limit slot is available."""
        now = time.monotonic()
        window = 60.0  # 1 minute window
        # Purge old timestamps
        while self._call_times and now - self._call_times[0] > window:
            self._call_times.popleft()
        if len(self._call_times) >= self._calls_per_min:
            wait = window - (now - self._call_times[0]) + 0.5
            logger.debug("Rate limit: waiting %.1fs", wait)
            time.sleep(wait)
            # Purge again after sleep
            now = time.monotonic()
            while self._call_times and now - self._call_times[0] > window:
                self._call_times.popleft()
        self._call_times.append(time.monotonic())

    def _get(self, path: str, params: dict | None = None) -> dict:
        """Make a rate-limited GET request. Handles 429 with retry."""
        self._wait_for_slot()
        url = f"{_BASE_URL}{path}"
        for attempt in range(3):
            resp = self._session.get(url, params=params or {}, timeout=30)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 15))
                logger.warning("Rate limited (429), waiting %ds", retry_after)
                time.sleep(retry_after)
                self._call_times.clear()  # Reset window after forced wait
                continue
            if resp.status_code == 403:
                data = resp.json()
                msg = data.get("message", "Not authorized")
                logger.warning("Polygon 403: %s (path=%s)", msg, path)
                return {"results": [], "resultsCount": 0, "status": "FORBIDDEN"}
            resp.raise_for_status()
            return resp.json()
        raise PolygonRateLimitError("Rate limited after 3 retries")

    # -- Core endpoints ------------------------------------------------------

    def get_grouped_daily(self, date_str: str) -> dict[str, dict]:
        """Fetch OHLCV for ALL US stocks on a single date.

        Returns {ticker: {"open": float, "high": float, "low": float,
                          "close": float, "volume": float}}
        """
        data = self._get(
            f"/v2/aggs/grouped/locale/us/market/stocks/{date_str}",
            params={"adjusted": "true"},
        )
        results = data.get("results", [])
        return {
            r["T"]: {
                "open": r["o"],
                "high": r["h"],
                "low": r["l"],
                "close": r["c"],
                "volume": r["v"],
            }
            for r in results
            if "T" in r
        }

    def get_daily_bars(
        self,
        ticker: str,
        start: str,
        end: str,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV bars for a single ticker.

        Returns DataFrame with DatetimeIndex and columns:
        [Open, High, Low, Close, Volume]
        """
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": "asc",
            "limit": _MAX_BARS_PER_REQUEST,
        }
        data = self._get(
            f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}",
            params=params,
        )
        results = data.get("results", [])
        if not results:
            return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

        df = pd.DataFrame(results)
        df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
        df = df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
        df = df.set_index("date")[["Open", "High", "Low", "Close", "Volume"]]
        df = df.sort_index()
        return df


# -- Singleton ---------------------------------------------------------------

_singleton: PolygonClient | None = None


def polygon_client(api_key: str | None = None) -> PolygonClient:
    """Get or create a singleton PolygonClient."""
    global _singleton
    if _singleton is None:
        _singleton = PolygonClient(api_key=api_key)
    return _singleton
