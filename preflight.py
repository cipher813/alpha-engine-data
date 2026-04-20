"""
Data-module preflight: connectivity + freshness checks run at the top of
``weekly_collector.main()`` before any real collection work starts.

Primitives live in ``alpha_engine_lib.preflight.BasePreflight``; this
module only composes them into a mode-specific sequence. See the
alpha-engine-lib README for the rationale and the 2026-04-14 failure
mode that motivated the library.
"""

from __future__ import annotations

import logging
import os

from alpha_engine_lib.preflight import BasePreflight

log = logging.getLogger(__name__)

# FMP /stable probe: cheapest auth-gated call that distinguishes
# (a) valid key on /stable from (b) a key that still works on the
# sunsetted v3 endpoints but would silently 402/403 across our real
# collector calls. AAPL is guaranteed to exist and returns a small
# payload. Added 2026-04-20 after the v3→/stable migration; the
# collectors had been silently zeroing fundamentals for two weeks
# before detection.
_FMP_STABLE_PROBE_URL = "https://financialmodelingprep.com/stable/key-metrics-ttm"
_FMP_STABLE_PROBE_SYMBOL = "AAPL"
_HTTP_TIMEOUT_SECS = 10.0


class DataPreflight(BasePreflight):
    """Preflight checks for the alpha-engine-data entrypoint.

    Mode determines which external services must be reachable:

    - ``"daily"`` — weekday DailyData step. ArcticDB must be readable
      and SPY must be ≤4 days stale (covers Fri→Tue long weekends +
      1 day of buffer).
    - ``"phase1"`` — Saturday DataPhase1. External APIs (FRED, polygon)
      needed; no ArcticDB freshness check (phase1 is what *populates*
      ArcticDB).
    - ``"phase2"`` — Saturday DataPhase2. FMP /stable + Finnhub + SEC
      EDGAR needed.
    """

    def __init__(self, bucket: str, mode: str):
        super().__init__(bucket)
        if mode not in ("daily", "phase1", "phase2"):
            raise ValueError(f"DataPreflight: unknown mode {mode!r}")
        self.mode = mode

    def run(self) -> None:
        self.check_env_vars("AWS_REGION")
        if self.mode == "phase1":
            self.check_env_vars("FRED_API_KEY", "POLYGON_API_KEY")
        elif self.mode == "phase2":
            self.check_env_vars("FMP_API_KEY", "FINNHUB_API_KEY", "EDGAR_IDENTITY")
            self._check_fmp_stable_reachable()

        self.check_s3_bucket()

        if self.mode == "daily":
            # SPY lives in the `macro` library (market-wide series). The
            # `universe` library holds per-stock OHLCV for S&P 500/400
            # constituents. daily_append writes to both libraries, so
            # macro/SPY freshness is a sufficient signal for the write
            # path being healthy end-to-end.
            # 4-day threshold covers Fri→Tue long weekends + 1 day of buffer.
            self.check_arcticdb_fresh("macro", "SPY", max_stale_days=4)

    # ── Mode-specific primitives ─────────────────────────────────────────

    def _check_fmp_stable_reachable(self) -> None:
        """Validate FMP /stable auth + endpoint availability.

        Guards against the exact failure mode from the 2026-04 incident:
        the v3 endpoints silently 403'd (or paid-tier endpoints 402'd),
        the per-ticker exceptions logged at debug level, the collector
        returned all-NEUTRAL, and two weeks of fundamentals were zeroed
        before anyone noticed. A /stable probe at startup fails the
        Step Function in ~1s instead.
        """
        import requests

        api_key = os.environ.get("FMP_API_KEY", "").strip()
        try:
            resp = requests.get(
                _FMP_STABLE_PROBE_URL,
                params={"symbol": _FMP_STABLE_PROBE_SYMBOL, "apikey": api_key},
                timeout=_HTTP_TIMEOUT_SECS,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Pre-flight: FMP /stable unreachable: {exc} — network outage or egress blocked."
            ) from exc

        if resp.status_code in (401, 403):
            raise RuntimeError(
                f"Pre-flight: FMP /stable auth failed (HTTP {resp.status_code}): "
                f"FMP_API_KEY invalid, revoked, or still pointing at the sunsetted v3 plan."
            )
        if resp.status_code == 402:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned HTTP 402 Payment Required on "
                f"key-metrics-ttm — the free tier no longer covers this endpoint. "
                f"Subscribe or move the collector to a different provider."
            )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned HTTP {resp.status_code} — upstream outage."
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned unexpected HTTP {resp.status_code} "
                f"on {_FMP_STABLE_PROBE_URL}: {resp.text[:200]}"
            )
        payload = resp.json()
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned 200 but body was empty/malformed "
                f"for {_FMP_STABLE_PROBE_SYMBOL}: {str(payload)[:200]}"
            )
        log.info("preflight: FMP /stable reachable + auth valid (HTTP 200)")
