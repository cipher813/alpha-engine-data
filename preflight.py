"""
Data-module preflight: connectivity + freshness checks run at the top of
``weekly_collector.main()`` before any real collection work starts.

Primitives live in ``alpha_engine_lib.preflight.BasePreflight``; this
module only composes them into a mode-specific sequence. See the
alpha-engine-lib README for the rationale and the 2026-04-14 failure
mode that motivated the library.
"""

from __future__ import annotations

from alpha_engine_lib.preflight import BasePreflight


class DataPreflight(BasePreflight):
    """Preflight checks for the alpha-engine-data entrypoint.

    Mode determines which external services must be reachable:

    - ``"daily"`` — weekday DailyData step. ArcticDB must be readable
      and SPY must be ≤4 days stale (covers Fri→Tue long weekends +
      1 day of buffer).
    - ``"phase1"`` — Saturday DataPhase1. External APIs (FRED, polygon)
      needed; no ArcticDB freshness check (phase1 is what *populates*
      ArcticDB).
    - ``"phase2"`` — Saturday DataPhase2. FMP + SEC EDGAR needed.
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
            self.check_env_vars("FMP_API_KEY", "EDGAR_IDENTITY")

        self.check_s3_bucket()

        if self.mode == "daily":
            # 4-day threshold would have caught the 2026-04-14 bug
            # (ArcticDB silently not writing) by 2026-04-17.
            self.check_arcticdb_fresh("universe", "SPY", max_stale_days=4)
