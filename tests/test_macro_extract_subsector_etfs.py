"""alpha-engine-config-I9289 — the 8 sub-sector benchmark ETFs must reach
``_extract_macro_series``'s output so the weekly raw-series write loop in
``builders.backfill.backfill()`` actually persists their full price-cache
history to ArcticDB.

Measured 2026-08-29: every one of SMH/IGV/XBI/PPH/XOP/KRE/ITA/GDX held exactly
26 rows in ``macro`` — one row per weekday since their 2026-07-23 birth —
because ``_extract_macro_series`` only recognised the fixed ``macro_keys`` set
plus ``XL*`` sector ETFs. ``builders/daily_append.py``'s per-day incremental
append is the ONLY writer that ever touched them.
"""

from __future__ import annotations

import pandas as pd

import builders.backfill as _bf
from collectors.prices import _SUB_SECTOR_ETFS


def _df(n: int) -> pd.DataFrame:
    idx = pd.bdate_range("2016-08-19", periods=n)
    return pd.DataFrame({"Close": range(n)}, index=idx)


def test_extract_macro_series_includes_all_sub_sector_etfs():
    price_data = {sym: _df(2514) for sym in _SUB_SECTOR_ETFS}
    price_data["SPY"] = _df(2514)

    macro = _bf._extract_macro_series(price_data)

    for sym in _SUB_SECTOR_ETFS:
        assert sym in macro, f"{sym} missing from _extract_macro_series output"
        assert len(macro[sym]) == 2514


def test_extract_macro_series_skips_a_sub_sector_etf_absent_from_price_data():
    """A sub-sector ETF genuinely missing from the price cache this run must
    not KeyError — it is absent, not corrupt."""
    price_data = {"SPY": _df(2514)}
    macro = _bf._extract_macro_series(price_data)
    for sym in _SUB_SECTOR_ETFS:
        assert sym not in macro
