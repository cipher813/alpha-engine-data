"""migrations/0002_add_mom_12_1_loading.py — the Barra MOMENTUM long-horizon
(RSTR) factor loading, ``mom_12_1_pct_zscore``.

The factor-loading set was documented as the canonical institutional
Barra-style matrix while its MOMENTUM family held only ``momentum_20d_zscore``
(short) and ``return_60d_zscore`` (medium). Barra's MOMENTUM factor is
canonically 12-1: trailing 12-month return excluding the most recent month.
The set was missing the horizon it was named for, and both members it did
carry sit inside the short-term-reversal window rather than the one the
Jegadeesh-Titman premium is defined over.

Consumer: the ``mom_12_1_sleeve`` scanner challenger arm in crucible-research
(alpha-engine-config-I7544), an observe-only shadow arm.

``backfill_policy: NAN`` — deliberately NOT recomputed, and this is the
reviewed decision rather than the lazy one.

The raw input ``mom_12_1_pct`` IS persisted across full history, so a naive
reading says this column is retro-computable. It is not, correctly.

A factor LOADING is a CROSS-SECTIONAL z-score: the value for ticker i at date
t is defined against the universe as it stood at date t. The migration
primitive available here (``rewrite_symbols_full``'s ``new_columns_fn``) is
per-symbol — it sees one ticker's history, never the panel — so any recompute
would have to build the cross-section from the symbols present in the library
TODAY. That is a survivorship-biased universe: names delisted or dropped
before today would be absent from every historical cross-section they were
actually part of, shifting every z-score in that cross-section by an amount
nobody could later detect or bound.

A silently-wrong historical loading is worse than an absent one. Any
backtest reading it would get plausible numbers computed against a universe
that never existed — the "some columns quietly wrong" class this whole
framework exists to prevent (config-I3236). NaN is honest: it says "this
loading did not exist before the cutover", which is true.

Going forward the column is populated by the daily cross-sectional pass
(``features/cross_sectional.py::apply_factor_zscores`` via
``features/compute.py``), which sees the real live cross-section. The
consumer only ever reads the latest cross-section, so the NaN history costs
it nothing.

If a historical loading series is ever genuinely needed, it must be rebuilt
from a point-in-time universe-membership record, not from today's symbol
list — that is a separate piece of work with a different input.
"""

from __future__ import annotations

import numpy as np

from migrations._base import (
    Migration,
    rewrite_symbols_full,
    verify_additive,
)

# The full frozen canonical column set AFTER this migration: migration 0001's
# 97 columns plus ``mom_12_1_pct_zscore``, inserted immediately after
# ``return_60d_zscore`` so the MOMENTUM family reads short/medium/long in
# horizon order — matching features.feature_engineer.FEATURES.
# Captured literally from store.arctic_store.canonical_universe_columns() on
# this branch (frozen per the chokepoint's anchor design — see _template.py).
COLUMNS_AFTER: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "Volume", "VWAP", "source", "rsi_14",
    "macd_cross", "macd_above_zero", "macd_line_last", "price_vs_ma50",
    "price_vs_ma200", "momentum_20d", "avg_volume_20d",
    "avg_volume_20d_raw", "dist_from_52w_high", "momentum_5d",
    "rel_volume_ratio", "return_vs_spy_5d", "vix_level",
    "dist_from_52w_low", "vol_ratio_10_60", "bollinger_pct",
    "sector_vs_spy_5d", "sector_vs_spy_10d", "sector_vs_spy_20d",
    "sub_sector_vs_benchmark_5d", "sub_sector_vs_benchmark_10d",
    "sub_sector_vs_benchmark_20d", "yield_10y", "yield_curve_slope",
    "gold_mom_5d", "oil_mom_5d", "vix_term_slope", "xsect_dispersion",
    "price_accel", "ema_cross_8_21", "atr_14_pct", "realized_vol_20d",
    "volume_trend", "obv_slope_10d", "rsi_slope_5d", "volume_price_div",
    "mom5d_x_vix", "rsi_x_vix", "sector_x_trend", "atr_x_vix",
    "vol_trend_x_vix", "earnings_surprise_pct", "days_since_earnings",
    "eps_revision_4w", "revision_streak", "put_call_ratio", "iv_rank",
    "iv_vs_rv", "pe_ratio", "pb_ratio", "debt_to_equity",
    "revenue_growth_yoy", "fcf_yield", "gross_margin", "roe",
    "current_ratio", "revenue_growth_3y", "eps_growth_3y", "payout_ratio",
    "dividend_yield", "capex_growth_5y", "market_cap_raw", "return_60d",
    "return_120d", "overnight_return_5d", "intraday_return_5d",
    "dist_from_5d_high", "dist_from_20d_high", "beta_60d", "idio_vol_60d",
    "vol_of_vol_30d", "max_drawdown_60d", "realized_vol_63d",
    "residual_momentum_ratio", "mom_12_1_pct", "sector_mom_pct",
    "factor_momentum_ratio", "momentum_20d_zscore", "return_60d_zscore",
    "mom_12_1_pct_zscore", "beta_60d_zscore", "idio_vol_60d_zscore",
    "realized_vol_63d_zscore", "dist_from_52w_high_zscore",
    "pe_ratio_zscore", "roe_zscore", "size_zscore", "vwap_divergence_pct",
    "cmf_20_ratio", "hy_oas_credit_spread_pct"
)

# Not retro-computable without a point-in-time universe (see module docstring).
# float32 fill, NOT a bare np.nan: feature columns are float32, and a float64
# fill lands the column at the wrong dtype and re-introduces a
# StreamDescriptorMismatch on the next real update_batch (config#2459 trap).
NEW_COLUMNS = {
    "mom_12_1_pct_zscore": np.float32("nan"),
}


def _run(lib, meta_lib) -> None:
    from store.schema_version import write_schema_version

    rewrite_symbols_full(
        lib, expected_columns=COLUMNS_AFTER, new_columns=NEW_COLUMNS
    )
    # Stamp LAST, only after the rewrite completes.
    write_schema_version(
        meta_lib,
        MIGRATION.schema_version_after,
        migration_number=MIGRATION.number,
        columns_after=COLUMNS_AFTER,
    )


def _verify(lib) -> None:
    verify_additive(lib, expected_columns=COLUMNS_AFTER)


MIGRATION = Migration(
    number=2,
    name="add_mom_12_1_loading",
    target_library="universe",
    symbol_scope="all universe symbols",
    schema_version_before=1,
    schema_version_after=2,
    columns_after=COLUMNS_AFTER,
    backfill_policy=(
        "NAN — a factor loading is a cross-sectional z-score defined against "
        "the universe as it stood at each date; the per-symbol migration "
        "primitive can only see today's symbol list, so a recompute would "
        "embed survivorship bias into every historical cross-section. "
        "Populated live from the cutover by apply_factor_zscores."
    ),
    run=_run,
    verify=_verify,
)
