"""`_FEATURE_WARMUP_ROWS` must cover the factor-momentum second pass too.

alpha-engine-config-I7539. `factor_momentum_ratio` was 0.0 for the whole
universe. `nousergon-data#1410` correctly made the second pass run on the daily
path, and it still produced nothing:

    Factor-momentum second pass (daily): 0/901 tickers got a non-NaN
    factor_momentum_ratio (window/skip warmup — rest fall back to 0.0)

The pass was never the problem. The WINDOW it was handed was.
`_FEATURE_WARMUP_ROWS` was derived from the deepest per-ticker STACKED window
(beta_60d 60 + resid window 231 + shift 21 = 312, plus buffer = 340). The
factor-momentum pass is a cross-sectional time series built ON TOP of the
per-ticker output, so its warmup COMPOSES with theirs rather than sitting
inside it:

  * `dist_from_52w_high` — the deepest `DEFAULT_FACTOR_LOADINGS` member — is
    NaN for its first `weeks_52_days` (252) rows, and `compute_features`
    deliberately does not dropna (removed 2026-04-21). So
    `compute_daily_factor_returns`' `min_names=20` gate drops every one of
    those dates for that factor.
  * `compute_factor_momentum_series` then needs
    `rolling(window - skip, min_periods=window - skip).sum().shift(skip)`
    = 231 + 21 = 252 dates of factor returns, all of which must survive the
    gate above.

252 + 252 = 504. At 340 rows only 88 dates carried a usable loading.

These tests derive the floor from the live constants, so the reasoning cannot
rot silently if any of them moves.
"""

from __future__ import annotations

import pytest

from features.compute import _FEATURE_WARMUP_ROWS
from features.factor_momentum import DEFAULT_FACTOR_LOADINGS
from features.feature_engineer import FEATURE_CFG, MIN_ROWS_FOR_FEATURES

# compute_factor_momentum_series' defaults.
_FM_WINDOW = 252
_FM_SKIP = 21


def _deepest_loading_warmup() -> int:
    """Rows the deepest DEFAULT_FACTOR_LOADINGS member needs before it is
    non-NaN. Keyed off FEATURE_CFG so a config change moves the floor with it."""
    per_loading = {
        "momentum_20d": 20,
        "return_60d": 60,
        "beta_60d": FEATURE_CFG.get("beta_window", 60),
        "idio_vol_60d": FEATURE_CFG.get("beta_window", 60),
        "realized_vol_63d": 63,
        "dist_from_52w_high": FEATURE_CFG["weeks_52_days"],
    }
    known = {k: v for k, v in per_loading.items() if k in DEFAULT_FACTOR_LOADINGS}
    assert known, "DEFAULT_FACTOR_LOADINGS changed — update the warmup map above"
    return max(known.values())


def _factor_momentum_floor() -> int:
    cum = max(_FM_WINDOW - _FM_SKIP, 1)
    return _deepest_loading_warmup() + cum + _FM_SKIP


def test_every_default_loading_has_a_declared_warmup():
    """Guard-the-guard: a new loading with no entry would silently lower the
    computed floor and make every assertion below vacuous."""
    per_loading = {
        "momentum_20d", "return_60d", "beta_60d", "idio_vol_60d",
        "realized_vol_63d", "dist_from_52w_high",
    }
    unknown = set(DEFAULT_FACTOR_LOADINGS) - per_loading
    assert not unknown, (
        f"DEFAULT_FACTOR_LOADINGS gained {sorted(unknown)} with no declared "
        "warmup — add it to _deepest_loading_warmup() and re-derive the floor."
    )


def test_warmup_covers_the_factor_momentum_second_pass():
    floor = _factor_momentum_floor()
    assert _FEATURE_WARMUP_ROWS >= floor, (
        f"_FEATURE_WARMUP_ROWS={_FEATURE_WARMUP_ROWS} is below the "
        f"factor-momentum floor of {floor} "
        f"(deepest loading warmup {_deepest_loading_warmup()} + rolling "
        f"{_FM_WINDOW - _FM_SKIP} + shift {_FM_SKIP}). factor_momentum_ratio "
        "will be NaN for the whole universe and fall back to 0.0 — the exact "
        "constant column alpha-engine-config-I7539 was filed about."
    )


def test_the_old_value_would_now_fail():
    """Pins that this test would have caught the defect, rather than being
    written to agree with whatever the constant happens to be."""
    assert 340 < _factor_momentum_floor()


def test_warmup_still_covers_the_per_ticker_stacked_window():
    """The original constraint is not traded away for the new one."""
    assert _FEATURE_WARMUP_ROWS >= MIN_ROWS_FOR_FEATURES
    # beta warmup + residual-momentum window + skip, the stack 340 was sized on.
    stacked = FEATURE_CFG.get("beta_window", 60) + FEATURE_CFG["resid_mom_window"] - 21 + 21
    assert _FEATURE_WARMUP_ROWS >= stacked


@pytest.mark.parametrize("factor", ["dist_from_52w_high"])
def test_the_binding_loading_is_still_in_the_default_set(factor):
    """If the deepest loading is ever dropped from the set the floor falls a
    long way, and someone should lower the constant deliberately rather than
    leave 585 unexplained."""
    assert factor in DEFAULT_FACTOR_LOADINGS
