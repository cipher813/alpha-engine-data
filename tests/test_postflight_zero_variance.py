"""alpha-engine-config-I7539: postflight zero-variance guard.

Null-coverage checks pass at 100% for a column that's fully populated with a
single repeated constant (measured: residual_momentum_ratio and
factor_momentum_ratio were both 901/901 non-null, std 0.0, on the 2026-08-14
technical.parquet snapshot). This guard catches that class directly.

Verified RED first: test_raises_on_synthetic_constant_column below fails
(no exception raised) against features/compute.py as it stood before this
PR — there was no zero-variance guard at all, so a constant column sailed
through untouched. It is green now that features/postflight.py exists and
is wired into features/compute.py::compute_and_write.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from features.postflight import (
    ZERO_VARIANCE_EXEMPT,
    assert_no_zero_variance_features,
    find_zero_variance_columns,
)


def _df(n=901, **cols):
    return pd.DataFrame({"ticker": [f"T{i}" for i in range(n)], **cols})


class TestFindZeroVarianceColumns:
    def test_flags_a_universe_wide_constant(self):
        df = _df(residual_momentum_ratio=[0.0] * 901)
        offending = find_zero_variance_columns(df, ["residual_momentum_ratio"])
        assert offending == {"residual_momentum_ratio": 901}

    def test_does_not_flag_a_varying_column(self):
        rng = np.random.default_rng(0)
        df = _df(mom_12_1_pct=rng.normal(size=901))
        assert find_zero_variance_columns(df, ["mom_12_1_pct"]) == {}

    def test_below_min_non_null_is_not_flagged(self):
        # Sparse alt-data column: only a handful of tickers had an event
        # today and they coincidentally tie — not the defect class this
        # guard targets (min_non_null mirrors factor_momentum's min_names).
        vals = [0.0] * 5 + [np.nan] * 896
        df = _df(eps_revision_4w=vals)
        assert find_zero_variance_columns(df, ["eps_revision_4w"]) == {}

    def test_exempt_binary_flag_never_flagged_even_at_full_population(self):
        for col in ZERO_VARIANCE_EXEMPT:
            df = _df(**{col: [0.0] * 901})
            assert find_zero_variance_columns(df, [col]) == {}

    def test_missing_column_is_skipped_not_errored(self):
        df = _df(mom_12_1_pct=[1.0, 2.0, 3.0] * 300 + [1.0])
        assert find_zero_variance_columns(df, ["not_a_real_column"]) == {}

    def test_all_nan_column_is_not_flagged(self):
        df = _df(residual_momentum_ratio=[np.nan] * 901)
        assert find_zero_variance_columns(df, ["residual_momentum_ratio"]) == {}


class TestAssertNoZeroVarianceFeatures:
    def test_raises_on_synthetic_constant_column(self):
        """The guard, verified RED: a synthetic all-zero column across the
        full universe must fail loud."""
        df = _df(factor_momentum_ratio=[0.0] * 901)
        with pytest.raises(RuntimeError, match="Zero-variance"):
            assert_no_zero_variance_features(df, ["factor_momentum_ratio"])

    def test_passes_on_real_variance(self):
        rng = np.random.default_rng(1)
        df = _df(
            residual_momentum_ratio=rng.normal(size=901),
            factor_momentum_ratio=rng.normal(size=901),
        )
        assert_no_zero_variance_features(
            df, ["residual_momentum_ratio", "factor_momentum_ratio"]
        )  # no raise

    def test_reproduces_the_measured_i7539_shape(self):
        """Both columns constant 0.0 at once, mirroring the exact measured
        production shape — both must be named in the raised error."""
        df = _df(
            residual_momentum_ratio=[0.0] * 901,
            factor_momentum_ratio=[0.0] * 901,
        )
        with pytest.raises(RuntimeError) as exc_info:
            assert_no_zero_variance_features(
                df, ["residual_momentum_ratio", "factor_momentum_ratio"]
            )
        msg = str(exc_info.value)
        assert "residual_momentum_ratio" in msg
        assert "factor_momentum_ratio" in msg
