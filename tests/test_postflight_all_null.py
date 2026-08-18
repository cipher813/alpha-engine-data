"""An entirely-empty registered column is a producer defect too
(alpha-engine-config-I7539, second half).

`find_zero_variance_columns` skips any column with fewer than
`DEFAULT_MIN_NON_NULL` (20) non-null values — deliberately, so a genuinely
sparse alt-data field is not reported as a constant. That skip leaves a hole
exactly where this issue lives: a column the producer never filled has ZERO
non-null values and is therefore invisible to the guard.

That hole is not hypothetical, and it opens the moment
`factor_momentum_ratio` stops being back-filled with a fabricated 0.0
(same PR): the column goes from LOUDLY constant to SILENTLY absent, which is
the detection blindness this fleet treats as outranking the defect it hides.

Measured on `features/2026-08-18/technical.parquet` (901 rows, 49 columns)
before the change: zero all-null columns and zero columns with fewer than 20
non-null values — so this guard has no false positive to answer for on the
live shape it will run against.

RED on 6f9b0b4d: `find_all_null_columns` does not exist.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from features.postflight import (
    assert_no_dead_feature_columns,
    find_all_null_columns,
    find_zero_variance_columns,
)


def _df(n=901, **cols):
    return pd.DataFrame({"ticker": [f"T{i}" for i in range(n)], **cols})


class TestFindAllNullColumns:
    def test_flags_a_column_the_producer_never_filled(self):
        df = _df(factor_momentum_ratio=[np.nan] * 901)
        assert find_all_null_columns(df, ["factor_momentum_ratio"]) == ["factor_momentum_ratio"]

    def test_does_not_flag_a_sparse_but_present_column(self):
        """Sparse is a coverage question, not a producer defect — the same
        reason `find_zero_variance_columns` has a `min_non_null` floor."""
        vals = [np.nan] * 900 + [1.5]
        assert find_all_null_columns(_df(earnings_revision_ratio=vals),
                                     ["earnings_revision_ratio"]) == []

    def test_does_not_flag_a_populated_column(self):
        rng = np.random.default_rng(0)
        assert find_all_null_columns(_df(mom_12_1_pct=rng.normal(size=901)),
                                     ["mom_12_1_pct"]) == []

    def test_a_column_absent_from_the_frame_is_not_reported_here(self):
        """A missing column is a schema failure with its own contract test;
        reporting it as 'all null' would send the reader to the wrong producer."""
        assert find_all_null_columns(_df(), ["never_emitted"]) == []

    def test_the_all_null_case_is_invisible_to_the_zero_variance_guard(self):
        """The reason this function has to exist, asserted rather than
        described: the older guard's `min_non_null` floor skips it."""
        df = _df(factor_momentum_ratio=[np.nan] * 901)
        assert find_zero_variance_columns(df, ["factor_momentum_ratio"]) == {}
        assert find_all_null_columns(df, ["factor_momentum_ratio"]) != []


class TestAssertNoDeadFeatureColumns:
    def test_raises_on_either_failure_mode(self):
        constant = _df(residual_momentum_ratio=[0.0] * 901)
        empty = _df(factor_momentum_ratio=[np.nan] * 901)
        for df, col in ((constant, "residual_momentum_ratio"),
                        (empty, "factor_momentum_ratio")):
            with pytest.raises(RuntimeError, match=col):
                assert_no_dead_feature_columns(df, [col])

    def test_silent_on_a_healthy_frame(self):
        rng = np.random.default_rng(1)
        assert_no_dead_feature_columns(_df(mom_12_1_pct=rng.normal(size=901)),
                                       ["mom_12_1_pct"])

    def test_the_message_names_both_kinds_when_both_are_present(self):
        rng = np.random.default_rng(2)
        df = _df(mom_12_1_pct=rng.normal(size=901),
                 residual_momentum_ratio=[0.0] * 901,
                 factor_momentum_ratio=[np.nan] * 901)
        with pytest.raises(RuntimeError) as exc:
            assert_no_dead_feature_columns(
                df, ["mom_12_1_pct", "residual_momentum_ratio", "factor_momentum_ratio"])
        msg = str(exc.value)
        assert "residual_momentum_ratio" in msg and "factor_momentum_ratio" in msg
        assert "mom_12_1_pct" not in msg
