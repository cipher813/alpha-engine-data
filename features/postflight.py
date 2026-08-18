"""Postflight zero-variance guard for the daily feature-store snapshot.

alpha-engine-config-I7539: ``residual_momentum_ratio`` and
``factor_momentum_ratio`` were both 901/901 non-null, std 0.0, on
``s3://alpha-engine-research/features/2026-08-14/technical.parquet`` — every
ticker got an identical constant 0.0. A null-coverage check passes at 100%
on a column like that; it provably cannot catch this failure class, because
the column IS fully populated, just with no information in it.

This module is the guard that catches it: a cross-sectional feature column
whose non-null values are all identical, over the whole universe, is a
producer defect (never computed / silently defaulted / a second pass that
never ran), not a legitimate shape.
"""

from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)

# Two FEATURES columns encode a genuinely binary (0/1) EVENT flag rather than
# a continuous per-ticker magnitude. An all-0 day (e.g. not one ticker in the
# whole universe had a MACD zero-line cross) is a plausible, non-defective
# outcome for a sparse event flag — exempt them by name rather than by a
# generic "few distinct values" heuristic, which would also hide a genuine
# defect that happens to collapse onto two values.
ZERO_VARIANCE_EXEMPT: frozenset[str] = frozenset({"macd_cross", "macd_above_zero"})

# min_non_null mirrors the ``min_names`` floor features/factor_momentum.py
# already uses before it will call a cross-section "populated enough to
# rank" (20). Below that, a handful of sparse alt-data rows (options/
# earnings-revision fields, only populated for tickers with an event that
# day) coincidentally tying at a shared value is a plausible non-defective
# outcome, not the failure class this guard targets.
DEFAULT_MIN_NON_NULL = 20


def find_zero_variance_columns(
    features_df: pd.DataFrame,
    feature_names: list[str],
    *,
    exempt: frozenset[str] = ZERO_VARIANCE_EXEMPT,
    min_non_null: int = DEFAULT_MIN_NON_NULL,
) -> dict[str, int]:
    """Return ``{column: non_null_count}`` for every column in
    ``feature_names`` whose cross-sectional std (over its non-null values,
    population std) is exactly 0.0.

    Callers are expected to have already excluded macro-broadcast columns
    (features.registry.GROUPS["macro"]) — those carry ONE value for the
    whole universe on a given date BY CONSTRUCTION (e.g. the VIX level),
    so zero cross-sectional variance there is the correct, expected shape,
    not a defect.
    """
    offending: dict[str, int] = {}
    for col in feature_names:
        if col in exempt or col not in features_df.columns:
            continue
        s = pd.to_numeric(features_df[col], errors="coerce").dropna()
        if len(s) < min_non_null:
            continue
        if s.std(ddof=0) == 0.0:
            offending[col] = int(len(s))
    return offending


def find_all_null_columns(
    features_df: pd.DataFrame,
    feature_names: list[str],
) -> list[str]:
    """Registered columns present in the frame with ZERO non-null values.

    The companion to :func:`find_zero_variance_columns`, and the reason it
    needs one: that function skips any column with fewer than
    ``min_non_null`` values, deliberately, so a genuinely sparse alt-data
    field is not reported as a constant. A column the producer never filled
    has zero values and falls straight through that floor.

    The hole opens the moment a producer stops back-filling an uncomputed
    column with a fabricated default: it goes from LOUDLY constant to
    SILENTLY absent (alpha-engine-config-I7539). A missing VALUE is the
    honest state; a column that is missing EVERY value is a dead producer,
    and both must be visible.

    Sparse-but-present is NOT reported here — that is a coverage question
    with its own surfaces. A column absent from the frame entirely is not
    reported either: that is a schema failure, and naming it here would send
    the reader to the wrong producer.
    """
    return [
        col for col in feature_names
        if col in features_df.columns
        and int(pd.to_numeric(features_df[col], errors="coerce").notna().sum()) == 0
    ]


def assert_no_dead_feature_columns(
    features_df: pd.DataFrame,
    feature_names: list[str],
    *,
    exempt: frozenset[str] = ZERO_VARIANCE_EXEMPT,
    min_non_null: int = DEFAULT_MIN_NON_NULL,
) -> None:
    """Raise loud (``RuntimeError``) if any registered column is dead in
    EITHER sense: a cross-sectional constant, or entirely empty.

    One entry point for the two halves of one question — "did this column's
    producer actually produce anything?" — so a caller cannot wire up the
    half that existed first and leave the other unchecked.
    """
    constant = find_zero_variance_columns(
        features_df, feature_names, exempt=exempt, min_non_null=min_non_null,
    )
    empty = [c for c in find_all_null_columns(features_df, feature_names)
             if c not in exempt]
    if not constant and not empty:
        return
    parts = []
    if constant:
        parts.append(
            "cross-sectional CONSTANT (every non-null value identical, so the "
            f"column passes every null-coverage check and carries zero signal): {constant}"
        )
    if empty:
        parts.append(
            "entirely EMPTY (zero non-null values over the whole universe — a "
            f"producer that ran and wrote nothing): {sorted(empty)}"
        )
    raise RuntimeError(
        "Dead feature column(s) detected (alpha-engine-config-I7539). "
        + " | ".join(parts)
    )


def assert_no_zero_variance_features(
    features_df: pd.DataFrame,
    feature_names: list[str],
    *,
    exempt: frozenset[str] = ZERO_VARIANCE_EXEMPT,
    min_non_null: int = DEFAULT_MIN_NON_NULL,
) -> None:
    """Raise loud (``RuntimeError``) if any column in ``feature_names`` is a
    cross-sectional constant. Intended as the LAST step before a feature
    snapshot is written — after per-ticker compute, the factor-momentum
    second pass, and the private-pack extension point have all had their
    chance to fill a column, a remaining zero-variance column is a producer
    defect, not a transient gap.
    """
    offending = find_zero_variance_columns(
        features_df, feature_names, exempt=exempt, min_non_null=min_non_null,
    )
    if offending:
        raise RuntimeError(
            "Zero-variance feature column(s) detected: every non-null value "
            "is identical across the whole universe. This passes every "
            "null-coverage check and carries zero signal "
            "(alpha-engine-config-I7539). Offending columns "
            f"(column: non_null_count): {offending}"
        )
