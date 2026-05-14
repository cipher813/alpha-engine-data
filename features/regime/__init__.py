"""
features/regime/ — Quantitative regime substrate (v3).

Produces a structured S3 artifact each Saturday SF run that the macro
economist agent consumes as a strong prior for its regime classification.
The macro agent remains the final authority; this substrate is informational.

Three independent quant components ensemble into the substrate JSON:

- ``hmm``       — 3-state Gaussian HMM (Hamilton 1989). Filter-only at
                  inference (no look-ahead). Smoother reserved for T1
                  retrospective ground-truth labeling with explicit lag.
- ``composite`` — AQR-style risk-on/risk-off macro z-score intensity.
                  Pure rule-based, zero estimation risk.
- ``bocpd``     — Adams & MacKay 2007 Bayesian online change-point
                  detection. Surfaces regime transitions.

The substrate orchestrator (``substrate.py``) assembles all three plus
guardrail flags (mirroring the macro agent's ``_validate_regime`` rules)
and writes the canonical-shape eval_artifacts JSON to S3.

Reference: ``alpha-engine-docs/private/regime-v3-260514.md``.
"""

from features.regime.composite import compute_composite_intensity
from features.regime.hmm import HMMRegimeClassifier, REGIME_STATES
from features.regime.bocpd import BOCPDDetector
from features.regime.substrate import (
    REGIME_FEATURES,
    SCHEMA_VERSION,
    build_regime_substrate,
    write_regime_substrate,
)

__all__ = [
    "compute_composite_intensity",
    "HMMRegimeClassifier",
    "REGIME_STATES",
    "BOCPDDetector",
    "REGIME_FEATURES",
    "SCHEMA_VERSION",
    "build_regime_substrate",
    "write_regime_substrate",
]
