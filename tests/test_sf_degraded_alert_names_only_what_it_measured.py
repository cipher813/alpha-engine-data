"""alpha-engine-config-I7301 — a fail-open alert may not assert a cause it did
not measure.

The 2026-08-13 finding. `PublishLibPinGateDegraded`'s Message read:

    "LibPinDriftCheck could not verify cross-repo pin parity (gate Lambda
     failed after retries, or returned a malformed payload)."

Both halves were false on every firing from 2026-07-31 onward. The gate Lambda
returned HTTP 200 in ~3s with a well-formed payload; it deliberately omitted
`has_drift` because `crucible-predictor` pinned nousergon-lib by commit SHA and
the parity comparison could not be made. The alert's parenthetical was not a
measurement, it was a guess baked into a constant — and because it named the
Lambda, it sent readers to investigate Lambda health while the real defect
(backtester v0.124.5 against a predictor SHA sitting ~v0.124.16 — a live
co-install parity break) sat unexamined for 13 days.

config#1819 requires these Subject/Message values to be hardcoded constants,
never `States.Format` against input. That constraint is correct and is not
relaxed here. It does mean a constant CANNOT name the run's actual cause — so
the obligation is the opposite one: a constant must not PRETEND to. It states
the class, and points at the execution-record field where the measured cause
lives.

This is the same bug class as I7048/I7277/I7171 one layer out: those were
payloads that could not distinguish "could not measure" from "measured
nothing"; this is an alert that could not distinguish them either, and
resolved the ambiguity by inventing the more alarming reading.
"""
from __future__ import annotations

import json
import pathlib
import re

import pytest

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def states() -> dict:
    return json.loads(_WEEKLY.read_text())["States"]


def test_libpin_degraded_alert_does_not_assert_the_lambda_failed(states):
    msg = states["PublishLibPinGateDegraded"]["Parameters"]["Message"]
    # The exact fabrication removed by I7301. Named as a literal so a future
    # edit that reintroduces the guess fails here rather than in an operator's
    # inbox 13 days later.
    assert "gate Lambda failed after retries" not in msg
    assert "returned a malformed payload" not in msg


def test_libpin_degraded_alert_points_at_the_field_carrying_the_real_cause(states):
    msg = states["PublishLibPinGateDegraded"]["Parameters"]["Message"]
    # A constant cannot carry the run's cause (config#1819). It must therefore
    # tell the reader where the cause IS, or the reader guesses — which is the
    # failure this test exists to prevent, one step downstream.
    assert "$.libpin_drift_result.Payload.reason" in msg
    assert "$.libpin_drift_error" in msg


def test_libpin_degraded_alert_says_it_is_not_a_reported_defect(states):
    # The operative distinction: a real drift, and an unverifiable pin on the
    # co-install pair, both HALT via HandleFailure. Anything arriving at this
    # alert is a could-not-measure. Saying so is what stops the alert being
    # read as a finding.
    msg = states["PublishLibPinGateDegraded"]["Parameters"]["Message"]
    assert "NOT A REPORTED DEFECT" in msg.upper()


# The four pre-spend gate degraded alerts share one shape. None of them may
# assert a cause; the lib-pin one is simply the instance that was caught.
_DEGRADED_PUBLISHERS = [
    "PublishLibPinGateDegraded",
    "PublishPipelineContractGateDegraded",
    "PublishEvaluatorGateDegraded",
    "PublishEvaluatorDirectorGateDegraded",
]

# Phrasings that assert a specific mechanism as THE cause of this firing.
# "or" does not launder a guess: enumerating two mechanisms still excludes
# every other one, which is how the lib-pin text managed to be wrong about
# both halves at once.
_ASSERTS_A_CAUSE = re.compile(
    r"\((?:[^)]*\b(?:Lambda failed|failed after retries|malformed payload)\b[^)]*)\)",
    re.IGNORECASE,
)


@pytest.mark.parametrize("publisher", _DEGRADED_PUBLISHERS)
def test_no_prespend_degraded_alert_asserts_a_mechanism_as_the_cause(states, publisher):
    msg = states[publisher]["Parameters"]["Message"]
    match = _ASSERTS_A_CAUSE.search(msg)
    assert match is None, (
        f"{publisher}'s Message asserts a cause it cannot have measured: "
        f"{match.group(0)!r}. This Message is a hardcoded constant "
        f"(config#1819) fired on every arrival at the degraded chain, so any "
        f"cause named in it is a guess. State the class and point at the "
        f"execution-record field instead (alpha-engine-config-I7301)."
    )
