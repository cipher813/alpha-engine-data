"""The deps step must not be the only thing that knows what pip did.

Ported from ``crucible-predictor`` per alpha-engine-config-I6922 — this repo's
``install_deps`` carried the pre-fix form (``pip install --quiet ... | tail -1``)
for as long as the predictor's did, and the two fixes below (config-I6949,
config-I6963) were never hand-carried back here.

The failure they pin (2026-08-11, `ne-weekly-freshness-pipeline` execution
`watch-rerun-2026-08-10-9`, in the predictor's copy of this step)::

    RuntimeError: flow-doctor is not installed but a flow_doctor_yaml was
    provided ... No module named 'flow_doctor'

The install that was supposed to provide it exited **0**. Its entire surviving
output, because the step piped pip through ``tail -1``, was::

    WARNING: Running pip as the 'root' user can result in broken permissions

Nothing in that record distinguishes "resolved the extra" from "skipped it"
from "never saw it", and the import failure lands minutes later in a different
SSM step with no upstream to read. pip reports a dropped extra as a WARNING on
a *successful* exit, so the one line ``tail -1`` keeps is the one line
guaranteed not to carry it.

This repo is squarely exposed: ``requirements.txt`` requests four extras on one
line (``nousergon-lib[arcticdb,flow-doctor,rag,contracts]``), and AL2023 ships
pip 23.2.1 — which predates PEP 685 extras normalisation, so it drops an extra
it does not match rather than failing on it.

The fleet copy of this step is ``krepis.spot_bootstrap.render_install_deps``;
the three are kept in step until this repo consumes the rendered version
(see tests/test_spot_bootstrap_invariants.py for why it cannot yet).
"""

from __future__ import annotations

from pathlib import Path

import pytest

_COMMON = Path(__file__).resolve().parents[1] / "infrastructure" / "_spot_common.sh"


@pytest.fixture(scope="module")
def install_deps_body() -> str:
    text = _COMMON.read_text(encoding="utf-8")
    # Anchor on the definition, not the header comment that also names it.
    start = text.index("\ninstall_deps() {")
    end = text.index("\n}", start)
    return text[start:end]


def test_pip_output_is_not_discarded_by_tail(install_deps_body: str):
    assert "| tail -1" not in install_deps_body


def test_a_failed_install_dumps_the_captured_log(install_deps_body: str):
    # Preserving the log buys nothing if the failure path does not print it —
    # the SSM step output is the only surface anyone reads after the fact.
    assert 'tail -80 "$_pip_log" >&2' in install_deps_body
    assert "exit 1" in install_deps_body


def test_a_dropped_extra_is_surfaced_on_a_successful_exit(install_deps_body: str):
    # The exact shape of the 2026-08-11 failure: rc=0, extra missing.
    assert "does not provide the extra" in install_deps_body


def test_a_dropped_extra_is_fatal_not_merely_reported(install_deps_body: str):
    """Reporting it is not enough — the step still passes and the failure moves.

    The whole point of catching it here is that the log is in hand and the
    cause is one line. A WARNING printed into a step that exits 0 reproduces
    the original defect with better formatting.
    """
    idx = install_deps_body.index("does not provide the extra")
    assert "exit 1" in install_deps_body[idx:], (
        "the dropped-extra grep must be followed by `exit 1` — an incomplete "
        "environment is a broken environment, not a note"
    )


def test_the_environment_is_checked_before_the_import_that_would_fail(
    install_deps_body: str,
):
    assert "pip check" in install_deps_body
