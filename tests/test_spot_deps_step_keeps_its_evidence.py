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

## What changed here (alpha-engine-config-I7372)

``install_deps()`` no longer carries a heredoc: it renders
``krepis.spot_bootstrap.render_install_deps``, the fleet copy these assertions
used to be kept "in step" with by hand. So the assertions moved with it — they
now extract the argv ``install_deps()`` actually passes, render it, and assert
against the RENDERED script. Restating the expected text here would be a
fourth copy of the invariant, drifting exactly as the heredocs did.

The reason this was still a heredoc after #1388 cut the BOOTSTRAP over is the
part worth remembering: the deps step is where the interpreter decides which
wheels get installed, and it was the copy still carrying

    command -v python3.12 >/dev/null && PY=python3.12 || PY=python3

so a box whose 3.12 install failed resolved ``requirements.txt`` against the
AMI's python3 in silence. The renderer has no fallback at all — the bootstrap
has already installed and asserted the interpreter.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest

from krepis.spot_bootstrap import PYTHON, SpotBootstrapSpec, render_install_deps

_COMMON = Path(__file__).resolve().parents[1] / "infrastructure" / "_spot_common.sh"


@pytest.fixture(scope="module")
def install_deps_body() -> str:
    text = _COMMON.read_text(encoding="utf-8")
    # Anchor on the definition, not the header comment that also names it.
    start = text.index("\ninstall_deps() {")
    end = text.index("\n}", start)
    return text[start:end]


@pytest.fixture(scope="module")
def render_deps_args(install_deps_body: str) -> list[str]:
    """The literal argv ``install_deps()`` passes to ``render-deps``."""
    m = re.search(
        r'"\$LIB_PYTHON"\s+-m\s+krepis\.spot_bootstrap\s+render-deps\s*\\(.*?)\)"',
        install_deps_body,
        re.S,
    )
    assert m, "install_deps() no longer dispatches krepis.spot_bootstrap render-deps"
    return shlex.split(m.group(1).replace("\\\n", " "))


def _flag_value(args: list[str], flag: str) -> str:
    assert flag in args, f"{flag} missing from the krepis.spot_bootstrap render-deps call"
    return args[args.index(flag) + 1]


@pytest.fixture(scope="module")
def rendered(render_deps_args: list[str]) -> str:
    spec = SpotBootstrapSpec(
        repo_url=_flag_value(render_deps_args, "--repo-url"),
        checkout=_flag_value(render_deps_args, "--checkout"),
        region=_flag_value(render_deps_args, "--region"),
    )
    return render_install_deps(spec)


def test_install_deps_no_longer_inlines_the_heredoc(install_deps_body: str):
    """Anti-regression: the cutover deletes this heredoc, never duplicates it."""
    assert "pip install" not in install_deps_body, (
        "install_deps() still inlines the pip commands — it must render "
        "krepis.spot_bootstrap.render_install_deps (alpha-engine-config-I7372)"
    )


def test_the_deps_step_installs_into_the_checkout_the_bootstrap_cloned(
    render_deps_args: list[str],
):
    """The two calls must agree on the checkout, or deps land beside the code.

    Derived from both call sites rather than restated, so moving the checkout
    in one and not the other fails here.
    """
    common = _COMMON.read_text(encoding="utf-8")
    bootstrap = re.search(
        r'"\$LIB_PYTHON"\s+-m\s+krepis\.spot_bootstrap\s+render\s*\\(.*?)\)"',
        common,
        re.S,
    )
    assert bootstrap, "bootstrap_spot() no longer dispatches krepis.spot_bootstrap render"
    bootstrap_args = shlex.split(bootstrap.group(1).replace("\\\n", " "))
    assert _flag_value(render_deps_args, "--checkout") == _flag_value(
        bootstrap_args, "--checkout"
    )


def test_pip_output_is_not_discarded_by_tail(rendered: str):
    assert "| tail -1" not in rendered


def test_a_failed_install_dumps_the_captured_log(rendered: str):
    # Preserving the log buys nothing if the failure path does not print it —
    # the SSM step output is the only surface anyone reads after the fact.
    assert 'tail -80 "$_pip_log" >&2' in rendered
    assert "exit 1" in rendered


def test_a_dropped_extra_is_surfaced_on_a_successful_exit(rendered: str):
    # The exact shape of the 2026-08-11 failure: rc=0, extra missing.
    assert "does not provide the extra" in rendered


def test_a_dropped_extra_is_fatal_not_merely_reported(rendered: str):
    """Reporting it is not enough — the step still passes and the failure moves.

    The whole point of catching it here is that the log is in hand and the
    cause is one line. A WARNING printed into a step that exits 0 reproduces
    the original defect with better formatting.
    """
    idx = rendered.index("does not provide the extra")
    assert "exit 1" in rendered[idx:], (
        "the dropped-extra grep must be followed by `exit 1` — an incomplete "
        "environment is a broken environment, not a note"
    )


def test_the_deps_step_has_no_interpreter_fallback(rendered: str):
    """The defect that survived #1388 in this exact step.

    ``requirements.txt`` is resolved against 3.12; the AMI's python3 resolves
    different wheels and says nothing about it.
    """
    assert f"{PYTHON} -m pip install" in rendered
    assert "PY=python3\n" not in rendered
    assert "|| PY=python3" not in rendered
