"""The launcher box builds a venv for alpha-engine-data's own code (config-I7427).

WeeklySubstrateHealthCheck runs three of its four checks out of
``/home/ec2-user/alpha-engine-data`` — ``validators.constituents_drift_check``,
``validators.phase_marker_sweep``, ``validators.stage_output_sweep``. Until this
venv existed they ran under the DASHBOARD interpreter, whose closure does not
contain alpha-engine-data's dependencies, so the constituents drift check never
once reached its comparison (measured 2026-08-15, weekly-SF execution
``watch-rerun-2026-08-15-2``):

    WARNING [collectors.constituents] Constituents fetch failed
      (`Import openpyxl` failed...); trying local cache...
    ERROR   [__main__] Drift check failed at stage=arctic_list:
      No module named 'arcticdb'

The two closures cannot be merged: the dashboard venv is pinned ``numpy<2``
(every spot workload's pyarrow is compiled against 1.x) and alpha-engine-data
declares ``numpy>=2.4.6``.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DISPATCHER = (
    _REPO_ROOT / "infrastructure" / "lambdas"
    / "weekly-freshness-spot-dispatcher" / "index.py"
)


def _src() -> str:
    return _DISPATCHER.read_text(encoding="utf-8")


def test_bootstrap_creates_the_data_venv():
    src = _src()
    assert "python3.12 -m venv .venv" in src
    assert '"$PYTHON_BIN" -m venv' not in src, (
        "the bootstrap deliberately names python3.12 literally — the "
        "`command -v python3.12 ... || PYTHON_BIN=python3` fallback resolves "
        "a different wheel set and says nothing when it does"
    )
    assert "cd /home/ec2-user/alpha-engine-data" in src, (
        "the bootstrap never enters the alpha-engine-data checkout to build "
        "its venv (config-I7427)"
    )


def test_data_venv_installs_that_repos_own_requirements():
    src = _src()
    assert ".venv/bin/pip install -q -r requirements.txt" in src, (
        "the data venv must install alpha-engine-data's OWN requirements — "
        "installing nothing leaves exactly the ModuleNotFoundError I7427 fixed"
    )


def test_data_venv_carries_krepis():
    """stage_output_sweep and the ssm_log_capture wrapper resolve krepis."""
    src = _src()
    assert re.search(r"\.venv/bin/pip install -q 'krepis>=0\.59\.\d+'", src), (
        "krepis is absent from alpha-engine-data's requirements.txt, so the "
        "data venv must install it explicitly or an ImportError in a validator "
        "reads as a domain finding"
    )


def test_data_venv_is_separate_from_the_dashboard_venv():
    """The numpy pins are incompatible; one venv cannot serve both."""
    src = _src()
    assert "pip install -q 'numpy<2'" in src, (
        "the dashboard venv's numpy<2 pin is what makes a SECOND venv "
        "mandatory rather than a preference"
    )
    data_reqs = (_REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert re.search(r"^numpy>=2", data_reqs, re.M), (
        "alpha-engine-data no longer declares numpy>=2 — re-examine whether "
        "the two venvs must still be separate before merging them"
    )


def test_data_venv_is_chowned_to_ec2_user():
    """Every stage runs as ec2-user via `sudo -u ec2-user`."""
    assert (
        "chown -R ec2-user:ec2-user /home/ec2-user/alpha-engine-data/.venv" in _src()
    )


def test_bootstrap_timeout_accounts_for_two_venv_builds():
    src = _src()
    m = re.search(
        r'WEEKLY_SPOT_BOOTSTRAP_TIMEOUT_SECONDS",\s*"(\d+)"', src
    )
    assert m, "bootstrap timeout default not found"
    assert int(m.group(1)) >= 1800, (
        "two venv builds (the second pulling arcticdb, a large native wheel) "
        "under a ceiling sized for one turns a slow mirror day into an "
        "infrastructure fault (config-I7427)"
    )


def test_data_venv_failures_are_fatal_not_swallowed():
    """Fail loud: a half-built venv must not reach the health check."""
    src = _src()
    for expected in (
        'fail "data venv create failed"',
        'fail "data requirements install failed"',
        'fail "data krepis install failed"',
    ):
        assert expected in src, f"missing hard failure for: {expected}"
