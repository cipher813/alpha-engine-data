"""install-metron-intraday.sh must prove the venv works BEFORE enabling the timer.

WHY
---
The installer copied units and ran `systemctl enable --now` without checking
that the interpreter named in the unit's ExecStart could import what the
collector needs.

On 2026-07-29 that shipped a timer firing every five minutes with zero
successful runs. The box's venv was built for daily-news -- boto3, feedparser,
anthropic -- and had neither pandas nor yfinance. Every `_yfinance_*` helper in
`collectors/metron_market_data.py` carries an `except ImportError: return {}`
branch marked `# pragma: no cover - yfinance/pandas are prod deps`, so the run
did not crash: it logged a warning, produced nothing, and wrote an empty
artifact over the previous good one while reporting `status: ok`.

Enabling a unit is a claim that it works. The claim now has to be tested at
install time, where a human is watching, rather than discovered at run time in
a background job.

The interpreter is the real constraint and the message must say so:
requirements.txt pins `numpy>=2.4.6`, which needs Python 3.11+, so the box's
3.9 venv cannot be repaired by installing older wheels -- it has to be rebuilt.
An error message that says "install pandas" would send the next reader down a
path that cannot work.

Source-text assertions: the script runs as root on an EC2 box via SSM, so
executing it in CI is not meaningful. What these pin is that the preflight
exists, covers the deps whose absence is silent, and runs before the enable.
"""

import re
from pathlib import Path

SCRIPT = (
    Path(__file__).parent.parent / "infrastructure" / "install-metron-intraday.sh"
).read_text()


def test_preflight_checks_the_silently_degrading_deps():
    """boto3 crashes loudly if missing; pandas and yfinance do not."""
    for module in ("pandas", "yfinance"):
        assert module in SCRIPT, (
            f"the preflight does not check {module}. Its absence does not raise -- "
            "the collector returns empty results and reports success."
        )


def test_preflight_runs_before_the_timer_is_enabled():
    """Order is the whole point: a passing check after `enable --now` proves nothing."""
    enable = SCRIPT.index("systemctl enable --now metron-intraday.timer")
    check = SCRIPT.index("find_spec")
    assert check < enable, (
        "the dependency check runs AFTER the timer is enabled, so a broken venv "
        "still gets a live timer"
    )


def test_preflight_failure_aborts_rather_than_warning():
    """A warning here is the same defect one layer up: a claim without a test."""
    m = re.search(r"cannot import the collector's required modules.*?exit 1", SCRIPT, re.S)
    assert m, "a failed preflight must exit non-zero, not print and continue"


def test_missing_interpreter_is_its_own_named_failure():
    assert re.search(r'if \[ ! -x "\$PY" \]', SCRIPT), (
        "an absent interpreter must be reported distinctly -- it is a different "
        "remedy from an under-provisioned one"
    )


def test_the_message_names_the_interpreter_constraint():
    """Otherwise the next reader tries to pip-install into the 3.9 venv and fails."""
    assert "numpy>=2.4.6" in SCRIPT and "3.11+" in SCRIPT, (
        "the remedy text must say the venv needs rebuilding on a newer Python, "
        "not that packages need installing"
    )
