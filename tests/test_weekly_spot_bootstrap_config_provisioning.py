"""The weekly spot bootstrap must provision gitignored configs the stages need.

Bug class this guards (rehearsal run 4, 2026-07-27): the launcher box cloned
every repo it needed and still could not run the backtest family::

    ERROR: config.yaml not found — copy from config.yaml.example
    ssm_log_capture: ERROR: [backtester] failed (rc=1)

``crucible-backtester``'s ``config.yaml`` is gitignored (the ``.example``
pattern), so cloning the repo does not produce it. The long-lived dashboard box
had carried a hand-made symlink since 2026-04-11::

    alpha-engine-backtester/config.yaml -> alpha-engine-config/backtester/config.yaml

Nobody wrote that down, so a box built from scratch lacked it.

This is the same shape as the missing predictor clone
(``test_weekly_spot_bootstrap_repo_coverage.py``) one layer down: **cloning the
right repos is necessary but not sufficient — the stages also need files that
deliberately are not in those repos.** Both are only observable on a fresh box,
which is why they survived review and why the invariants belong in CI.

The symlink target must be the TRACKED copy in ``alpha-engine-config``, not a
loose file: ``spot_backtest.sh``'s own pre-launch check warns when the target
is not inside a git repo, because operator flags without an audit trail vanish
on rebuild.
"""

from __future__ import annotations

import re
from pathlib import Path

_DISPATCHER = (
    Path(__file__).resolve().parents[1]
    / "infrastructure"
    / "lambdas"
    / "weekly-freshness-spot-dispatcher"
    / "index.py"
)
_SRC = _DISPATCHER.read_text(encoding="utf-8")

# Gitignored files a stage hard-requires, mapped to the tracked path the
# bootstrap must link them to. Add a row whenever a launcher grows a
# `if [ ! -f <config> ]; then exit` style precondition.
_REQUIRED_LINKS = {
    "/home/ec2-user/alpha-engine-backtester/config.yaml": (
        "/home/ec2-user/alpha-engine-config/backtester/config.yaml"
    ),
}


def test_bootstrap_provisions_every_required_config():
    for link, target in _REQUIRED_LINKS.items():
        assert target in _SRC and link in _SRC, (
            f"bootstrap never provisions {link}. It is gitignored, so cloning "
            f"the repo does not create it, and the stage that needs it hard-exits "
            f"on a freshly-launched box. Link it to {target}."
        )


def test_config_links_point_at_a_tracked_repo():
    """Targets must live inside a cloned git repo, not a loose path.

    spot_backtest.sh warns when config.yaml resolves outside a git repo:
    operator flags with no audit trail vanish on rebuild.
    """
    for link, target in _REQUIRED_LINKS.items():
        assert "/alpha-engine-config/" in target, (
            f"{link} -> {target} does not resolve into the version-controlled "
            f"alpha-engine-config checkout"
        )


def test_symlink_ownership_is_set():
    """`ln -s` as root leaves a root-owned link; stages run as ec2-user."""
    assert re.search(r"chown -h ec2-user:ec2-user", _SRC), (
        "bootstrap creates config symlinks but never `chown -h` them to "
        "ec2-user — the bootstrap runs as root and every stage runs as ec2-user"
    )
