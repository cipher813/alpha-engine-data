"""A systemd unit whose ExecStart never returns must not be Type=oneshot.

The failure this pins (2026-08-10, `ne-weekly-freshness-pipeline` runs
`watch-rerun-2026-08-10-1` and `-2`): ``infrastructure/_spot_common.sh``'s
bootstrap wrote an ``ec2-spot-watchdog.service`` declaring::

    Type=oneshot
    ExecStart=/usr/local/bin/ec2-spot-watchdog.sh   # while true; do ... done
    RemainAfterExit=yes

``systemctl start`` on a ``Type=oneshot`` unit BLOCKS until ExecStart exits,
and for oneshot units ``TimeoutStartSec`` defaults to infinity — so the
bootstrap step hung until SSM SIGKILLed it at its budget: rc=137 at exactly
``PT5M0.1S``, stderr ending on the ``systemctl enable`` symlink line with
nothing after it. Latent from #1122 until #1269 repointed the weekly SF onto
the per-stage spot scripts (2026-08-09); MorningEnrich failed twice on the
first weekly run over the new path, and DataPhase1 + RAGIngestion embed the
same helper, so all three stages were dead.

Unit files here are written INSIDE bash heredocs, so no systemd linter sees
them. This test parses them out of the shell scripts and checks the one
property whose violation hangs a pipeline: an endless ExecStart under a
service type that waits for it to finish.

Scope note: a genuinely one-shot job (a timer-driven backup, a drift check)
is correctly ``Type=oneshot`` and is not flagged — the test keys on the
ExecStart script's own body containing an unbounded loop, not on the type.

## Post-cutover (alpha-engine-config-I6922, 2026-08-14)

``_spot_common.sh``'s ``bootstrap_spot()`` no longer embeds the
``ec2-spot-watchdog`` unit as heredoc text — it dispatches
``krepis.spot_bootstrap render`` and sends the rendered script over SSM. The
generic ``*.sh`` sweep below still runs unmodified (it finds nothing embedded
in this file now, and that is correct — the unit lives in krepis). The two
anchored assertions on the specific unit that hung the pipeline are rendered
from the same argv ``bootstrap_spot()`` actually passes, so they still catch a
future regression, just against the rendered script.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest

from krepis.spot_bootstrap import ConfigCopy, SpotBootstrapSpec, render_bootstrap
from nousergon_lib.shell_guards import (
    embedded_units,
    endless_execstart_violations,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"


def _shell_scripts() -> list[Path]:
    return sorted(p for p in _INFRA.rglob("*.sh") if p.is_file())


@pytest.mark.parametrize("script", _shell_scripts(), ids=lambda p: p.name)
def test_endless_execstart_is_never_a_blocking_service_type(script: Path):
    violations = endless_execstart_violations(script.read_text())
    assert not violations, (
        f"{script.name}: " + "; ".join(violations) +
        " (2026-08-10: the weekly SF's MorningEnrich bootstrap, rc=137 at PT5M0.1S)"
    )


def _rendered_bootstrap() -> str:
    """Render the actual script bootstrap_spot() sends, from its real argv."""
    text = (_INFRA / "_spot_common.sh").read_text()
    block_m = re.search(r"bootstrap_spot\(\)\s*\{(.*?)\n\}", text, re.S)
    assert block_m, "bootstrap_spot() not found in _spot_common.sh"
    call_m = re.search(
        r'"\$LIB_PYTHON"\s+-m\s+krepis\.spot_bootstrap\s+render\s*\\(.*?)\)"',
        block_m.group(1),
        re.S,
    )
    assert call_m, "bootstrap_spot() no longer dispatches krepis.spot_bootstrap render"
    args = shlex.split(call_m.group(1).replace("\\\n", " "))

    def flag(name: str) -> str:
        return args[args.index(name) + 1]

    source, dest, chown = flag("--config-copy").split(":")
    spec = SpotBootstrapSpec(
        repo_url=flag("--repo-url"),
        checkout=flag("--checkout"),
        region=flag("--region"),
        branch="main",
        config_copies=(ConfigCopy(source_name=source, dest=dest, chown=chown),),
        exports={"S3_STAGING": "s3://placeholder/staging"},
    )
    return render_bootstrap(spec)


def test_the_spot_watchdog_unit_is_a_simple_service():
    """Anchored assertion on the specific unit that hung the pipeline."""
    text = _rendered_bootstrap()
    units = [
        u for u in embedded_units(text)
        if "ec2-spot-watchdog" in u.body or "EC2 Spot Watchdog" in u.body
    ]
    assert units, "ec2-spot-watchdog unit not found in _spot_common.sh's rendered bootstrap"
    for unit in units:
        assert unit.service_type == "simple", unit.body
        assert "RemainAfterExit" not in unit.body, (
            "RemainAfterExit belongs to oneshot units; a supervised daemon "
            "uses Restart= instead"
        )


def test_the_watchdog_enable_call_is_time_bounded():
    """A future regression must fail loudly in seconds, not silently at the
    SSM budget. The 2026-08-10 hang produced NO stderr past the symlink line,
    which is why it read as a slow bootstrap rather than a blocked systemctl."""
    text = _rendered_bootstrap()
    assert re.search(r"timeout\s+\d+\s+systemctl\s+enable\s+--now\s+ec2-spot-watchdog", text), (
        "the watchdog enable call must be wrapped in `timeout N systemctl "
        "enable --now ec2-spot-watchdog` with an explicit error message"
    )
