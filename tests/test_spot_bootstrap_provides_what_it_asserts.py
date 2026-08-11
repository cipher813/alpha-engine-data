"""A spot bootstrap must INSTALL what it asserts is present.

The failure this pins (2026-08-11, `watch-rerun-2026-08-10-3`): the per-stage
bootstrap in ``infrastructure/_spot_common.sh`` carried

    command -v python3.12 >/dev/null || { echo "ERROR: python3.12 not found"; exit 1; }

and nothing that installs it. The retired ``spot_data_weekly.sh`` monolith ran
``dnf install -y -q python3.12 …`` at exactly that point; the per-stage split
(#1122) replaced the install with a bare assertion, encoding an AMI contract
nothing provides. It stayed latent until #1269 repointed the weekly SF onto
these scripts (2026-08-09) — MorningEnrich, DataPhase1 and RAGIngestion all
inherit this helper, so all three were dead on the first run over the new path.

An assertion that a tool exists is a PRECONDITION on the image. A bootstrap's
job is to establish preconditions, not to require them — so every `command -v`
guard here must be preceded, in the same script, by something that provides the
binary.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from nousergon_lib.shell_guards import BINARY_PROVIDERS, unprovided_binary_violations

_REPO_ROOT = Path(__file__).resolve().parent.parent
_COMMON = _REPO_ROOT / "infrastructure" / "_spot_common.sh"


def _bootstrap_block() -> str:
    text = _COMMON.read_text()
    m = re.search(r"bootstrap_spot\(\)\s*\{(.*?)\n\}", text, re.S)
    assert m, "bootstrap_spot() not found in _spot_common.sh"
    return m.group(1)


def test_every_asserted_binary_is_installed_first():
    violations = unprovided_binary_violations(_bootstrap_block())
    assert not violations, (
        "_spot_common.sh: " + "; ".join(violations) +
        " This is the 2026-08-11 MorningEnrich failure "
        "(`ERROR: python3.12 not found`), inherited by DataPhase1 and "
        "RAGIngestion too."
    )


def test_bootstrap_installs_python312_explicitly():
    """Anchored: the interpreter the whole stage depends on."""
    assert re.search(r"dnf install[^\n]*python3\.12", _bootstrap_block()), (
        "the bootstrap must install python3.12 explicitly"
    )


@pytest.mark.parametrize("tool", ["git", "gcc"])
def test_bootstrap_installs_the_tools_the_later_steps_use(tool):
    """git for the clone, gcc for source-built wheels in requirements.txt."""
    block = _bootstrap_block()
    assert any(p in block for p in BINARY_PROVIDERS) and tool in block, (
        f"{tool} is used by the bootstrap or the deps step but is not installed"
    )
