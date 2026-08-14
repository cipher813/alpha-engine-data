"""The spot bootstrap must stage config.yaml where the resolver actually looks.

Bug class (config#6846, live failure ``watch-rerun-2026-08-10-4``,
2026-08-11T04:44Z): the bootstrap staged the config to a path that
``weekly_collector.load_config`` never searches, so MorningEnrich died on::

    FileNotFoundError: Config data/config.yaml not found. Searched:
      ['/home/ec2-user/alpha-engine-config/experiments/reference/data/config.yaml',
       '/home/ec2-user/alpha-engine-config/data/config.yaml',
       'config.yaml']

The retired ``spot_data_weekly.sh`` monolith staged to the second candidate.
The per-stage split (#1122) changed the destination and #1269 repointed the
weekly SF onto the per-stage scripts, so the mismatch only became observable in
production — a shell heredoc and a Python resolver agreeing on a literal path
is exactly the coupling nothing else checks.

The candidate list here is DERIVED from
``nousergon_lib.config.resolve_experiment_config`` rather than hardcoded: if the
resolver's search order changes, this test fails instead of the pipeline.

## Post-cutover (alpha-engine-config-I6922, 2026-08-14)

``bootstrap_spot()`` no longer writes an ``aws s3 cp`` literal into
``_spot_common.sh`` — it passes a ``--config-copy source:dest:chown`` argument
to ``krepis.spot_bootstrap render`` and dispatches the rendered script. This
test now extracts that argument and renders the actual script through
``krepis.spot_bootstrap.render_bootstrap`` before asserting the ``aws s3 cp``
destination against the resolver, so it is exercising exactly what the spot
box receives, not a hand-parsed proxy for it.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

from krepis.spot_bootstrap import ConfigCopy, SpotBootstrapSpec, render_bootstrap
from nousergon_lib.config import resolve_experiment_config

# Where the bootstrap clones nousergon-data on the spot box, and the working
# directory every stage's SSM heredoc cds into before invoking python.
_REMOTE_CHECKOUT = Path("/home/ec2-user/data")

_SPOT_COMMON = Path(__file__).resolve().parents[1] / "infrastructure" / "_spot_common.sh"

# `aws s3 cp "${S3_STAGING}/config.yaml" "<dest>"` inside the RENDERED script.
_STAGE_CP = re.compile(
    r'aws\s+s3\s+cp\s+"\$\{S3_STAGING\}/config\.yaml"\s+"?(?P<dest>[^"\s]+)"?'
)


def _rendered_bootstrap() -> str:
    """Render the actual script bootstrap_spot() sends, from its real argv."""
    text = _SPOT_COMMON.read_text(encoding="utf-8")
    block_m = re.search(r"\nbootstrap_spot\(\)\s*\{(.*?)\n\}", text, re.S)
    assert block_m, f"bootstrap_spot() not found in {_SPOT_COMMON.name}"
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


def _resolver_candidates() -> list[str]:
    """The exact paths load_config would try on the spot box.

    Mirrors ``weekly_collector.load_config``: same subdir, filename, repo_root
    and CWD-relative repo_local_fallback, with ``resolve=False`` so we get the
    candidate list rather than a FileNotFoundError.
    """
    candidates = resolve_experiment_config(
        "data",
        "config.yaml",
        repo_root=_REMOTE_CHECKOUT,
        repo_local_fallback=Path("config.yaml"),
        resolve=False,
    )
    # CWD-relative candidates resolve against the checkout the stages cd into.
    return [str(_REMOTE_CHECKOUT / c) if not Path(c).is_absolute() else str(c) for c in candidates]


def test_bootstrap_stages_config_to_a_resolver_candidate():
    src = _rendered_bootstrap()
    dests = [m.group("dest") for m in _STAGE_CP.finditer(src)]
    assert dests, (
        f"{_SPOT_COMMON.name}'s rendered bootstrap no longer stages config.yaml — if "
        "that is deliberate (prebaked image), delete this test with the reason; "
        "otherwise the stages will fail on a fresh box."
    )

    candidates = _resolver_candidates()
    assert any(d in candidates for d in dests), (
        f"the bootstrap stages config.yaml to {dests}, none of which "
        f"weekly_collector.load_config searches on a box with the checkout at "
        f"{_REMOTE_CHECKOUT}. Resolver candidates: {candidates}. This is config#6846: "
        "the copy succeeds, the box looks healthy, and the workload dies on "
        "FileNotFoundError several minutes later."
    )


def test_staged_config_is_readable_by_the_stage_user():
    """The bootstrap runs as root; every stage workload runs as ec2-user."""
    src = _rendered_bootstrap()
    dests = [m.group("dest") for m in _STAGE_CP.finditer(src)]
    for dest in dests:
        owner_root = str(Path(dest).parent.parent)
        assert re.search(
            rf"chown -R ec2-user:ec2-user {re.escape(owner_root)}\b", src
        ) or re.search(rf"chown -R ec2-user:ec2-user {re.escape(str(Path(dest).parent))}\b", src), (
            f"config staged to {dest} by the root bootstrap is never chowned to "
            "ec2-user, which is the uid every stage's SSM heredoc runs under"
        )
