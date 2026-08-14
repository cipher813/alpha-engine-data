"""The spot bootstrap in this repo must not drift from the fleet's canonical one.

## Why this test exists

``infrastructure/_spot_common.sh`` used to carry its own hand-written copy of
the spot bootstrap — watchdog unit, interpreter install, clone, config
staging — independently from ``crucible-predictor``'s copy of the same shape.
Neither was forked from the other; both were built by applying the same
written standard (ARCHITECTURE.md §111), eight days apart, and the standard
named a *shape* and no implementation. They then diverged.

What that cost, measured on the weekly Step Function over 2026-08-10/11
(alpha-engine-config-I6922):

| Defect | Fixed in one copy | Reached the other |
|---|---|---|
| watchdog unit ``Type=oneshot`` blocks its own bootstrap | `nousergon-data#1294` | `crucible-predictor#461`, 16h later |
| bootstrap ASSERTS ``python3.12`` instead of installing it | `nousergon-data#1296` | `crucible-predictor#462`, 23h later |

## The cutover (alpha-engine-config-I6922, 2026-08-14)

``bootstrap_spot()`` no longer builds a heredoc at all — it calls
``krepis.spot_bootstrap`` (shipped 0.46.0) via the same ``$LIB_PYTHON``
interpreter ``run_ssm`` already uses, and pipes the rendered script straight
into ``run_ssm``. The blocker that kept this a duplicated heredoc instead of a
shared renderer — ``LIB_PYTHON`` resolving to a co-tenant's venv with no
declared floor — cleared in ``alpha-engine-config-I7343``: the launcher now
resolves through ``/opt/nousergon/bin/lib-python``, the ops-owned guard with an
enforced floor.

## What this test asserts, and why it is not a second hardcoded copy

1. There is no inline systemd unit / ``dnf install`` / ``git clone`` left in
   ``bootstrap_spot()`` — the anti-regression against the heredoc creeping
   back.
2. The CLI arguments ``bootstrap_spot()`` actually passes to
   ``krepis.spot_bootstrap render`` are extracted and asserted against the
   spec this repo owes the renderer (repo, checkout, config destination,
   chown, the ``S3_STAGING`` export) — so a wrong argument fails here, not on
   a Saturday.
3. Those extracted arguments are fed through
   ``krepis.spot_bootstrap.render_bootstrap`` and the invariants below are
   re-run against the RENDERED output. The invariants are unchanged from
   before the cutover; only their subject moved from a heredoc literal to a
   rendered string, because the shared parts (watchdog, interpreter) are now
   owned by ``krepis``, not restated here.

Deriving rather than restating is the whole point: a change to the canonical
renderer moves this test's expectations automatically. A test that hardcoded
``Type=simple`` would be a *new* copy of the invariant and would drift exactly
like the two heredocs did.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest

from krepis.spot_bootstrap import (
    PYTHON,
    SYSTEMCTL_ENABLE_TIMEOUT_SEC,
    ConfigCopy,
    SpotBootstrapSpec,
    render_bootstrap,
)

_SPOT_COMMON = Path(__file__).resolve().parents[1] / "infrastructure" / "_spot_common.sh"


@pytest.fixture(scope="module")
def bootstrap_block() -> str:
    """The body of ``bootstrap_spot()`` — now just the krepis dispatch."""
    text = _SPOT_COMMON.read_text(encoding="utf-8")
    m = re.search(r"\nbootstrap_spot\(\)\s*\{(.*?)\n\}", text, re.S)
    assert m, f"bootstrap_spot() not found in {_SPOT_COMMON.name}"
    return m.group(1)


@pytest.fixture(scope="module")
def render_args(bootstrap_block: str) -> list[str]:
    """The literal argv passed to ``krepis.spot_bootstrap render``.

    Extracted from the multi-line ``"$LIB_PYTHON" -m krepis.spot_bootstrap
    render \\`` invocation by stripping line continuations and shlex-splitting
    what remains, so this test reads the actual call rather than restating it.
    """
    m = re.search(
        r'"\$LIB_PYTHON"\s+-m\s+krepis\.spot_bootstrap\s+render\s*\\(.*?)\)"',
        bootstrap_block,
        re.S,
    )
    assert m, "bootstrap_spot() no longer dispatches krepis.spot_bootstrap render"
    joined = m.group(1).replace("\\\n", " ")
    return shlex.split(joined)


#: Stands in for the runtime ``$MAX_RUNTIME_SECONDS`` the launcher passes.
_PLACEHOLDER_RUNTIME_SECONDS = 5400


def _flag_value(args: list[str], flag: str) -> str:
    assert flag in args, f"{flag} missing from the krepis.spot_bootstrap render call"
    return args[args.index(flag) + 1]


@pytest.fixture(scope="module")
def rendered(render_args: list[str]) -> str:
    """The actual script this repo's bootstrap sends, rendered exactly as
    ``bootstrap_spot()`` would — same repo, checkout, config-copy shape.

    ``--branch`` is ``${BRANCH:-main}`` in the shell (a runtime shell
    expansion, asserted separately below), ``--export`` embeds the runtime
    ``$_S3_STAGING`` value and ``--max-runtime-seconds`` the runtime
    ``$MAX_RUNTIME_SECONDS`` — none is a literal this test can read without a
    shell, so each is substituted with a placeholder here. None affects the
    watchdog or interpreter blocks this test asserts against.
    """
    config_copy_raw = _flag_value(render_args, "--config-copy")
    parts = config_copy_raw.split(":")
    assert len(parts) == 3, f"--config-copy must be source:dest:chown, got {config_copy_raw!r}"
    spec = SpotBootstrapSpec(
        repo_url=_flag_value(render_args, "--repo-url"),
        checkout=_flag_value(render_args, "--checkout"),
        region=_flag_value(render_args, "--region"),
        branch="main",
        config_copies=(ConfigCopy(source_name=parts[0], dest=parts[1], chown=parts[2]),),
        exports={"S3_STAGING": "s3://placeholder/staging"},
        max_runtime_seconds=(
            _PLACEHOLDER_RUNTIME_SECONDS
            if "--max-runtime-seconds" in render_args
            else None
        ),
    )
    return render_bootstrap(spec)


# ── The dispatch itself — no heredoc creeping back ───────────────────────────


def test_bootstrap_spot_no_longer_inlines_the_heredoc(bootstrap_block: str):
    """Anti-regression: the whole point of the cutover is this heredoc is gone."""
    assert "systemctl enable" not in bootstrap_block, (
        "bootstrap_spot() still inlines the systemd watchdog unit — the cutover to "
        "krepis.spot_bootstrap (alpha-engine-config-I6922) is meant to delete it, "
        "not duplicate it"
    )
    assert "dnf install" not in bootstrap_block, (
        "bootstrap_spot() still inlines a dnf install — that belongs in "
        "krepis.spot_bootstrap._interpreter_block() now"
    )
    assert "git clone" not in bootstrap_block, (
        "bootstrap_spot() still inlines a git clone — that belongs in "
        "krepis.spot_bootstrap._clone_block() now"
    )


def test_bootstrap_spot_dispatches_through_lib_python(bootstrap_block: str):
    assert '"$LIB_PYTHON" -m krepis.spot_bootstrap render' in bootstrap_block.replace(
        "\\\n", " "
    ) or re.search(r'"\$LIB_PYTHON"\s+-m\s+krepis\.spot_bootstrap\s+render', bootstrap_block), (
        "bootstrap_spot() must render via \"$LIB_PYTHON\" -m krepis.spot_bootstrap, "
        "the same interpreter run_ssm() already dispatches through"
    )


# ── The spec bootstrap_spot() owes the renderer ──────────────────────────────


def test_repo_url_and_checkout(render_args: list[str]):
    assert _flag_value(render_args, "--repo-url") == (
        "https://github.com/nousergon/nousergon-data.git"
    )
    assert _flag_value(render_args, "--checkout") == "/home/ec2-user/data"


def test_branch_is_a_launcher_side_literal_with_the_main_default(bootstrap_block: str):
    """Passed as ``"${BRANCH:-main}"`` — a literal baked at render time, not a
    remote shell expansion. crucible-predictor#463: a value interpolated into
    the heredoc but never exported resolved to an empty string on the spot;
    passing it through the renderer's argv removes that class of bug entirely.
    """
    assert re.search(r'--branch\s+"\$\{BRANCH:-main\}"', bootstrap_block), (
        'bootstrap_spot() must pass --branch "${BRANCH:-main}" — a launcher-side '
        "literal preserving the pre-cutover default, not a remote expansion"
    )


def test_s3_staging_export_is_load_bearing(render_args: list[str], bootstrap_block: str):
    """The config-copy block's `${S3_STAGING}/...` needs this in the remote env."""
    assert re.search(r'--export\s+"S3_STAGING=\$\{_S3_STAGING\}"', bootstrap_block), (
        "bootstrap_spot() must pass --export \"S3_STAGING=${_S3_STAGING}\" — the "
        "config-copy block's aws s3 cp references ${S3_STAGING} in the remote "
        "environment and dies without it"
    )


def test_config_copy_spec(render_args: list[str]):
    raw = _flag_value(render_args, "--config-copy")
    source, dest, chown = raw.split(":")
    assert source == "config.yaml"
    assert dest == "/home/ec2-user/alpha-engine-config/data/config.yaml"
    assert chown == "/home/ec2-user/alpha-engine-config"


def test_region_is_the_pre_cutover_literal(render_args: list[str]):
    """The old heredoc hardcoded us-east-1 and never read $AWS_REGION — this
    preserves that exactly, rather than silently picking up whatever
    $AWS_REGION happens to resolve to at call time.
    """
    assert _flag_value(render_args, "--region") == "us-east-1"


def test_the_hard_runtime_cap_is_armed(render_args: list[str], bootstrap_block: str):
    """A GAINED guarantee, and the one most easily un-shipped in silence.

    Neither this function nor the heredoc it replaced ever armed a hard-timeout
    timer: the pre-cutover copy installed the ``ec2-spot-watchdog`` unit only,
    and ``MAX_RUNTIME_SECONDS`` was the SSM command budget plus an input to
    ``relaunch-decision``. Only the retired ``spot_data_weekly.sh`` monolith
    carried the timer — the inverse fork. The unit answers "the SSM agent died
    and nothing can ever reach this box again"; the timer answers "the workload
    itself hung". They are separate guarantees, always rendered together, and a
    launcher carrying one is uncovered against the other's failure mode.

    Asserted on the ARGUMENT, because the shell value is a runtime expansion:
    dropping the flag is a one-character edit that changes no rendered output
    this test could otherwise see (alpha-engine-config-I7372).
    """
    assert re.search(
        r'--max-runtime-seconds\s+"\$MAX_RUNTIME_SECONDS"', bootstrap_block
    ), (
        'bootstrap_spot() must pass --max-runtime-seconds "$MAX_RUNTIME_SECONDS" — '
        "spot_launch() has already hard-exited if it is empty, so the value is "
        "guaranteed non-empty here"
    )


def test_the_hard_timeout_timer_refuses_to_start_an_uncapped_workload(rendered: str):
    """Arming it is FATAL if it fails — an uncapped hung spot bills until
    somebody notices, and "the cap could not be armed" is exactly the condition
    under which the run must not start."""
    m = re.search(r"systemd-run --on-active=(\d+)[^\n]*\n(.*?)\n\}", rendered, re.S)
    assert m, "no systemd-run hard-timeout timer in the rendered bootstrap"
    assert int(m.group(1)) == _PLACEHOLDER_RUNTIME_SECONDS
    assert "exit 1" in m.group(2), (
        "a hard-timeout timer that could not be armed must abort the bootstrap"
    )


# ── The watchdog unit (asserted against the RENDERED output) ────────────────


def _service_directives(script: str) -> set[str]:
    m = re.search(r"\[Service\]\n(.*?)(?=\n\[|\nUNIT\b)", script, re.S)
    assert m, "no [Service] section found in the systemd unit"
    return {
        line.strip()
        for line in m.group(1).splitlines()
        if "=" in line and not line.strip().startswith("#")
    }


def test_watchdog_unit_is_never_oneshot(rendered: str):
    """The anchored instance, kept explicit because it cost two weekly runs.

    ``systemctl start`` on a ``Type=oneshot`` unit BLOCKS until ``ExecStart``
    exits, and ``TimeoutStartSec`` defaults to infinity for oneshot.
    """
    assert "Type=oneshot" not in _service_directives(rendered), (
        "the ec2-spot-watchdog unit declares Type=oneshot; its ExecStart never "
        "returns, so `systemctl enable --now` blocks forever and the whole "
        "bootstrap dies under SIGKILL with no output (nousergon-data#1294, "
        "crucible-predictor#461)"
    )


def test_enabling_the_watchdog_is_bounded_by_the_canonical_timeout(rendered: str):
    assert re.search(
        rf"timeout\s+{SYSTEMCTL_ENABLE_TIMEOUT_SEC}\s+systemctl\s+enable\s+--now\s+ec2-spot-watchdog",
        rendered,
    ), (
        "`systemctl enable --now ec2-spot-watchdog` must be wrapped in "
        f"`timeout {SYSTEMCTL_ENABLE_TIMEOUT_SEC}` "
        "(krepis.spot_bootstrap.SYSTEMCTL_ENABLE_TIMEOUT_SEC)"
    )


def test_a_failed_enable_explains_itself_and_aborts(rendered: str):
    m = re.search(
        r"timeout\s+\d+\s+systemctl\s+enable\s+--now\s+ec2-spot-watchdog\s*\|\|\s*\{(.*?)\}",
        rendered,
        re.S,
    )
    assert m, "the guarded enable must have a `|| { ... }` failure branch"
    branch = m.group(1)
    assert "exit 1" in branch, "a watchdog that cannot be enabled must abort the bootstrap"
    assert ">&2" in branch, "the failure must reach stderr, which is what SSM captures"


# ── The interpreter (asserted against the RENDERED output) ──────────────────


def test_the_interpreter_is_installed_not_merely_asserted(rendered: str):
    assert re.search(rf"dnf install[^\n]*{re.escape(PYTHON)}\b", rendered), (
        f"the bootstrap must install {PYTHON} explicitly — the AL2023 spot AMI "
        f"does not ship it (nousergon-data#1296, crucible-predictor#462)"
    )


def test_the_interpreter_check_is_a_postcondition_not_a_precondition(rendered: str):
    install = re.search(rf"dnf install[^\n]*{re.escape(PYTHON)}\b", rendered)
    check = re.search(rf"command -v {re.escape(PYTHON)}\b", rendered)
    assert install and check, "expected both a dnf install and a command -v guard"
    assert install.start() < check.start(), (
        f"`command -v {PYTHON}` appears BEFORE the dnf install that provides it."
    )


def test_a_missing_interpreter_after_install_is_fatal(rendered: str):
    m = re.search(rf"command -v {re.escape(PYTHON)} >/dev/null \|\| \{{(.*?)\}}", rendered)
    assert m, f"the post-install `command -v {PYTHON}` guard is missing its abort branch"
    assert "exit 1" in m.group(1)
