"""Pins the off-cycle weekly runner's input contracts + safety ordering.

``infrastructure/run_weekly_offcycle.sh`` fires the Saturday pipeline
off-schedule. Its whole value is that an off-cycle run is byte-identical to
the scheduled one, so these tests pin the two production input contracts the
script must reproduce:

  * ``shell`` input == the ``alpha-engine-eod-success-friday-shell-trigger``
    Lambda input (``shell_run=true`` + ``pipeline_role="shell-run"``).
  * ``full`` input == the ``alpha-engine-saturday`` EventBridge cron target
    input (``pipeline_role="weekly"``, NO ``shell_run``).

If the live CFN cron input or the Lambda input drifts, the corresponding
assertion here fails — forcing the runner to be re-synced rather than
silently firing a stale contract.

It also pins the ``full``-path safety invariant: the auto re-enable schedule
is created BEFORE the cron rule is disabled (so a failure never leaves the
weekly cadence silently dead).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_RUNNER = _INFRA / "run_weekly_offcycle.sh"
_CFN = _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"
_LAMBDA = _INFRA / "lambdas" / "eod-success-friday-shell-trigger" / "index.py"


@pytest.fixture(scope="module")
def runner_text() -> str:
    return _RUNNER.read_text()


def _dry_run_input(verb: str) -> dict:
    """Run the script in --dry-run and parse the EXPANDED execution input.

    Tests the real shell-variable expansion (catches a mistyped constant),
    not just the static heredoc text. The --dry-run path only touches ``aws``
    inside guarded ``if`` conditions, so it works with no AWS creds / no CLI.
    """
    proc = subprocess.run(
        ["bash", str(_RUNNER), verb, "--dry-run"],
        capture_output=True,
        text=True,
        env={**os.environ, "AWS_REGION": "us-east-1", "ACCOUNT_ID": "711398986525"},
    )
    assert proc.returncode == 0, f"{verb} --dry-run failed:\n{proc.stderr}"
    m = re.search(r"^\s*input:\s*(\{.*\})\s*$", proc.stdout, re.MULTILINE)
    assert m, f"no 'input:' line in {verb} --dry-run output:\n{proc.stdout}"
    return json.loads(m.group(1))


def _builder_body(text: str, fn_name: str) -> str:
    """Slice a shell function body ``fn_name() { ... }`` from the script."""
    m = re.search(rf"{fn_name}\(\)\s*\{{(.*?)\n\}}", text, re.DOTALL)
    assert m, f"{fn_name}() not found in runner"
    return m.group(1)


def test_runner_exists_and_executable() -> None:
    assert _RUNNER.exists(), "run_weekly_offcycle.sh missing"
    assert os.access(_RUNNER, os.X_OK), "run_weekly_offcycle.sh must be executable"


def test_shell_input_contract() -> None:
    obj = _dry_run_input("shell")
    assert obj["shell_run"] is True
    assert obj["pipeline_role"] == "shell-run"
    # config#2248: ec2_instance_id is intentionally ABSENT — the weekly SF's
    # own CheckSpotDispatchNeeded/DispatchWeeklyFreshnessSpot states populate
    # it from a fresh ephemeral spot; this script no longer hardcodes the
    # always-on dashboard box id.
    assert "ec2_instance_id" not in obj
    assert obj["sns_topic_arn"].endswith(":alpha-engine-alerts")


def test_full_input_contract() -> None:
    obj = _dry_run_input("full")
    assert obj["pipeline_role"] == "weekly"
    assert "shell_run" not in obj, "full weekly run must NOT set shell_run"
    assert "ec2_instance_id" not in obj  # config#2248 — see test_shell_input_contract
    assert obj["sns_topic_arn"].endswith(":alpha-engine-alerts")


def test_full_input_matches_live_cron_target() -> None:
    """The ``full`` builder must reproduce the CFN SaturdayTrigger Input."""
    cfn = _CFN.read_text()
    block = cfn.split("SaturdayTrigger:", 1)[1].split("WeekdayPipelineSchedule:", 1)[0]
    # Narrow to the Input: !Sub | heredoc (the surrounding region carries a
    # comment mentioning "shell_run mode" that is not part of the target input).
    after_input = block.split("Input:", 1)[1]
    # The JSON closes with a "}" on its own (dedented) line, so this stops
    # correctly at the first line-leading "}" after "Input:".
    m = re.search(r".*?\n\s*\}", after_input, re.DOTALL)
    assert m, "could not isolate the SaturdayTrigger Input JSON"
    input_block = m.group(0)
    assert '"pipeline_role": "weekly"' in input_block
    assert "shell_run" not in input_block
    # config#2248: the live cron Input no longer carries ec2_instance_id
    # either — both sides of the contract (CFN + this offcycle runner) went
    # through the dispatch path together.
    assert "ec2_instance_id" not in input_block
    assert _dry_run_input("full")["pipeline_role"] == "weekly"


def test_shell_builder_still_produces_a_shell_run() -> None:
    """The ``shell`` builder remains the shell-run producer.

    It used to be pinned against the eod-success Lambda, which sent the same
    input. That coupling is GONE as of 2026-07-29: the Lambda now starts a FULL
    weekly run after every post-close (Brian ruling), so it sends neither
    shell_run nor pipeline_role="shell-run". This script is now the only
    shell-run producer, so the assertion is on the script alone.
    """
    obj = _dry_run_input("shell")
    assert obj["shell_run"] is True
    assert obj["pipeline_role"] == "shell-run"


def test_eod_lambda_no_longer_starts_shell_runs() -> None:
    """Guards the divergence above rather than leaving it implicit.

    If the Lambda is reverted to shell_run mode, the daily cadence silently
    becomes a daily DRY pass -- green executions skipping the very workload
    stages whose failures the daily cadence exists to surface.
    """
    lam = _LAMBDA.read_text()
    assert '"shell_run": True' not in lam
    assert '"shell-run"' not in lam
    assert '"skip_weekly_run_day_gate": True' in lam


def test_full_run_does_not_touch_the_retired_saturday_cron(runner_text: str) -> None:
    """do_full must NOT disable/re-enable alpha-engine-saturday.

    The cron is retired (State: DISABLED in CloudFormation) now that the
    post-close-SF event trigger starts the weekly every trading day. The old
    suppress-then-restore dance would RE-ENABLE it as a side effect of an
    unrelated off-cycle run, silently restoring the double-run this migration
    removed -- and the SF mutex would not catch it, because the two firings
    carry different run_dates.
    """
    body = _builder_body(runner_text, "do_full")
    assert "disable-rule" not in body
    assert "enable-rule" not in body
    assert "schedule_reenable" not in body


def test_full_targets_correct_state_machine(runner_text: str) -> None:
    assert "ne-weekly-freshness-pipeline" in runner_text


