"""
Guard the wiring that makes the stage-output assertion actually run
(alpha-engine-config-I7167).

The module and its 70-odd unit tests prove the assertion is CORRECT. They prove
nothing about it being INVOKED — and an observability check that is correct and
unreachable is the failure mode I7167 exists to end, reproduced one layer up.
These tests assert the SF still hands `WeeklySubstrateHealthCheck` the two
things the sweep cannot work without.
"""

from __future__ import annotations

import json
import pathlib

import pytest

SF_PATH = pathlib.Path(__file__).resolve().parent.parent / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def substrate_command() -> str:
    definition = json.loads(SF_PATH.read_text())
    state = definition["States"]["WeeklySubstrateHealthCheck"]
    return state["Parameters"]["Parameters"]["commands.$"]


def test_substrate_stage_invokes_the_health_check_script(substrate_command):
    assert "bash infrastructure/substrate_health_check.sh" in substrate_command


def test_execution_arn_is_threaded_to_the_script(substrate_command):
    """`$$.Execution.Id` is the execution ARN, and it supplies BOTH halves of
    the sweep's context: the start time (via DescribeExecution) and the
    entered-stage set (via GetExecutionHistory).

    Without it the sweep cannot tell a gated-out run — which terminates
    SUCCEEDED in ~5s having done nothing, and is roughly two of every three
    firings — from a deep run in which every stage went silent. It would report
    ~30 missing artifacts on the former, and a detector that cries wolf twice a
    week is a detector that gets muted.
    """
    assert "--execution-arn {}" in substrate_command
    assert "$$.Execution.Id" in substrate_command


def test_run_date_is_still_threaded(substrate_command):
    assert "--run-date {}" in substrate_command
    assert "$.run_date" in substrate_command


def test_format_argument_order_matches_the_placeholders(substrate_command):
    """`States.Format` is positional, so an argument appended in the wrong
    order silently swaps the run date and the execution ARN — the sweep would
    then head keys under a date that is an ARN and report every artifact
    missing, with no error anywhere.
    """
    start = substrate_command.index("States.Format(")
    fragment = substrate_command[start:]
    assert fragment.index("$$.Execution.Name") < fragment.index("$.run_date")
    assert fragment.index("$.run_date") < fragment.index("$$.Execution.Id")
    # Three placeholders consumed by the format string, three arguments after it.
    format_string_end = fragment.index("',", fragment.index("run --correlation-id"))
    assert fragment[:format_string_end].count("{}") == 3


def test_the_stage_still_runs_through_the_log_capture_wrapper(substrate_command):
    """alpha-engine-config-I7047: this stage previously died at rc=127 on an
    inline `trap 'aws s3 cp ...' EXIT` wrapper, before running a single check.
    Adding an argument must not walk that back.
    """
    assert "krepis.ssm_log_capture" in substrate_command
    assert "trap 'aws s3 cp" not in substrate_command
