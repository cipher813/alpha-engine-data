"""Unit tests for alpha-engine-research-eval-judge-spot-dispatcher
(alpha-engine-config-I9329).

Hermetic: ``nousergon_lib.spot_dispatch`` is stubbed in ``sys.modules`` BEFORE
``import index`` — the same shape every sibling dispatcher's test uses.
``krepis.spot_bootstrap`` is imported for real, because the rendered bootstrap
is half of what these tests are asserting about and a stub would let a
renderer contract change pass silently here.

What is pinned, and why each one is a defect class this fleet has already paid
for:

* the box must be tagged so the Step Function's ``ssm:SendCommand`` grant
  matches it — a tag typo is an AccessDenied on a stage that fail-softs, i.e.
  a silently missing week of evals;
* the router addressing must reach the box through a FILE, not through the
  bootstrap process's environment, because the SF's run is a SEPARATE SSM
  command and therefore a separate shell;
* the bootstrap must NOT run the judge and must NOT self-terminate;
* the watchdog must exceed bootstrap + judge budget, or it truncates the
  corpus and a coverage shortfall — a HARD failure by Brian's 2026-08-29
  ruling — is reported for a run that was healthy.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent


class SpotLaunchError(Exception):
    pass


class SpotProbeError(Exception):
    pass


@pytest.fixture
def index_mod(monkeypatch):
    sd = types.ModuleType("nousergon_lib.spot_dispatch")
    sd.SpotLaunchError = SpotLaunchError
    sd.SpotCapacityExhausted = type("SpotCapacityExhausted", (SpotLaunchError,), {})
    sd.SpotQuotaExceededError = type("SpotQuotaExceededError", (SpotLaunchError,), {})
    sd.SpotProbeError = SpotProbeError
    sd.launch_with_fallback = MagicMock(return_value=("i-evaljudge", "spot"))
    sd.wait_ssm_online = MagicMock()
    sd.send_async_command = MagicMock(return_value="cmd-eval-judge-1")
    sd.terminate_on_failure = MagicMock()
    lib = types.ModuleType("nousergon_lib")
    lib.spot_dispatch = sd
    monkeypatch.setitem(sys.modules, "nousergon_lib", lib)
    monkeypatch.setitem(sys.modules, "nousergon_lib.spot_dispatch", sd)
    monkeypatch.syspath_prepend(str(SCRIPT_DIR))
    sys.modules.pop("index", None)
    index = importlib.import_module("index")
    return index, sd


def _sent_command(sd) -> str:
    return sd.send_async_command.call_args.args[1]


class TestDispatch:
    def test_returns_the_four_fields_the_step_function_threads(self, index_mod):
        index, sd = index_mod
        out = index.handler({"run_date": "2026-08-29"}, None)
        assert out["instance_id"] == "i-evaljudge"
        assert out["market"] == "spot"
        assert out["command_id"] == "cmd-eval-judge-1"
        assert out["run_token"]
        # alpha-engine-config-I10172: the stage-coverage self-assertion —
        # never raises even without live AWS credentials in this unit test
        # (record_verdict is fail-soft), and never alters the SF-threaded
        # fields above.
        assert out["stage_coverage"]["stage"] == "DispatchEvalJudgeSpot"

    def test_box_carries_the_tag_the_sf_send_command_grant_is_scoped_to(self, index_mod):
        """`alpha-engine-step-functions-role`'s SendCommandEvalJudgeSpot
        statement is conditioned on `ssm:resourceTag/Name`. A tag that does not
        match is an AccessDenied on a fail-soft stage — a silently missing week
        of eval coverage, which is the shape I9309 exists to end."""
        index, sd = index_mod
        index.handler({}, None)
        assert sd.launch_with_fallback.call_args.kwargs["tag_name"] == (
            "alpha-engine-eval-judge-spot"
        )

    def test_box_runs_under_the_executor_profile(self, index_mod):
        index, sd = index_mod
        index.handler({}, None)
        assert sd.launch_with_fallback.call_args.kwargs["iam_instance_profile"] == (
            "alpha-engine-executor-profile"
        )

    def test_identity_and_deadline_tags_ride_the_launch_call(self, index_mod):
        """Atomic with RunInstances, never a post-launch create_tags: a box
        reaped inside the tagging window would be unlookupable either way."""
        index, sd = index_mod
        index.handler(
            {"execution_id": "arn:x", "run_date": "2026-08-29", "pipeline_role": "weekly"},
            None,
        )
        tags = sd.launch_with_fallback.call_args.kwargs["extra_tags"]
        assert tags["execution-id"] == "arn:x"
        assert tags["run-date"] == "2026-08-29"
        assert tags["pipeline-role"] == "weekly"
        assert tags["watchdog-deadline"]

    def test_force_on_demand_is_threaded_for_the_bounded_re_dispatch(self, index_mod):
        index, sd = index_mod
        index.handler({"force_on_demand": True}, None)
        assert sd.launch_with_fallback.call_args.kwargs["force_on_demand"] is True


class TestBootstrapCommand:
    def test_writes_the_router_addressing_to_a_file_not_to_the_process_env(
        self, index_mod
    ):
        """The SF's judge run is a SEPARATE ssm:sendCommand and therefore a
        separate shell. An export that lived only in the bootstrap process
        would be gone by the time the judge runs, and `judge_exec_context()`
        would silently answer "lambda" from a spot box."""
        index, sd = index_mod
        index.handler({}, None)
        cmd = _sent_command(sd)
        assert f"cat > {index.ENV_FILE}" in cmd
        for line in (
            "KREPIS_EXEC_CONTEXT=ec2",
            "KREPIS_LITELLM_PROXY_URL=https://router.nousergon.ai:8443",
            "KREPIS_ROUTER_CREDENTIAL_SECRET=ROUTER_CONSUMER_RESEARCH",
            "KREPIS_APPCONFIG_APPLICATION=yq405wh",
            "KREPIS_APPCONFIG_CONFIG_PROFILE=llm-model-registry",
            "KREPIS_COST_SINK_BUCKET=alpha-engine-research",
            "KREPIS_COST_SINK_PREFIX=decision_artifacts/_cost_raw",
        ):
            assert line in cmd, f"missing from the env file heredoc: {line}"

    def test_never_names_a_model_a_provider_or_a_direct_endpoint(self, index_mod):
        """principles.md §2.8 and Brian's 2026-08-29 ruling that the direct
        Anthropic API is retired. Addressed by registry GROUP through the
        router, never by vendor identity."""
        index, sd = index_mod
        index.handler({}, None)
        cmd = _sent_command(sd).lower()
        for forbidden in (
            "api.anthropic.com",
            "anthropic_api_key",
            "openrouter",
            "claude-",
            "gpt-",
            "litellm_master_key",
        ):
            assert forbidden not in cmd, f"bootstrap names {forbidden!r}"

    def test_execs_the_repo_owned_setup_script_and_not_the_judge(self, index_mod):
        """The heavy setup is version-controlled in crucible-research so a
        change to the box's shape is a PR there, not a Lambda redeploy. The RUN
        is the Step Function's, issued separately — this command must not start
        it, or the SF would poll a command that is already grading and the
        coverage verdict would reach no stage status."""
        index, sd = index_mod
        index.handler({}, None)
        cmd = _sent_command(sd)
        assert "bash infrastructure/eval_judge_spot_bootstrap.sh" in cmd
        assert "judge_spot_run" not in cmd

    def test_clones_research_and_only_research(self, index_mod):
        """crucible-research is PUBLIC, so it is cloned by literal URL with no
        credential in argv or .git/config. The one PRIVATE clone the judge needs
        (alpha-engine-config, for the gitignored prompts) happens ON THE BOX in
        the repo script, reading the PAT from SSM via the instance profile — so
        the credential is never known to this Lambda and never appears in the
        SSM document."""
        index, sd = index_mod
        index.handler({}, None)
        cmd = _sent_command(sd)
        assert "github.com/nousergon/crucible-research.git" in cmd
        assert "x-access-token" not in cmd
        assert "get-parameter" not in cmd

    def test_arms_both_timers_and_the_ssm_liveness_watchdog(self, index_mod):
        """Three distinct guarantees, and none substitutes for another: the
        dead-man ("the bootstrap died before arming its cap"),
        max_runtime_seconds ("the workload hung"), and the ec2-spot-watchdog
        unit ("the SSM agent died and nothing can ever reach this box again").
        For a box driven ENTIRELY over SSM the third is the failure mode that
        strands the whole stage."""
        index, sd = index_mod
        index.handler({}, None)
        cmd = _sent_command(sd)
        assert str(index.WATCHDOG_SECONDS) in cmd
        assert str(index.DEADMAN_SECONDS) in cmd
        assert "eval-judge-spot" in cmd


class TestFailLoud:
    def test_kill_switch_raises_rather_than_returning_a_quiet_no_op(
        self, index_mod, monkeypatch
    ):
        index, _ = index_mod
        monkeypatch.setattr(index, "DISPATCH_ENABLED", False)
        with pytest.raises(RuntimeError, match="EVAL_JUDGE_SPOT_DISPATCH_ENABLED"):
            index.handler({}, None)

    def test_launch_failure_propagates(self, index_mod):
        index, sd = index_mod
        sd.launch_with_fallback.side_effect = SpotLaunchError("no capacity")
        with pytest.raises(SpotLaunchError):
            index.handler({}, None)

    def test_post_launch_failure_terminates_the_box_before_re_raising(self, index_mod):
        """Between launch and the bootstrap command landing there is no
        watchdog or trap on the box yet — those are armed BY the bootstrap it
        has not received. Anything failing in here would orphan it."""
        index, sd = index_mod
        sd.wait_ssm_online.side_effect = RuntimeError("ssm never came online")
        with pytest.raises(RuntimeError):
            index.handler({}, None)
        sd.terminate_on_failure.assert_called_once()

    def test_refuses_to_launch_when_the_watchdog_could_preempt_the_judge(
        self, index_mod, monkeypatch
    ):
        """A watchdog firing mid-run truncates the corpus, and a coverage
        shortfall is a HARD stage failure (Brian, 2026-08-29). Refusing at
        launch turns a silent weekly eval failure into a loud one at the
        moment the misconfiguration is introduced."""
        index, sd = index_mod
        monkeypatch.setattr(
            index,
            "WATCHDOG_SECONDS",
            index.BOOTSTRAP_TIMEOUT_SECONDS + index.EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS,
        )
        with pytest.raises(ValueError, match="WATCHDOG_SECONDS"):
            index.handler({}, None)
        sd.launch_with_fallback.assert_not_called()

    def test_the_shipped_default_satisfies_that_inequality(self, index_mod):
        """The guard above only helps if the default it guards is correct."""
        index, _ = index_mod
        assert index.WATCHDOG_SECONDS > (
            index.BOOTSTRAP_TIMEOUT_SECONDS + index.EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS
        )
