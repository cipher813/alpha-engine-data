"""A failure path must not delete the evidence its own message points at.

**The 2026-08-15 weekly-SF failure (alpha-engine-config-I7396 → I7442).** The
`PredictorBacktest` stage died and printed:

    ERROR: SSM step 'predictor-backtest' terminal status=Failed …
      — full remote log: s3://alpha-engine-research/tmp/spot_predictor-backtest/
        20260815T123311Z-i-08a4371deec28ef07/ssm-output/

Four lines later, the same exit path printed *"Instance terminated; S3 staging
cleaned."* The prefix the error named was **empty** by the time anyone read it,
and so was its parent.

That copy is not redundant. SSM's ``GetCommandInvocation`` returns only the
**first** 24 KB of stdout, so on any long stage the tail — which is where a
traceback lives — exists nowhere else. A message pointing at evidence the same
exit path just removed is worse than no message.

**What changed at I7442, in this repo.** `_spot_common.sh` (shared by
spot_morning_enrich.sh / spot_data_phase1.sh / spot_rag_ingestion.sh) and
`spot_data_weekly.sh` (the still-live monolith the weekly SF's DataPhase2
state invokes with --phase2-only) both ran an unguarded
``aws s3 rm "$S3_STAGING" --recursive`` in their `cleanup()`, on every exit
path, success or failure. Teardown now runs through
``krepis.spot_evidence teardown``, which copies the prefix to
``_spot_evidence/`` and deletes staging only if that copy succeeded — so the
ordering is a property of that module's call graph rather than of a branch
here, and it holds for every launcher in this repo at once.

These tests therefore pin the CLASS property: no launcher in this repo may
carry an unguarded staging delete, and the teardown must degrade to retention
rather than to deletion.
"""

from __future__ import annotations

from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
COMMON = INFRA / "_spot_common.sh"
WEEKLY = INFRA / "spot_data_weekly.sh"


def _fn_body(text: str, signature: str) -> str:
    start = text.index(signature)
    end = text.index("\n}\n", start)
    return text[start:end]


@pytest.fixture(scope="module")
def common_body() -> str:
    assert COMMON.is_file(), f"{COMMON} missing"
    return COMMON.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def weekly_body() -> str:
    assert WEEKLY.is_file(), f"{WEEKLY} missing"
    return WEEKLY.read_text(encoding="utf-8")


class TestNoLauncherDeletesItsOwnStaging:
    """The class guard. Fixing one call site of a systemic defect is not a fix."""

    def test_no_shell_script_in_infrastructure_removes_S3_STAGING(self):
        offenders = []
        for path in sorted(INFRA.glob("*.sh")):
            for n, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            ):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if "aws s3 rm" in stripped and "S3_STAGING" in stripped:
                    offenders.append(f"{path.name}:{n}: {stripped}")
        assert not offenders, (
            "an unguarded staging delete is back — this is the "
            "alpha-engine-config-I7442 defect, and it destroys the only "
            "un-truncated copy of a failure's output:\n  "
            + "\n  ".join(offenders)
        )


class TestSharedCommonTeardownGoesThroughTheChokepoint:
    def test_cleanup_calls_the_shared_teardown(self, common_body):
        cleanup_fn = _fn_body(common_body, "cleanup() {")
        assert 'teardown_staging "$_exit_code"' in cleanup_fn

    def test_on_exit_threads_the_real_exit_status_into_cleanup(self, common_body):
        on_exit_fn = _fn_body(common_body, "on_exit() {")
        assert 'cleanup "$rc"' in on_exit_fn

    def test_launch_only_path_passes_zero_not_a_workload_status(self, common_body):
        cleanup_fn = _fn_body(common_body, "cleanup() {")
        assert "teardown_staging 0" in cleanup_fn

    def test_the_teardown_helper_invokes_krepis(self, common_body):
        fn = _fn_body(common_body, "teardown_staging() {")
        assert "krepis.spot_evidence teardown" in fn
        assert "--exit-code" in fn, (
            "the workload's exit status is what decides whether evidence is "
            "preserved; without it every run looks like a success"
        )
        assert "--staging" in fn and "--slug" in fn

    def test_an_unavailable_chokepoint_degrades_to_RETENTION_not_deletion(
        self, common_body
    ):
        """The merge-order safety property, and the fail-safe direction.

        A box whose krepis pin predates `spot_evidence` must keep the evidence,
        never fall back to the delete this whole change exists to remove.
        """
        fn = _fn_body(common_body, "teardown_staging() {")
        assert "RETAINED" in fn
        code = "\n".join(
            line for line in fn.splitlines() if not line.strip().startswith("#")
        )
        assert "aws s3 rm" not in code

    def test_the_teardown_never_aborts_the_exit_path(self, common_body):
        fn = _fn_body(common_body, "teardown_staging() {")
        assert fn.rstrip().endswith("return 0"), (
            "a janitor that can change the trap's exit status masks the "
            "workload's own failure"
        )


class TestWeeklyMonolithTeardownGoesThroughTheChokepoint:
    """spot_data_weekly.sh --phase2-only is STILL the SF's live DataPhase2
    path, so it needs the identical fix, not a deprecation notice."""

    def test_cleanup_calls_the_shared_teardown(self, weekly_body):
        cleanup_fn = _fn_body(weekly_body, "cleanup() {")
        assert 'teardown_staging "$_exit_code"' in cleanup_fn

    def test_on_exit_threads_the_real_exit_status_into_cleanup(self, weekly_body):
        on_exit_fn = _fn_body(weekly_body, "on_exit() {")
        assert 'cleanup "$rc"' in on_exit_fn

    def test_launch_only_path_passes_zero_not_a_workload_status(self, weekly_body):
        cleanup_fn = _fn_body(weekly_body, "cleanup() {")
        assert "teardown_staging 0" in cleanup_fn

    def test_the_teardown_helper_invokes_krepis(self, weekly_body):
        fn = _fn_body(weekly_body, "teardown_staging() {")
        assert "krepis.spot_evidence teardown" in fn
        assert "--exit-code" in fn
        assert "--staging" in fn and "--slug" in fn

    def test_an_unavailable_chokepoint_degrades_to_RETENTION_not_deletion(
        self, weekly_body
    ):
        fn = _fn_body(weekly_body, "teardown_staging() {")
        assert "RETAINED" in fn
        code = "\n".join(
            line for line in fn.splitlines() if not line.strip().startswith("#")
        )
        assert "aws s3 rm" not in code

    def test_the_teardown_never_aborts_the_exit_path(self, weekly_body):
        fn = _fn_body(weekly_body, "teardown_staging() {")
        assert fn.rstrip().endswith("return 0"), (
            "a janitor that can change the trap's exit status masks the "
            "workload's own failure"
        )


class TestTheResourceLimitFlagWaitsForThePinBump:
    """`--resource-limit` is deliberately ABSENT, and that is load-bearing.

    It is a NEW `krepis.ssm_dispatcher` flag (krepis-PR161). `$LIB_PYTHON` is
    the dispatch box's venv, pinned to a krepis release that predates it, so
    argparse would reject the unknown flag and EVERY SSM step would fail on
    merge — in exactly the window before the pin bump. `krepis.spot_evidence`
    degrades safely when absent (the teardown retains); this flag has no safe
    degradation at all.

    It lands with the pin bump, `alpha-engine-config-I7556`. Until then this
    test is what stops it being reintroduced by someone reading
    sf-pipeline-policy §3 obligation 3 and adding the obvious line.
    """

    def test_no_launcher_passes_the_flag_yet(self):
        offenders = []
        for path in sorted(INFRA.glob("*.sh")):
            for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if line.strip().startswith("#"):
                    continue
                if "--resource-limit" in line:
                    offenders.append(f"{path.name}:{n}")
        assert not offenders, (
            "--resource-limit is passed to krepis.ssm_dispatcher before the "
            "dispatch box's krepis pin ships it (alpha-engine-config-I7556). "
            "An unknown argparse flag fails EVERY SSM step: "
            + ", ".join(offenders)
        )
