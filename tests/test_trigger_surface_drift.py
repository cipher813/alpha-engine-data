"""Contract tests for the playbook -> AWS-surface linkage (alpha-engine-config-I9045).

**The condition under test, measured live 2026-08-28.** All four
``alpha-engine-alert-drain-*utc`` EventBridge Scheduler schedules were
``DISABLED`` with ``Description: null``; the ``alert-drain`` playbook ran every
single day that week off ``alpha-engine-freshness-monitor-cron``'s event-time
leg; and nothing on any AWS resource connected the two. The AWS surface gave the
wrong answer in both directions and the correction lived only in a YAML comment.

``overseer-policy.md`` invariant 13 — *a guard is not a guard until it has been
observed failing* — is what shapes this file. Every finding kind the checker can
emit has a test that SEEDS the disagreement and asserts the checker goes red on
it, and ``could_not_measure`` has its own tests proving it is distinguishable
from a pass rather than collapsing into one. Two prior fleet defects are the
reason that is spelled out: a gate that never executed a single job in 100 runs
(``alpha-engine-config-I8729``), and a test that asserted an alert's message
STRING and therefore could not catch a wrong tier. These assert BEHAVIOUR — the
verdict and the exit code — never the text of a description.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
OVERSEER = ROOT / "infrastructure" / "overseer"
REGISTRY_PATH = OVERSEER / "playbooks.yaml"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def td():
    return _load("trigger_descriptions", OVERSEER / "trigger_descriptions.py")


@pytest.fixture(scope="module")
def drift(td):
    return _load("trigger_surface_drift", OVERSEER / "trigger_surface_drift.py")


@pytest.fixture(scope="module")
def registry():
    return yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))


#: The registry shape the real defect had: a playbook woken by four schedules
#: AND by an event-time leg that rides a separate EventBridge rule.
def _drain_registry():
    return {
        "playbooks": {
            "alert-drain": {
                "triggers": [
                    {"surface": "scheduler", "name": "sched-a"},
                    {
                        "surface": "event-time",
                        "ref": "freshness CRITICAL -> router",
                        "reason": "the Lambda invokes the router in-process",
                        "depends_on": [{"surface": "events", "name": "rule-a"}],
                    },
                ]
            }
        }
    }


def _describe(mapping):
    """A ``live_description`` stub. Unnamed resources are ABSENT, never blank."""
    return lambda surface, name: mapping.get(f"{surface}:{name}")


def _no_router_schedules():
    return lambda: []


# ── the derivation ───────────────────────────────────────────────────────────

def test_the_event_time_leg_puts_the_playbook_on_the_rule_that_dispatches_it(td):
    """The whole finding in one assertion.

    ``alpha-engine-freshness-monitor-cron`` is an EventBridge RULE whose name,
    schedule and old description say only "freshness". It is also the thing that
    actually ran alert-drain every day. The marker is what makes
    ``aws events describe-rule`` answer that.
    """
    marker = td.marker_for(_drain_registry(), "events", "rule-a")
    assert "[wakes: alert-drain]" in marker


def test_a_disabled_schedule_names_the_leg_doing_the_work(td):
    """Read off a DISABLED schedule, ``sibling-legs`` is the answer to
    "then what is running this?" — the question the AWS surface could not
    answer on 2026-08-28."""
    marker = td.marker_for(_drain_registry(), "scheduler", "sched-a")
    assert "[wakes: alert-drain]" in marker
    assert "events:rule-a" in marker


def test_an_undeclared_resource_cannot_be_stamped(td):
    """A deploy.sh asking for a marker it cannot get is creating a wake path
    nothing records. It must fail the deploy, not emit a blank."""
    with pytest.raises(KeyError):
        td.marker_for(_drain_registry(), "scheduler", "never-declared")


def test_a_description_aws_would_truncate_is_refused(td):
    """AWS caps both description fields at 512 and truncates on some paths. A
    truncated marker parses to the WRONG answer, which is worse than none."""
    with pytest.raises(td.DescriptionTooLong):
        td.description_for(_drain_registry(), "events", "rule-a", "x" * 600)


def test_every_real_resource_fits_under_the_aws_ceiling(td, registry):
    """The live registry, not a fixture — the ceiling is only useful if the
    actual descriptions clear it."""
    for key in td.resource_index(registry):
        surface, name = key.split(":", 1)
        assert len(td.marker_for(registry, surface, name)) <= td.MAX_DESCRIPTION_CHARS


# ── seeded failures: the checker must be observed going red ──────────────────

def test_a_missing_marker_is_a_finding(drift):
    """The 2026-08-28 state itself: the resource exists, the description is
    empty, and nothing on AWS says what it wakes."""
    findings, _ = drift.check(
        registry=_drain_registry(),
        covered={"events:rule-a": "infrastructure/lambdas/x/deploy.sh"},
        describe=_describe({"events:rule-a": ""}),
        list_router_schedules=_no_router_schedules(),
    )
    assert [f["kind"] for f in findings] == ["marker-missing"]


def test_a_stale_marker_is_a_finding_not_a_pass(drift, td):
    """The drift case the whole check exists for: a marker is PRESENT, so a
    check that only asked "is it stamped" would go green, while the set of legs
    it names is no longer the set playbooks.yaml declares."""
    stale = "[wakes: alert-drain] [sibling-legs: scheduler:some-retired-slot]"
    findings, _ = drift.check(
        registry=_drain_registry(),
        covered={"events:rule-a": "infrastructure/lambdas/x/deploy.sh"},
        describe=_describe({"events:rule-a": f"prose here {stale}"}),
        list_router_schedules=_no_router_schedules(),
    )
    assert [f["kind"] for f in findings] == ["marker-drift"]
    assert findings[0]["actual"] == stale


def test_a_correct_marker_passes(drift, td):
    """The negative control. Without it, every test above passes on a checker
    that returns a finding unconditionally."""
    reg = _drain_registry()
    good = td.description_for(reg, "events", "rule-a", "Daily freshness probe")
    findings, _ = drift.check(
        registry=reg,
        covered={"events:rule-a": "infrastructure/lambdas/x/deploy.sh"},
        describe=_describe({"events:rule-a": good}),
        list_router_schedules=_no_router_schedules(),
    )
    assert findings == []


def test_a_reconciled_resource_that_vanished_is_a_finding(drift):
    findings, _ = drift.check(
        registry=_drain_registry(),
        covered={"events:rule-a": "infrastructure/lambdas/x/deploy.sh"},
        describe=_describe({}),
        list_router_schedules=_no_router_schedules(),
    )
    assert [f["kind"] for f in findings] == ["trigger-absent"]


def test_a_live_wake_path_nobody_declared_is_a_finding(drift):
    """The reverse direction, and it caught a REAL one on its first live run:
    ``alpha-engine-sf-watch-canary-drill-weekly`` dispatched playbook
    ``sf-watch`` while playbooks.yaml declared it only under ``canary-replay``.
    Nothing else in the fleet compares live Target Input against the registry."""
    findings, _ = drift.check(
        registry=_drain_registry(),
        covered={},
        describe=_describe({}),
        list_router_schedules=lambda: [
            {"name": "sched-nobody-declared", "playbook": "alert-drain"}
        ],
    )
    assert [f["kind"] for f in findings] == ["undeclared-trigger"]


def test_a_declared_schedule_dispatching_a_different_playbook_is_a_finding(drift):
    findings, _ = drift.check(
        registry=_drain_registry(),
        covered={},
        describe=_describe({}),
        list_router_schedules=lambda: [
            {"name": "sched-a", "playbook": "some-other-playbook"}
        ],
    )
    assert [f["kind"] for f in findings] == ["undeclared-trigger"]


def test_an_unreconciled_resource_is_pending_not_a_finding(drift):
    """Coverage is discovered by scanning the deploy scripts, so a resource no
    script reconciles is NAMED and counted rather than graded — and there is no
    allowlist here that could be widened to hide one."""
    findings, pending = drift.check(
        registry=_drain_registry(),
        covered={},
        describe=_describe({}),
        list_router_schedules=_no_router_schedules(),
    )
    assert findings == []
    assert {p["resource"] for p in pending} == {"scheduler:sched-a", "events:rule-a"}


# ── could-not-measure is its own state, never a pass ─────────────────────────

@pytest.mark.parametrize("stderr", [
    "An error occurred (AccessDeniedException) ... is not authorized",
    "Unable to locate credentials. You can configure credentials by ...",
    "The security token included in the request is expired (ExpiredToken)",
])
def test_losing_our_own_access_is_not_a_pass(drift, stderr):
    """A detector that grades itself green by losing its access is the failure
    mode the third exit code exists for."""
    with pytest.raises(drift.CouldNotMeasure):
        drift._raise_if_unmeasurable(stderr, "reading events:x")


def test_a_genuine_not_found_is_absence_not_unmeasurable(drift):
    """The opposite direction: ``ResourceNotFoundException`` IS an answer, and
    collapsing it into could-not-measure would hide a vanished trigger."""
    drift._raise_if_unmeasurable(
        "An error occurred (ResourceNotFoundException) when calling DescribeRule",
        "reading events:x",
    )


def test_could_not_measure_exits_2_and_drift_exits_1(drift, monkeypatch):
    """The two states a caller acts on differently, proven at the exit code —
    the surface CI actually reads."""
    def boom(**kwargs):
        raise drift.CouldNotMeasure("no credentials")
    monkeypatch.setattr(drift, "check", boom)
    assert drift.main([]) == 2
    assert drift.main(["--allow-unmeasured"]) == 0

    monkeypatch.setattr(
        drift, "check",
        lambda **kw: ([{"kind": "marker-missing", "resource": "events:x",
                        "detail": "d"}], []),
    )
    assert drift.main([]) == 1
    # --allow-unmeasured must never downgrade a REAL finding.
    assert drift.main(["--allow-unmeasured"]) == 1

    monkeypatch.setattr(drift, "check", lambda **kw: ([], []))
    assert drift.main([]) == 0


def test_a_scoped_run_with_nothing_to_grade_is_unmeasurable_not_green(drift):
    """`--source-file` pointing at a script that reconciles nothing must not
    report "no drift" — an assertion covering zero resources has not passed."""
    with pytest.raises(drift.CouldNotMeasure):
        drift.check(
            registry=_drain_registry(),
            covered={"events:rule-a": "infrastructure/lambdas/x/deploy.sh"},
            describe=_describe({}),
            list_router_schedules=_no_router_schedules(),
            source_file="infrastructure/lambdas/typo/deploy.sh",
        )


# ── coverage discovery is real, not a constant someone must remember ─────────

def test_the_two_reconciling_deploy_scripts_are_discovered(drift):
    """The array in each deploy.sh is the single source both the deploy and this
    check read. If the scan regex ever stops matching, coverage silently drops
    to zero and every graded resource becomes `marker-pending` — green. This is
    the test that makes that loud."""
    covered = drift.discover_reconciled_triggers()
    assert covered["scheduler:alpha-engine-alert-drain-0400utc"] == (
        "infrastructure/lambdas/alert-drain-dispatcher/deploy.sh")
    assert covered["events:alpha-engine-freshness-monitor-cron"] == (
        "infrastructure/lambdas/freshness-monitor/deploy.sh")
    assert len(covered) == 7


def test_every_reconciled_resource_is_declared_in_playbooks_yaml(drift, td, registry):
    """A deploy script that reconciles a resource playbooks.yaml does not
    declare would crash mid-deploy on the marker lookup. Catch it here."""
    index = td.resource_index(registry)
    for key in drift.discover_reconciled_triggers():
        assert key in index, f"{key} is reconciled by a deploy.sh but undeclared"


def test_all_four_drain_slots_are_codified(drift):
    """Measured 2026-08-28: deploy.sh codified only the 1000/2200 slots while
    0400 and 1600 existed live, so config#2902's zero-retry fix reached two of
    four (the other two still carried 185 attempts / 86400s) and no check in the
    repo could see it. The array is the fix; this pins it."""
    covered = drift.discover_reconciled_triggers()
    for slot in ("0400", "1000", "1600", "2200"):
        assert f"scheduler:alpha-engine-alert-drain-{slot}utc" in covered


# ── the deploy scripts actually expose the mode CI invokes ───────────────────

@pytest.mark.parametrize("lambda_dir", ["alert-drain-dispatcher", "freshness-monitor"])
def test_reconcile_triggers_is_a_real_mode_that_exits_without_deploying(lambda_dir):
    """Behaviour, not grep. ``--reconcile-triggers --dry-run`` must run to
    completion and print no code-deploy step — the nous-ergon-ops-I520 defect
    was a mode that applied its own effect and then FELL THROUGH into the full
    deploy because its block had no `exit`."""
    script = ROOT / "infrastructure" / "lambdas" / lambda_dir / "deploy.sh"
    env = dict(os.environ)
    # No credentials and no profile: the mode must complete on the strength of
    # the repo alone. The only live call it makes is a read-only existence
    # probe, wrapped in an `if`, so an AccessDenied or a missing `aws` binary
    # selects create-schedule rather than aborting.
    env.pop("AWS_PROFILE", None)
    env["AWS_REGION"] = "us-east-1"
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    proc = subprocess.run(
        ["bash", str(script), "--reconcile-triggers", "--dry-run"],
        capture_output=True, text=True, check=False, cwd=ROOT, env=env,
    )
    combined = proc.stdout + proc.stderr
    assert "Nothing else was touched" in combined, combined[-2000:]
    for forbidden in ("update-function-code", "Packaged ", "lambda_pip_install"):
        assert forbidden not in combined, f"{forbidden} ran under --reconcile-triggers"
