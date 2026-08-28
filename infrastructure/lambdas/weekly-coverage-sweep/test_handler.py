"""The coverage-sweep handler's OUTCOME ROUTING, against stubbed lib calls.

Named ``test_handler.py`` because that is the only filename either gate looks
for: ``.github/workflows/ci.yml`` globs ``infrastructure/lambdas/*/test_handler.py``
pre-merge, and ``_shared/run_handler_tests.sh`` returns 0 for a lambda that has
none.

**What is under test, and what deliberately is not.** The sweep itself —
coverage derivation, the cycle union, the marker merge — is
``nousergon_lib.pipeline_status``'s, tested there against captured live
executions. What lives HERE and nowhere else is the mapping from what the
sweep did to the three outcomes the state machine routes on, and that mapping
has exactly one property worth pinning: **``unavailable`` is never collapsed
into either of the others.** A sweep that could not run, and a sweep that ran
and found nothing, are different facts; only the second means the coverage
surface is observed. Every test below is that property from a different angle.

The lib is stubbed via ``sys.modules`` rather than installed: the tests must
run on a bare deploy runner that has not pulled the git-only dependency, and
stubs take precedence over anything installed (``run_handler_tests.sh``).
"""
from __future__ import annotations

import sys
import types

import pytest


def _install_stubs(
    *,
    sweep=None,
    read_raises: Exception | None = None,
    publish_raises: Exception | None = None,
    augment_raises: Exception | None = None,
    alerts_raises: Exception | None = None,
    calls: dict | None = None,
):
    """Stub boto3, krepis and nousergon_lib in sys.modules; return the module."""
    calls = calls if calls is not None else {}

    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *a, **k: object()
    sys.modules["boto3"] = boto3

    krepis = types.ModuleType("krepis")
    region_mod = types.ModuleType("krepis.aws_region")
    region_mod.resolve_region = lambda: "us-east-1"
    alerts_mod = types.ModuleType("krepis.alerts")

    def _publish(*a, **k):
        calls["alerted"] = calls.get("alerted", 0) + 1
        if alerts_raises:
            raise alerts_raises

    alerts_mod.publish = _publish
    krepis.alerts = alerts_mod
    krepis.aws_region = region_mod
    sys.modules["krepis"] = krepis
    sys.modules["krepis.alerts"] = alerts_mod
    sys.modules["krepis.aws_region"] = region_mod

    nl = types.ModuleType("nousergon_lib")
    ps = types.ModuleType("nousergon_lib.pipeline_status")
    cov = types.ModuleType("nousergon_lib.pipeline_status.coverage")
    cm = types.ModuleType("nousergon_lib.pipeline_status.completion_marker")

    def _read(**kwargs):
        # alpha-engine-config-I8809: recorded so a test can assert BOTH date
        # families reach the reader — the whole point of the migration window.
        calls["read_kwargs"] = kwargs
        if read_raises:
            raise read_raises
        return sweep

    def _publish_sweep(*a, **k):
        calls["published"] = calls.get("published", 0) + 1
        if publish_raises:
            raise publish_raises

    def _augment(*a, **k):
        calls["augmented"] = calls.get("augmented", 0) + 1
        calls["augment_kwargs"] = k
        if augment_raises:
            raise augment_raises

    cov.read_coverage_sweep = _read
    cov.publish_sweep = _publish_sweep
    cm.augment_marker = _augment
    sys.modules["nousergon_lib"] = nl
    sys.modules["nousergon_lib.pipeline_status"] = ps
    sys.modules["nousergon_lib.pipeline_status.coverage"] = cov
    sys.modules["nousergon_lib.pipeline_status.completion_marker"] = cm

    sys.modules.pop("index", None)
    import index

    return index, calls


class _Sweep:
    def __init__(
        self,
        *,
        should_alert: bool,
        cycle=object(),
        partitions_read=("2026-08-21", "2026-08-22"),
        legacy_partition_rows=0,
    ):
        self.should_alert = should_alert
        self.cycle = cycle
        # alpha-engine-config-I8809: the sweep now reports which date
        # partitions it unioned. The handler threads both onto its result and
        # into augment_marker, so a stub without them makes every outcome
        # `unavailable` — which is exactly what the real handler does with a
        # nousergon-lib pin predating the field, and why the pin floor is
        # asserted in tests/test_weekly_partition_family_contract.py.
        self.partitions_read = partitions_read
        self.legacy_partition_rows = legacy_partition_rows

    def explain(self):
        return "sweep says so"


@pytest.fixture(autouse=True)
def _clean():
    yield
    for name in list(sys.modules):
        if name.startswith(("nousergon_lib", "krepis")) or name in ("boto3", "index"):
            sys.modules.pop(name, None)


def test_a_clean_sweep_is_clean_and_augments_the_marker():
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert out["marker_augmented"] is True
    assert calls.get("alerted") is None, "a clean sweep must not page"


def test_a_finding_is_findings_and_pages_once():
    index, calls = _install_stubs(sweep=_Sweep(should_alert=True))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_FINDINGS
    assert calls["alerted"] == 1


def test_a_sweep_that_cannot_run_is_unavailable_never_clean():
    """The whole point. A crash reading the cycle means the coverage surface is
    UNOBSERVED for this run — rendering that as clean is principles.md §2.7's
    'no data rendered green'."""
    index, _ = _install_stubs(read_raises=RuntimeError("AccessDenied"))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_UNAVAILABLE
    assert "AccessDenied" in out["reason"]


def test_a_sweep_that_ran_but_could_not_publish_is_unavailable():
    """It ran, but nothing downstream can read what it found — including the
    marker, which keeps its bare envelope claim. Unobserved, not clean."""
    index, _ = _install_stubs(
        sweep=_Sweep(should_alert=False), publish_raises=OSError("no such bucket")
    )
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_UNAVAILABLE
    assert "could not publish" in out["reason"]


def test_a_missing_run_date_is_unavailable_not_clean():
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({}, None)
    assert out["outcome"] == index.OUTCOME_UNAVAILABLE


def test_an_unreadable_cycle_leaves_the_marker_alone_and_still_reports():
    """`--augment-marker` with no cycle: the marker keeps cycle_verdict unknown,
    which resolves to UNKNOWN downstream. The sweep itself still ran, so the
    outcome is its own finding — not unavailable."""
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False, cycle=None))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert out["marker_augmented"] is False
    assert calls.get("augmented") is None


def test_a_failed_page_does_not_turn_a_finding_into_a_clean_result():
    index, _ = _install_stubs(
        sweep=_Sweep(should_alert=True), alerts_raises=RuntimeError("SNS down")
    )
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_FINDINGS


def test_dry_run_reports_the_real_outcome_not_a_hardcoded_clean():
    """A rehearsal that reports green whatever it saw certifies nothing.

    Measured 2026-08-22 on the FIRST live dry invocation: the sweep found 28
    absent verdicts and 1 finding, and this branch returned `outcome: clean`
    anyway. That is the same "no data rendered as healthy" defect
    (principles.md 2.7) the whole sweep exists to detect, shipped inside the
    detector. What dry_run withholds is the WRITES and the page — never the
    verdict.
    """
    index, calls = _install_stubs(sweep=_Sweep(should_alert=True))
    out = index.handler({"run_date": "2026-08-22", "dry_run": True}, None)
    assert out["outcome"] == index.OUTCOME_FINDINGS
    assert out["dry_run"] is True
    assert calls.get("published") is None, "a dry run must not write"
    assert calls.get("augmented") is None, "a dry run must not touch the marker"
    assert calls.get("alerted") is None, "a dry run must not page"


def test_dry_run_writes_nothing():
    """The Friday-PM preflight exercises the read path and every IAM grant it
    needs, and must not touch the marker or the artifact."""
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({"run_date": "2026-08-22", "dry_run": True}, None)
    assert out["dry_run"] is True
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert calls.get("published") is None
    assert calls.get("augmented") is None


def test_the_handler_never_raises_on_any_stub_failure():
    """An observe-only tail downstream of the success terminal must not be able
    to fail a completed weekly run (sf-pipeline-policy §2.1)."""
    for kwargs in (
        {"read_raises": RuntimeError("boom")},
        {"sweep": _Sweep(should_alert=False), "publish_raises": RuntimeError("boom")},
        {"sweep": _Sweep(should_alert=False), "augment_raises": RuntimeError("boom")},
        {"sweep": _Sweep(should_alert=True), "alerts_raises": RuntimeError("boom")},
    ):
        index, _ = _install_stubs(**kwargs)
        out = index.handler({"run_date": "2026-08-22"}, None)
        assert out["outcome"] in {
            index.OUTCOME_CLEAN,
            index.OUTCOME_FINDINGS,
            index.OUTCOME_UNAVAILABLE,
        }


# ── alpha-engine-config-I8809 ────────────────────────────────────────────────


def test_the_legacy_partition_is_threaded_into_the_reader():
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    index.handler(
        {"run_date": "2026-08-28", "calendar_date": "2026-08-29"}, None
    )
    assert calls["read_kwargs"]["run_date"] == "2026-08-28"
    assert calls["read_kwargs"]["calendar_date"] == "2026-08-29"


def test_no_calendar_date_is_a_single_partition_sweep_not_an_error():
    """The post-cutover shape, and any caller that predates the field."""
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({"run_date": "2026-08-28"}, None)
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert calls["read_kwargs"]["calendar_date"] is None


def test_the_result_says_which_partitions_it_unioned():
    index, _ = _install_stubs(
        sweep=_Sweep(
            should_alert=False,
            partitions_read=("2026-08-28", "2026-08-29"),
            legacy_partition_rows=28,
        )
    )
    out = index.handler(
        {"run_date": "2026-08-28", "calendar_date": "2026-08-29"}, None
    )
    assert out["partitions_read"] == ["2026-08-28", "2026-08-29"]
    assert out["legacy_partition_rows"] == 28


def test_the_marker_is_augmented_in_every_partition_that_was_read():
    index, calls = _install_stubs(
        sweep=_Sweep(should_alert=False, partitions_read=("2026-08-28", "2026-08-29"))
    )
    index.handler({"run_date": "2026-08-28", "calendar_date": "2026-08-29"}, None)
    assert calls["augment_kwargs"]["also_dates"] == ("2026-08-28", "2026-08-29")
