"""Handler tests for the WeeklyPreflight pre-spend gate.

Why this file exists (2026-08-10): it did not, and `_shared/run_handler_tests.sh`
returns 0 when a lambda has no `test_handler.py`, so BOTH pre-merge gates —
ci.yml's glob step and deploy.sh's pytest gate — reported green on a Lambda
that could not execute a single line of its own gate logic. The first real
Saturday invocation returned
``ModuleNotFoundError: No module named 'nousergon_lib'`` and halted
ne-weekly-freshness-pipeline.

These tests stub ``sf_preflight`` in ``sys.modules``, so they pin the
handler's CONTRACT (which capability profile it asks for, how it classifies
skips, that an all-skipped run is not a pass). They deliberately cannot catch
a packaging gap — that is the job of
``tests/test_sf_preflight.py::test_lambda_profile_imports_are_packaged`` and of
deploy.sh's post-deploy smoke invoke against the real runtime.
"""

import os
import sys
import types
import unittest
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@dataclass
class _Result:
    name: str
    status: str
    message: str = ""
    details: dict = field(default_factory=dict)
    elapsed_seconds: float = 0.0


def _install_stub(results, n_fail=None, raises=None):
    """Install a stub sf_preflight module and return the recorded call kwargs."""
    recorded = {}
    stub = types.ModuleType("sf_preflight")
    stub.LAMBDA_CAPABILITIES = frozenset({"aws"})
    stub.FULL_CAPABILITIES = frozenset({"aws", "arctic", "repo_modules", "checkout", "polygon"})

    def run_preflight(bucket=None, capabilities=None, run_date=None, skip_flags=None):
        recorded["bucket"] = bucket
        recorded["capabilities"] = capabilities
        # alpha-engine-config-I7443: the handler forwards the SF execution
        # input so check_skip_flag_artifact_coherence can verify each skip
        # claim before spot spend. Recorded so the forwarding is asserted,
        # not assumed.
        recorded["run_date"] = run_date
        recorded["skip_flags"] = skip_flags
        if raises is not None:
            raise raises
        fails = n_fail if n_fail is not None else sum(1 for r in results if r.status == "fail")
        return fails, results

    stub.run_preflight = run_preflight
    sys.modules["sf_preflight"] = stub
    return recorded


class WeeklyPreflightHandlerTests(unittest.TestCase):
    def tearDown(self):
        sys.modules.pop("sf_preflight", None)
        sys.modules.pop("index", None)

    def _handler(self):
        sys.modules.pop("index", None)
        import index
        return index.handler

    def test_requests_the_lambda_capability_profile(self):
        """The gate must NOT run the full laptop/spot profile.

        Running it is not a degraded gate: check_arctic_connectivity and
        check_tool_contracts return status="fail" in a Lambda by
        construction, so the pipeline halts however healthy the system is.
        """
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        out = self._handler()({}, None)
        self.assertEqual(recorded["capabilities"], frozenset({"aws"}))
        self.assertEqual(out["status"], "OK")
        self.assertFalse(out["has_violation"])

    def test_skips_are_not_violations(self):
        recorded = _install_stub([
            _Result("sf_iam_reachability", "ok"),
            _Result("arctic_connectivity", "skip", "Not run: ... arctic"),
            _Result("tool_contracts", "skip", "Not run: ... checkout"),
        ])
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "OK")
        self.assertFalse(out["has_violation"])
        self.assertEqual(out["skip_count"], 2)
        self.assertEqual(out["ran_count"], 1)
        self.assertEqual(recorded["capabilities"], frozenset({"aws"}))

    def test_all_skipped_is_an_error_not_a_pass(self):
        """Zero checks run is an unobserved gate, never a green one."""
        _install_stub([
            _Result("arctic_connectivity", "skip"),
            _Result("tool_contracts", "skip"),
        ])
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "ERROR")
        self.assertTrue(out["has_violation"])
        self.assertIn("0 checks", out["error"])

    def test_real_failure_still_halts(self):
        _install_stub([
            _Result("sf_iam_reachability", "fail", "role cannot invoke"),
            _Result("arctic_connectivity", "skip"),
        ])
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "FAIL")
        self.assertTrue(out["has_violation"])
        self.assertEqual(out["failures"], ["sf_iam_reachability"])

    def test_run_preflight_raising_is_reported_as_error(self):
        """The 2026-08-10 shape: a missing dependency in the PROLOGUE."""
        _install_stub([], raises=ModuleNotFoundError("No module named 'nousergon_lib'"))
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "ERROR")
        self.assertTrue(out["has_violation"])
        self.assertIn("nousergon_lib", out["error"])

    def test_bucket_override_from_event(self):
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        self._handler()({"bucket": "some-other-bucket"}, None)
        self.assertEqual(recorded["bucket"], "some-other-bucket")


class WeeklyPreflightExecutionInputForwardingTests(unittest.TestCase):
    """alpha-engine-config-I7443.

    The SF Task invokes this Lambda with no explicit Payload, so the whole
    state input arrives as ``event`` — run_date and every skip_* flag are
    already present and were simply discarded. Forwarding them is what lets
    the pre-spend gate reject an incoherent recovery input (a skip claim
    with no artifact for that run_date) in seconds, instead of the in-SF
    guard catching it after a spot dispatch and ~18 minutes.
    """

    def tearDown(self):
        sys.modules.pop("sf_preflight", None)
        sys.modules.pop("index", None)

    def _handler(self):
        sys.modules.pop("index", None)
        import index
        return index.handler

    def test_run_date_and_skip_flags_reach_run_preflight(self):
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        event = {
            "run_date": "2026-08-16",
            "skip_predictor_training": True,
            "skip_scanner": True,
            "skip_aggregate_costs": False,
            "pipeline_role": "watch-rerun",
            "sns_topic_arn": "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
        }
        self._handler()(event, None)
        self.assertEqual(recorded["run_date"], "2026-08-16")
        self.assertEqual(
            recorded["skip_flags"],
            {
                "skip_predictor_training": True,
                "skip_scanner": True,
                "skip_aggregate_costs": False,
            },
        )

    def test_non_skip_keys_are_not_forwarded_as_skip_flags(self):
        """Only skip_* keys — pipeline_role and sns_topic_arn are not claims."""
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        self._handler()(
            {"run_date": "2026-08-16", "pipeline_role": "watch-rerun"}, None
        )
        self.assertEqual(recorded["skip_flags"], {})

    def test_bare_event_forwards_nothing_and_still_passes(self):
        """A bare {} test invoke must keep working. An absent payload is
        'nothing claimed', never a violation — this gate must not begin
        halting the pipeline over a shape it previously ignored."""
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        out = self._handler()({}, None)
        self.assertIsNone(recorded["run_date"])
        self.assertEqual(recorded["skip_flags"], {})
        self.assertEqual(out["status"], "OK")
        self.assertFalse(out["has_violation"])


if __name__ == "__main__":
    unittest.main()
