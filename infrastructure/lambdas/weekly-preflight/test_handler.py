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

    def run_preflight(bucket=None, capabilities=None):
        recorded["bucket"] = bucket
        recorded["capabilities"] = capabilities
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


if __name__ == "__main__":
    unittest.main()
