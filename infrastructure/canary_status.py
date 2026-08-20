"""Classify a Lambda invoke response for deploy.sh's post-deploy canary.

Extracted from ``deploy.sh``'s inline ``python3 -c ...`` (2026-08-20,
alpha-engine-config-I7855) so the classification logic is unit-testable and
the Lambda-native runtime-error envelope isn't silently swallowed.

Detection blindness this closes: an init-time crash (e.g. a ``ConfigError``
raised at module import, before the handler's own try/except can run) makes
Lambda return its OWN error shape —
``{"errorMessage": "...", "errorType": "...", "stackTrace": [...]}`` — which
carries none of this handler's own contract keys (``status``, ``statusCode``,
``error``). The old parser fell through every branch to
``d.get('error', 'UNKNOWN')``, so the canary reported the uninformative
literal ``UNKNOWN`` and the real diagnosis (the ConfigError's message) was
discarded — exactly what happened to alpha-engine-data-collector v341's
rollback: CloudWatch had the real error, the canary output did not.
"""
from __future__ import annotations

import json
import sys


def classify(payload: dict) -> str:
    """Return a short classification string for a canary invoke response.

    Checked in order:
      1. The handler's own contract: ``status`` in (OK, SKIPPED) passes.
      2. ``statusCode == 500`` -> ``ENV_ERROR`` (handler's own error shape).
      3. Lambda's NATIVE runtime-error envelope (``errorMessage``/
         ``errorType`` — present on any init-time or unhandled-exception
         crash, regardless of what the handler's own contract defines) ->
         surfaced verbatim as ``"{errorType}: {errorMessage}"`` so the
         actual cause reaches deploy.sh's stdout instead of ``UNKNOWN``.
      4. The handler's own free-form ``error`` field, else the literal
         ``UNKNOWN``.
    """
    status = payload.get("status", "")
    if status in ("OK", "SKIPPED"):
        return status
    if payload.get("statusCode") == 500:
        return "ENV_ERROR"
    if "errorMessage" in payload or "errorType" in payload:
        error_type = payload.get("errorType", "UnknownError")
        error_message = payload.get("errorMessage", "")
        return f"{error_type}: {error_message}"
    return str(payload.get("error", "UNKNOWN"))


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: canary_status.py <path-to-invoke-response.json>", file=sys.stderr)
        return 2
    try:
        with open(sys.argv[1]) as f:
            payload = json.load(f)
    except Exception:
        print("PARSE_ERROR")
        return 0
    print(classify(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
