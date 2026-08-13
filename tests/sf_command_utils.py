"""Re-export shim — the parser now lives in ``infrastructure/sf_commands.py``.

It moved because it stopped being test-only: ``infrastructure/preflight_sweep_stages.py``
derives the daily sweep's stage list, and each stage's exact shell command,
from ``infrastructure/step_function.json`` in PRODUCTION. Two copies of an ASL
command parser is exactly the fork ``policy-shared-code`` exists to prevent, so
the implementation moved and this module keeps the historical import path
working for the wiring tests that already use it.

``extract_commands`` behaviour is unchanged — a ``States.Format`` element still
renders as its template string with ``{}`` intact, which is what the wiring
tests' substring and ordering assertions depend on. The stricter
``render_commands`` added alongside it is new surface, not a change to this one.

Import ``infrastructure.sf_commands`` directly in new code.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from infrastructure.sf_commands import (  # noqa: E402,F401  (re-export)
    UnresolvedReference,
    _split_top_level,
    _unescape_asl,
    extract_commands,
    render_commands,
)

__all__ = [
    "UnresolvedReference",
    "extract_commands",
    "render_commands",
    "_split_top_level",
    "_unescape_asl",
]
