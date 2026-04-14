"""
Structured logging + Flow Doctor integration.

JSON mode activates when ALPHA_ENGINE_JSON_LOGS=1.
Text mode (default) preserves human-readable format for local dev.

Flow Doctor integration owns the single shared FlowDoctor instance for the
alpha-engine-data process. Entrypoints call ``setup_logging("data-collector")``
exactly once at startup; subsequent call sites retrieve the instance via
``get_flow_doctor()`` rather than re-initializing.

Flow Doctor activates only when FLOW_DOCTOR_ENABLED=1 (set on EC2 via the
DailyData / DataPhase1 SSM commands or the ``.alpha-engine.env``). Local
dev runs do not fire flow-doctor notifications. Logs at ERROR level or
above (including exceptions raised out of the hardened collectors) are
captured by the FlowDoctorHandler and dispatched per ``flow-doctor.yaml``.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

_FLOW_DOCTOR_YAML_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "flow-doctor.yaml"
)

# Singleton — populated once by setup_logging() and retrieved by call sites
# via get_flow_doctor(). None until setup_logging() runs with
# FLOW_DOCTOR_ENABLED=1, or if the init itself failed.
_fd_instance: Optional[object] = None


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "func": record.funcName,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exc"] = self.formatException(record.exc_info)
        if hasattr(record, "ctx"):
            entry["ctx"] = record.ctx
        return json.dumps(entry, default=str)


def get_flow_doctor():
    """Return the shared flow-doctor instance, or None if not initialized."""
    return _fd_instance


def _attach_flow_doctor(name: str) -> None:
    """Initialize the shared flow-doctor instance and attach a log handler.

    Mirrors the executor's integration (alpha-engine/executor/log_config.py).
    An ERROR-level log or an uncaught exception propagated through the
    logging system fires the FlowDoctorHandler, which dispatches per the
    yaml config (email + GitHub issue with dedup + rate limits).
    """
    global _fd_instance
    import flow_doctor
    _fd_instance = flow_doctor.init(config_path=_FLOW_DOCTOR_YAML_PATH)
    handler = flow_doctor.FlowDoctorHandler(_fd_instance, level=logging.ERROR)
    logging.getLogger().addHandler(handler)


def setup_logging(name: str = "data-collector") -> None:
    """
    Configure root logger for alpha-engine-data entrypoints.

    JSON mode: ALPHA_ENGINE_JSON_LOGS=1 (for EC2 / production)
    Text mode: default (for local dev / dry-run)
    Flow Doctor: FLOW_DOCTOR_ENABLED=1 (for EC2 / production)
    """
    json_mode = os.environ.get("ALPHA_ENGINE_JSON_LOGS", "0") == "1"
    handler = logging.StreamHandler()
    if json_mode:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            f"%(asctime)s %(levelname)s [{name}] %(message)s"
        ))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    if os.environ.get("FLOW_DOCTOR_ENABLED", "0") == "1":
        _attach_flow_doctor(name)
