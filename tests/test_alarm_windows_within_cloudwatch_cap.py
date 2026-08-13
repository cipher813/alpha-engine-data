"""No `put-metric-alarm` in this repo may declare a window CloudWatch rejects.

WHY
---
CloudWatch enforces `EvaluationPeriods * Period <= 604800` for any alarm with
`period >= 3600`. A declaration that violates it is not a degraded alarm — the
API call fails with a ValidationError, so **the alarm does not exist**, and
under `set -euo pipefail` every alarm declared after it in the same script is
never applied either.

Nothing reports that. The component reads as monitored because a file in the
repo says it is.

Measured 2026-08-12: `setup_horizon_grading_alarms.sh` declared both of its
alarms at `Period=604800, EvaluationPeriods=3` (1,814,400). Neither alarm
existed in the account, and the script had been failing on its first call for
as long as the declaration had been there. The same cap took out four of six
alarms on the router degraded-mode drill in nous-ergon-ops the same day,
including that component's absence detector.

This is a source-text assertion because the alarms are created by shell scripts
run against a live account; what it pins is that the declaration is applicable
at all.
"""

import re
from pathlib import Path

INFRA = Path(__file__).resolve().parents[1] / "infrastructure"

MAX_WINDOW_SECONDS = 604800
CAP_APPLIES_AT_PERIOD = 3600

_CALL = re.compile(r"put-metric-alarm(.*?)(?=put-metric-alarm|\Z)", re.S)
_PERIOD = re.compile(r"--period[= ]+\"?(\d+)")
_EVAL = re.compile(r"--evaluation-periods[= ]+\"?(\d+)")
_NAME = re.compile(r"--alarm-name[= ]+\"?([^\"\s\\]+)")


def _declarations():
    for path in sorted(INFRA.rglob("*.sh")):
        text = path.read_text(errors="ignore")
        if "put-metric-alarm" not in text:
            continue
        for chunk in _CALL.findall(text):
            period, evals = _PERIOD.search(chunk), _EVAL.search(chunk)
            if not period or not evals:
                continue
            name = _NAME.search(chunk)
            yield (path.name, name.group(1) if name else "<unnamed>",
                   int(period.group(1)), int(evals.group(1)))


def test_every_alarm_declares_a_window_cloudwatch_will_accept():
    declarations = list(_declarations())
    assert declarations, (
        "No put-metric-alarm declaration found under infrastructure/. This "
        "assertion is here so the test cannot pass vacuously if the alarm "
        "scripts move or change shape."
    )
    for script, name, period, evals in declarations:
        if period < CAP_APPLIES_AT_PERIOD:
            continue
        assert period * evals <= MAX_WINDOW_SECONDS, (
            f"{script}: alarm {name} declares period={period} x "
            f"evaluation-periods={evals} = {period * evals}s, over the "
            f"{MAX_WINDOW_SECONDS}s cap that applies at period >= "
            f"{CAP_APPLIES_AT_PERIOD}. put-metric-alarm will reject this, so "
            f"the alarm will not exist and every alarm after it in "
            f"{script} will never be applied."
        )
