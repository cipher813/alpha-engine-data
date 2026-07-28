"""Every SF SNS Subject must render inside SNS's 100-character limit.

Bug class this guards (found by the 2026-07-27 rehearsal,
offcycle-shell-20260727-155656): `PublishPredictorFailureImmediate` rendered
105 characters and SNS rejected the publish outright::

    SNS.InvalidParameterException: Invalid parameter: Subject

The predictor branch had genuinely failed, and the alert designed to tell us
so **could not send**. That is the worst shape a defect can take: the failure
is real, and the thing whose entire job is to surface it fails silently
alongside it.

All three offenders were the ``*FailureImmediate`` early-warning notifiers —
exactly the states that only ever run when something is already wrong, and so
are the least likely to be exercised in a healthy run.

The limit is on the RENDERED string, not the template. These Subjects are
``States.Format`` templates interpolating ``$.pipeline_label``, which is empty
on a normal weekly run but ``" Preflight"`` on a shell run — ten characters
that pushed one Subject over. A template that looks fine in the repo can still
fail live, so this test renders with the longest known label.

Note: em-dashes are fine. An earlier pass of this analysis flagged all 14
Subjects as non-ASCII violations; that was wrong — ``HandleFailure`` (49 chars,
em-dash) published successfully in the same run. Length is the operative
constraint.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

# SNS caps Subject at 100 characters. Held one below to leave no boundary doubt.
_SNS_SUBJECT_MAX = 99

# Longest value $.pipeline_label takes. "" on a cadence run, " Preflight" on a
# shell run — the variant that actually broke.
_LONGEST_PIPELINE_LABEL = " Preflight"

_SF_JSON = Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json"
_FORMAT_RE = re.compile(r"States\.Format\('((?:[^'\\]|\\.)*)'")


def _collect_subjects(states: dict, out: list | None = None) -> list[tuple[str, str]]:
    """(state_name, rendered_subject) for every SNS publish, at any nesting."""
    if out is None:
        out = []
    for name, body in states.items():
        params = body.get("Parameters") or {}
        subject = params.get("Subject") or params.get("Subject.$")
        if subject:
            match = _FORMAT_RE.search(subject)
            literal = match.group(1) if match else subject.strip("'")
            out.append((name, literal.replace("{}", _LONGEST_PIPELINE_LABEL)))
        if body.get("Type") == "Parallel":
            for branch in body.get("Branches", []):
                _collect_subjects(branch["States"], out)
        for key in ("Iterator", "ItemProcessor"):
            if key in body:
                _collect_subjects(body[key]["States"], out)
    return out


_SUBJECTS = _collect_subjects(json.loads(_SF_JSON.read_text(encoding="utf-8"))["States"])


def test_subjects_were_found():
    """Guard the guard — a broken collector must not silently pass."""
    assert _SUBJECTS, "no SNS Subjects found in the weekly SF definition"


@pytest.mark.parametrize(
    "state,subject", _SUBJECTS, ids=[name for name, _ in _SUBJECTS]
)
def test_subject_fits_sns_limit(state: str, subject: str):
    assert len(subject) <= _SNS_SUBJECT_MAX, (
        f"{state}: Subject renders to {len(subject)} chars with "
        f"pipeline_label={_LONGEST_PIPELINE_LABEL!r}, over SNS's 100-char limit — "
        f"the publish fails with InvalidParameterException and the notification "
        f"is never sent. Shorten the template; detail belongs in Message, which "
        f"has no such limit.\n  {subject}"
    )


@pytest.mark.parametrize(
    "state,subject", _SUBJECTS, ids=[name for name, _ in _SUBJECTS]
)
def test_subject_has_no_newlines_or_control_characters(state: str, subject: str):
    """SNS also rejects line feeds and control characters in Subject."""
    offenders = [c for c in subject if c == "\n" or c == "\r" or ord(c) < 32]
    assert not offenders, (
        f"{state}: Subject contains newline/control characters {offenders!r}, "
        f"which SNS rejects"
    )
