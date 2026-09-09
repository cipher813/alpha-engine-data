"""The dated `research.db` backup has a writer again, and it cannot be lost.

`alpha-engine-config-I10202`: until 2026-07-12 one method wrote both
`research.db` and `backups/research_<date>.db`. Its caller was deleted, the
pointer acquired a new writer here and the dated backup acquired none, and the
freshness row named `research_db_backup` kept reading healthy because it
watched the pointer. 59 days, 407 MB, no surface reported it.

These tests pin the property that made the loss possible: the two keys must be
written by the SAME call. A test that only asserted "a dated backup is
uploaded" would pass again the day someone adds a second pointer-only writer.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from collectors.research_db_upload import (
    BACKUP_KEY_TEMPLATE,
    DB_KEY,
    backup_key,
    upload_research_db,
)

REPO = Path(__file__).resolve().parents[1]
COLLECTORS = REPO / "collectors"


class _RecordingS3:
    def __init__(self, fail_on: str | None = None):
        self.calls: list[tuple[str, str, str]] = []
        self._fail_on = fail_on

    def upload_file(self, path, bucket, key):
        if self._fail_on is not None and key == self._fail_on:
            raise RuntimeError(f"simulated S3 failure on {key}")
        self.calls.append((path, bucket, key))


def test_both_keys_are_written_by_one_call():
    s3 = _RecordingS3()
    out = upload_research_db(s3, "/tmp/research.db", "alpha-engine-research", "2026-09-12")
    assert [c[2] for c in s3.calls] == [DB_KEY, "backups/research_2026-09-12.db"]
    assert out == {
        "pointer_key": DB_KEY,
        "backup_key": "backups/research_2026-09-12.db",
    }


def test_a_failed_backup_upload_raises_rather_than_warning():
    """The pointer landing while the backup silently did not is the exact
    shape of the 59-day outage. A producer write that fails must fail the run
    (fleet rule: fail loud and fast, no graceful-degrade on a writer)."""
    s3 = _RecordingS3(fail_on="backups/research_2026-09-12.db")
    with pytest.raises(RuntimeError):
        upload_research_db(s3, "/tmp/research.db", "alpha-engine-research", "2026-09-12")


def test_a_failed_pointer_upload_raises_too():
    s3 = _RecordingS3(fail_on=DB_KEY)
    with pytest.raises(RuntimeError):
        upload_research_db(s3, "/tmp/research.db", "alpha-engine-research", "2026-09-12")


def test_backup_key_is_iso_dated():
    """ISO, not the pre-2026-07-11 compressed form: ARTIFACT_REGISTRY.yaml can
    only render `{date}` as YYYY-MM-DD, and a template that cannot render the
    key it watches reports UNMEASURED forever (alpha-engine-config-I10200)."""
    assert backup_key("2026-09-12") == "backups/research_2026-09-12.db"
    assert "{date}" in BACKUP_KEY_TEMPLATE


def test_backup_key_refuses_an_empty_run_date():
    with pytest.raises(ValueError):
        backup_key("")


def _uploads_research_db_directly(path: Path) -> list[int]:
    """Line numbers of `*.upload_file(..., "research.db")` calls in `path`."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    hits: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "upload_file"):
            continue
        for arg in node.args:
            if isinstance(arg, ast.Constant) and arg.value == DB_KEY:
                hits.append(node.lineno)
    return hits


def test_no_collector_writes_the_pointer_without_the_backup():
    """The regression guard. `research.db` may only be uploaded through
    `upload_research_db`; a direct `upload_file(db_path, bucket, "research.db")`
    anywhere in `collectors/` re-creates the divergence, because that call
    keeps the freshness row green while writing no dated object."""
    offenders: list[str] = []
    for path in sorted(COLLECTORS.rglob("*.py")):
        if path.name == "research_db_upload.py":
            continue  # the one owning writer
        for line in _uploads_research_db_directly(path):
            offenders.append(f"{path.relative_to(REPO)}:{line}")
    assert not offenders, (
        "these sites upload the research.db pointer directly, leaving the dated "
        "backup unwritten — route them through "
        "collectors.research_db_upload.upload_research_db "
        f"(alpha-engine-config-I10202): {offenders}"
    )
