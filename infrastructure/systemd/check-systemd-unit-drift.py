#!/usr/bin/env python3
"""check-systemd-unit-drift.py — Diff installed systemd units against the
repo copies in `infrastructure/systemd/` (config#2352).

**Background.** `daily-news.{service,timer}` and `metron-intraday.{service,
timer}` are version-tracked but only ever reach `/etc/systemd/system/` via a
manual (or, after config#2352's deploy-on-merge workflows/boot-pull sync)
install-script run. Both delivery paths can still drift from the repo in
practice:

  * daily-news: the deploy-on-merge workflow (deploy-daily-news-units.yml)
    could fail silently, or someone could hand-edit the on-box unit file
    without touching the repo.
  * metron-intraday: relies on the trading box picking up the repo copy at
    its next boot-pull run (2026-07-13 operator ruling on config#2352,
    "queue on merge, apply on next boot") — a box that stays up unusually
    long, or a boot-pull systemd-sync regression, could leave it stale
    for longer than the "pages within a day" acceptance bar.

This script is a same-box, read-only comparison — it runs AS a systemd timer
ON the box that owns the units (no cross-box SSM reach, no new IAM grant:
see the config#2352 PR description for why the alternative, a GHA-side SSM
hash-pull, would have needed a new SendCommand grant on the trading-box
instance that github-actions-lambda-deploy does not currently have).
Reports divergence via flow-doctor, mirroring every other on-box self-report
in this repo (e.g. scripts/run_daily_news_standalone.sh's fail-loud git
sync). Sibling in shape (repo-vs-live diff, exit 0/1/2) to
../step-functions/check-drift.py and check-definition-drift.py, but LOCAL
file compare instead of an AWS API call — there is no "live AWS state" for
a systemd unit, only "what's on this box's disk".

Usage:
  ./infrastructure/systemd/check-systemd-unit-drift.py               # every unit this box installs
  ./infrastructure/systemd/check-systemd-unit-drift.py --unit NAME   # one unit (e.g. daily-news.timer)
  ./infrastructure/systemd/check-systemd-unit-drift.py --report      # on divergence, also flow-doctor report

Exit codes: 0 clean, 1 drift found, 2 config/source error.

**Scope correction (2026-08-08, alpha-engine-config-I6656).** The above
describes four units. The dashboard box has **60** locally-installed unit
files, and this script's closing line read:

    Systemd unit drift check PASSED (installed units match repo).

That is a claim about "installed units" made after comparing four of them —
and not even the six this repo codifies, since its own
`systemd-unit-drift-check.{service,timer}` were absent from `ALL_UNITS`.
From outside, a check with narrow scope and a check with full coverage are
the same green line. Measured while raising `TimeoutStartSec` on
`morning-signal.service`: applied to a live box with nothing anywhere that
would notice it being reverted, hours after this check reported PASSED.

So the scope is now **every regular `.service` / `.timer` file in
`/etc/systemd/system`**, each classified `clean`, `drift` or `uncodified`.
Symlinks are skipped — they are systemd's own aliases into `/usr/lib/systemd`
(`dbus.service` and friends), not units anybody here authored.

Three rules follow:

  * **PASSED is reserved for full coverage.** With 54 of 60 units uncodified,
    no wording of "passed" is true, so the run states the shortfall instead.
  * **The known-uncodified set does not fail the run.** There are 54 of them;
    failing on the backlog would page once per unit on the first run and
    teach exactly one lesson, which is to disable the check. They live in
    `UNCODIFIED_BASELINE_FILE` — a work queue that should only shrink — and
    a unit uncodified but NOT in it is named individually as
    `uncodified-NEW`, because new is the only kind a given run can act on.
    `--strict` fails on anything uncodified and is the end state once the
    baseline is empty.
  * **The counts are emitted** (`--metric`), including zeros: a number that
    only appears when something is wrong cannot distinguish healthy from
    dead (`principles.md` §2.7).

"Codified" means a file of the same name under a `--codified-root` (default:
this script's directory). Units owned by other repos — `metron-*`,
`crucible-dash-*`, `nousergon-*`, `vires`, `telos-web`, `mnemon*` — are not
codified *from here* and sit in the baseline with their owning repo noted.
Teaching this script to read eight repo trees would move the ownership
question into a script instead of answering it; `infrastructure-ownership-
policy` answers it, and the baseline is the register of where that answer has
not been applied yet.
"""

from __future__ import annotations

import argparse
import hashlib
import socket
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent
INSTALLED_DIR = Path("/etc/systemd/system")
UNCODIFIED_BASELINE_FILE = SCRIPT_DIR / "uncodified-units-baseline.txt"

UNIT_SUFFIXES = (".service", ".timer")

#: CloudWatch namespace for the coverage counts. Shares the box-health
#: namespace deliberately — coverage of the unit set is a property of the box,
#: and a separate namespace is one more place to remember to look.
METRIC_NAMESPACE = "NousErgon/BoxHealth"

# The units this repo codifies and installs. NO LONGER THE SCOPE OF THE
# CHECK — scope is now whatever is installed (see the docstring). Kept
# because install-daily-news.sh / install-metron-intraday.sh still describe
# which pair belongs on which box (dashboard: daily-news; trading:
# metron-intraday), and because a codified unit missing from a box is
# reported rather than silently dropped from the comparison.
ALL_UNITS: tuple[str, ...] = (
    "daily-news.service",
    "daily-news.timer",
    "metron-intraday.service",
    "metron-intraday.timer",
)


def _sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except (FileNotFoundError, IsADirectoryError):
        return None


def installed_units(installed_dir: Path | None = None) -> list[str]:
    """Every locally-installed unit file, sorted.

    Regular files only. A symlink here is systemd's own alias mechanism
    pointing into `/usr/lib/systemd`; counting those would put `dbus.service`
    in our work queue.
    """
    d = installed_dir or INSTALLED_DIR
    if not d.is_dir():
        return []
    return sorted(
        p.name
        for p in d.iterdir()
        if p.name.endswith(UNIT_SUFFIXES) and p.is_file() and not p.is_symlink()
    )


def codified_units(roots: list[Path] | None = None) -> list[str]:
    """Every unit file present in the codified roots, sorted."""
    rs = roots or [SCRIPT_DIR]
    return sorted({
        p.name for r in rs if r.is_dir()
        for p in r.iterdir() if p.name.endswith(UNIT_SUFFIXES) and p.is_file()
    })


def load_baseline(path: Path | None = None) -> set[str]:
    """Unit names already known to be uncodified. Blank lines and `#`
    comments ignored; the comments carry which repo should own each one."""
    f = path or UNCODIFIED_BASELINE_FILE
    if not f.is_file():
        return set()
    out = set()
    for line in f.read_text().splitlines():
        name = line.split("#", 1)[0].strip()
        if name:
            out.add(name)
    return out


def codified_path(name: str, roots: list[Path] | None = None) -> Path | None:
    for root in (roots or [SCRIPT_DIR]):
        candidate = root / name
        if candidate.is_file():
            return candidate
    return None


def check_unit(
    name: str,
    roots: list[Path] | None = None,
    installed_dir: Path | None = None,
) -> tuple[str, str]:
    """Returns (status, detail).

    status in {"clean", "drift", "uncodified", "not-installed"}.

    `SCRIPT_DIR` / `INSTALLED_DIR` are read at CALL time when the arguments
    are omitted, so a caller (or a test) can repoint the module globals.

    Note the retired status: installed-with-no-repo-copy used to be
    `source-error`, i.e. a fault in this script's configuration. It is now
    `uncodified` — a gap in coverage, which is what it always was. 54 of the
    60 units on the dashboard box are in that state, and calling that a
    config error made the honest reading unavailable.

    `not-installed` means a codified unit this box does not host. A box
    legitimately runs a subset (dashboard: daily-news; trading:
    metron-intraday), so it is informational, never a finding.
    """
    d = installed_dir or INSTALLED_DIR
    installed_hash = _sha256(d / name)
    repo_path = codified_path(name, roots)

    if installed_hash is None:
        if repo_path is None:
            return "not-installed", f"{name}: neither installed here nor codified"
        return "not-installed", f"{name}: codified, not present on this box ({d / name})"

    if repo_path is None:
        return "uncodified", f"{name}: installed here, codified in no known root"

    repo_hash = _sha256(repo_path)
    if installed_hash != repo_hash:
        return "drift", f"{name}: installed ({installed_hash[:12]}) != repo ({repo_hash[:12]})"

    return "clean", f"{name}: OK"


def _emit_metrics(counts: dict) -> None:
    """Put the coverage counts to CloudWatch. Best-effort, loudly.

    Every value goes on every run, zeros included: `principles.md` §2.7 — a
    metric that only appears when something is wrong makes "healthy" and "the
    emitter is dead" the same shape on the graph.
    """
    try:
        import boto3

        host = socket.gethostname()
        boto3.client("cloudwatch").put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {
                    "MetricName": f"SystemdUnits{key}",
                    "Dimensions": [{"Name": "Host", "Value": host}],
                    "Value": float(value),
                    "Unit": "Count",
                }
                for key, value in counts.items()
            ],
        )
        print(f"[metric] emitted {len(counts)} coverage counts for {host}")
    except Exception as e:  # noqa: BLE001 - a metric failure must not mask the check
        # Recorded, never swallowed: this is the surface that says whether the
        # coverage number is real, so its absence has to be attributable.
        print(
            f"[check-systemd-unit-drift] METRIC EMIT FAILED — coverage is "
            f"UNOBSERVED: {e}",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", help="check a single unit by filename (e.g. daily-news.timer)")
    parser.add_argument(
        "--report",
        action="store_true",
        help="on a finding, also publish an alert (in addition to the exit code + stdout)",
    )
    parser.add_argument("--metric", action="store_true", help="emit the coverage counts to CloudWatch")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="treat ANY uncodified unit as a failure, not only ones outside the baseline",
    )
    parser.add_argument(
        "--codified-root",
        action="append",
        default=None,
        help="directory holding codified unit files (repeatable; default: this script's directory)",
    )
    args = parser.parse_args()

    roots = [Path(r).resolve() for r in args.codified_root] if args.codified_root else [SCRIPT_DIR]
    baseline = load_baseline()

    if args.unit:
        names = [args.unit]
    else:
        # The union: what is installed, plus anything codified that is not —
        # so a unit deleted from the box still appears rather than silently
        # dropping out of the comparison.
        names = sorted(set(installed_units()) | set(codified_units(roots)))

    buckets: dict[str, list[str]] = {"clean": [], "drift": [], "uncodified": [], "not-installed": []}
    details: dict[str, str] = {}
    for name in names:
        status, detail = check_unit(name, roots)
        buckets[status].append(name)
        details[name] = detail
        label = "uncodified-NEW" if (status == "uncodified" and name not in baseline) else status
        print(f"[{label}] {detail}")

    uncodified = buckets["uncodified"]
    uncodified_new = [n for n in uncodified if n not in baseline]
    installed_count = len(buckets["clean"]) + len(buckets["drift"]) + len(uncodified)

    print(
        "SUMMARY "
        f"installed={installed_count} "
        f"clean={len(buckets['clean'])} "
        f"drift={len(buckets['drift'])} "
        f"uncodified={len(uncodified)} "
        f"uncodified_new={len(uncodified_new)} "
        f"codified_not_installed={len(buckets['not-installed'])}"
    )

    if args.metric:
        _emit_metrics({
            "Installed": installed_count,
            "Clean": len(buckets["clean"]),
            "Drifted": len(buckets["drift"]),
            "Uncodified": len(uncodified),
            "UncodifiedNew": len(uncodified_new),
        })

    exit_code = 0
    findings: list[str] = []
    if buckets["drift"]:
        findings.extend(details[n] for n in buckets["drift"])
        exit_code = max(exit_code, 1)
    if uncodified_new:
        findings.append(
            f"{len(uncodified_new)} unit(s) installed but codified nowhere and absent from the "
            f"baseline: {', '.join(uncodified_new)}"
        )
        if args.strict:
            exit_code = max(exit_code, 1)
    if uncodified and args.strict:
        exit_code = max(exit_code, 1)

    if findings:
        print(f"FINDINGS: {len(findings)}", file=sys.stderr)
        for f in findings:
            print(f"  {f}", file=sys.stderr)
        if args.report:
            _report_drift(findings)

    # PASSED is reserved for a box where every installed unit is codified AND
    # matches. Anything else states the shortfall: there is no phrasing of
    # "passed" that is true while 54 units sit outside the comparison.
    if not installed_count:
        print("No unit files installed on this box — nothing to compare.")
    elif not buckets["drift"] and not uncodified:
        print("Systemd unit coverage PASSED (every installed unit is codified and matches).")
    else:
        print(
            f"Systemd unit coverage INCOMPLETE: {len(buckets['clean'])}/{installed_count} "
            f"installed units are codified and clean; {len(uncodified)} uncodified, "
            f"{len(buckets['drift'])} drifted. This is not a pass."
        )

    return exit_code


def _report_drift(findings: list[str]) -> None:
    """Alert on detected systemd unit drift.

    This used to construct flow-doctor by hand and had been BROKEN for an
    unknown length of time (alpha-engine-config-I4509). Two independent faults,
    either one fatal:

      1. `flow_doctor.init()` does not exist. flow-doctor 0.8.7 exports
         FlowDoctor / FlowDoctorBuilder and no `init`, so the call raised
         AttributeError every time.
      2. The env hydration was incomplete anyway -- flow-doctor.yaml references
         more ${VAR}s than were being set, so even with (1) fixed, construction
         raises ConfigError.

    Neither was noticed because this function only runs when drift is FOUND,
    and the daily check normally passes -- the same reason boot-pull's copy of
    this bug survived. A reporting path that is only exercised on failure needs
    a test that exercises failure; see the accompanying test module.

    `krepis.alerts` is the canonical alert CLI (config#1649). It resolves its
    own secrets, so there is no hydration list here to drift out of sync with
    flow-doctor.yaml.
    """
    message = (
        f"systemd unit drift on {socket.gethostname()}: "
        f"installed units no longer match the repo -- {'; '.join(findings)}"
    )
    try:
        from krepis.alerts import publish

        publish(
            message=message,
            severity="error",
            source="check-systemd-unit-drift",
            # Dedup on the FINDINGS, not the message: the same drift persisting
            # alerts once a day, while a new finding changes the key and pages.
            dedup_key="unit-drift-" + hashlib.sha256(
                "|".join(sorted(findings)).encode()
            ).hexdigest()[:16],
            dedup_window_min=1440,
        )
    except Exception as e:
        # Fail LOUD. A silent failure here is the exact defect this rewrite
        # fixes: the drift was detected and the telling threw.
        print(
            f"[check-systemd-unit-drift] ALERT PUBLISH FAILED — drift is "
            f"UNREPORTED: {e}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    sys.exit(main())
