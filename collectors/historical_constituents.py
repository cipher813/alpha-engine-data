"""
historical_constituents.py — Point-in-time S&P 500 index membership (G12).

Survivorship-bias mitigation, Phase 1 (research memo
``nousergon-docs/survivorship-bias-research.md``): the backtester/predictor
today see only *currently-listed* constituents, so 10y synthetic backtests
silently exclude every name that was delisted, acquired, or index-dropped —
an upward (survivor) bias on backtest credibility (~1-4%/yr overstatement
class).

This module reconstructs as-of-date membership by **replaying index changes
backward from today's roster**: each change (ticker *added* on date D, ticker
*removed* on date D) is undone walking from newest to oldest, so the membership
set immediately *before* date D is recovered. The output is a
``{date: [tickers]}`` map the backtester reads to define the point-in-time
universe for each backtest date.

WHERE THOSE CHANGES COME FROM (alpha-engine-config-I6946). Originally, all of
them were scraped from Wikipedia's "Selected changes to the list" table on
every run. On 2026-08-11 an editor moved that table to a different article and
the weekly pipeline failed three hours later — a load-bearing backtest input
taken down by a formatting edit. Two producers replaced it, split at
``SNAPSHOT_CUTOVER``:

  * **Before the cutover** — ``collectors/data/sp500_changes_frozen_*.json``,
    a committed, content-hashed reconstruction. 1976-2026 index changes are
    settled; re-deriving them weekly from an editable page re-ran the risk to
    re-learn facts that had not moved.
  * **On and after** — a diff of our own dated roster snapshots at
    ``market_data/weekly/{date}/constituents.json``, themselves sourced from
    SSGA's SPY holdings (config#2812). Membership is OBSERVED, not replayed.

Wikipedia is still fetched, but only to ATTEST the post-cutover window — never
to produce it. A page that moves, 404s or is vandalised now costs an
attestation and a WARNING, not the pipeline. Disagreement between the two
derivations is itself the signal worth having: it means a bad upstream edit or
a gap in our own collection.

Layers, deliberately separated so the risky parsing is unit-tested with no
network:
  * ``parse_changes_table(df)`` — Wikipedia changes DataFrame -> list of
    structured ``ConstituentChange`` events (pure). Attestation only.
  * ``changes_from_snapshots(snapshots)`` -> the same list type, from observed
    dated rosters (pure).
  * ``load_frozen_changes()`` — the hash-verified pre-cutover history.
  * ``build_pit_membership(current_tickers, changes)`` -> ``{date: [tickers]}``
    point-in-time map (pure).
  * ``divergences(observed, reference, since=...)`` — the attestation (pure).
  * ``collect(...)`` — read + build + write to S3 (the I/O shell).

S&P 500 only (the changes table on the S&P 400 page is sparser); the memo
flags S&P 400 mid-cap as a follow-on. Delisted-ticker *prices* are memo
Phase 2 — out of scope here; this ships the membership list.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

#: First date with a dated roster snapshot at
#: ``market_data/weekly/{date}/constituents.json``. On and after this date
#: membership is DIRECTLY OBSERVED and no wiki is consulted to produce it;
#: before it, history comes from the frozen artifact below.
SNAPSHOT_CUTOVER = "2026-04-04"

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: Index changes from 1976 to the cutover are settled history — they do not
#: change, so re-deriving them from an editable wiki on every weekly run buys
#: nothing and costs an outage when the page moves (alpha-engine-config-I6944,
#: 2026-08-11). Reconstructed once, hashed, and committed.
_FROZEN_CHANGES_PATH = (
    Path(__file__).resolve().parent / "data" / "sp500_changes_frozen_pre_2026_04_04.json"
)

#: Ticker changes a holdings diff cannot tell apart from index churn, and
#: which Polygon does not report as ``ticker_change``. Same reasoning as the
#: frozen history: a settled fact, written down once.
_KNOWN_RETICKERS_PATH = (
    Path(__file__).resolve().parent / "data" / "sp500_known_retickers.json"
)

# The changes table is not pinned to one page: on 2026-08-11 a Wikipedia editor
# split it out of "List of S&P 500 companies" into its own article
# ("move to [[Historical components of the S&P 500]], format"), which failed
# this collector 3h later. Both candidates are tried in order and the first
# carrying a date+added+removed table wins, so a future move back — or to a
# third title added here — degrades to a slower fetch rather than a hard stop.
_SP500_CHANGES_URLS = (
    "https://en.wikipedia.org/wiki/Historical_components_of_the_S%26P_500",
    "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
)
_HEADERS = {"User-Agent": "alpha-engine-data/1.0 (historical-constituents)"}

ADDED = "added"
REMOVED = "removed"


@dataclass(frozen=True)
class ConstituentChange:
    """One index-membership change event from the Wikipedia changes table."""

    date: str  # ISO YYYY-MM-DD
    ticker: str
    action: str  # ADDED or REMOVED


def _normalize_ticker(raw: object) -> str | None:
    """Wikipedia uses BRK.B etc.; strip footnote markers + whitespace.

    Returns ``None`` for empty / placeholder cells (the changes table leaves
    the added or removed cell blank when only one side changed)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "—", "-", "none"}:
        return None
    # Drop bracketed footnote refs like "ABC[1]" and trailing notes.
    s = re.sub(r"\[.*?\]", "", s).strip()
    # Keep the symbol token only (uppercase letters, digits, dot, dash).
    m = re.match(r"[A-Z0-9.\-]+", s.upper())
    return m.group(0) if m else None


def _parse_date(raw: object) -> str | None:
    """Parse a changes-table date cell to ISO ``YYYY-MM-DD`` (or None)."""
    if raw is None:
        return None
    s = re.sub(r"\[.*?\]", "", str(raw)).strip()
    if not s or s.lower() == "nan":
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]
    return df


def select_changes_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    """Pick the "Selected changes to the list" table from read_html output.

    Identified by columns: a Date column plus *Added* and *Removed* groups
    (each typically a Ticker sub-column). Mirrors the column-based selection
    in ``constituents._select_constituents_table`` (position is unstable —
    Wikipedia inserts banner tables without notice)."""
    for df in tables:
        flat = _flatten_columns(df)
        cols = [str(c).lower() for c in flat.columns]
        has_date = any("date" in c for c in cols)
        has_added = any("added" in c for c in cols)
        has_removed = any("removed" in c for c in cols)
        if has_date and has_added and has_removed:
            return flat
    raise RuntimeError(
        "No 'Selected changes to the list' table found on the S&P 500 "
        "Wikipedia page (need columns matching date + added + removed). "
        "Wikipedia layout drift — extractor needs update."
    )


def _pick_col(cols: list[str], *, contains: str, prefer: str) -> str | None:
    """Find the column whose lowercased name contains ``contains`` (and,
    when several match, the one also containing ``prefer`` — e.g. the
    'Added Ticker' rather than 'Added Security')."""
    matches = [c for c in cols if contains in c.lower()]
    if not matches:
        return None
    preferred = [c for c in matches if prefer in c.lower()]
    return (preferred or matches)[0]


def parse_changes_table(df: pd.DataFrame) -> list[ConstituentChange]:
    """Wikipedia changes DataFrame -> ordered list of ``ConstituentChange``.

    Each row may carry an addition, a removal, or both. Rows with an
    unparseable date or no valid ticker on either side are skipped. The
    returned list is sorted oldest-first (deterministic replay order)."""
    df = _flatten_columns(df)
    cols = list(df.columns)
    date_col = _pick_col([str(c) for c in cols], contains="date", prefer="date")
    added_ticker_col = _pick_col(
        [str(c) for c in cols], contains="added", prefer="ticker"
    )
    removed_ticker_col = _pick_col(
        [str(c) for c in cols], contains="removed", prefer="ticker"
    )
    if not date_col or not (added_ticker_col or removed_ticker_col):
        raise RuntimeError(
            "Changes table missing a usable date/added/removed column set; "
            f"saw columns {cols}."
        )

    changes: list[ConstituentChange] = []
    for _, row in df.iterrows():
        iso = _parse_date(row.get(date_col))
        if iso is None:
            continue
        if added_ticker_col:
            t = _normalize_ticker(row.get(added_ticker_col))
            if t:
                changes.append(ConstituentChange(iso, t, ADDED))
        if removed_ticker_col:
            t = _normalize_ticker(row.get(removed_ticker_col))
            if t:
                changes.append(ConstituentChange(iso, t, REMOVED))

    changes.sort(key=lambda c: (c.date, c.ticker, c.action))
    return changes


def load_frozen_changes(path: Path = _FROZEN_CHANGES_PATH) -> list[ConstituentChange]:
    """Pre-cutover index changes from the committed, content-hashed artifact.

    The hash is verified, and a mismatch RAISES. This artifact defines the
    universe every 10y backtest runs against; silently accepting an altered
    copy would change what the whole system believes about the past without
    anything failing. Regenerating it legitimately means regenerating the
    hash in the same commit, where a reviewer sees both.
    """
    body = json.loads(path.read_text())
    payload = json.dumps(body["changes"], sort_keys=True, separators=(",", ":")).encode()
    actual = hashlib.sha256(payload).hexdigest()
    if actual != body["content_sha256"]:
        raise RuntimeError(
            f"{path.name} content hash mismatch: recorded "
            f"{body['content_sha256'][:16]}…, computed {actual[:16]}… — the "
            "frozen index history has been altered without its hash being "
            "regenerated. Refusing to build a backtest universe from it."
        )
    return [
        ConstituentChange(c["date"], c["ticker"], c["action"]) for c in body["changes"]
    ]


def same_date_swaps(snapshots: dict[str, list[str]]) -> dict[str, list[str]]:
    """``{date: [tickers that left on that date]}`` where something also joined.

    A ticker RENAME shows up in a holdings diff exactly like an index change:
    the old symbol disappears and the new one appears on the same date. Both
    the 2026-05-22 ``BK`` -> ``BNY`` and the 2026-06-25 ``SATS`` -> ``ECHO``
    reticker did, and treating them as membership events would tell the
    backtester that a company left the index and a different one joined —
    a fabricated churn event in the exact dataset built to make the universe
    honest. This narrows the set worth asking Polygon about; it does not
    decide anything.
    """
    out: dict[str, list[str]] = {}
    dates = sorted(snapshots)
    for prev_date, date in zip(dates, dates[1:]):
        before, after = set(snapshots[prev_date]), set(snapshots[date])
        left, joined = sorted(before - after), sorted(after - before)
        if left and joined:
            out[date] = left
    return out


def changes_from_snapshots(
    snapshots: dict[str, list[str]],
    renames: dict[str, str] | None = None,
) -> tuple[list[ConstituentChange], list[str]]:
    """Dated rosters -> membership changes, by diffing consecutive snapshots.

    ``snapshots`` maps ``YYYY-MM-DD`` to that date's S&P 500 roster. A ticker
    present on date D+1 and absent on the preceding snapshot D is ADDED on
    D+1; present on D and absent on D+1 is REMOVED on D+1. Pure — the caller
    does the S3 reads.

    This is the whole point of the collector'"'"'s rewrite: after the cutover,
    membership is something we OBSERVED rather than something we replayed
    from a third party'"'"'s prose. The earliest snapshot yields no changes —
    it is the baseline the rest are diffed against, not an event.

    The change is dated to the LATER snapshot because that is the first date
    we can attest to it. A weekly cadence therefore dates a change up to a
    week late; that is a known and bounded imprecision, and it is the honest
    one — claiming the exact effective date would assert something the
    snapshots do not contain.

    ``renames`` maps an old ticker to its new one (the caller resolves these
    via :func:`corporate_actions.detect_renames`); a matching same-date
    disappear/appear pair is dropped, because the company never left the
    index. Returns ``(changes, unresolved)`` where ``unresolved`` names the
    same-date swaps no rename explains — those changes ARE emitted, since a
    swap is far more often real index churn than a rename, but they are
    reported so a run cannot quietly decide either way. See
    :func:`same_date_swaps`.
    """
    renames = renames or {}
    changes: list[ConstituentChange] = []
    unresolved: list[str] = []
    dates = sorted(snapshots)
    for prev_date, date in zip(dates, dates[1:]):
        before = set(snapshots[prev_date])
        after = set(snapshots[date])
        left, joined = before - after, after - before
        renamed_away = {t for t in left if renames.get(t) in joined}
        renamed_into = {renames[t] for t in renamed_away}
        for ticker in sorted(joined - renamed_into):
            changes.append(ConstituentChange(date, ticker, ADDED))
        for ticker in sorted(left - renamed_away):
            changes.append(ConstituentChange(date, ticker, REMOVED))
        still_left, still_joined = left - renamed_away, joined - renamed_into
        if still_left and still_joined:
            unresolved.append(
                f"{date}: out={sorted(still_left)} in={sorted(still_joined)}"
            )
    changes.sort(key=lambda c: (c.date, c.ticker, c.action))
    return changes, unresolved


def divergences(
    observed: list[ConstituentChange],
    reference: list[ConstituentChange],
    *,
    since: str,
) -> list[str]:
    """Human-readable disagreements between two derivations, on/after ``since``.

    The frozen artifact is only as good as the wiki was on the day it was
    frozen, and nothing about a committed file makes it true. Comparing the
    observed post-cutover changes against the same window of the live wiki
    every run is what turns "we froze it and hope" into a claim under test:
    a disagreement means either a bad wiki edit or a gap in our own
    collection, and both are worth knowing about.

    Dates are NOT compared — the snapshot derivation dates a change to the
    first snapshot that shows it, which is up to a cadence-interval later
    than the wiki'"'"'s effective date. Comparing them would report a
    disagreement on every single change. Membership of the (ticker, action)
    set is the claim being tested.
    """
    obs = {(c.ticker, c.action) for c in observed if c.date >= since}
    ref = {(c.ticker, c.action) for c in reference if c.date >= since}
    out = []
    for ticker, action in sorted(obs - ref):
        out.append(f"observed {action} {ticker} not in reference")
    for ticker, action in sorted(ref - obs):
        out.append(f"reference {action} {ticker} not observed")
    return out


def build_pit_membership(
    current_tickers: list[str],
    changes: list[ConstituentChange],
) -> dict[str, list[str]]:
    """Replay ``changes`` backward from ``current_tickers`` to a PIT map.

    Returns ``{change_date: sorted_tickers_immediately_before_that_date}``.
    The membership *after* the most recent change equals the current roster;
    walking each change date from newest to oldest, undo it to recover the
    set that held just before that date:
      * undo an ADDED ticker -> it was NOT a member before that date -> remove
      * undo a REMOVED ticker -> it WAS a member before that date -> add

    Same-date changes are applied as a group so the snapshot for date D is the
    membership the instant before D's changes took effect.
    """
    members = set(current_tickers)
    # Group changes by date, newest first.
    by_date: dict[str, list[ConstituentChange]] = {}
    for c in changes:
        by_date.setdefault(c.date, []).append(c)

    pit: dict[str, list[str]] = {}
    for date in sorted(by_date, reverse=True):
        for c in by_date[date]:
            if c.action == ADDED:
                members.discard(c.ticker)  # wasn't a member before D
            elif c.action == REMOVED:
                members.add(c.ticker)  # was a member before D
        pit[date] = sorted(members)
    return pit


def _fetch_changes_table(
    urls: tuple[str, ...] = _SP500_CHANGES_URLS,
) -> tuple[pd.DataFrame, str]:
    """Return the changes table and the URL it actually came from.

    Tries each candidate in order; a fetch error or a page with no matching
    table falls through to the next. Raises only when every candidate fails,
    naming what was tried and why each one did not serve."""
    failures = []
    for url in urls:
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            df = select_changes_table(pd.read_html(StringIO(resp.text)))
        except Exception as exc:  # noqa: BLE001 — recorded and re-raised below
            failures.append(f"{url}: {type(exc).__name__}: {exc}")
            continue
        if failures:
            logger.warning(
                "historical_constituents: changes table served by fallback %s "
                "(earlier candidates failed: %s)", url, "; ".join(failures),
            )
        return df, url
    raise RuntimeError(
        "No S&P 500 'Selected changes to the list' table found on any known "
        "Wikipedia page (need columns matching date + added + removed). "
        "Wikipedia layout drift — add the new article to _SP500_CHANGES_URLS. "
        "Tried: " + " | ".join(failures)
    )


def _sp500_roster(snapshot: dict) -> list[str] | None:
    """The S&P 500 slice of one dated ``constituents.json``, or None.

    ``constituents.collect`` writes a single combined ``tickers`` list —
    the SPY holdings followed by the MDY holdings, deduped preserving order
    — plus ``sp500_count`` / ``sp400_count``. Explicit per-index lists were
    added later, so newer snapshots carry ``sp500_tickers`` and are read
    directly; the 84 already on S3 are not, and for those the prefix is the
    only way in.

    That prefix is only sound when nothing was lost to the dedupe, so the
    counts must account for the whole list. When they do not, this returns
    None rather than slicing anyway: a roster silently short by the overlap
    would surface as a burst of fabricated ADDED/REMOVED events on that
    date, which is worse than a gap because it looks like real index churn.
    """
    explicit = snapshot.get("sp500_tickers")
    if explicit:
        return list(explicit)
    tickers = snapshot.get("tickers") or []
    n500 = snapshot.get("sp500_count") or 0
    n400 = snapshot.get("sp400_count") or 0
    if not tickers or not n500 or n500 + n400 != len(tickers):
        return None
    return list(tickers[:n500])


def load_roster_snapshots(
    bucket: str,
    s3=None,
    prefix: str = "market_data/weekly/",
) -> dict[str, list[str]]:
    """Read every dated ``constituents.json`` under ``prefix`` from S3.

    Returns ``{YYYY-MM-DD: sp500_roster}``. Snapshots whose S&P 500 slice
    cannot be established are logged and skipped — see :func:`_sp500_roster`.
    """
    s3 = s3 or boto3.client("s3")
    snapshots: dict[str, list[str]] = {}
    unusable: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/constituents.json"):
                continue
            date = key[len(prefix):].split("/", 1)[0]
            if not _ISO_DATE_RE.match(date):
                continue
            body = json.loads(
                s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            )
            roster = _sp500_roster(body)
            if roster is None:
                unusable.append(date)
                continue
            snapshots[date] = roster
    if unusable:
        logger.warning(
            "historical_constituents: %d roster snapshot(s) skipped — no "
            "usable S&P 500 slice (sp500_count + sp400_count != len(tickers)): %s",
            len(unusable), sorted(unusable)[:20],
        )
    return snapshots


def resolve_renames(swaps: dict[str, list[str]]) -> dict[str, str]:
    """Ask Polygon which of the disappearing tickers were retickers.

    Reuses ``corporate_actions.detect_renames`` — the same detection the
    prune path runs, so a reticker is classified once in this repo rather
    than twice with two answers. Imported lazily: this module's pure layer
    is unit-tested without the polygon client or its config.

    A detection failure returns no rename for that candidate, which lands
    the pair in ``unresolved`` and emits it as index churn with a WARNING.
    That is the opposite of the prune path's history-safety default, and
    deliberately so: prune DELETES history on a wrong answer, where this
    only mis-dates one membership event that the attestation then flags.
    """
    if not swaps:
        return {}
    candidates = sorted({t for tickers in swaps.values() for t in tickers})

    # Committed retickers first. Polygon does NOT report these as
    # ticker_change — verified 2026-08-12, `get_ticker_events("BK")` returns
    # `[]` for the BNY Mellon rebrand — so detection alone would emit them as
    # index churn. A reticker is settled history exactly like a pre-cutover
    # index change, so it gets the same treatment: written down once, reviewed
    # in a diff, and not re-derived weekly from a source that does not have it.
    known = json.loads(_KNOWN_RETICKERS_PATH.read_text())
    renames = {
        r["old"]: r["new"]
        for r in known["retickers"]
        if r["old"] in candidates
    }
    candidates = [t for t in candidates if t not in renames]
    if not candidates:
        return renames

    try:
        from builders.prune_delisted_tickers import _build_rename_client

        import corporate_actions as ca

        client = _build_rename_client()
        if client is None:
            raise RuntimeError("no polygon client for rename detection")
        detection = ca.detect_renames(candidates, client=client)
    except Exception as exc:  # noqa: BLE001 — recorded, and callers warn
        logger.warning(
            "historical_constituents: rename detection unavailable (%s) — %d "
            "same-date swap(s) will be emitted as index changes unclassified",
            exc, len(candidates),
        )
        return renames
    renames.update({a.ticker: a.new_ticker for a in detection.renames})
    if renames:
        logger.info(
            "historical_constituents: %d reticker(s) resolved and excluded "
            "from membership changes: %s", len(renames), renames,
        )
    return renames


def collect(
    bucket: str,
    current_tickers: list[str],
    s3_prefix: str = "market_data/",
    dry_run: bool = False,
) -> dict:
    """Build the point-in-time S&P 500 membership map and write to S3.

    ``current_tickers`` is today's roster (the caller already has it from
    ``constituents.collect``); this avoids a second live fetch of the live
    roster and keeps the two collectors' rosters consistent. Writes
    ``{s3_prefix}historical_constituents.json`` per the memo's recommended
    path.

    Two producers, joined at ``SNAPSHOT_CUTOVER``: the committed frozen
    artifact for settled pre-cutover history, and a diff of our own dated
    roster snapshots for everything after. Wikipedia is fetched only to
    ATTEST the post-cutover window and never to produce it, so a page that
    moves or 404s costs an attestation, not the pipeline
    (alpha-engine-config-I6946).
    """
    frozen = load_frozen_changes()
    snapshots = load_roster_snapshots(bucket)
    renames = resolve_renames(same_date_swaps(snapshots))
    observed, unresolved = changes_from_snapshots(snapshots, renames)
    changes = sorted(
        frozen + observed, key=lambda c: (c.date, c.ticker, c.action)
    )
    pit = build_pit_membership(current_tickers, changes)

    # Attestation. Non-fatal by deliberate carve-out from fail-loud:
    # (a) the failure swallowed is an unreachable or restructured wiki page,
    # which says nothing about the membership map already built from two
    # sources that do not involve it; (b) it is recorded as a WARNING on this
    # phase's log and as `attestation` in the written artifact, so a run that
    # could not attest is distinguishable from one that attested cleanly —
    # `divergences` null is not the same value as `[]`.
    attestation: dict = {"status": "skipped", "divergences": None}
    try:
        reference = parse_changes_table(_fetch_changes_table()[0])
    except Exception as exc:  # noqa: BLE001 — recorded above and below
        attestation["status"] = "unavailable"
        attestation["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "historical_constituents: post-cutover attestation UNAVAILABLE "
            "(%s) — membership was still built from the frozen artifact + %d "
            "observed snapshots; nothing cross-checked it this run",
            exc, len(snapshots),
        )
    else:
        found = divergences(observed, reference, since=SNAPSHOT_CUTOVER)
        attestation = {
            "status": "diverged" if found else "agreed",
            "divergences": found,
            "since": SNAPSHOT_CUTOVER,
        }
        if found:
            # An unresolved same-date swap is NOT reported on its own: a real
            # index change is a swap most weeks (one name in, one out), so
            # warning on every one of them would fire on healthy runs and be
            # tuned out long before the week it meant something. It becomes
            # interesting only where it ALSO diverges from the reference —
            # which is exactly how the BK->BNY and SATS->ECHO retickers were
            # caught on this collector's first observed run.
            logger.warning(
                "historical_constituents: observed membership DISAGREES with "
                "the reference on %d change(s) since %s — a bad upstream edit, "
                "a gap in our snapshots, or a reticker missing from %s: %s "
                "(unresolved same-date swaps this run: %s)",
                len(found), SNAPSHOT_CUTOVER, _KNOWN_RETICKERS_PATH.name,
                found[:20], unresolved[:20],
            )

    result = {
        "schema_version": 2,
        "source": {
            "frozen": _FROZEN_CHANGES_PATH.name,
            "observed": f"s3://{bucket}/market_data/weekly/*/constituents.json",
            "cutover": SNAPSHOT_CUTOVER,
        },
        "index": "S&P 500",
        "current_count": len(current_tickers),
        "n_changes": len(changes),
        "n_changes_frozen": len(frozen),
        "n_changes_observed": len(observed),
        "n_roster_snapshots": len(snapshots),
        "renames_excluded": renames,
        "unresolved_swaps": unresolved,
        "n_snapshots": len(pit),
        "attestation": attestation,
        "membership": pit,  # {date: [tickers as-of just before that date]}
        "built_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        logger.info(
            "[dry-run] historical_constituents: %d changes (%d frozen + %d "
            "observed from %d roster snapshots) -> %d PIT snapshots "
            "(current roster %d); attestation=%s",
            len(changes), len(frozen), len(observed), len(snapshots),
            len(pit), len(current_tickers), attestation["status"],
        )
        return {"status": "ok_dry_run", "n_changes": len(changes), "n_snapshots": len(pit)}

    s3 = boto3.client("s3")
    key = f"{s3_prefix}historical_constituents.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(result, indent=2),
        ContentType="application/json",
    )
    logger.info(
        "Wrote historical_constituents.json to s3://%s/%s (%d changes, %d snapshots)",
        bucket, key, len(changes), len(pit),
    )
    return {"status": "ok", "n_changes": len(changes), "n_snapshots": len(pit)}
