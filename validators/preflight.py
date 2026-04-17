"""
Data-module preflight: fail-fast external-dependency + config checks that
run at the START of DataPhase1, before any collector executes.

Complements ``validators/postflight.py`` which checks producer-side output
contracts AFTER writes. Preflight checks the opposite side: are our inputs
and sinks reachable + correctly configured? Catches the class of failure
where a credential drift, outage, or misconfiguration would otherwise
burn ~55 minutes of spot-EC2 time before failing on the 40th collector
call. Fail-fast economics: ~10 seconds of HTTP HEADs vs. a wasted spot
run + failure-notification fan-out.

Scope (intentionally narrow):

  1. Required env vars present + non-empty (``POLYGON_API_KEY``, ``FRED_API_KEY``)
  2. Polygon.io reachable + auth valid (cheap reference-data call)
  3. FRED reachable + auth valid (single-series observation call)
  4. S3 bucket HEAD-able (read + write-permissions validation via sentinel PUT/DELETE)
  5. ArcticDB connectable + expected libraries present (``universe`` + ``macro``)

Out of scope (those are postflight's job): value-range validation, row
counts, freshness of existing data, contract shape checks. Preflight asks
"can we run at all?"; postflight asks "did what we produced satisfy the
downstream contract?".

Failure semantics: raises ``PreflightError`` (a ``RuntimeError`` subclass)
with a specific named message. ``weekly_collector._run_phase1`` catches,
returns a ``status=preflight_failed`` result dict, and lets ``main()``'s
SystemExit(1) propagate through SSM → Step Function HandleFailure →
CloudWatch alarm — same failure surface as postflight.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

log = logging.getLogger(__name__)

# HTTP timeout for external-API reachability probes. 10s tolerates one
# TCP retry on a cold connection but fails fast on a sustained outage.
_HTTP_TIMEOUT_SECS = 10.0

# Polygon.io reference-data endpoint — cheapest auth-gated call that
# validates both network reachability AND API-key validity. Returns a
# small JSON payload for a known ticker.
_POLYGON_PROBE_URL = "https://api.polygon.io/v3/reference/tickers/AAPL"

# FRED observation endpoint — matches collectors/macro.py usage. DFF
# (Federal Funds Rate) is a well-known series that should always exist.
_FRED_PROBE_URL = "https://api.stlouisfed.org/fred/series/observations"
_FRED_PROBE_SERIES = "DFF"


class PreflightError(RuntimeError):
    """Raised when a DataPhase1 fail-fast dependency check fails."""
    pass


class DataPreflight:
    """Fail-fast external-dependency checks for DataPhase1 startup.

    Parameters
    ----------
    bucket : str
        S3 bucket hosting ArcticDB + market_data/ prefixes.
    phase : int
        Phase number. Only Phase 1 runs preflight today; Phase 2 is a
        Lambda with a different dependency surface (no ArcticDB writes).
    """

    def __init__(self, bucket: str, phase: int) -> None:
        self.bucket = bucket
        self.phase = phase
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        self._s3: Any = None

    # ── Lazy handles ─────────────────────────────────────────────────────────

    def _s3_client(self) -> Any:
        if self._s3 is None:
            import boto3
            self._s3 = boto3.client("s3", region_name=self.region)
        return self._s3

    # ── Checks ───────────────────────────────────────────────────────────────

    def _check_required_env_vars(self) -> None:
        """Every collector that talks to an external API requires these.

        Failing here avoids a 10-minute collector run that errors on the
        first polygon call with an opaque auth failure.
        """
        missing: list[str] = []
        for var in ("POLYGON_API_KEY", "FRED_API_KEY"):
            if not os.environ.get(var, "").strip():
                missing.append(var)
        if missing:
            raise PreflightError(
                f"Required env vars missing or empty: {missing}. "
                f"Collectors will fail immediately on first API call. "
                f"Check /alpha-engine/{{var}} SSM params + .alpha-engine.env sourcing."
            )
        log.info("preflight: required env vars present (POLYGON_API_KEY, FRED_API_KEY)")

    def _check_polygon_reachable(self) -> None:
        """Validate polygon.io network + auth via a cheap reference-data call.

        Catches: expired API key, polygon outage, blocked egress. Does NOT
        catch: rate-limit ceiling (next collector call will still retry/fail
        by design).
        """
        import requests

        api_key = os.environ.get("POLYGON_API_KEY", "").strip()
        try:
            resp = requests.get(
                _POLYGON_PROBE_URL,
                params={"apiKey": api_key},
                timeout=_HTTP_TIMEOUT_SECS,
            )
        except requests.RequestException as exc:
            raise PreflightError(
                f"polygon.io unreachable: {exc} — network outage or egress blocked."
            ) from exc

        if resp.status_code == 401 or resp.status_code == 403:
            raise PreflightError(
                f"polygon.io auth failed (HTTP {resp.status_code}): "
                f"POLYGON_API_KEY is invalid or revoked."
            )
        if resp.status_code >= 500:
            raise PreflightError(
                f"polygon.io returned HTTP {resp.status_code} on a reference-data call "
                f"— upstream outage. Check status.polygon.io."
            )
        if resp.status_code != 200:
            raise PreflightError(
                f"polygon.io returned unexpected HTTP {resp.status_code} "
                f"on {_POLYGON_PROBE_URL}: {resp.text[:200]}"
            )
        log.info("preflight: polygon.io reachable + auth valid (HTTP 200)")

    def _check_fred_reachable(self) -> None:
        """Validate FRED network + auth via a single-observation DFF call."""
        import requests

        api_key = os.environ.get("FRED_API_KEY", "").strip()
        try:
            resp = requests.get(
                _FRED_PROBE_URL,
                params={
                    "series_id": _FRED_PROBE_SERIES,
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 1,
                },
                timeout=_HTTP_TIMEOUT_SECS,
            )
        except requests.RequestException as exc:
            raise PreflightError(
                f"FRED unreachable: {exc} — network outage or egress blocked."
            ) from exc

        if resp.status_code == 400:
            # FRED returns 400 with body containing "api_key" on bad key
            body = resp.text[:200].lower()
            if "api_key" in body or "invalid" in body:
                raise PreflightError(
                    f"FRED auth failed (HTTP 400): FRED_API_KEY is invalid. "
                    f"Response: {resp.text[:200]}"
                )
        if resp.status_code >= 500:
            raise PreflightError(
                f"FRED returned HTTP {resp.status_code} on DFF call "
                f"— upstream outage."
            )
        if resp.status_code != 200:
            raise PreflightError(
                f"FRED returned unexpected HTTP {resp.status_code}: {resp.text[:200]}"
            )
        log.info("preflight: FRED reachable + auth valid (HTTP 200)")

    def _check_s3_writeable(self) -> None:
        """Validate S3 bucket exists + IAM grants read + write + delete.

        HEAD-buckets, then PUT a sentinel key, then DELETE it. The sentinel
        approach catches the subtle case where HEAD succeeds but PutObject
        is blocked by a bucket policy or IAM deny — which would surface
        40 minutes into a spot run as "collector wrote 0 rows silently."
        """
        s3 = self._s3_client()
        try:
            s3.head_bucket(Bucket=self.bucket)
        except Exception as exc:
            raise PreflightError(
                f"S3 bucket s3://{self.bucket} HEAD failed: {exc} — "
                f"bucket missing, credentials invalid, or IAM lacks s3:ListBucket."
            ) from exc

        sentinel_key = f"preflight/sentinel-{uuid.uuid4().hex}.txt"
        try:
            s3.put_object(
                Bucket=self.bucket,
                Key=sentinel_key,
                Body=b"preflight-sentinel",
                ContentType="text/plain",
            )
        except Exception as exc:
            raise PreflightError(
                f"S3 PUT s3://{self.bucket}/{sentinel_key} failed: {exc} — "
                f"IAM lacks s3:PutObject or bucket policy blocks writes."
            ) from exc

        try:
            s3.delete_object(Bucket=self.bucket, Key=sentinel_key)
        except Exception as exc:
            # Non-fatal: the sentinel exists but we can still proceed. Log loudly.
            log.warning(
                "preflight: sentinel DELETE failed (%s) — preflight-sentinel objects "
                "may accumulate in s3://%s/preflight/. Check s3:DeleteObject IAM grant.",
                exc, self.bucket,
            )
        log.info("preflight: S3 bucket s3://%s read + write + delete OK", self.bucket)

    def _check_arcticdb_connectable(self) -> None:
        """Validate ArcticDB connection + presence of expected libraries.

        Matches the URI format used by postflight + every downstream
        consumer. Failure here means the collectors would fail to write
        the universe/macro libraries — catching early avoids wasted compute.
        """
        try:
            import arcticdb as adb
        except ImportError as exc:
            raise PreflightError(
                f"arcticdb package not importable: {exc}. "
                f"Check requirements.txt + venv build."
            ) from exc

        uri = (
            f"s3s://s3.{self.region}.amazonaws.com:{self.bucket}"
            "?path_prefix=arcticdb&aws_auth=true"
        )
        try:
            arctic = adb.Arctic(uri)
            libs = set(arctic.list_libraries())
        except Exception as exc:
            raise PreflightError(
                f"ArcticDB connection failed at {uri}: {exc}. "
                f"Check s3 prefix + credentials + arcticdb version."
            ) from exc

        missing = {"universe", "macro"} - libs
        if missing:
            raise PreflightError(
                f"ArcticDB missing expected libraries: {sorted(missing)} "
                f"(found: {sorted(libs)}). Run backfill or verify path_prefix."
            )
        log.info(
            "preflight: ArcticDB connectable (libraries: universe, macro present)"
        )

    # ── Entry point ──────────────────────────────────────────────────────────

    def run(self) -> None:
        """Run every check in sequence. Fail on the first violation.

        Ordered cheapest-first so a trivially-broken run fails in <1s:
          1. env vars (local lookup)
          2. S3 bucket (~ms, local IAM)
          3. ArcticDB (~100ms, IAM + list_libraries)
          4. FRED (~200ms, HTTP)
          5. polygon.io (~200ms, HTTP)
        """
        if self.phase != 1:
            log.info(
                "preflight: phase=%d is not gated today (only Phase 1). Skipping.",
                self.phase,
            )
            return

        self._check_required_env_vars()
        self._check_s3_writeable()
        self._check_arcticdb_connectable()
        self._check_fred_reachable()
        self._check_polygon_reachable()
        log.info("preflight: all DataPhase1 fail-fast checks passed")
