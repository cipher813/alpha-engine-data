# alpha-engine-preopen-deploy-readiness-probe

Pre-preopen deploy-readiness probe with runway (**alpha-engine-config-I7800** deliverable #2).

## Why

2026-08-19: `Deploy Infrastructure` failed 3x on `nousergon-data@main`. `notify-main-failure` fired each time as an ordinary CI-red Telegram alert. Nothing connected those failures to the fact that they had already decided the outcome of the 2026-08-20 05:15 PT preopen — the live SF stamp was frozen behind `main`, so `DeployDriftGate` would halt trading. It did. Detected-and-paged is partial coverage; this Lambda closes the loop: detect the same drift the preopen SF will hit, try to self-heal before the deadline, and page with runway to spare if self-heal doesn't work.

## What it does

Fires **04:30 America/Los_Angeles MON-FRI** — 45 minutes before the 05:15 PT preopen trigger:

1. **Not a NYSE trading day** → no-op, log only.
2. **Trading day** → invoke `alpha-engine-predictor-inference:live` `action=check_deploy_drift` — the SAME probe the preopen SF's own `DeployDriftCheck` state calls.
3. **`sf_drift=false`** → clean. Verdict written, no page.
4. **`sf_drift=true`** → self-heal: `workflow_dispatch` on `nousergon-data`'s `deploy-infrastructure.yml` (idempotent), poll the run to completion, re-probe.
   - Cleared → log the recovery, no page.
   - Still drifted (or the dispatch itself failed, e.g. PAT scope) → page `alpha-engine-alerts` (severity=critical) naming the diagnostics and that the 05:15 PT preopen will halt.

Every invocation writes a verdict to `s3://alpha-engine-research/deploy_readiness/{date}.json` — a silent probe is not an observation (sf-pipeline-policy §2.7).

## Fail-loud posture

The initial `check_deploy_drift` invoke and the S3 verdict write raise on any unexpected error. The self-heal dispatch/poll and the page are best-effort — a GitHub API hiccup on the self-heal leg must not crash the probe before it can page.

## Deploy / safe rollout

```bash
# first-time create (Scheduler schedule created per the automation-pause manifest's state)
bash infrastructure/lambdas/preopen-deploy-readiness-probe/deploy.sh --bootstrap
# code update only
bash infrastructure/lambdas/preopen-deploy-readiness-probe/deploy.sh
```

IAM role/policy creation requires `iam:CreateRole`/`iam:PutRolePolicy`, which the CI OIDC role deliberately does not hold — `--bootstrap` must be run by an operator with `AWS_PROFILE=ne-admin`. **Merging this PR has ZERO live effect until that operator step runs**, same as every sibling scheduled probe in this directory (`eod-snapshot-existence-check`, `ci-watch-liveness-probe`, …).

**Operator verification needed on first bootstrap**: this Lambda's self-heal leg reuses the existing `/alpha-engine/saturday_sf_watch/github_pat` SSM parameter (the same fine-grained PAT `saturday-sf-watch-dispatcher` already uses for cross-repo GitHub calls). Its documented scope is "the SF-path repos", which should already cover `actions:write` on `nousergon-data` itself — but that has not been live-verified for *this* specific permission (workflow dispatch, as opposed to `alpha-engine-config` repository_dispatch). If the PAT lacks `actions:write` on `nousergon-data`, the dispatch call fails loud (HTTP 403, logged and folded into the page message) rather than silently — the probe still pages correctly, it just can't self-heal. No new secret is requested; this is a scope check, not a provisioning ask.

## Config

| env var | default |
|---|---|
| `PROBE_BUCKET` | `alpha-engine-research` |
| `ALPHA_ENGINE_ALERTS_SNS_TOPIC_ARN` | `…:alpha-engine-alerts` |
| `DRIFT_PROBE_FUNCTION` | `alpha-engine-predictor-inference:live` |
| `GITHUB_PAT_SSM_PARAM` | `/alpha-engine/saturday_sf_watch/github_pat` |
| `DISPATCH_REPO` | `nousergon/nousergon-data` |
| `DISPATCH_WORKFLOW` | `deploy-infrastructure.yml` |
| `DISPATCH_POLL_ATTEMPTS` | `20` |
| `DISPATCH_POLL_INTERVAL_SEC` | `20` |

Schedule: `cron(30 4 ? * MON-FRI *)` in `America/Los_Angeles` (Scheduler-native timezone — DST-correct with no seasonal cron edit).
