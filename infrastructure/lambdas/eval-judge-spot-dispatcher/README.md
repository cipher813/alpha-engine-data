# eval-judge-spot-dispatcher

Launches the dedicated EC2 spot box the weekly pipeline's `EvalJudgeProcess`
stage runs on. `alpha-engine-config-I9329`, following `-I9309`.

## Why

Brian, 2026-08-29, verbatim: *"perhaps if the lambda times out then we need to
put the judge on a spot instance"*, and *"ok lets keep 100% coverage and move
to spot."*

`EvalJudgeProcess` ran as a Lambda under a 960s state ceiling against a 900s
function. At the measured 45–105s per synchronous judge call it covered roughly
8–15 of an ~83-artifact corpus, reported `complete=False` honestly, and returned
SUCCESS. `crucible-research-PR766` made coverage a pass/fail verdict and added
`evals/judge_spot_run.py`, the substrate-independent entrypoint with no
deadline. This Lambda is the substrate.

## Shape — and which sibling it mirrors

This is the **weekly-freshness-spot-dispatcher** shape, not the
**thinktank-spot-dispatcher** one, and the difference is the whole design:

| | thinktank | weekly-freshness | **eval-judge (this)** |
|---|---|---|---|
| what the async SSM command does | the entire workload | bootstrap only | bootstrap only |
| who drives the run | nobody — fire and forget | the SF, per stage | the SF, one stage |
| who polls | nobody | the SF | the SF |
| self-terminates at bootstrap end | yes | no | **no** |

The Step Function has to own the run because the judge's outcome is a
**coverage verdict**, and the SSM `ResponseCode` the SF reads is the only
signal that survives before anyone opens a log.

The consequence is stated here because it inverts the thinktank contract:
`crucible-research/infrastructure/eval_judge_spot_bootstrap.sh` **must not
self-terminate**. A `shutdown -h now` at the end of bootstrap would pull the box
out from under the stage about to use it.

## Teardown — three layers, none of them on the happy path except the first

1. The SF's own run command arms a 120s `systemd-run` shutdown timer in an
   `EXIT` trap, so the box dies on success and on failure alike. The delay is
   what lets SSM report the terminal status before the instance disappears.
2. `krepis.spot_bootstrap`'s `max_runtime_seconds` (4.5h) and dead-man (5h)
   timers, armed before the repo's bootstrap script can fail.
3. The fleet `spot-orphan-reaper` age cap.

`handler` **refuses to launch** if the watchdog does not exceed
`BOOTSTRAP_TIMEOUT_SECONDS + EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS`: a watchdog
firing mid-run truncates the corpus, and a coverage shortfall is a HARD stage
failure — so that misconfiguration would present as a weekly eval failure with
no obvious cause.

## Router addressing

`KREPIS_EXEC_CONTEXT=ec2`, the authenticated edge at
`https://router.nousergon.ai:8443`, the `ROUTER_CONSUMER_RESEARCH` credential
NAME (never `LITELLM_MASTER_KEY` — `krepis.secrets` resolves SSM *before*
`os.environ`, so sharing the name collapses this box into the director's
identity), and the AppConfig triple for the model registry.

These are written to a FILE on the box, not exported into the bootstrap's own
environment, because the SF's later `ssm:sendCommand` is a **separate shell**.
An export that lived only in the bootstrap would be gone by the time the judge
runs, and `judge_exec_context()` would answer `"lambda"` from a spot box —
asking the router the wrong question with no error anywhere.

Verified live 2026-08-29: `/alpha-engine/ROUTER_CONSUMER_RESEARCH` exists, and
`alpha-engine-executor-role` (the instance profile) already grants
`ssm:GetParameter` on `/alpha-engine/*` plus the AppConfig session. **No IAM
change is needed for the box.**

## Deploy

```
bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh             # code only, runs on merge
bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh --bootstrap # OPERATOR: create role + function
bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh --apply-iam # re-apply iam-policy.json only
bash infrastructure/lambdas/eval-judge-spot-dispatcher/deploy.sh --dry-run
```

`--bootstrap` is operator-only and is a real privilege boundary: the
`github-actions-lambda-deploy` OIDC role deliberately holds no
`iam:CreateRole` / `iam:PutRolePolicy`, fleet-wide after four IAM-clobber
incidents in two months. Two independent detectors stay RED until it runs —
this Lambda's deploy workflow (whose preflight prints the exact command) and
`infrastructure/step-functions/check-lambda-existence.py` via
`sf-arn-drift-check.yml`, on every push to main and daily at 09:30 UTC.

**Ordering:** `step_function.json` auto-deploys on merge. Bootstrap this Lambda
BEFORE merging the SF change, or `DispatchEvalJudgeSpot` 404s on the invoke the
next Saturday — fail-soft into `MarkEvalJudgeDegraded`, so the run survives but
the week's eval coverage does not.

## IAM

- **This Lambda's execution role** — `iam-policy.json` here: `ec2:RunInstances`
  + tag-scoped `CreateTags`/`TerminateInstances`, `iam:PassRole` for
  `alpha-engine-executor-role` to EC2 only, and `ssm:SendCommand` scoped to
  `Name=alpha-engine-eval-judge-spot`.
- **The Step Functions role** — no invoke grant needed: its existing
  `lambda:InvokeFunction` resource wildcard
  `...:function:alpha-engine-research-eval-judge*` already covers this function
  name, which is why the name was chosen. The one grant the cutover did need,
  `ssm:SendCommand` on the box's tag, ships in `nous-ergon-ops`
  (`nous-ergon-ops-PR935`) where `iam-apply-on-merge.yml` applies it
  automatically.

## Observability

CloudWatch: `/alpha-engine/eval-judge-spot`. Bootstrap log ships to
`s3://alpha-engine-research/_ssm_logs/eval-judge-spot/bootstrap/{date}/`; the
judge run ships via `krepis.ssm_log_capture --slug eval-judge` to
`s3://alpha-engine-research/_ssm_logs/eval-judge/`. The run's own outcome
record — `coverage.status`, `planned`, `graded`, `degraded_transport` — is
`s3://alpha-engine-research/decision_artifacts/_eval_batch_plans/{date}/spot_run.json`.
Per-call cost rows land under `decision_artifacts/_cost_raw/{date}/` with
producer `evaljudge-sync`, which `AggregateCosts` now REQUIRES. Descriptors:
`nous-ergon-ops/governance/observability.d/`.
