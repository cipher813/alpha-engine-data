# IAM policies (alpha-engine-data — orchestration infra)

Source-of-truth for inline IAM policies on the cross-cutting orchestration
roles. This repo owns these roles because the **grants are derived from
code that lives in this repo**:

- `alpha-engine-step-functions-role` — grants reflect the Lambdas the SF
  JSON invokes + the EC2 instances it sends SSM to + the trading
  instance it starts/stops. Source: `infrastructure/cloudformation/`
  + `infrastructure/deploy_step_function*.sh`.
- `alpha-engine-eventbridge-sfn-role` — grants reflect which Step
  Functions the EventBridge cron rules target. Source: same as above.
- `github-actions-lambda-deploy` — grants reflect the set of Lambdas
  any GitHub Action in any alpha-engine-* repo can deploy. Cross-cutting
  by design.

Module-specific roles live in their owning repo's `infrastructure/iam/`:

| Role | Home repo |
|---|---|
| `alpha-engine-executor-role` | `alpha-engine` |
| `alpha-engine-predictor-role` | `alpha-engine-predictor` |
| `github-actions-iam-drift-check` | `alpha-engine` (workflow-specific) |

## Layout

Flat one-file-per-role:

```
infrastructure/iam/
├── apply.sh
├── check-drift.py
├── README.md
├── alpha-engine-step-functions-role.json
├── alpha-engine-eventbridge-sfn-role.json
└── github-actions-lambda-deploy.json
```

The filename (minus `.json`) is the IAM role name; the inline policy
name on AWS is `{role-name}-policy` (enforced by `apply.sh`).

## Usage

```bash
# Apply every policy
./infrastructure/iam/apply.sh

# Apply one specific role
./infrastructure/iam/apply.sh alpha-engine-step-functions-role

# Print planned commands without executing
./infrastructure/iam/apply.sh --dry-run

# Check drift against live AWS
./infrastructure/iam/check-drift.py
```

`apply.sh` calls `aws iam put-role-policy`, which is idempotent —
re-running overwrites the existing inline policy on the role.

## Single-writer rule

Each codified role must have **exactly one writer** — `apply.sh` in the
home repo. Any deploy script that calls `aws iam put-role-policy` against
a codified role from anywhere else is a regression risk.

This rule is enforced by `check-no-foreign-writers.py` in the alpha-engine
repo, which scans every sibling alpha-engine-* repo on every PR + daily.
4 IAM-clobber incidents in 2 months traced to this exact pattern (PR
review missed inline `put-role-policy` blocks in alpha-engine-data deploy
scripts that competed with codified state); the static check closes that
regression class.

## Trust policies + role creation

Out of scope for this directory. Trust policies + initial role creation
are handled inline in the deploy scripts that need them
(`infrastructure/deploy_step_function.sh` for the SF + EB-SFN roles).
This directory only manages the **inline policy documents** on roles
that already exist.

## Drift detection

`.github/workflows/iam-drift-check.yml` runs `check-drift.py` on every
PR touching `infrastructure/iam/**`, daily at 09:30 UTC, and on
manual `workflow_dispatch`.

Auth: OIDC via the shared `github-actions-iam-drift-check` role (defined
in alpha-engine; trust policy permits both alpha-engine and
alpha-engine-data); read-only `iam:ListRolePolicies` + `iam:GetRolePolicy`
scoped to the codified roles only.

## When you add a new inline policy

1. Apply it to AWS first (e.g. via `aws iam put-role-policy ...`)
2. Save the JSON document as `<role-name>.json` in this directory
3. Commit the file with a description of why the grant was needed

If the role is module-specific rather than orchestration-shared, codify
it in the owning module's repo instead.
