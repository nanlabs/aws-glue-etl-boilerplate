# Optional Code Registry deployment

This repository is a starter template. Its GitHub Actions workflows show how a
project can package and deploy the Glue libraries, but this template does not
deploy to AWS by default.

The `Deploy to Code Registry` workflow skips all deployment jobs unless the
repository variable `CODE_REGISTRY_DEPLOY_ENABLED` is set to the exact string
`true`. After creating a project from this template, set these repository
variables in **Settings → Secrets and variables → Actions → Variables** to opt
that project in:

| Variable | Required | Purpose |
|---|---|---|
| `CODE_REGISTRY_DEPLOY_ENABLED` | Yes | Set to `true` to enable push, release, and manual deployment jobs. |
| `AWS_ROLE_TO_ASSUME` | Yes | IAM role ARN that GitHub Actions assumes with OIDC. |
| `CODE_REGISTRY_BUCKET` | Yes | S3 bucket used by the Code Registry deployment script. |
| `AWS_REGION` | No | AWS region; defaults to `us-west-2`. |

The deployment jobs use the `code-registry-deploy` GitHub environment and
request the `id-token: write` permission. Configure the IAM role's OIDC trust
policy for audience `sts.amazonaws.com` and the generated project's repository
and environment subject. Do not copy this template's previous organization
role or bucket identifiers into a new project.

Once enabled, pushes to `main` that touch packaged code deploy the `latest`
artifact, published GitHub releases deploy their version and `stable` tags, and
the manual workflow supports redeploying a chosen tag. Keep the variable unset
in the template repository itself.

For a local deployment, use the same bucket explicitly with `--bucket` or
`CODE_REGISTRY_BUCKET`. The script uses the AWS CLI's default credential chain
unless `AWS_PROFILE` or `--profile` selects a named profile. It intentionally
has no organization-specific profile or bucket defaults.
