# Deployment Guide

## Overview

Deployments lean on lightweight scripts for POC and development, while production flows are handled by infrastructure automation described in **[OPERATIONS.md](./OPERATIONS.md#deployment)**. Use this page to choose the correct path and find the primary entrypoints; detailed procedures live in the linked docs and script READMEs.

## Essentials

- Build artifacts with `make package` (wrapper around `scripts/package.sh`). Outputs land in `build/` and are summarized in **[PACKAGING.md](./PACKAGING.md)**.
- Deploy code to S3 and manage Glue jobs through infrastructure automation described in **[OPERATIONS.md](./OPERATIONS.md#deployment)**.
- Run jobs through AWS Glue console or CI/CD pipelines as documented in **[OPERATIONS.md](./OPERATIONS.md)**.

## Deployment Paths

| Use case | Recommended path | Notes |
| --- | --- | --- |
| POC / ad-hoc testing | Script-based flow (package → deploy → run) | Fast iteration; relies on manual S3 uploads and Glue job creation |
| Team-shared development | Script flow plus documented conventions in **[DEVELOPMENT.md](./DEVELOPMENT.md)** | Ensure SSO profile and S3 buckets match the shared environment |
| Production rollout | Infra-as-code and CI/CD described in **[OPERATIONS.md](./OPERATIONS.md#production-pipeline)** | Includes Terraform modules, approval gates, monitoring, and rollback patterns |

## Deployment Flow

1. `make package` – build `libs.zip`, layer job bundles, wheels.
2. Deploy artifacts and manage Glue jobs through infrastructure automation (Terraform/CI/CD) as described in **[OPERATIONS.md](./OPERATIONS.md#production-pipeline)**.

Keep parameters DRY: prefer the shared Terraform outputs or environment configuration in `DEVELOPMENT.md` instead of hard-coding.

## Glue Configuration Highlights

- Always include `--extra-py-files` pointing at the latest `libs.zip` artifact.
- PyShell jobs must also set `--additional-python-modules` for third-party dependencies; see **[JOB_DEVELOPMENT.md](./JOB_DEVELOPMENT.md)**.
- Spark jobs may attach wheels or rely on AWS-managed libraries—consult **[JOB_DEVELOPMENT.md](./JOB_DEVELOPMENT.md)** before uploading custom wheels.
- Verify job defaults with `aws glue get-job --job-name <name> | jq '.Job.DefaultArguments'` when debugging; troubleshooting paths are covered in **[AWS_GLUE_TROUBLESHOOTING.md](./AWS_GLUE_TROUBLESHOOTING.md)**.

## Guardrails & Checklists

- Use least-privilege IAM roles and store secrets in AWS Secrets Manager (**[SECRETS_MANAGEMENT.md](./SECRETS_MANAGEMENT.md)**).
- Maintain artifact history in versioned S3 prefixes; roll back by copying the prior object key when needed.
- Align branch/commit/PR metadata with the corresponding ClickUp ticket; see **[ClickUp Workflow](./CLICKUP_WORKFLOW.md)** for ticket management.
- Before promoting beyond POC, ensure monitoring, cost controls, and incident runbooks are set per **[OPERATIONS.md](./OPERATIONS.md#production-readiness)**.

## Related References

- [Packaging Guide](./PACKAGING.md)
- [Scripts README](../scripts/README.md)
- [Development Guide](./DEVELOPMENT.md)
- [Operations Guide](./OPERATIONS.md)
- [AWS Glue Troubleshooting](./AWS_GLUE_TROUBLESHOOTING.md)
- [Best Practices](./BEST_PRACTICES.md)

Keep this summary lean. Update the linked docs or script READMEs when procedures change, and mirror those updates here only when the high-level flow needs adjustment.
