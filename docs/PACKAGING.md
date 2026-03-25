# Packaging Guide

## Overview

Our packaging pipeline builds the artifacts required by AWS Glue jobs and local tooling. This guide gives the quick reference; for detailed workflows see **[DEVELOPMENT.md](./DEVELOPMENT.md#build--package)** and the deployment steps in **[DEPLOYMENT.md](./DEPLOYMENT.md#publishing-artifacts)**.

## Build Commands

- `make package` – primary entrypoint that shells out to `./scripts/package.sh` and writes artifacts to `build/`.
- `make clean` – removes previous build outputs before rebuilding.
- CI mirrors these scripts; avoid ad-hoc zips or manual S3 uploads.

## Artifact Summary

| Artifact | Located at | Purpose | When to Use |
| --- | --- | --- | --- |
| `libs.zip` | `build/libs.zip` | Bundled `libs/` package (common, pyspark, pyshell) | Attach via `--extra-py-files` for all Glue jobs; upload through deployment scripts |
| Layer job zips (`bronze_jobs.zip`, `silver_jobs.zip`, `gold_jobs.zip`, `raw_jobs.zip`) | `build/` | Convenience bundles of job scripts | Optional for selective promotion; scripts default to S3 sync in deployment tooling |
| `jobs.zip` | `build/jobs.zip` | All job scripts | Used by legacy job definitions; prefer per-layer bundles |
| `wheels-all.zip` & `build/wheels/` | `build/` | Third-party wheels for Spark runtimes | Only when Spark job requires pre-built wheels; PyShell should prefer `--additional-python-modules` |
| Python wheel (`internal-data-lake-jobs-*.whl`) | `build/wheels/` | Installable library for local dev or Glue Spark | `pip install build/wheels/...` when developing locally or packaging Spark dependencies |

Artifact contents mirror the repository structure documented in **[LIBS_STRUCTURE.md](./LIBS_STRUCTURE.md)**.

## Glue Job Integration

- Always reference the S3 object produced by the packaging pipeline (uploaded via infrastructure automation).
- PyShell jobs must also pin third-party modules through `--additional-python-modules`; see **[Job Development Guide](./JOB_DEVELOPMENT.md)**.
- Spark jobs can attach wheels, but confirm compatibility in **[Job Development Guide](./JOB_DEVELOPMENT.md)** before promoting changes.

## Local Development Tips

- Add the repo root to `PYTHONPATH` (`export PYTHONPATH=$(pwd):$PYTHONPATH`) to import `libs.*` without zipping.
- Use `pip install build/wheels/internal-data-lake-jobs-*.whl` for isolated environments that mimic Glue Spark behavior.
- Keep packaging outputs out of version control; rely on the Make targets instead of manual `zip` commands.

## Troubleshooting

Issue surfaces in Glue? Run through the checks in **[AWS_GLUE_TROUBLESHOOTING.md](./AWS_GLUE_TROUBLESHOOTING.md)**. Common packaging-related fixes include:

- Missing `libs` imports → artifact path mismatch or stale upload (rebuild with `make package`).
- PyShell dependency install failures → prefer `--additional-python-modules`; avoid uploading incompatible wheels.
- Out-of-date scripts → rerun deployment script to sync the latest `jobs/` files to S3.

## Related References

- [Development Guide](./DEVELOPMENT.md#build--package)
- [Deployment Guide](./DEPLOYMENT.md#publishing-artifacts)
- [Job Development Guide](./JOB_DEVELOPMENT.md)
- [AWS Glue Troubleshooting](./AWS_GLUE_TROUBLESHOOTING.md)

Keep this file short; adjust the packaging process via the scripts or Make targets and update the linked docs when behavior changes.
