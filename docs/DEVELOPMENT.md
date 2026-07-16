# Development Guide

## First contribution walkthrough

This guide provides a minimal path for first-time contributors to validate the project locally.

You can complete the steps below without AWS credentials or access to private infrastructure.

### Option 1: Use the DevContainer (recommended)

Follow the DevContainer workflow described in the README before continuing.

### Option 2: Local environment

If you are not using the DevContainer, follow the setup instructions in the README before continuing.

## Recommended local flow

### 1. Bootstrap the project

Complete the project setup described in the README.

```bash
make bootstrap
```

After the bootstrap completes, continue with the validation steps below.

### 2. Validate the environment

```bash
make check-env
```

### 3. Run quality checks

```bash
make lint
make type-check
```

### 4. Run unit tests

```bash
make test-unit
```

Unit tests can be executed locally and do not require AWS credentials.

Optional NaNLABS commands (if available):

```bash
make nan-health
make nan-skills
```

## Troubleshooting optional baseline checks

- `nan-* command not found`:
  install or update your NaNLABS workstation baseline, or continue with the required project checks.
- `nan-doctor` reports non-compliant:
  fix the reported dependencies and rerun `make nan-health`.
- Running in external/non-NaNLABS environments:
  `make check-env` and `make nan-health` are safe and will skip optional commands when unavailable.

## Example local ETL flow

A minimal local validation path is:

```text
Raw → Bronze → Silver → Gold
```

Run the jobs in order:

```bash
make run-raw DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-bronze DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-silver DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-gold DATA_SOURCE=public_api ENTITY_TYPE=posts
```

## Direct runs

```bash
python jobs/raw/public_api_raw_job.py --ENTITY_TYPE posts
python jobs/bronze/public_api_bronze_job.py --ENTITY_TYPE posts
python jobs/silver/public_api_silver_job.py --ENTITY_TYPE posts
python jobs/gold/public_api_gold_job.py --ENTITY_TYPE posts
```
