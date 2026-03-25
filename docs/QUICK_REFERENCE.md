# Quick Reference Guide for Data Engineers

> **Focus on building pipelines, not infrastructure.** This guide provides copy-paste commands for common tasks.

## Prerequisites

- DevContainer running (VS Code → "Reopen in Container" or `devcontainer up --workspace-folder .`)
- Environment configured: `direnv allow` (one-time)
- LocalStack prepared: `make prepare-localstack` (one-time per session)

## Running Pipelines

> **Note**: Each layer may use different entity types. Always check `docs/sources/*/README.md` for source-specific entity mappings (e.g., Team Tailor: `candidates` → `candidate_profiles` → `talent_time_to_fill`).

### Run Layers Individually

```bash
# 1. Extract raw data (API → S3)
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates

# 2. Process to Bronze (S3 → Iceberg)
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--CREATE_TABLES true"

# 3. Transform to Silver (Bronze → Silver)
make run-silver DATA_SOURCE=teamtailor ENTITY_TYPE=candidate_profiles \
  ARGS="--CREATE_TABLES true"

# 4. Aggregate to Gold (Silver → Gold)
make run-gold DATA_SOURCE=teamtailor ENTITY_TYPE=talent_time_to_fill \
  ARGS="--CREATE_TABLES true"
```

## Running Individual Jobs

### PyShell Jobs (Raw Layer)

```bash
make pyshell-run JOB=jobs/raw/teamtailor_raw_job.py \
  ARGS="--ENTITY_TYPE candidates --START_DATE 2025-01-01"
```

### Spark Jobs (Bronze/Silver/Gold Layers)

```bash
make spark-submit JOB=jobs/bronze/teamtailor_bronze_job.py \
  ARGS="--CREATE_TABLES true --ENTITY_TYPE candidates"
```

## Testing

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests only
make test-integration

# Run with coverage
make test-coverage
```

## Code Quality

```bash
# Format code
make format

# Auto-fix formatting and imports
make autofix

# Lint code
make lint
```

## Common Workflows

### First Time Setup

```bash
# 1. Open in DevContainer (VS Code → "Reopen in Container")
# 2. Configure environment
direnv allow
make prepare-localstack

# 3. Verify setup
make services-status
make validate
```

### Daily Development

```bash
# 1. Ensure environment is loaded (automatic with direnv)
# 2. Run your pipeline (run each layer individually)
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates ARGS="--CREATE_TABLES true"
# ... continue with silver and gold layers

# 3. Test changes
make test

# 4. Format before committing
make format
make lint
```

### Debugging

```bash
# Check service status
make services-status

# View LocalStack resources
awslocal s3 ls s3://local-bucket/
awslocal glue get-databases

# Check logs (when jobs are running)
# Spark UI: http://localhost:4040
# Jupyter Lab: http://localhost:8888
```

## Source-Specific Examples

### Team Tailor

```bash
# Complete pipeline (note: different entity types per layer)
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates ARGS="--CREATE_TABLES true"
make run-silver DATA_SOURCE=teamtailor ENTITY_TYPE=candidate_profiles ARGS="--CREATE_TABLES true"
make run-gold DATA_SOURCE=teamtailor ENTITY_TYPE=talent_time_to_fill ARGS="--CREATE_TABLES true"
```

**See**: [Team Tailor Integration](./sources/teamtailor/README.md) for complete documentation.

### ClickUp

```bash
# Run layers individually
make run-raw DATA_SOURCE=clickup ENTITY_TYPE=tasks
make run-bronze DATA_SOURCE=clickup ENTITY_TYPE=tasks ARGS="--CREATE_TABLES true"
make run-silver DATA_SOURCE=clickup ENTITY_TYPE=tasks ARGS="--CREATE_TABLES true"
make run-gold DATA_SOURCE=clickup ENTITY_TYPE=tasks ARGS="--CREATE_TABLES true"
```

**See**: [ClickUp Integration](./sources/clickup/README.md) for complete documentation.

## Troubleshooting

### Services Not Running

```bash
make services-status  # Check service health
```

If LocalStack is down, rebuild the container:

- VS Code: Command Palette → "Dev Containers: Rebuild Container"
- CLI: `devcontainer rebuild --workspace-folder .`

### Environment Not Loaded

```bash
direnv allow  # Enable automatic environment loading
```

### Secrets Not Found

```bash
make prepare-localstack  # Sync secrets from AWS to LocalStack
```

### Tests Failing

```bash
make services-status  # Verify LocalStack is running
make validate         # Validate environment
```

## Related Documentation

- **[Development Guide](./DEVELOPMENT.md)** - Complete setup and workflow
- **[Job Development Guide](./JOB_DEVELOPMENT.md)** - Building new jobs
- **[Source-Specific Docs](./sources/)** - Pipeline documentation per source

---

**Last Updated**: 2025-01-XX  
**Focus**: Quick copy-paste commands for data engineers
