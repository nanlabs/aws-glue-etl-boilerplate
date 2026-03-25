# Best Practices Guide

## Overview

This guide captures the conventions that keep jobs, libraries, and documentation consistent across the data warehouse. It links to the canonical references so updates live in one place.

## Glue Job Configuration

- Provision jobs through the shared Terraform modules or scripts in `scripts/`; avoid ad‑hoc CLI commands.
- Configure Iceberg catalogs through Glue default arguments (see the **[Deployment Guide](./DEPLOYMENT.md)**).
- Keep job and bucket regions aligned, and grant least-privilege IAM roles as documented in **[SECRETS_MANAGEMENT.md](./SECRETS_MANAGEMENT.md)**.

## Job Implementation Pattern

- Extend the appropriate Medallion base (`BronzeJobBase`, `SilverJobBase`, `GoldJobBase`).
- Declare `config_class` and override `extract()`, `transform()`, `load()`, plus optional hooks (`create_tables()`, `validate()`).
- Use `MyJob.run_from_cli()` in the entrypoint; the base handles session creation and teardown.

## Code Organization

- Place jobs under `jobs/<layer>/` and shared utilities under `libs/` following the structure in **[LIBS_STRUCTURE.md](./LIBS_STRUCTURE.md)**.
- Follow isort/ruff groupings for imports: standard library → third party → internal.
- Keep modules focused; factor reusable logic into helpers within `libs/`.

## Configuration System

- Rely on `ConfigBase` resolution order (CLI args → environment → defaults). Document required fields via `Field(description=...)`.
- Use `ClassVar` for immutable attributes such as `source_name` and compute derived values in `model_post_init`.

```python
from typing import ClassVar
from libs.common import BronzeJobConfig


class TeamTailorBronzeConfig(BronzeJobConfig):
    source_name: ClassVar[str] = "teamtailor"
    entity_type: str = Field(..., description="Entity type (candidates, jobs, etc.)")

    def model_post_init(self, __context):
        if not self.raw_table:
            self.raw_table = f"teamtailor__talent__{self.entity_type}_raw"
        if not self.bronze_table:
            self.bronze_table = f"teamtailor__talent__{self.entity_type}_bronze"
```

## Error Handling & Validation

- Wrap pipeline execution in try/except and log failures with context.
- Implement lightweight config validators for path suffixes, enum checks, and cross-field dependencies.

```python
def run(self):
    self.logger.info("Starting pipeline")
    try:
        df = self.transform(self.extract())
        self.load(df)
    except Exception as exc:
        self.logger.exception("Pipeline failed", exc_info=exc)
        raise
    self.logger.info("Pipeline completed")
```

## Logging

- Use the logger supplied by the job base classes; configuration resides in `libs/common/logging.py`.
- Avoid `print()` and never log secrets or PII—see **[SECRETS_MANAGEMENT.md](./SECRETS_MANAGEMENT.md)** for redaction rules.

## Data Processing

- Favor explicit column references (`col(...)`) and shared schemas. Complex parsing should live in reusable helpers within `libs/`.
- Preserve raw payloads in Bronze for replay, and keep transformations layer-appropriate (business logic belongs to Silver/Gold).

## Testing

- Follow **[TESTING.md](./TESTING.md)** for unit, integration, and e2e patterns.
- Reuse fixtures in `tests/fixtures/` and leverage config factories instead of rebuilding objects manually.

## AWS Client Initialization

- **Always use `create_boto3_client()`** from `libs.common.utils.aws` instead of directly calling `boto3.client()`.
- The utility function provides automatic region detection, LocalStack endpoint support, and consistent error handling.
- Region detection priority: explicit parameter → environment variables → boto3.Session() auto-detection → fallback.

```python
# ✅ Correct: Use create_boto3_client
from libs.common.utils.aws import create_boto3_client

s3_client = create_boto3_client('s3')  # Auto-detects region
ssm_client = create_boto3_client('ssm', region_name='us-west-2')  # Explicit region

# ❌ Avoid: Direct boto3.client() calls
import boto3
s3_client = boto3.client('s3')  # May fail in AWS Glue without explicit region
```

## Performance & Security

- Partitioning, worker sizing, and caching guidance lives in **[JOB_DEVELOPMENT.md](./JOB_DEVELOPMENT.md)**.
- Apply security practices from **[SECRETS_MANAGEMENT.md](./SECRETS_MANAGEMENT.md)** and the security handbook (IAM, network boundaries, secrets hygiene).

## Deployment & Monitoring

- Use the Makefile targets and deployment scripts described in **[DEPLOYMENT.md](./DEPLOYMENT.md)**.
- For runbooks, dashboards, and alerting, follow **[OPERATIONS.md](./OPERATIONS.md)**.

## Related Documentation

- [Medallion Job Pattern](./MEDALLION_JOB_PATTERN.md)
- [Job Development Guide](./JOB_DEVELOPMENT.md)
- [Medallion Architecture](./MEDALLION_ARCHITECTURE.md)
