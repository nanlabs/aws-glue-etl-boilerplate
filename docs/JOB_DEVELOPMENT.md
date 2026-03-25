# Job Development Guide

> **📖 Read time: ~8 minutes**

This guide covers building both Spark and PyShell jobs for the NaNLABS Data Warehouse platform.

## Overview

### When to Use Spark vs PyShell

| Job Type | Use Case | Layer | Startup Time | Cost |
|----------|----------|-------|--------------|------|
| **PyShell** | API extraction, lightweight transforms | Raw Zone | ~30 seconds | Lower |
| **Spark** | Large-scale data processing | Bronze/Silver/Gold | ~3-5 minutes | Higher |

### Architecture

```
External APIs → [PyShell Jobs] → Raw Zone S3 → [Spark Jobs] → Bronze → Silver → Gold → Analytics
```

**Technologies**:

- **Spark**: 3.5.4 (AWS Glue 5.0)
- **Python**: 3.11
- **Iceberg**: 1.7.1
- **Catalogs**: `spark_catalog` (external), `glue_catalog` (Iceberg)

## Common Patterns

### Configuration Pattern

Both job types use Pydantic-based configuration:

```python
from typing import ClassVar
from libs.common import BronzeJobConfig, RawJobConfig
from pydantic import Field

# Spark job config
class MyBronzeConfig(BronzeJobConfig):
    source_name: ClassVar[str] = "my_source"
    custom_param: str = Field(default="value")

# PyShell job config
class MyRawConfig(RawJobConfig):
    source_name: ClassVar[str] = "my_source"
    entity_type: str
```

### Testing Pattern

```python
# Both job types support local testing
if __name__ == "__main__":
    config = MyConfig.from_args()
    job = MyJob(config)
    job.run()
```

## Spark Jobs (Bronze/Silver/Gold)

### Quick Start

#### 1. Create Configuration

```python
from typing import ClassVar
from libs.common import BronzeJobConfig
from pydantic import Field

class TeamTailorBronzeConfig(BronzeJobConfig):
    """Team Tailor Bronze configuration with sensible defaults."""

    source_name: ClassVar[str] = "teamtailor"
    entity_type: str = Field(..., description="Entity type (candidates, jobs, etc.)")

    def model_post_init(self, __context) -> None:
        if not self.raw_table:
            self.raw_table = f"teamtailor__talent__{self.entity_type}_raw"
        if not self.bronze_table:
            self.bronze_table = f"teamtailor__talent__{self.entity_type}_bronze"
```

#### 2. Implement Job

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp
from libs.pyspark import MedallionJobBase, SparkSessionFactory

class TeamTailorBronzeJob(MedallionJobBase):
    """Team Tailor Bronze job implementation."""

    config_class = TeamTailorBronzeConfig

    def extract(self) -> DataFrame:
        if self.is_local:
            # Local: Direct S3 read
            return self.read_from_s3(
                s3_path=f"{self.config.raw_zone_path}teamtailor/{self.config.entity_type}/",
                schema=self.define_schema()
            )
        # Glue: External table
        return self.read_external_table(
            database=self.config.raw_database,
            table=self.config.raw_table
        )

    def transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("id").alias("candidate_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            to_timestamp(col("created_at")).alias("created_at"),
        )

    def load(self, df: DataFrame) -> None:
        self.write_to_iceberg(
            df=df,
            database=self.config.bronze_database,
            table=self.config.bronze_table,
            partition_by=[] if self.config.entity_type in ["candidates", "jobs"] else ["year", "month"],
        )

if __name__ == "__main__":
    session = SparkSessionFactory.create_session()
    config = TeamTailorBronzeConfig.from_args()
    TeamTailorBronzeJob(session.spark, config).run()
    session.spark.stop()
```

#### 3. Run Locally

**Recommended (using tier-specific targets):**

```bash
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--CREATE_TABLES true --SHOW_COUNTS true"
```

**Alternative (using spark-submit directly):**

```bash
make spark-submit JOB=jobs/bronze/teamtailor_bronze_job.py \
  ARGS="--CREATE_TABLES true --SHOW_COUNTS true --ENTITY_TYPE candidates"
```

### Environment Detection

Jobs automatically detect local vs cloud environment:

```python
# LOCAL MODE (no --JOB_NAME)
python jobs/bronze/my_job.py
# → Creates SparkSession with Derby + LocalStack

# GLUE MODE (with --JOB_NAME)
python jobs/bronze/my_job.py --JOB_NAME my_job
# → Creates GlueContext with Glue Data Catalog + AWS S3
```

### Apache Iceberg Integration

#### Writing to Iceberg

```python
# Append data
self.write_to_iceberg(
    df=df,
    database="bronze",
    table="teamtailor__talent__candidates_bronze",
    partition_by=["event_date"],
    mode="append"
)

# Overwrite
self.write_to_iceberg(
    df=df,
    database="silver",
    table="profiles",
    mode="overwrite"
)
```

#### Reading from Iceberg

```python
# Query Iceberg table
df = self.spark.table("glue_catalog.bronze.teamtailor__talent__candidates_bronze")

# Time travel query
df = self.spark.sql("""
    SELECT * FROM glue_catalog.bronze.teamtailor__talent__candidates_bronze
    FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-01 00:00:00'
""")
```

See [Schema Evolution Guide](./SCHEMA_EVOLUTION.md) for detailed Iceberg management.

## PyShell Jobs (Raw Zone)

### Quick Start

#### 1. Create Configuration

```python
from typing import ClassVar
from libs.common import RawJobConfig
from libs.pyshell import PyShellJobBase

class TeamTailorRawConfig(RawJobConfig):
    """Team Tailor Raw job configuration."""

    source_name: ClassVar[str] = "teamtailor"
    entity_type: str  # candidates, jobs, applications, etc.
```

#### 2. Implement Job

```python
from libs.pyshell import PyShellJobBase
from libs.common import RawJobConfig

class TeamTailorRawJob(PyShellJobBase):
    """Team Tailor Raw job implementation."""

    config_class = TeamTailorRawConfig

    def extract(self):
        """Fetch data from Team Tailor API."""
        # API extraction logic
        return api_data

    def load(self, data):
        """Write data to S3."""
        self.write_to_s3(
            data=data,
            s3_path=f"{self.config.raw_zone_path}teamtailor/{self.config.entity_type}/",
            partition_by=["year", "month"] if self.config.entity_type in ["applications", "interviews", "activities"] else None
        )

if __name__ == "__main__":
    config = TeamTailorRawConfig.from_args()
    job = TeamTailorRawJob(config)
    job.run()
```

#### 3. Run Locally

**Recommended (using tier-specific targets):**

```bash
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--START_DATE 2025-01-01"
```

**Alternative (using pyshell-run directly):**

```bash
make pyshell-run JOB=jobs/raw/teamtailor_raw_job.py \
  ARGS="--ENTITY_TYPE candidates --START_DATE 2025-01-01"
```

### Common Parameters

```bash
--job_name               # Name of the Glue job (required for AWS)
--environment           # Environment: develop/staging/prod
--stage                 # Stage: local/dev/staging/prod
--start_date            # Start date: YYYY-MM-DD format
--end_date              # End date: YYYY-MM-DD format
--s3_bucket             # S3 bucket name
--s3_prefix             # S3 prefix/path
```

### Source-Specific Examples

**API-Based (Team Tailor):**

```python
# Fetch from API, write to S3
def extract(self):
    api_client = TeamTailorAPIClient(api_token=self.config.api_token)
    return api_client.get_candidates(
        page_size=30
    )
```

**SFTP-Based (Example):**

```python
# Download from SFTP, write to S3
def extract(self):
    sftp_client = SFTPClient(
        host=self.config.sftp_host,
        credentials=self.get_secret("sftp-credentials")
    )
    return sftp_client.download_files(self.config.remote_path)
```

## Configuration & Parameters

### Three-Tier Resolution

Parameters resolved in priority order:

1. **Job Parameters**: `--param value` (highest priority)
2. **Environment Variables**: `PARAM_NAME`
3. **Config Defaults**: Class defaults (lowest priority)

### Common Parameters

```bash
--source_name           # Source system identifier
--warehouse_path        # Iceberg warehouse S3 path
--create_tables         # Create tables if missing (true/false)
--show_counts           # Display record counts (true/false)
--log_level             # Logging level (INFO/DEBUG/WARN/ERROR)
```

### Layer-Specific Parameters

**Bronze**: `--raw_database`, `--raw_table`, `--bronze_database`, `--bronze_table`  
**Silver**: `--bronze_database`, `--bronze_table`, `--silver_database`, `--silver_table`  
**Gold**: `--silver_database`, `--silver_table`, `--gold_database`, `--gold_table`

## Testing

### Unit Tests

```python
import pytest
from unittest.mock import Mock

@pytest.mark.unit
def test_transformation_logic():
    """Test business logic with mocked dependencies."""
    # Test transformation logic
    pass
```

### Integration Tests

```python
@pytest.mark.integration
def test_end_to_end_workflow():
    """Test complete workflow with LocalStack."""
    # Test with LocalStack services
    pass
```

See [Testing Guidelines](./TESTING.md) for detailed testing patterns.

## Deployment

📋 **Full deployment procedures**: [ClickUp: AWS Glue Deployment Runbook](link-to-clickup)

Quick checklist:

- [ ] Package artifacts: `make package`
- [ ] Verify job configuration
- [ ] Test job run locally first
- [ ] Deploy to AWS Glue

## Best Practices

### 1. Always Test Locally First

```bash
# Test locally using tier-specific targets (recommended)
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates

# Or use direct job execution
make spark-submit JOB=jobs/bronze/my_job.py
make pyshell-run JOB=jobs/raw/my_job.py ARGS="--START_DATE 2025-01-01"
```

### 2. Use Configuration Defaults

Define sensible defaults in your config class:

```python
class MyJobConfig(BronzeJobConfig):
    source_name: str = Field(default="my_source")
    raw_database: str = Field(default="my_raw_db")
    warehouse_path: str = Field(default="s3://local-bucket/warehouse/")
```

### 3. Environment-Aware Code

```python
def extract(self):
    if self.is_local:
        # Local: Direct S3 read
        return self.read_from_s3(...)
    else:
        # Glue: External table
        return self.read_external_table(...)
```

### 4. Error Handling

```python
try:
    data = self.extract()
except APIError as e:
    self.logger.error(f"API error: {e}")
    raise
```

## Troubleshooting

### Common Issues

| Issue | Quick Fix | Full Runbook |
|-------|-----------|--------------|
| ModuleNotFoundError | Add to `--additional-python-modules` | [ClickUp: Dependency Troubleshooting](link) |
| LocalStack connection | Check `make services-status` | [ClickUp: Local Development Issues](link) |
| Iceberg write errors | Check catalog configuration | [ClickUp: Iceberg Troubleshooting](link) |

📋 **Complete troubleshooting guide**: [ClickUp: Troubleshooting Playbook](link-to-clickup)

## Related Documentation

- [Development Guide](./DEVELOPMENT.md) - DevContainer setup and workflow
- [Medallion Architecture](./MEDALLION_ARCHITECTURE.md) - Layer requirements and patterns
- [Medallion Job Pattern](./MEDALLION_JOB_PATTERN.md) - Detailed API reference
- [Schema Evolution](./SCHEMA_EVOLUTION.md) - Iceberg schema management
- [Testing Guidelines](./TESTING.md) - Testing requirements
- [Data Source Docs](./sources/) - Source-specific examples and patterns

---

**Last Updated**: 2025-01-XX  
**Focus**: Unified job development guide
