# Medallion Job Pattern - MedallionJobBase

**Reading Time**: ~4 minutes

## Overview

The **MedallionJobBase** pattern provides a standardized, configuration-driven approach to building Bronze, Silver, and Gold layer jobs in our data warehouse. It combines Pydantic validation, three-tier parameter resolution, and object-oriented design to create maintainable, reusable data processing jobs.

**Key Benefits**:
- ✅ Type-safe configuration with automatic validation
- ✅ Minimal overrides: implement `extract()`, `transform()`, `load()`
- ✅ Source-specific defaults reduce parameters needed
- ✅ Consistent error handling and logging
- ✅ Dual catalog support (spark_catalog for external, glue_catalog for Iceberg)

## Job Class Hierarchy

```
ConfigBase (libs/common/config/config_base.py)
    ↓
JobConfigBase (libs/common/config/job_config.py)
    ↓
BronzeJobConfig / SilverJobConfig / GoldJobConfig (libs/common)
    ↓
SourceSpecificConfig (e.g., TeamTailorBronzeConfig in job file)
    ↓ (used by)
MedallionJobBase (libs/pyspark)
    ↓
SourceSpecificJob (e.g., TeamTailorBronzeJob in job file)
```

### Configuration Hierarchy

**1. ConfigBase** - Foundation (libs/common)
- Three-tier parameter resolution (env vars → job params → defaults)
- AWS Glue argument parsing
- Common parameters (job_name, environment, region, log_level)

**2. JobConfigBase** - Common Job Parameters (libs/common)
- `source_name`: Source system identifier
- `warehouse_path`: Iceberg warehouse location
- `create_tables`: Whether to create tables
- `show_counts`: Whether to show record counts

**3. Layer-Specific Configs** (libs/common - BASE classes only)
- **BronzeJobConfig**: `raw_database`, `raw_table`, `bronze_database`, `bronze_table`
- **SilverJobConfig**: `bronze_database`, `bronze_table`, `silver_database`, `silver_table`
- **GoldJobConfig**: `silver_database`, `silver_table`, `gold_database`, `gold_table`

**4. Source-Specific Configs** (Defined in each job file for modularity)
- Extend layer config with source-specific defaults and auto-configuration
- Example: `TeamTailorBronzeConfig` (in `teamtailor_bronze_job.py`) auto-configures table names and S3 paths
- 💡 **Location**: Each job file defines its own config class at the top of the file

### Job Implementation Hierarchy

**MedallionJobBase** provides:
- Spark and Glue context management
- Iceberg catalog configuration
- Read/write methods for external and Iceberg tables
- Abstract methods: `extract()`, `transform()`, `load()`

**Source-Specific Jobs** implement:
- `extract()`: Read from source (external table or Iceberg)
- `transform()`: Apply business logic and transformations
- `load()`: Write to target Iceberg table
- `create_tables()`: (Optional) Create table schemas

## Implementation Pattern

### Step 1: Create Source-Specific Config (in job file)

Define a configuration class at the top of your job file with sensible defaults:

```python
# jobs/bronze/teamtailor_bronze_job.py

from typing import ClassVar, Optional
from pydantic import Field
from libs.common import BronzeJobConfig

class TeamTailorBronzeConfig(BronzeJobConfig):
    """Team Tailor Bronze configuration with auto-configuration."""

    source_name: ClassVar[str] = "teamtailor"
    entity_type: str = Field(
        description="Team Tailor entity type (candidates, jobs, applications, etc.)"
    )
    teamtailor_raw_path: Optional[str] = Field(
        default=None,
        description="Auto-configured S3 path for raw data"
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure table names and paths based on entity_type."""
        if not self.raw_table_name:
            self.raw_table_name = f"teamtailor__talent__{self.entity_type}_raw"
        if not self.bronze_table_name:
            self.bronze_table_name = f"teamtailor__talent__{self.entity_type}_bronze"

        # Auto-configure S3 path
        self.teamtailor_raw_path = f"{self.raw_zone_path}teamtailor/{self.entity_type}/"
```

> 💡 **Tip**: Configs are now defined IN the job file for better modularity. See `jobs/bronze/teamtailor_bronze_job.py` for a real example.

### Step 2: Create Job Class (in same file)

Extend layer-specific base class and implement ETL methods:

```python
# jobs/bronze/teamtailor_bronze_job.py (continued)

from libs.pyspark import BronzeJobBase  # Or SilverJobBase, GoldJobBase
from pyspark.sql.functions import col, current_timestamp, lit

class TeamTailorBronzeJob(BronzeJobBase):
    """Team Tailor Bronze Job - processes raw to bronze layer."""

    def extract(self):
        """Extract data from raw external table."""
        return self.read_external_table(
            self.config.raw_database_name,
            self.config.raw_table_name
        )

    def transform(self, df):
        """Transform data with business logic."""
        return df.select(
            col("payload"),
            col("metadata"),
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            col("year"),
            col("month")
        )

    def load(self, df):
        """Load DataFrame to Iceberg bronze table."""
        self.write_to_iceberg(
            df,
            self.config.bronze_database_name,
            self.config.bronze_table_name
        )

    def create_tables(self):
        """Create raw external table and bronze Iceberg table."""
        # Create raw external table pointing to S3
        self.spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.config.raw_database_name}.{self.config.raw_table_name}
            (candidate_id STRING, candidate_name STRING, created_at TIMESTAMP, ...)
            STORED AS JSONL
            LOCATION '{self.config.teamtailor_raw_path}'
        """)

        # Create bronze Iceberg table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}
            (candidate_id STRING, candidate_name STRING, ...)
            USING iceberg
            PARTITIONED BY (year, month)
        """)
```

> 💡 **Tip**: See complete examples in `jobs/bronze/teamtailor_bronze_job.py`.

### Step 3: Main Execution (Ultra-simplified!)

```python
if __name__ == "__main__":
    session = SparkSessionFactory.create_session()
    config = TeamTailorBronzeConfig.from_args()
    TeamTailorBronzeJob(session.spark, config).run()
    session.spark.stop()
```

`run()` automatically prepares databases, creates tables when requested, and orchestrates the ETL steps with consistent logging.

## Three-Tier Parameter Resolution

Parameters are resolved in this priority order:

1. **Environment Variables** (highest priority)
   ```bash
   export TEAMTAILOR_BRONZE_PIPELINE_RAW_DATABASE="custom_raw_db"
   ```

2. **Job Parameters** (command line)
   ```bash
   --RAW_DATABASE custom_raw_db
   ```

3. **Config Defaults** (lowest priority)
   ```python
   raw_database: str = Field(default="inline_develop_raw_zone_ingestion")
   ```

### Minimal Execution

With source-specific defaults, you only need to provide required parameters:

```bash
python jobs/bronze/teamtailor_bronze_job.py \
  --JOB_NAME teamtailor_bronze_pipeline \
  --ENTITY_TYPE candidates
```

### Full Override

Override any default when needed:

```bash
python jobs/bronze/teamtailor_bronze_job.py \
  --JOB_NAME teamtailor_bronze_pipeline \
  --ENTITY_TYPE candidates \
  --RAW_DATABASE_NAME custom_raw_db \
  --BRONZE_DATABASE_NAME custom_bronze_db \
  --WAREHOUSE_PATH s3://custom-bucket/warehouse/
```

## Base Class Features

### MedallionJobBase Provides

**Catalog Management**:
- `spark_catalog`: For external tables (Hive-compatible)
- `glue_catalog`: For Iceberg tables (AWS Glue Catalog)

**Read Methods**:
- `read_external_table(database, table, filters=None)`: Read from external table
- `read_iceberg_table(database, table, filters=None)`: Read from Iceberg table

**Write Methods**:
- `write_to_iceberg(df, database, table, mode="append")`: Write to Iceberg table

**Table Creation**:
- `create_external_table(...)`: Create external table in spark_catalog
- `create_iceberg_table(...)`: Create Iceberg table in glue_catalog

**Utilities**:
- Automatic Iceberg catalog configuration
- Layer detection (bronze/silver/gold)
- Consistent logging

## Best Practices

### 1. Configuration
- ✅ Define source-specific config **in the job file** (not in libs/common)
- ✅ Always extend the appropriate layer config (`BronzeJobConfig`, `SilverJobConfig`, `GoldJobConfig`)
- ✅ Use `model_post_init()` for auto-configuration of table names and S3 paths
- ✅ Provide sensible defaults and descriptive field descriptions
- ✅ Keep libs/common generic and reusable across projects

### 2. Job Implementation
- ✅ Keep `extract()`, `transform()`, `load()` focused and simple
- ✅ Use `self.config` to access all configuration parameters
- ✅ Use `self.spark` for Spark operations
- ✅ Use `self.logger` for consistent logging
- ✅ Implement `create_tables()` as a separate method
- ✅ Let `job.run()` handle infrastructure setup automatically

### 3. Data Type Handling
- ✅ **CRITICAL**: Use Python values in dictionaries, not Spark Column expressions
  ```python
  # ❌ WRONG - Column expression in dictionary
  row_data = {
      "created_at": current_timestamp(),  # ERROR!
      "updated_at": current_timestamp()   # ERROR!
  }
  df = spark.createDataFrame([row_data], schema)

  # ✅ CORRECT - Python datetime object
  from datetime import datetime
  row_data = {
      "created_at": datetime.now(),  # Python datetime
      "updated_at": datetime.now()   # Python datetime
  }
  df = spark.createDataFrame([row_data], schema)
  ```
- ✅ Use Spark functions (`current_timestamp()`, `lit()`, etc.) in DataFrame operations, not dict creation
- ✅ Match Python types to Spark schema types:
  - `datetime.datetime` → `TimestampType()`
  - `datetime.date` → `DateType()`
  - `int` → `IntegerType()`
  - `float` → `DoubleType()`
  - `str` → `StringType()`
  - `bool` → `BooleanType()`

### 4. Error Handling
- ✅ Let Pydantic handle validation errors automatically
- ✅ Add try-catch in transform logic for data quality issues
- ✅ Log errors with context using `self.logger.error()`

### 5. Testing
- ✅ Test with minimal parameters (defaults)
- ✅ Test with full parameter override
- ✅ Validate configuration before running ETL
- ✅ Check table creation separately from ETL

## Related Documentation

- [Medallion Architecture](./MEDALLION_ARCHITECTURE.md) - Complete architecture reference with requirements and patterns
- [Team Tailor Bronze Layer](./sources/teamtailor/BRONZE.md) - Team Tailor-specific implementation
- [Configuration Base](../libs/common/config/config_base.py) - ConfigBase implementation
- [Job Configs (BASE)](../libs/common/config/job_config.py) - Layer-specific BASE configurations
- [Real Examples](../jobs/) - Browse `jobs/bronze/`, `jobs/silver/` for complete implementations

## Quick Reference

- Define config and job class in the same module so defaults stay close to the implementation.
- Override only the ETL hooks you need (`extract()`, `transform()`, `load()`, optional `create_tables()`).
- Rely on `job.run()` for orchestration; avoid duplicating setup logic in the main block.

For deeper dives, see the Spark jobs, local development, and architecture guides linked near the top of this document.
