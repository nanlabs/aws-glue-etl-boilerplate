# Schema Evolution & Migrations Guide

**Read time: 3 minutes** | Last updated: 2025-09-30

Complete guide to managing database schemas and migrations in the NaNLABS Data Lakehouse using Apache Iceberg and the migration framework.

---

## Overview

Our platform uses **migration files** to manage all schema changes:
- ✅ Create tables (Iceberg and External)
- ✅ Add/remove columns
- ✅ Change data types
- ✅ Environment-specific configurations
- ✅ Automatic rollback on failures

---

## Quick Start

### 1. Create Migration File

```bash
migrations/005_add_customer_segment.sql
```

Format: `{number}_{description}.sql` - files execute in numerical order.

### 2. Write Migration SQL

```sql
-- Migration: id=005, author=data_team, type=iceberg, runAlways=false
-- Description: Add customer segmentation to silver layer
-- Rollback-Start
-- ALTER TABLE ${catalog}.${database}.events_silver DROP COLUMN customer_segment;
-- Rollback-End

-- Add new column
ALTER TABLE ${catalog}.${database}.events_silver
ADD COLUMN customer_segment STRING COMMENT 'Customer segment (high/medium/low value)';
```

### 3. Run Migration

```bash
# In dev container
make migrate

# Or manual execution
spark-submit jobs/utils/migration_runner_job.py --JOB_NAME migration_runner
```

---

## Common Scenarios

### Adding Columns (Safe)

```sql
-- Add nullable columns (metadata-only operation)
ALTER TABLE ${catalog}.${database}.events_silver
ADD COLUMN new_field STRING COMMENT 'Description',
ADD COLUMN another_field INT COMMENT 'Numeric field';
```

### Renaming Columns (Safe)

```sql
-- Rename without data rewrite
ALTER TABLE ${catalog}.${database}.events_silver
RENAME COLUMN old_name TO new_name;
```

### Dropping Columns (Caution)

```sql
-- May break downstream queries
ALTER TABLE ${catalog}.${database}.events_silver
DROP COLUMN unused_field;
```

### Creating Tables

**Iceberg Table** (Bronze/Silver/Gold):
```sql
CREATE TABLE ${catalog}.${database}.customer_events (
    event_id STRING NOT NULL,
    customer_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_timestamp))
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'gzip'
);
```

**External Table** (Raw Zone):
```sql
CREATE EXTERNAL TABLE spark_catalog.${database}.raw_events (
    raw_payload STRING COMMENT 'Raw JSON data'
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3a://${bucket_name}/raw/events/'
TBLPROPERTIES ('classification'='json');
```

---

## Variable Interpolation

Use variables for environment-specific values:

| Variable | Example | Description |
|----------|---------|-------------|
| `${catalog}` | `glue_catalog` | Iceberg catalog name |
| `${database}` | `develop_silver` | Target database |
| `${bucket_name}` | `data-lake-dev` | S3 bucket |
| `${environment}` | `develop` | Environment |

```sql
-- Variables replaced at runtime
CREATE TABLE ${catalog}.${database}.my_table (id STRING)
LOCATION 's3a://${bucket_name}/warehouse/${environment}/my_table/';
```

---

## Layer-Specific Strategies

### Bronze Layer
**Conservative**: Preserve all raw data, add metadata only

```sql
ALTER TABLE ${catalog}.${database}.events_bronze
ADD COLUMN api_version STRING COMMENT 'API version tracking';
```

### Silver Layer  
**Balanced**: Support business logic evolution

```sql
ALTER TABLE ${catalog}.${database}.events_silver
ADD COLUMN data_quality_score DOUBLE COMMENT 'Quality score (0.0-1.0)',
ADD COLUMN validation_flags ARRAY<STRING> COMMENT 'Validation issues';
```

### Gold Layer
**Aggressive**: Adapt to changing analytics needs

```sql
ALTER TABLE ${catalog}.${database}.customer_analytics
ADD COLUMN predicted_churn DOUBLE COMMENT 'Churn probability',
ADD COLUMN lifetime_value DECIMAL(18,2) COMMENT 'Customer LTV';
```

---

## Migration Header Reference

Every migration must include:

```sql
-- Migration: id={number}, author={name}, type={iceberg|external|utility}, runAlways={true|false}
-- Description: {clear description}
-- Preconditions: {optional conditions}
-- Rollback-Start
-- {SQL to undo changes}
-- Rollback-End
```

**Fields**:
- `id`: Unique number (matches filename)
- `type`: Table type (`iceberg`, `external`, `utility`)
- `runAlways`: Run every time (`true`) or once (`false`)
- `Rollback`: SQL statements to reverse the migration

---

## Best Practices

### Planning
✅ Test in development first  
✅ Document the business reason  
✅ Check downstream impact  

### Implementation
✅ One change per migration  
✅ Use descriptive names  
✅ Include rollback SQL  

### Safety
✅ Always use `IF NOT EXISTS` / `IF EXISTS`  
✅ Add helpful comments  
✅ Monitor after deployment  

---

## Troubleshooting

**Migration already executed?**
```sql
SELECT * FROM ${catalog}.${database}.migration_tracking
WHERE migration_file = 'your_migration.sql';
```

**Column already exists?**
```sql
-- Use IF NOT EXISTS
ALTER TABLE ${catalog}.${database}.table_name
ADD COLUMN IF NOT EXISTS new_column STRING;
```

**Need to rollback?**
Create reverse migration with higher number:
```sql
-- migrations/006_rollback_005.sql
ALTER TABLE ${catalog}.${database}.events_silver
DROP COLUMN customer_segment;
```

---

## Configuration

Set environment variables before running migrations:

```bash
export STAGE=develop
export BUCKET_NAME=my-data-lake
export DATABASE_PREFIX=nan
export ICEBERG_DATABASE=develop_silver
```

Or use Makefile (in dev container):
```bash
make migrate                # Run all pending migrations
make migrate-dry-run        # Preview without execution
make migrate-upload         # Upload files to S3 only
```

---

**Next Steps**:
- [Development Guide](./DEVELOPMENT.md) - Set up local environment
- [Medallion Architecture](./MEDALLION_ARCHITECTURE.md) - Complete architecture reference
- [Best Practices](./BEST_PRACTICES.md) - Development standards
