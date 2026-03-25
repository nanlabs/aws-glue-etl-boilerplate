# Medallion Architecture - Complete Reference

**Reading Time**: ~8 minutes

## Overview

The Medallion Architecture is the industry-standard multi-layered data lakehouse pattern that progressively refines data from raw ingestion to business-ready analytics through four distinct layers: **Raw → Bronze → Silver → Gold**.

This architecture provides:
- ✅ **Clear data lineage** from source to analytics
- ✅ **Incremental quality improvement** at each layer
- ✅ **Flexibility** to reprocess data without re-ingestion
- ✅ **Performance optimization** tailored to each stage
- ✅ **Compliance** through immutable audit trails

## TL;DR

| Layer  | Primary Goal                     | Ownership | Default Storage | Key Docs |
|--------|----------------------------------|-----------|-----------------|----------|
| Raw    | Immutable landing + audit trail | PyShell   | S3 external tbl | [Job Development Guide](./JOB_DEVELOPMENT.md) • Source docs |
| Bronze | Typed staging with payload       | Spark     | Iceberg         | [Job Development Guide](./JOB_DEVELOPMENT.md) • Source docs |
| Silver | Business logic + data quality    | Spark     | Iceberg         | [Job Development Guide](./JOB_DEVELOPMENT.md) • Source docs |
| Gold   | Denormalized analytics sets      | Spark     | Iceberg         | [Job Development Guide](./JOB_DEVELOPMENT.md) • [Best practices](./BEST_PRACTICES.md) |

Cross-cutting concerns: configuration (`libs/common/config`), logging (`libs/common/logging.py`), secrets (`docs/SECRETS_MANAGEMENT.md`), and testing (`docs/TESTING.md`).

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEMS                                │
│            (APIs, Databases, Files, Streams)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │ Extraction Jobs (PyShell, Glue, etc)
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  RAW ZONE (Landing - Immutable Source of Truth)                 │
│  ─────────────────────────────────────────────────────────────  │
│  • Format: Native (JSONL, CSV, Parquet, etc)                    │
│  • Schema: None or minimal                                       │
│  • Tables: External tables (Hive/Spark catalog)                 │
│  • Processing: NONE - preserve as-is                             │
│  • Retention: Permanent (archive after X months)                │
└────────────────────────┬────────────────────────────────────────┘
                         │ Bronze Jobs (PySpark)
                         │ • JSON parsing → Typed columns
                         │ • Basic validation
                         │ • Deduplication
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE ZONE (Staging - Normalized)                             │
│  ─────────────────────────────────────────────────────────────  │
│  • Format: Iceberg (Parquet files + metadata)                   │
│  • Schema: Normalized entities (1 table per entity)             │
│  • Processing: Minimal - parse and type                          │
│  • Structure: Preserve payloads + extract key fields            │
│  • Retention: 2-3 years                                          │
└────────────────────────┬────────────────────────────────────────┘
                         │ Silver Jobs (PySpark)
                         │ • Business rules
                         │ • Data quality checks
                         │ • Enrichment
                         │ • SCD Type 2
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  SILVER ZONE (Clean & Conformed - Normalized SSOT)             │
│  ─────────────────────────────────────────────────────────────  │
│  • Format: Iceberg (Parquet + optimized layout)                 │
│  • Schema: Fully typed, validated, normalized                   │
│  • Processing: Full business logic applied                       │
│  • Structure: Normalized tables with FKs                         │
│  • Purpose: Single Source of Truth (SSOT)                       │
│  • Retention: Permanent                                          │
└────────────────────────┬────────────────────────────────────────┘
                         │ Gold Jobs (PySpark)
                         │ • Denormalization
                         │ • Pre-aggregation
                         │ • KPI calculation
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│  GOLD ZONE (Business-Ready - Denormalized)                     │
│  ─────────────────────────────────────────────────────────────  │
│  • Format: Iceberg (optimized for reads)                         │
│  • Schema: Denormalized for specific use cases                  │
│  • Processing: Joins, aggregations, KPIs                         │
│  • Structure: Wide tables, pre-joined dimensions                │
│  • Purpose: Analytics, BI, ML                                    │
│  • Retention: Variable by use case                               │
└─────────────────────────────────────────────────────────────────┘
                         │
                         ↓
        ┌────────────────┴────────────────┐
        │                                  │
    BI Tools                          ML Models
  (Tableau, etc)                    (SageMaker)
```

---

## Layer Responsibilities

### 📁 Raw zone – landing & audit trail

- **Purpose**: Immutable capture of source data exactly as received.
- **Format**: Native files (JSONL/CSV/Parquet) stored in S3 external tables.
- **Processing**: No transformations—only metadata enrichment (timestamps, batch ids).
- **Retention**: Long-lived; archive instead of delete.
- **Jobs**: PyShell extract-load patterns (`jobs/raw/*`).
- **Read more**: [Job Development Guide](./JOB_DEVELOPMENT.md) • Source-specific docs under `docs/sources/`.

### 🥉 Bronze zone – typed staging

- **Purpose**: Parse raw payloads into typed columns while preserving full JSON for reprocessing.
- **Format**: Iceberg tables with append-only ingestion and lineage columns.
- **Processing**: Basic parsing, type conversion, and deduplication; no business logic.
- **Retention**: Multi-year to support replay and backfills.
- **Jobs**: Spark medallion jobs (`jobs/bronze/*`).
- **Read more**: [Job Development Guide](./JOB_DEVELOPMENT.md) • Source bronze docs (e.g., `docs/sources/teamtailor/BRONZE.md`).

### 🥈 Silver zone – clean & conformed

- **Purpose**: Apply business rules, data quality checks, and normalization to build the SSOT.
- **Format**: Iceberg fact/dimension tables with SCD support and validation flags.
- **Processing**: Enrichment, surrogate keys, metrics, data quality scoring.
- **Retention**: Permanent history for analytics and regulatory review.
- **Jobs**: Spark medallion jobs (`jobs/silver/*`).
- **Read more**: [Medallion Job Pattern](./MEDALLION_JOB_PATTERN.md) • Source silver docs (when available).

### 🥇 Gold zone – business-ready analytics

- **Purpose**: Denormalized views and aggregations optimized for BI, ML, and downstream services.
- **Format**: Iceberg tables tuned for read performance (partition pruning, clustering).
- **Processing**: Joins, aggregations, window calculations, KPI preparation.
- **Retention**: Depends on consumer needs (often 6–24 months).
- **Jobs**: Spark medallion jobs (`jobs/gold/*`).
- **Read more**: [Best Practices](./BEST_PRACTICES.md) • Source gold docs (when available).
  customer_id STRING,
  date DATE,

  -- Event counts
  total_events INT,
  page_views INT,
  purchases INT,

  -- Revenue metrics
  total_revenue DECIMAL(10,2),
  avg_order_value DECIMAL(10,2),

  -- Engagement metrics
  session_count INT,
  avg_session_duration INT,

  -- Computed KPIs
  engagement_score DOUBLE,
  churn_risk STRING,

  computed_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (date)
```

#### Transformations:
- ✅ Joins (facts with dimensions)
- ✅ Aggregations (SUM, AVG, COUNT, etc)
- ✅ Window functions (ranks, moving averages)
- ✅ Complex KPIs and metrics
- ✅ ML feature engineering

#### Best Practices:
- ✅ **Denormalize for performance**: Pre-join frequently accessed data
- ✅ **Aggregate intelligently**: Balance granularity vs performance
- ✅ **Multiple Gold tables**: Different views for different use cases
- ✅ **Document business logic**: Clear definitions for metrics
- ✅ **Refresh strategy**: Incremental updates where possible
- ❌ Don't duplicate Silver (only create what's needed)

#### Job Type:
- **Tool**: AWS Glue PySpark
- **Pattern**: Read from Silver → Join/Aggregate → Write to Iceberg
- **Example**: `jobs/gold/source_gold_kpis_job.py`

---

## Data Flow Example - Team Tailor Candidates

### 1. RAW Zone:
```json
// s3://bucket/raw-zone/teamtailor/candidates/year=2025/month=01/*.jsonl
{"payload": "{\"id\":\"123\",\"type\":\"candidates\",\"attributes\":{...}}", "metadata": {...}}
```

### 2. BRONZE Zone:
```sql
SELECT
  '123' as candidate_id,
  'John Doe' as candidate_name,
  'john.doe@example.com' as email,
  TIMESTAMP '2025-01-15 10:30:00' as created_at,
  '{"resume_url":"https://...","status":"applied"}' as candidate_payload
FROM bronze_db.teamtailor__talent__candidates_bronze
```

### 3. SILVER Zone:
```sql
SELECT
  candidate_id,
  candidate_name,
  email,
  'Applied' as application_status,
  created_at,
  STRUCT('https://...' as resume_url, 'applied' as status) as properties,
  TRUE as is_active
FROM silver_db.talent_dim_candidate_profiles
```

### 4. GOLD Zone:
```sql
SELECT
  c.candidate_id,
  c.created_at,
  c.application_status,
  j.job_title,
  j.department,
  a.application_date,
  a.time_to_fill_days
FROM gold_db.talent_time_to_fill_gold
-- Pre-joined, ready for BI
```

---

## Comparison Matrix

| Aspect | RAW | BRONZE | SILVER | GOLD |
|--------|-----|--------|--------|------|
| **Format** | Native (JSONL/CSV) | Iceberg (Parquet) | Iceberg (Parquet) | Iceberg (Parquet) |
| **Schema** | Minimal/None | Typed + Payload | Fully typed | Denormalized |
| **Normalization** | N/A | Normalized | Normalized | Denormalized |
| **Transformations** | None | Parsing | Full business logic | Joins + Aggregations |
| **Data Quality** | None | Basic validation | Full validation | Validated |
| **Purpose** | Audit trail | Staging | SSOT | Analytics |
| **Consumers** | Reprocessing | Silver jobs | Gold jobs | BI, ML, APIs |
| **Retention** | Permanent | 2-3 years | Permanent | Variable |
| **Update pattern** | Append only | Append only | Append/Merge | Overwrite/Merge |

---

## Key Principles

### 1. **Immutability in Raw**
- Raw data is **never modified or deleted**
- Serves as permanent audit trail
- Enables full reprocessing of all layers

### 2. **Normalization in Silver**
- Silver is **normalized** (not denormalized)
- Serves as Single Source of Truth (SSOT)
- Dimensions separate from facts

### 3. **Denormalization in Gold**
- Gold is **denormalized** for performance
- Multiple Gold tables for different use cases
- Pre-joined and pre-aggregated

### 4. **Progressive Quality Improvement**
- Each layer adds validation and enrichment
- Data quality improves from Raw → Gold
- Bad data can be fixed without re-ingestion

### 5. **Read from Tables, Not Files**
- Bronze reads from Raw **external tables** (not S3 directly)
- Silver reads from Bronze **Iceberg tables**
- Gold reads from Silver **Iceberg tables**

---

## Bronze Job Requirements

### Pre-Job Execution Checklist

Before launching any bronze job:

1. **Refresh External Table Partitions**:
   ```sql
   MSCK REPAIR TABLE {source_database}.{raw_zone_table};
   ```

2. **Verify Data Availability**:
   ```sql
   SELECT COUNT(*) FROM {source_database}.{raw_zone_table}
   WHERE year = {target_year} AND month = {target_month};
   ```

3. **Check Partition Structure**:
   ```sql
   SHOW PARTITIONS {source_database}.{raw_zone_table};
   ```

### Required Data Reading Pattern

Bronze jobs MUST read from external tables:

```python
def read_raw_data(self) -> DataFrame:
    """Read raw data from external table with proper partitioning."""
    query = f"""
        SELECT
            raw_json_data,
            year,
            month,
            day
        FROM {self.parameters.source_database}.{self.parameters.raw_zone_table}
        WHERE raw_json_data IS NOT NULL
            AND year >= {start_year}
            AND year <= {end_year}
            AND (year > {start_year} OR month >= {start_month})
            AND (year < {end_year} OR month <= {end_month})
    """
    return self.spark.sql(query)
```

## Required Standards

### Metadata Columns (All Layers)

```sql
-- Required in every table
job_run_id STRING COMMENT 'Job execution identifier',
processing_date DATE COMMENT 'Processing date (partition key)',
record_version INTEGER COMMENT 'Record version for SCD',
is_deleted BOOLEAN COMMENT 'Soft delete flag'
```

### File Size Standards

| Layer | Target Size | Purpose |
|-------|-------------|---------|
| Bronze | 64MB | Fast ingestion |
| Silver | 128MB | Balanced performance |
| Gold | 256MB | Query optimization |

### Partitioning Strategy

- **Bronze**: `PARTITIONED BY (processing_date)`
- **Silver**: `PARTITIONED BY (processing_date)` + optional business dimension
- **Gold**: Multi-dimensional for performance

## Common Data Patterns

### Customer Data Pattern

```sql
-- Bronze: Raw customer data
raw_customer_data STRING,
source_timestamp TIMESTAMP,
processing_date DATE

-- Silver: Clean customer profile
customer_id STRING NOT NULL,
email STRING,
first_name STRING,
customer_tier STRING,  -- Calculated
data_quality_score DOUBLE,

-- Gold: Customer metrics
customer_id STRING NOT NULL,
total_lifetime_value DOUBLE,
avg_order_value DOUBLE,
engagement_score DOUBLE
```

### Event Data Pattern

```sql
-- Bronze: Raw events
raw_event_data STRING,
event_timestamp TIMESTAMP,
processing_date DATE

-- Silver: Structured events
event_id STRING NOT NULL,
event_type STRING,
user_id STRING,
event_properties MAP<STRING, STRING>,

-- Gold: Event aggregations
user_id STRING NOT NULL,
event_date DATE,
total_events INTEGER,
unique_event_types INTEGER,
engagement_rate DOUBLE
```

## Data Quality Framework

### Quality Score Calculation

def calculate_quality_score(record, layer):
## Cross-Cutting Practices

- **Configuration** – managed through `libs/common/config` with details in [Configuration System](./README.md#configuration-system).
- **Data quality** – shared validation helpers and scoring patterns live in `libs/validation/`; see [Testing Guidelines](./TESTING.md) for enforcement strategies.
- **Observability** – unified logging, metrics, and alerts are documented in [Operations Guide](./OPERATIONS.md) and [Best Practices](./BEST_PRACTICES.md).

## Related Documentation

- [Medallion Job Pattern](./MEDALLION_JOB_PATTERN.md) - Building jobs with MedallionJobBase
- [Job Development Guide](./JOB_DEVELOPMENT.md) - Building Spark and PyShell jobs
- [Team Tailor Architecture](./sources/teamtailor/README.md) - Real implementation example

---

**Last Updated**: 2025-01-15  
**Version**: 2.0  
**Maintainer**: Data Engineering Team
