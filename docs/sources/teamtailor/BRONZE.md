# Team Tailor Bronze Layer

## Overview

The Bronze layer processes raw Team Tailor data into structured Iceberg tables with normalized schemas and intelligent merge strategies.

**Job**: `jobs/bronze/teamtailor_bronze_job.py`  
**Format**: Apache Iceberg  
**Storage**: S3 with Iceberg table format  
**Update Frequency**: Daily (after raw extraction)

## Processing Strategy

### Merge Strategies

| Entity Type | Strategy | Reason |
|-------------|----------|--------|
| **candidates** | UPSERT by `candidate_id` | Master data - updates replace existing records |
| **jobs** | UPSERT by `job_id` | Master data - updates replace existing records |
| **users** | UPSERT by `user_id` | Master data - updates replace existing records |
| **departments** | UPSERT by `department_id` | Master data - updates replace existing records |
| **stages** | UPSERT by `stage_id` | Master data - updates replace existing records |
| **applications** | Append | Time-series data - all records preserved |
| **interviews** | Append | Time-series data - all records preserved |
| **nps_responses** | Append | Time-series data - all records preserved |

> **⚠️ Deprecated**: `application_stage_transitions` was removed due to Activities API removal.

### Partitioning

- **Time-based entities** (applications, interviews, nps_responses): Partitioned by `year/month`
- **Snapshot entities** (candidates, jobs, users, departments, stages): No partitions

## Schema Design

### Common Fields

All bronze tables include:
- Entity-specific key fields (e.g., `candidate_id`, `job_id`)
- `source`: Source system name (`teamtailor`)
- `ingestion_ts`: Timestamp of ingestion
- `payload`: Full JSON API payload (STRING)
- `metadata`: Ingestion metadata (STRING)

### Entity-Specific Schemas

#### Candidates Bronze Table

```sql
CREATE TABLE teamtailor__talent__candidates_bronze (
  candidate_id STRING NOT NULL,
  email STRING,
  first_name STRING,
  last_name STRING,
  candidate_created TIMESTAMP,
  candidate_updated TIMESTAMP,
  source STRING,
  ingestion_ts TIMESTAMP,
  payload STRING,
  metadata STRING
)
USING ICEBERG
```

#### Applications Bronze Table

```sql
CREATE TABLE teamtailor__talent__applications_bronze (
  application_id STRING NOT NULL,
  candidate_id STRING,
  job_id STRING,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  source STRING,
  ingestion_ts TIMESTAMP,
  payload STRING,
  metadata STRING,
  year INT,
  month INT
)
USING ICEBERG
PARTITIONED BY (year, month)
```

## Table Naming Convention

Pattern: `teamtailor__talent__{entity}_bronze`

Examples:
- `teamtailor__talent__candidates_bronze`
- `teamtailor__talent__applications_bronze`
- `teamtailor__talent__interviews_bronze`

## Data Quality

- **Deduplication**: Snapshot entities deduplicated by primary key
- **Null Handling**: Preserved from source (no forced defaults)
- **Type Validation**: Timestamps validated and cast appropriately

## Related Documentation

- [Raw Zone](./RAW.md) - Source data structure
- [Silver Layer](./SILVER.md) - Next layer processing
- [Source Overview](./README.md) - Complete integration overview
