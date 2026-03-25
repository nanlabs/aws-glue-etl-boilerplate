# ClickUp Bronze Layer

## Overview

The Bronze layer parses raw ClickUp task data into structured, typed columns while preserving full JSON payloads for reprocessing.

**Job**: `jobs/bronze/clickup_bronze_job.py`  
**Format**: Apache Iceberg  
**Storage**: S3 with Iceberg table format  
**Update Frequency**: Daily (after raw ingestion)  
**Merge Strategy**: Append (time-based partitioning)

## Table Schema

### `clickup__technology__requests_bronze`

Main bronze table containing parsed ClickUp tasks.

**Partitioning**: `year`, `month` (based on date_created)

**Key Columns**:

```sql
task_id STRING NOT NULL COMMENT 'ClickUp task ID',
task_name STRING COMMENT 'Task name',
status STRING COMMENT 'Task status',
priority STRING COMMENT 'Task priority (urgent, high, normal, low)',
date_created TIMESTAMP COMMENT 'Task creation timestamp',
date_updated TIMESTAMP COMMENT 'Task last update timestamp',
date_closed TIMESTAMP COMMENT 'Task closure timestamp (nullable)',
due_date TIMESTAMP COMMENT 'Task due date (nullable)',
assignee_email STRING COMMENT 'Primary assignee email',
assignee_username STRING COMMENT 'Primary assignee username',
list_id STRING COMMENT 'ClickUp list ID',
list_name STRING COMMENT 'ClickUp list name',
folder_name STRING COMMENT 'ClickUp folder name',
space_id STRING COMMENT 'ClickUp space ID',
tags_json STRING COMMENT 'Tags JSON array',
watchers_json STRING COMMENT 'Watchers JSON array',
dependencies_json STRING COMMENT 'Dependencies JSON array',
checklists_json STRING COMMENT 'Checklists JSON array',
custom_fields_json STRING COMMENT 'Raw custom fields JSON array',
time_logged_hours DOUBLE COMMENT 'Time logged in hours',
time_estimate_hours DOUBLE COMMENT 'Time estimate in hours',
resolution_days INT COMMENT 'Days from creation to closure (calculated)',
sla_compliant BOOLEAN COMMENT 'Whether task was closed before due date',
source STRING COMMENT 'Source system name (clickup)',
ingestion_ts TIMESTAMP COMMENT 'Ingestion timestamp',
payload STRING COMMENT 'Full raw JSON payload',
metadata STRING COMMENT 'Ingestion metadata JSON',
year INT COMMENT 'Partition: year',
month INT COMMENT 'Partition: month'
```

## Transformations

### Date Parsing

ClickUp uses **milliseconds timestamps**. Transformations:
- Convert milliseconds to seconds: `timestamp_ms / 1000`
- Parse to TIMESTAMP: `to_timestamp(timestamp_s)`
- Handle null values gracefully

### Custom Fields

Custom fields are preserved as JSON array in `custom_fields_json` column for parsing in Silver layer:
- Full custom fields array extracted from task payload
- Parsing with UDFs will be done in Silver layer for flexibility

### Metadata Fields

Additional metadata extracted from task payload:
- `list_name`: ClickUp list name (e.g., "Architecture Requests")
- `folder_name`: ClickUp folder name containing the list
- `space_id`: ClickUp space ID

### Task Relationships

Task relationship data preserved as JSON arrays for later processing:
- `tags_json`: Task tags JSON array
- `watchers_json`: Task watchers JSON array
- `dependencies_json`: Task dependencies JSON array
- `checklists_json`: Task checklists JSON array

### Derived Fields

**resolution_days**: Calculated as `datediff(date_closed, date_created)` when `date_closed` is not null.

**sla_compliant**: Boolean indicating if `date_closed <= due_date` when both exist.

### Time Tracking

Time fields converted from milliseconds to hours:
- `time_logged_hours`: `time_spent / 3600000.0`
- `time_estimate_hours`: `time_estimate / 3600000.0`

## Data Quality

- **Null Handling**: Dates and optional fields handled with null checks
- **Type Validation**: All fields cast to appropriate types
- **Partition Fallback**: If date_created parsing fails, use ingestion timestamp for partitioning

## Merge Strategy

**Append-only**: All records are appended to the table. No deduplication at bronze layer.

## Related Documentation

- [Raw Zone](./RAW.md) - Source data structure
- [Silver Layer](./SILVER.md) - Next layer processing
- [Source Overview](./README.md) - Complete integration overview
