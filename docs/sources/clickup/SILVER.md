# ClickUp Silver Layer

## Overview

The Silver layer applies business rules, data quality checks, and normalization to build the Single Source of Truth (SSOT) for Technology Requests analytics.

**Job**: `jobs/silver/clickup_silver_job.py`  
**Format**: Apache Iceberg  
**Storage**: S3 with Iceberg table format  
**Update Frequency**: Daily (after bronze processing)  
**Merge Strategy**: Append (time-based partitioning)

## Tables

### 1. `technology_dim_requests`

Main dimension table with clean request data.

**Partitioning**: `year`, `month`

**Key Columns**:

```sql
request_id STRING NOT NULL COMMENT 'ClickUp task ID',
request_name STRING COMMENT 'Request name',
status STRING COMMENT 'Request status',
priority STRING COMMENT 'Priority level',
date_created TIMESTAMP COMMENT 'Creation timestamp',
date_updated TIMESTAMP COMMENT 'Last update timestamp',
date_closed TIMESTAMP COMMENT 'Closure timestamp',
due_date TIMESTAMP COMMENT 'Due date',
assignee STRING COMMENT 'Assignee email',
list_id STRING COMMENT 'ClickUp list ID',
list_name STRING COMMENT 'ClickUp list name',
folder_name STRING COMMENT 'ClickUp folder name',
space_id STRING COMMENT 'ClickUp space ID',
account STRING COMMENT 'Account/client labels (comma-separated)',
external_satisfaction_value STRING COMMENT 'External satisfaction value (1-5, NULL if not evaluated)',
internal_satisfaction_value STRING COMMENT 'Internal satisfaction value (1-5, NULL if not evaluated)',
external_satisfaction_score INT COMMENT 'External satisfaction score (2-10, NULL if not evaluated)',
internal_satisfaction_score INT COMMENT 'Internal satisfaction score (2-10, NULL if not evaluated)',
time_logged_hours DOUBLE COMMENT 'Time logged',
time_estimate_hours DOUBLE COMMENT 'Time estimate',
resolution_days INT COMMENT 'Resolution time in days',
sla_compliant BOOLEAN COMMENT 'SLA compliance',
stakeholder_areas STRING COMMENT 'Stakeholder areas (comma-separated)',
requesters STRING COMMENT 'Requester emails (comma-separated)',
team_members STRING COMMENT 'Team member emails (comma-separated)',
architecture_responsibility_domain STRING COMMENT 'Architecture responsibility domains (comma-separated)',
rd_practices STRING COMMENT 'R&D Practices (Studios only, comma-separated)',
days_in_backlog INT COMMENT 'Days request has been in backlog status',
days_in_progress INT COMMENT 'Days request has been in progress status',
days_in_review INT COMMENT 'Days request has been in review/testing status',
completeness_score DOUBLE COMMENT 'Data completeness score (0-1)',
urgency_score DOUBLE COMMENT 'Request urgency score (0-1)',
source STRING COMMENT 'Source system',
ingestion_ts TIMESTAMP COMMENT 'Ingestion timestamp',
year INT COMMENT 'Partition: year',
month INT COMMENT 'Partition: month'
```

**Note**: Array fields (`account`, `stakeholder_areas`, `requesters`, `team_members`, `architecture_responsibility_domain`, `rd_practices`) are stored as comma-separated strings using `concat_ws()` for compatibility with Iceberg table storage. They can be split back to arrays in Gold layer transformations using `split()`.

### 2. `technology_dim_stakeholder_areas`

Stakeholder areas dimension (exploded from requests).

**Partitioning**: `year`, `month`

**Key Columns**:

```sql
stakeholder_area STRING NOT NULL COMMENT 'Normalized stakeholder area name',
request_id STRING COMMENT 'Request ID',
year INT COMMENT 'Partition: year',
month INT COMMENT 'Partition: month'
```

### 3. `technology_dim_teams`

Teams dimension (exploded from requests).

**Partitioning**: `year`, `month`

**Key Columns**:

```sql
team_member STRING NOT NULL COMMENT 'Normalized team member email/username',
request_id STRING COMMENT 'Request ID',
year INT COMMENT 'Partition: year',
month INT COMMENT 'Partition: month'
```

### 4. `technology_fact_request_events`

Fact table with request events (created, updated).

**Partitioning**: `year`, `month`

**Key Columns**:

```sql
request_id STRING NOT NULL COMMENT 'Request ID',
event_date TIMESTAMP COMMENT 'Event timestamp',
event_type STRING COMMENT 'Event type (created, updated)',
status STRING COMMENT 'Status at event time',
priority STRING COMMENT 'Priority at event time',
list_id STRING COMMENT 'List ID',
external_satisfaction_score INT COMMENT 'External satisfaction score',
internal_satisfaction_score INT COMMENT 'Internal satisfaction score',
resolution_days INT COMMENT 'Resolution days',
sla_compliant BOOLEAN COMMENT 'SLA compliance',
time_logged_hours DOUBLE COMMENT 'Time logged',
time_estimate_hours DOUBLE COMMENT 'Time estimate',
source STRING COMMENT 'Source system',
ingestion_ts TIMESTAMP COMMENT 'Ingestion timestamp',
year INT COMMENT 'Partition: year',
month INT COMMENT 'Partition: month'
```

## Transformations

### Custom Fields Parsing

Custom fields are parsed using UDFs:

**Emoji Fields**: Extracted as numeric values (1-5) and converted to satisfaction scores (2-10):
- Value 0 → NULL (not evaluated - 0 means the field was not filled)
- Value 1 → Score 2
- Value 2 → Score 4
- Value 3 → Score 6
- Value 4 → Score 8
- Value 5 → Score 10

**IMPORTANT**: A value of 0 in ClickUp emoji fields means "not evaluated", NOT a score of 0. The minimum valid score is 1 star.

**Label Fields**: Extracted as arrays of label names with ID-to-name mapping:
- **Account**: Account/client labels
- **Stakeholder Area**: Stakeholder area labels
- **Architecture Responsibility Domain**: Architecture responsibility domain labels (all lists)
- **R&D Practices**: R&D practice labels (Studios list only)

**User Fields**: Extracted as arrays of user emails/usernames:
- **Requesters**: Request requesters
- **Team**: Team assigned to request

### Data Normalization

- **Stakeholder Areas**: Normalized (trimmed, lowercased) for consistency
- **Team Members**: Normalized (trimmed, lowercased) for consistency
- **Account**: Labels extracted with ID-to-name mapping
- **Architecture Responsibility Domain**: Labels extracted with ID-to-name mapping
- **R&D Practices**: Labels extracted with ID-to-name mapping (Studios only)
- **Satisfaction Scores**: Calculated from numeric emoji values (1-5 → 2-10 scale, 0 = not evaluated → NULL)

### Derived Fields

**Temporal Fields**:
- `days_in_backlog`: Days since creation for requests in backlog/to do/open status
- `days_in_progress`: Days since last update for requests in progress/doing/development status
- `days_in_review`: Days since last update for requests in review/testing/qa status

**Quality Metrics**:
- `completeness_score` (0-1): Calculated based on filled fields (task_name, status, priority, assignee, account, stakeholder_areas)
- `urgency_score` (0-1): Calculated based on priority level and proximity to due date

### Business Rules

- **Satisfaction Score**: Uses external score if available, falls back to internal score
- **Resolution Days**: Only calculated for closed requests
- **SLA Compliance**: Only calculated when both date_closed and due_date exist
- **Array to String Conversion**: Array fields converted to comma-separated strings for storage compatibility

## Data Quality

- **Validation**: Satisfaction scores validated (0-10 range)
- **Normalization**: Names normalized for consistency
- **Null Handling**: Proper null handling for optional fields

## Related Documentation

- [Bronze Layer](./BRONZE.md) - Source data structure
- [Gold Layer](./GOLD.md) - Next layer processing
- [Source Overview](./README.md) - Complete integration overview
