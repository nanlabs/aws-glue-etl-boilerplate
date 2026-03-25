# Team Tailor Integration - Talent Data Pipeline

## Overview

This integration extracts data from Team Tailor, a talent acquisition and recruitment platform, to support NaNLABS' Talent process KPIs and analytics.

**Source System**: Team Tailor  
**Update Frequency**: Daily (configurable)  
**Data Types**: Candidates, Jobs, Applications, Interviews, Users, Departments, Stages, NPS Responses  
**API Format**: JSON API Specification  
**Authentication**: Bearer Token (API Key)

> **⚠️ Note**: The Activities API endpoint was **removed by TeamTailor** on 2020-10-01. Stage transition tracking is done via `changed-stage-at` field in applications.

## Quick Start

> **💡 For Local Development**: All commands below run inside the DevContainer. See [Development Guide](../DEVELOPMENT.md) for setup.

### Complete Pipeline Example

Run the full pipeline end-to-end:

```bash
# 1. Extract raw data from API (Raw Layer)
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--MAX_PAGES_PER_BATCH 2"

# 2. Process raw data into Bronze Iceberg table
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--CREATE_TABLES true"

# 3. Transform Bronze to Silver (cleaned, validated data)
make run-silver DATA_SOURCE=teamtailor ENTITY_TYPE=candidate_profiles \
  ARGS="--CREATE_TABLES true"

# 4. Aggregate Silver to Gold (business KPIs)
make run-gold DATA_SOURCE=teamtailor ENTITY_TYPE=talent_time_to_fill \
  ARGS="--CREATE_TABLES true"
```

### Individual Layer Execution

**Extract Raw Data:**
```bash
# Extract candidates
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates

# Extract applications
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=applications
```

**Process to Bronze:**
```bash
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--CREATE_TABLES true"
```

**Process to Silver:**
```bash
make run-silver DATA_SOURCE=teamtailor ENTITY_TYPE=candidate_profiles \
  ARGS="--CREATE_TABLES true"
```

**Process to Gold (KPIs):**
```bash
make run-gold DATA_SOURCE=teamtailor ENTITY_TYPE=talent_time_to_fill \
  ARGS="--CREATE_TABLES true"
```

> **Note**: For AWS Glue execution, see [Deployment Guide](../DEPLOYMENT.md). Local development uses `make` commands which automatically configure LocalStack.

## Data Flow

```
Team Tailor API (JSON API)
    ↓
Raw Layer (S3 JSONL)
    ↓
Bronze Layer (Iceberg - Parsed & Normalized)
    ↓
Silver Layer (Iceberg - Clean SSOT)
    ↓
Gold Layer (Iceberg - Talent KPIs)
```

## Supported Entities

### Raw Layer (8 entities)
- **candidates**: Candidate profiles and information
- **jobs**: Job postings and positions
- **applications**: Candidate applications to jobs (includes `changed-stage-at` for stage tracking)
- **interviews**: Interview events and outcomes
- **users**: Recruiters and hiring managers
- **departments**: Department structure
- **stages**: Application stages
- **nps_responses**: NPS (Net Promoter Score) survey responses (for Candidate NPS use case)

> **⚠️ Deprecated**: `application_stage_transitions` entity was removed because TeamTailor's Activities API endpoint was discontinued on 2020-10-01.

### Bronze Layer (8 tables)
Same as Raw entities, processed into Iceberg tables.

### Silver Layer (6 tables)
- **candidate_profiles**: Enriched candidate data with data quality scores
- **job_postings**: Job details with status tracking
- **application_pipeline**: Application journey with stages (includes `changed_stage_at`, `stage_id`)
- **interview_events**: Interview tracking
- **recruiter_performance**: User/recruiter metrics
- **candidate_nps**: Candidate NPS survey responses (for UC6)

> **⚠️ Deprecated**: `application_stage_transitions` table was removed due to Activities API removal.

### Gold Layer (7 KPI tables)
- **talent_time_to_fill**: Average days from job posting to offer acceptance (UC1)
- **talent_source_effectiveness**: Quality and conversion by source (UC5)
- **talent_pipeline_health**: Application flow through stages (UC2)
- **talent_recruiter_performance**: Recruiter efficiency and quality (UC4)
- **talent_interview_metrics**: Interview efficiency and outcomes (UC3)
- **talent_candidate_journey**: Complete candidate lifecycle with Time to Hire metrics (UC1b, UC7)
- **talent_candidate_nps**: Candidate NPS metrics and segmentation (UC6)

> **⚠️ Deprecated**: `talent_stage_transitions` table was removed due to Activities API removal. Stage analysis is partially covered by `talent_candidate_journey` using `changed_stage_at` field.

## Additional Endpoints

### NPS Responses Endpoint

Team Tailor provides a dedicated `/v1/nps-responses` endpoint that complements our activity-based approach for NPS data collection.

**Implementation**: The `nps_responses` entity type is now implemented and available in both Raw and Bronze layers.

**Benefits**:
- Structured NPS response data directly from the endpoint
- Complete NPS metadata (scores, comments, timestamps, survey details)
- Direct access to NPS responses without traversing candidate relationships
- Provides structured NPS data directly from the endpoint

**Use Case**: UC6 - Candidate NPS. The `nps_responses` endpoint provides structured NPS data directly.

**Reference**: [Team Tailor NPS Responses API](https://docs.teamtailor.com/#55639fbf-f69a-4981-b775-05522a00f5fb)

## Documentation

- **[RAW.md](./RAW.md)** - Raw layer details, API reference, entity types
- **[BRONZE.md](./BRONZE.md)** - Bronze layer schema, merge strategies
- **[SILVER.md](./SILVER.md)** - Silver layer transformations, business logic
- **[GOLD.md](./GOLD.md)** - Gold layer KPIs, use cases, query examples
- **[API_REFERENCE.md](./API_REFERENCE.md)** - Team Tailor API details, authentication, endpoints
- **[ATHENA_QUERIES.md](./ATHENA_QUERIES.md)** - Practical Athena SQL queries for all layers
- **[BACKFILL.md](./BACKFILL.md)** - Historical data backfill guide for collecting all historical records
- **[API_FILTERING.md](./API_FILTERING.md)** - How API filtering works and how to get all records (including closed jobs)

## Related Documentation

- [Job Development Guide](../../JOB_DEVELOPMENT.md) - Building Spark and PyShell jobs
- [Medallion Architecture](../../MEDALLION_ARCHITECTURE.md) - Complete architecture reference
