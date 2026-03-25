# Team Tailor Raw Zone

## Overview

The Raw zone stores immutable, unaltered data extracted from Team Tailor API in JSONL format.

**Job**: `jobs/raw/teamtailor_raw_job.py`  
**Format**: JSONL (JSON Lines)  
**Storage**: S3 with intelligent partitioning  
**Update Frequency**: Daily (configurable)

## Extraction Method

### API Integration

- **Base URL**: `https://api.teamtailor.com` (EU) or `https://api.na.teamtailor.com` (US West)
- **Authentication**: Bearer Token (API Key)
- **Format**: JSON API Specification
- **Rate Limits**: Configurable (default: 2 requests/second)

### Supported Entities

| Entity | Endpoint/Method | Description | Partitioning |
|--------|-----------------|-------------|--------------|
| **candidates** | `/v1/candidates` | Candidate profiles and information | None (snapshot) |
| **jobs** | `/v1/jobs` | Job postings and positions | None (snapshot) |
| **applications** | `/v1/job-applications` | Candidate applications to jobs (includes `changed-stage-at`) | year/month (time-based) |
| **interviews** | `/v1/interviews` | Interview events and outcomes | year/month (time-based) |
| **users** | `/v1/users` | Recruiters and hiring managers | None (snapshot) |
| **departments** | `/v1/departments` | Department structure | None (snapshot) |
| **stages** | `/v1/stages` | Application stages | None (snapshot) |
| **nps_responses** | `/v1/nps-responses` | NPS (Net Promoter Score) survey responses (for Candidate NPS) | year/month (time-based) |

> **⚠️ Deprecated**: `application_stage_transitions` entity was removed because the Activities API endpoint (`/v1/activities`) was **discontinued by TeamTailor on 2020-10-01**. Stage tracking is now done via `changed-stage-at` field in applications.

## Storage Structure

### S3 Path Pattern

```
s3://bucket/raw-zone/teamtailor/{entity_type}/
  [year=YYYY/month=MM/]  # Only for time-based entities
    teamtailor_data_YYYYMMDD_HHMMSS_batch_NNN.jsonl
```

### Partitioning Strategy

- **Time-based entities** (applications, interviews, nps_responses): Partitioned by `year/month` of ingestion
- **Snapshot entities** (candidates, jobs, users, departments, stages): Flat structure (no partitions)

**How it works**:
1. Fetch all parent resources (e.g., all candidates)
2. For each parent, fetch activities using relationship links (`?include=activities` or `/relationships/activities`)
3. Filter by activity type if specified (e.g., only `stage-transition` activities)
4. Enrich activities with parent resource metadata

## Data Format

### JSON API Structure

Team Tailor uses JSON API Specification format:

```json
{
  "id": "123",
  "type": "candidates",
  "attributes": {
    "email": "candidate@example.com",
    "first-name": "John",
    "last-name": "Doe",
    "created-at": "2025-01-15T10:30:00Z",
    "updated-at": "2025-01-20T14:45:00Z"
  },
  "relationships": {
    "job-applications": {
      "data": [
        {"id": "456", "type": "job-applications"}
      ]
    }
  }
}
```

### Record Structure

Each line in the JSONL file contains:

```json
{
  "id": "123",
  "type": "candidates",
  "attributes": { ... },
  "relationships": { ... },
  "metadata": {
    "request_timestamp": "2025-01-20T10:30:00Z",
    "ingestion_timestamp": "2025-01-20T10:30:15Z",
    "ingestion_job": "teamtailor_raw_candidates",
    "ingestion_source": "teamtailor",
    "record_id": "20250120_103015_001_000000",
    "batch_id": "20250120_103015_batch_001"
  }
}
```

## Configuration

### Required Parameters

- `--JOB_NAME`: Job name (e.g., `teamtailor_raw_candidates`)
- `--ENTITY_TYPE`: Entity type (candidates, jobs, applications, etc.)
- `--TEAMTAILOR_API_SECRET_NAME`: AWS Secrets Manager secret name containing API token

### Optional Parameters

- `--API_BASE_URL`: Team Tailor API base URL (default: `https://api.teamtailor.com`)
- `--RATE_LIMIT_PER_SECOND`: Rate limit (default: 2.0)
- `--MAX_PAGES_PER_BATCH`: Max pages per batch (default: 50)
- `--BATCH_SIZE`: Batch size for S3 writes (default: 1000)
- `--API_FILTER_PARAMS`: Additional API filter parameters as JSON (default: None - fetches ALL records including closed/inactive)

**Note**: The job applies entity-specific default filters to ensure ALL records are fetched:
- **Jobs**: `filter[status]=all` (gets all jobs including closed/archived)
- **Other entities**: No filters (gets all records)

You can override defaults using `--API_FILTER_PARAMS` if needed.

## Authentication

API credentials are stored in AWS Secrets Manager:

```json
{
  "api_token": "your-teamtailor-api-token",
  "api_base_url": "https://api.teamtailor.com"
}
```

## Historical Data Backfill

For collecting all historical data (including closed jobs, old applications, etc.), see the [Backfill Guide](./BACKFILL.md).

**Note**: By default, the raw job limits pagination to 50 pages (1,500 records) per execution. For historical backfills, increase `--MAX_PAGES_PER_BATCH` to a higher value (e.g., 10,000).

## Related Documentation

- [Backfill Guide](./BACKFILL.md) - How to perform historical data backfill
- [Bronze Processing](./BRONZE.md) - Next layer processing
- [API Reference](./API_REFERENCE.md) - Complete API documentation
- [Source Overview](./README.md) - Complete integration overview
