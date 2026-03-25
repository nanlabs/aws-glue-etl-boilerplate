# ClickUp Raw Zone

## Overview

The Raw zone stores immutable, unaltered data extracted from ClickUp API v2 in JSONL format.

**Job**: `jobs/raw/clickup_raw_job.py`  
**Format**: JSONL (JSON Lines)  
**Storage**: S3 with intelligent partitioning by date created  
**Update Frequency**: Daily (configurable)

## Extraction Method

### API Integration

- **Base URL**: `https://api.clickup.com`
- **Authentication**: Bearer Token (API Key)
- **Format**: REST JSON
- **Rate Limits**: 100 requests/minute (Business plan)
- **Pagination**: Page-based (page parameter)

### Supported Lists (Technology Processes)

| Process | List ID | Description | Partitioning |
|---------|---------|-------------|--------------|
| **Architecture** | `900201093820` | Architecture-related requests and consultancy | year/month (time-based) |
| **Studios** | `901703323552` | Learning & R&D requests | year/month (time-based) |
| **Modernization & Infrastructure** | `901702911170` | Infrastructure and modernization requests | year/month (time-based) |

### Custom Fields

**Common Fields** (all lists):
- **Account** (labels): Account/client associated with request
- **external value (customer satisfaction)** (emoji): External CSAT/NPS (values 1-5)
- **internal value** (emoji): Internal NPS (values 1-5)
- **🏠 Stakeholder Area** (labels): Stakeholder area
- **🙋‍♂️ Requesters** (users): Requesters
- **🫂 Team** (users): Assigned team
- **📑 Architecture Responsibility Domain** (labels): Responsibility domain

**List-Specific Fields**:
- **🤖 R&D Practices** (labels): R&D Practices (only Studios list)

## Storage Structure

### S3 Path Pattern

```
s3://bucket/raw-zone/clickup/tasks/
  year=YYYY/month=MM/
    clickup_data_YYYYMMDD_HHMMSS_batch_NNN.jsonl
```

### Partitioning Strategy

All tasks are partitioned by `year/month` based on `date_created` field from ClickUp.

## Data Format

### ClickUp Task Structure

ClickUp API v2 returns tasks in JSON format:

```json
{
  "id": "86dyf02b2",
  "name": "Review code repository to assess SynergySign",
  "status": {
    "status": "ready for internal review",
    "color": "#d3d3d3"
  },
  "priority": {
    "priority": "urgent",
    "color": "#ff0000"
  },
  "date_created": "1734431544065",
  "date_updated": "1734431536738",
  "date_closed": null,
  "due_date": "1734393600000",
  "assignees": [
    {
      "id": "123",
      "username": "esteban.damico",
      "email": "esteban@example.com"
    }
  ],
  "custom_fields": [
    {
      "id": "abc123",
      "name": "external value (customer satisfaction) (emoji)",
      "value": "😊"
    },
    {
      "id": "def456",
      "name": "🏠 Stakeholder Area (labels)",
      "value": [
        {"id": "1", "name": "Pre-Sales"},
        {"id": "2", "name": "Area Support"}
      ]
    }
  ],
  "time_spent": 3600000,
  "time_estimate": 1486800000
}
```

### Record Structure

Each line in the JSONL file contains:

```json
{
  "payload": {
    "id": "86dyf02b2",
    "name": "Review code repository...",
    "status": {...},
    "custom_fields": [...],
    ...
  },
  "year": 2025,
  "month": 1,
  "metadata": {
    "request_timestamp": "2025-01-15T10:30:00Z",
    "request_url": "https://api.clickup.com/api/v2/list/12345678/task",
    "response_status_code": 200,
    "response_time_ms": 150,
    "list_id": "12345678",
    "tasks_fetched": 1,
    "ingestion_timestamp": "2025-01-15T10:30:00Z",
    "ingestion_job": "clickup_raw_requests",
    "ingestion_source": "clickup",
    "ingestion_method": "api_polling",
    "record_id": "20250115_103000_batch_001_000001",
    "batch_id": "20250115_103000_batch_001",
    "file_name": "clickup_data_20250115_103000_batch_001.jsonl"
  }
}
```

## Extraction Process

1. **List Configuration**: Job receives comma-separated list IDs
2. **Custom Fields Discovery**: For each list, fetch custom field definitions
3. **Task Extraction**: Fetch all tasks from each list with pagination
4. **Custom Fields Inclusion**: Include custom fields in API requests
5. **Partitioning**: Partition by year/month based on `date_created`
6. **S3 Storage**: Write JSONL files to S3 with metadata

## Rate Limiting

ClickUp Business plan allows **100 requests per minute**. The client implements:
- Automatic rate limiting with minimum interval between requests
- Exponential backoff on rate limit errors (429)
- Retry logic with configurable max retries

## Related Documentation

- [Bronze Processing](./BRONZE.md) - Next layer processing
- [Source Overview](./README.md) - Complete integration overview
- [API Reference](./API_REFERENCE.md) - ClickUp API v2 details
