# Team Tailor API Reference

## Overview

Team Tailor provides a RESTful API following the JSON API Specification for accessing talent management data.

**Base URLs**:
- EU: `https://api.teamtailor.com`
- US West: `https://api.na.teamtailor.com`

**Documentation**: https://docs.teamtailor.com/

## Authentication

### Bearer Token

Team Tailor uses Bearer token authentication:

```
Authorization: Token token=your-api-token
```

### API Version Header

Team Tailor requires the API version to be specified in the request header:

```
X-Api-Version: 20240904
```

This header is required for all API requests. The current supported version is `20240904`.

### API Token

API tokens are obtained from Team Tailor dashboard:
1. Navigate to Settings → API
2. Generate a new API token
3. Store in AWS Secrets Manager

## API Format

### JSON API Specification

Team Tailor follows JSON API Specification format:

```json
{
  "data": [
    {
      "id": "123",
      "type": "candidates",
      "attributes": {
        "email": "candidate@example.com",
        "first-name": "John",
        "last-name": "Doe"
      },
      "relationships": {
        "job-applications": {
          "data": [
            {"id": "456", "type": "job-applications"}
          ]
        }
      }
    }
  ],
  "links": {
    "self": "https://api.teamtailor.com/v1/candidates",
    "next": "https://api.teamtailor.com/v1/candidates?page[number]=2"
  }
}
```

## Endpoints

### Candidates

**Endpoint**: `/v1/candidates`

**Description**: Retrieve candidate profiles

**Pagination**: Supported via `links.next`

**Example Response**:
```json
{
  "data": [
    {
      "id": "123",
      "type": "candidates",
      "attributes": {
        "email": "candidate@example.com",
        "first-name": "John",
        "last-name": "Doe",
        "created-at": "2025-01-15T10:30:00Z",
        "updated-at": "2025-01-20T14:45:00Z"
      }
    }
  ]
}
```

### Jobs

**Endpoint**: `/v1/jobs`

**Description**: Retrieve job postings

**Pagination**: Supported

**Filtering**: The raw job automatically applies `filter[status]=all` by default to get ALL jobs (including closed, archived, draft, etc.). This ensures historical data collection.

**Filter Parameters**:
- `filter[status]`: Filter by job status
  - `"all"`: Get all jobs regardless of status (default in our implementation)
  - `"active"`: Only active jobs
  - `"closed"`: Only closed jobs
  - `"archived"`: Only archived jobs
  - `"draft"`: Only draft jobs

### Applications

**Endpoint**: `/v1/job-applications`

**Description**: Retrieve candidate applications

**Note**: The API endpoint uses `job-applications` (with hyphen), not `applications`.

**Pagination**: Supported

### Interviews

**Endpoint**: `/v1/interviews`

**Description**: Retrieve interview events

**Pagination**: Supported

### Users

**Endpoint**: `/v1/users`

**Description**: Retrieve users (recruiters/hiring managers)

**Pagination**: Supported

### Departments

**Endpoint**: `/v1/departments`

**Description**: Retrieve department structure

**Pagination**: Supported

### Stages

**Endpoint**: `/v1/stages`

**Description**: Retrieve application stages

**Pagination**: Supported

### NPS Responses

**Endpoint**: `/v1/nps-responses`

**Description**: Retrieve NPS (Net Promoter Score) survey responses from candidates.

**Pagination**: Supported

**Filtering**: The raw job fetches all NPS responses by default (no filters applied) to ensure historical data collection.

**Use Case**: UC6 - Candidate NPS. Provides structured NPS response data including scores, comments, and candidate relationships.

**Reference**: https://docs.teamtailor.com/#55639fbf-f69a-4981-b775-05522a00f5fb

### Activities (Fetched as Relationships)

**Approach**: Activities are fetched as relationships from parent resources, not via a direct endpoint.

**Why**: The `/v1/activities` endpoint may not be available in all TeamTailor accounts. The relationship-based approach is more reliable and provides better context.

**How to Fetch Activities**:

From candidates:
```http
GET /v1/candidates/{candidate_id}?include=activities
GET /v1/candidates/{candidate_id}/relationships/activities
```

From applications:
```http
GET /v1/job-applications/{application_id}?include=activities
GET /v1/job-applications/{application_id}/relationships/activities
```

From jobs:
```http
GET /v1/jobs/{job_id}?include=activities
GET /v1/jobs/{job_id}/relationships/activities
```

**Activity Types Supported**:
- `stage-transition`: Stage changes in the hiring pipeline (for Candidate Journey and Time to Hire)
- `feedback`: Candidate feedback and NPS surveys (for Candidate NPS)
- `note`: Notes added by recruiters
- `email`: Email communications
- `call`: Phone call records

**Pagination**: Supported via relationship links

**Note**: The `/v1/nps-responses` endpoint (documented above) complements activity-based NPS collection. The direct endpoint provides more structured NPS-specific data, while activities provide broader feedback context.

## Pagination

Team Tailor uses cursor-based pagination via `links.next`:

```json
{
  "links": {
    "self": "https://api.teamtailor.com/v1/candidates",
    "next": "https://api.teamtailor.com/v1/candidates?page[number]=2"
  }
}
```

## Rate Limits

- **Default**: 2 requests/second
- **Configurable**: Via job parameters
- **Retry Logic**: Exponential backoff implemented in client

## Error Handling

### Common Errors

- **401 Unauthorized**: Invalid API token
- **429 Too Many Requests**: Rate limit exceeded
- **500 Internal Server Error**: Server error (retry with backoff)

## Related Documentation

- [Raw Zone](./RAW.md) - API extraction details
- [Source Overview](./README.md) - Complete integration overview
