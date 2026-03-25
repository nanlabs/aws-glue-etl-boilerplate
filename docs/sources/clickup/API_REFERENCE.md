# ClickUp API v2 Reference

## Overview

ClickUp provides a RESTful API v2 for accessing task and list data.

**Base URL**: `https://api.clickup.com`  
**Documentation**: https://developer.clickup.com/docs

## Authentication

### Bearer Token

ClickUp uses Bearer token authentication:

```
Authorization: {api_token}
```

**Note**: ClickUp API uses the token directly in the Authorization header (not "Bearer {token}").

### API Token

API tokens are obtained from ClickUp:
1. Navigate to Settings → Apps
2. Generate a new API token
3. Store in AWS Secrets Manager

## API Format

### REST JSON

ClickUp API v2 returns standard REST JSON format:

```json
{
  "tasks": [
    {
      "id": "86dyf02b2",
      "name": "Task name",
      "status": {
        "status": "in progress",
        "color": "#d3d3d3"
      },
      "date_created": "1734431544065",
      "custom_fields": [...]
    }
  ]
}
```

## Endpoints

### Get Tasks from List

**Endpoint**: `GET /api/v2/list/{list_id}/task`

**Description**: Retrieve tasks from a specific list

**Query Parameters**:
- `page`: Page number (0-indexed, default: 0)
- `include_closed`: Whether to include closed tasks (default: true)
- `order_by`: Field to order by (e.g., "created", "updated", "due_date")
- `reverse`: Reverse order (default: false)
- `custom_fields[]`: Custom field IDs to include (can be repeated)

**Example Request**:
```http
GET /api/v2/list/12345678/task?page=0&include_closed=true&custom_fields[]=abc123&custom_fields[]=def456
Authorization: pk_xxx
```

**Example Response**:
```json
{
  "tasks": [
    {
      "id": "86dyf02b2",
      "name": "Task name",
      "status": {...},
      "custom_fields": [...]
    }
  ]
}
```

### Get Task by ID

**Endpoint**: `GET /api/v2/task/{task_id}`

**Description**: Retrieve a single task with full details

**Query Parameters**:
- `custom_fields[]`: Custom field IDs to include

**Example Request**:
```http
GET /api/v2/task/86dyf02b2?custom_fields[]=abc123
Authorization: pk_xxx
```

### Get List Custom Fields

**Endpoint**: `GET /api/v2/list/{list_id}/field`

**Description**: Retrieve custom field definitions for a list

**Example Request**:
```http
GET /api/v2/list/12345678/field
Authorization: pk_xxx
```

**Example Response**:
```json
{
  "fields": [
    {
      "id": "abc123",
      "name": "external value (customer satisfaction) (emoji)",
      "type": "emoji"
    },
    {
      "id": "def456",
      "name": "🏠 Stakeholder Area (labels)",
      "type": "labels"
    }
  ]
}
```

## Pagination

ClickUp uses **page-based pagination**:

- Pages are 0-indexed
- Typical page size is 100 tasks
- No explicit pagination metadata in response
- Continue fetching until page returns fewer than 100 tasks

## Rate Limits

### Business Plan

- **Limit**: 100 requests per minute
- **Headers**: Rate limit info in response headers (if available)
- **Error**: 429 Too Many Requests when limit exceeded

### Rate Limit Handling

The client implements:
- Automatic rate limiting (minimum interval between requests)
- Exponential backoff on 429 errors
- Retry logic with configurable max retries

## Custom Fields

### Field Types

- **Emoji**: Single emoji value (e.g., "😊")
- **Labels**: Array of label objects
- **Users**: Array of user objects

### Field Structure

```json
{
  "custom_fields": [
    {
      "id": "abc123",
      "name": "field name",
      "value": "value"  // or {"value": "value"} or [{"value": "value"}]
    }
  ]
}
```

## Error Handling

### Common Errors

- **401 Unauthorized**: Invalid API token
- **404 Not Found**: List or task not found
- **429 Too Many Requests**: Rate limit exceeded
- **500 Internal Server Error**: Server error (retry with backoff)

## Timestamps

ClickUp uses **milliseconds timestamps**:
- `date_created`: Milliseconds since epoch
- `date_updated`: Milliseconds since epoch
- `date_closed`: Milliseconds since epoch (nullable)
- `due_date`: Milliseconds since epoch (nullable)

**Conversion**: Divide by 1000 to get seconds, then parse as timestamp.

## Related Documentation

- [Raw Zone](./RAW.md) - API extraction details
- [Source Overview](./README.md) - Complete integration overview
