# Team Tailor API Filtering - Getting All Records

## Overview

This document explains how the Team Tailor integration handles API filtering to ensure we collect **ALL records** (including closed jobs, historical NPS responses, etc.), not just active/recent ones.

## Current Implementation

### Default Behavior: Entity-Specific Filters Applied

The raw job applies **entity-specific default filters** to ensure ALL records are fetched:

- ✅ **Jobs**: `filter[status]=all` (explicitly gets all jobs including closed/archived)
- ✅ **NPS Responses**: No filters (gets all responses)
- ✅ **Applications**: No filters (gets all applications)
- ✅ **Other Entities**: No filters (gets all records)

### Code Implementation

In `jobs/raw/teamtailor_raw_job.py`, each entity has default filters configured:

```python
TEAMTAILOR_ENTITIES = {
    "jobs": {
        "default_api_filters": {"filter[status]": "all"},  # Get all jobs
        # ...
    },
    "nps_responses": {
        "default_api_filters": {},  # No filters - get all
        # ...
    },
    # ...
}
```

The job automatically applies these defaults:

```python
# Apply default API filters for this entity type
default_filters = self.entity_config.get("default_api_filters", {})
if default_filters:
    api_params.update(default_filters)

# Override with custom filters if provided
if self.config.api_filter_params:
    api_params.update(self.config.api_filter_params)
```

## Verifying You're Getting All Records

### Check Job Logs

When the job runs, look for these log messages:

```
Applied default API filters for jobs: {'filter[status]': 'all'}
Fetching ALL jobs with filters: ['filter[status]']
```

For entities without filters:
```
Fetching ALL nps_responses (no status filters applied)
```

If you see these, the job is configured correctly to fetch all records.

### Verify in Bronze Layer

After processing to Bronze, check the status distribution:

```sql
-- Check job status distribution
SELECT
  status,
  COUNT(*) as count,
  MIN(created_at) as earliest_job,
  MAX(created_at) as latest_job
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__jobs_bronze
GROUP BY status
ORDER BY count DESC;

-- You should see multiple statuses:
-- - active
-- - closed
-- - archived
-- - draft
-- etc.
```

### Check NPS Responses Date Range

```sql
-- Check NPS responses date range
SELECT
  DATE_TRUNC('year', created_at) as year,
  COUNT(*) as nps_count
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__nps_responses_bronze
GROUP BY DATE_TRUNC('year', created_at)
ORDER BY year DESC;

-- Should show historical responses, not just recent ones
```

## If You're Only Getting Active/Recent Records

### Possible Causes

1. **Team Tailor API Default Behavior**: The API may filter by status by default
2. **API Permissions**: Your API token may not have access to closed/historical records
3. **Pagination Limit**: You may be hitting the `max_pages_per_batch` limit (see [BACKFILL.md](./BACKFILL.md))

### Solutions

#### 1. Check API Documentation

Review the [Team Tailor API documentation](https://docs.teamtailor.com/) to see if:
- There are default filters applied
- You need to pass specific parameters to get all records
- There are permission requirements for accessing closed jobs

#### 2. Test API Directly

Test the API directly to see what it returns:

```bash
# Test jobs endpoint
curl -X GET "https://api.teamtailor.com/v1/jobs?page[size]=100" \
  -H "Authorization: Token token=YOUR_API_TOKEN" \
  -H "X-Api-Version: 20240904" \
  -H "Accept: application/vnd.api+json"

# Check the response - are there closed jobs?
# Look at the `status` field in the attributes
```

#### 3. Pass Explicit Filter Parameters

If the API requires explicit parameters, you can pass them via `--API_FILTER_PARAMS`:

```bash
# Example: If API requires filter[status]=all
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_jobs \
  --ENTITY_TYPE jobs \
  --API_FILTER_PARAMS '{"filter[status]": "all"}'
```

**Note**: The exact parameter format depends on Team Tailor's API specification. Check their documentation for the correct filter syntax.

#### 4. Contact Team Tailor Support

If you're still only getting active records:
1. Contact Team Tailor support
2. Ask about API default filtering behavior
3. Verify your API token permissions
4. Request documentation on accessing closed/historical records

## Configuration Options

### Default (Recommended): No Filters

```bash
# This fetches ALL records
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_jobs \
  --ENTITY_TYPE jobs
```

### Custom Filters (If Needed)

```bash
# Pass custom filter parameters as JSON
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_jobs \
  --ENTITY_TYPE jobs \
  --API_FILTER_PARAMS '{"filter[status]": "all", "filter[archived]": "false"}'
```

**Warning**: Only use custom filters if you understand what they do. Incorrect filters may exclude data you want.

## Related Documentation

- [RAW.md](./RAW.md) - Raw layer extraction details
- [BACKFILL.md](./BACKFILL.md) - Historical data backfill guide
- [API_REFERENCE.md](./API_REFERENCE.md) - Team Tailor API reference

---

**Last Updated**: 2025-01-20  
**Status**: Active  
**Maintained By**: Data Engineering Team
