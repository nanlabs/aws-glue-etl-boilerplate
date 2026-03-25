# Team Tailor - Historical Data Backfill Guide

## Overview

This guide explains how to perform a historical data backfill for Team Tailor entities. A backfill is necessary when you need to collect all historical data (including closed jobs, old applications, historical NPS responses, etc.) that may not be captured in regular daily incremental runs.

## Why Backfill is Needed

### Default Behavior

By default, the Team Tailor raw job has a **pagination limit** to prevent excessive API calls during regular daily runs:

- **Default limit**: `max_pages_per_batch = 50` pages
- **Default page size**: `page_size = 30` records
- **Effective limit**: 50 × 30 = **1,500 records per execution**

**Important**: The job is configured to fetch **ALL records** by default (no filters applied), which means it will get:
- ✅ All jobs (active, closed, archived)
- ✅ All NPS responses (including historical)
- ✅ All applications (all statuses)
- ✅ All other entities without status filters

This limit is appropriate for **daily incremental runs** but may not capture all historical data if your Team Tailor account has:

- More than 1,500 jobs (including closed jobs)
- More than 1,500 applications
- More than 1,500 NPS responses
- Extensive historical activity data

**Note**: The job applies default filters automatically:
- **Jobs**: `filter[status]=all` (explicitly gets all jobs)
- **Other entities**: No filters (gets all records)

If you're only seeing active jobs, check:
1. Pagination limit (`max_pages_per_batch` may be too low)
2. API permissions (verify token can access closed jobs)
3. Logs to confirm filters are being applied

### When to Perform Backfill

Perform a historical backfill when:

1. **Initial Setup**: First time setting up the data lake
2. **Missing Historical Data**: You notice gaps in historical data (closed jobs, old applications, etc.)
3. **Data Quality Issues**: KPIs show incomplete historical trends
4. **After Long Downtime**: If the pipeline was down for an extended period

## Backfill Strategy

### Option 1: Increase Pagination Limit (Recommended)

Increase `max_pages_per_batch` to a very high number to allow complete pagination through all historical data.

**Advantages**:

- Simple configuration change
- No code modifications needed
- Can be done per entity type
- Safe - still respects API rate limits

**Example**:

```bash
# Backfill all jobs (including closed ones)
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_jobs_backfill \
  --ENTITY_TYPE jobs \
  --MAX_PAGES_PER_BATCH 10000

# Backfill all applications
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_applications_backfill \
  --ENTITY_TYPE applications \
  --MAX_PAGES_PER_BATCH 10000

# Backfill all NPS responses
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_nps_responses_backfill \
  --ENTITY_TYPE nps_responses \
  --MAX_PAGES_PER_BATCH 10000
```

### Option 2: Remove Limit Temporarily

For very large accounts, you can set an extremely high limit or modify the code temporarily to remove the limit check.

**Note**: This approach requires careful monitoring to avoid:

- Excessive API rate limiting
- Very long execution times
- High AWS costs

## Backfill Process

### Step-by-Step Guide

#### 1. Identify Entities Needing Backfill

Check your current data to identify gaps:

```sql
-- Check job status distribution
SELECT
  status,
  COUNT(*) as count
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__jobs_bronze
GROUP BY status;

-- Check date range of applications
SELECT
  MIN(created_at) as earliest,
  MAX(created_at) as latest,
  COUNT(*) as total
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__applications_bronze;

-- Check NPS responses count
SELECT COUNT(*) as total_nps_responses
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__nps_responses_bronze;
```

#### 2. Estimate Data Volume

Before running backfill, estimate how much data you'll collect:

- **Jobs**: Typically 100-5000 jobs per account
- **Applications**: Can be 10,000-100,000+ depending on account size
- **NPS Responses**: Usually 100-10,000 responses
- **Activities**: Can be very large (100,000+ records)

**Calculation**:

```
Estimated pages = (Total records) / (page_size)
Recommended max_pages_per_batch = Estimated pages × 1.5 (safety margin)
```

#### 3. Run Raw Layer Backfill

Execute backfill for each entity type:

```bash
# Snapshot entities (no date filtering needed)
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_jobs_backfill \
  --ENTITY_TYPE jobs \
  --MAX_PAGES_PER_BATCH 10000

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_candidates_backfill \
  --ENTITY_TYPE candidates \
  --MAX_PAGES_PER_BATCH 10000

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_users_backfill \
  --ENTITY_TYPE users \
  --MAX_PAGES_PER_BATCH 1000

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_departments_backfill \
  --ENTITY_TYPE departments \
  --MAX_PAGES_PER_BATCH 100

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_stages_backfill \
  --ENTITY_TYPE stages \
  --MAX_PAGES_PER_BATCH 1000

# Time-based entities (all historical data)
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_applications_backfill \
  --ENTITY_TYPE applications \
  --MAX_PAGES_PER_BATCH 50000

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_interviews_backfill \
  --ENTITY_TYPE interviews \
  --MAX_PAGES_PER_BATCH 50000

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_nps_responses_backfill \
  --ENTITY_TYPE nps_responses \
  --MAX_PAGES_PER_BATCH 10000

# Activity entities (can be very large)
python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_application_stage_transitions_backfill \
  --ENTITY_TYPE application_stage_transitions \
  --MAX_PAGES_PER_BATCH 100000
```

#### 4. Process to Bronze Layer

After raw backfill completes, process all data to Bronze:

```bash
# Process all entities to Bronze
python jobs/bronze/teamtailor_bronze_job.py \
  --JOB_NAME teamtailor_bronze_jobs_backfill \
  --ENTITY_TYPE jobs

python jobs/bronze/teamtailor_bronze_job.py \
  --JOB_NAME teamtailor_bronze_applications_backfill \
  --ENTITY_TYPE applications

# ... repeat for all entity types
```

#### 5. Process to Silver Layer

Process Bronze data to Silver:

```bash
python jobs/silver/teamtailor_silver_job.py \
  --JOB_NAME teamtailor_silver_candidate_profiles_backfill \
  --ENTITY_TYPE candidate_profiles

python jobs/silver/teamtailor_silver_job.py \
  --JOB_NAME teamtailor_silver_job_postings_backfill \
  --ENTITY_TYPE job_postings

# ... repeat for all Silver entity types
```

#### 6. Process to Gold Layer

Finally, regenerate all Gold KPI tables with complete historical data:

```bash
python jobs/gold/teamtailor_gold_job.py \
  --JOB_NAME teamtailor_gold_time_to_fill_backfill \
  --ENTITY_TYPE talent_time_to_fill

python jobs/gold/teamtailor_gold_job.py \
  --JOB_NAME teamtailor_gold_candidate_journey_backfill \
  --ENTITY_TYPE talent_candidate_journey

# ... repeat for all Gold entity types
```

## Monitoring Backfill Progress

### Check Raw Layer Progress

```sql
-- Count records per entity in Raw layer
SELECT
  entity_type,
  COUNT(*) as record_count,
  MIN(ingestion_timestamp) as earliest,
  MAX(ingestion_timestamp) as latest
FROM (
  SELECT
    'jobs' as entity_type,
    ingestion_timestamp
  FROM glue_catalog.nan_raw_zone.teamtailor__talent__jobs_raw
  UNION ALL
  SELECT
    'applications' as entity_type,
    ingestion_timestamp
  FROM glue_catalog.nan_raw_zone.teamtailor__talent__applications_raw
  -- ... add other entities
)
GROUP BY entity_type;
```

### Verify Data Completeness

```sql
-- Check for closed jobs (should exist after backfill)
SELECT
  status,
  COUNT(*) as count
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__jobs_bronze
GROUP BY status
ORDER BY count DESC;

-- Check historical date range
SELECT
  DATE_TRUNC('year', created_at) as year,
  COUNT(*) as applications_count
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__applications_bronze
GROUP BY DATE_TRUNC('year', created_at)
ORDER BY year DESC;

-- Verify NPS responses coverage
SELECT
  DATE_TRUNC('month', created_at) as month,
  COUNT(*) as nps_count
FROM glue_catalog.nan_bronze_zone.teamtailor__talent__nps_responses_bronze
GROUP BY DATE_TRUNC('month', created_at)
ORDER BY month DESC;
```

## Best Practices

### 1. Run During Off-Peak Hours

Backfills can take hours for large accounts. Schedule during:

- Low-traffic periods
- Maintenance windows
- Weekends

### 2. Monitor Rate Limits

Team Tailor API has rate limits. The default `rate_limit_per_second = 2.0` should be safe, but monitor for:

- 429 Too Many Requests errors
- Slow response times
- API throttling

### 3. Process Incrementally

For very large accounts, consider:

- Running backfill for one entity type at a time
- Processing in batches (e.g., by year)
- Monitoring S3 costs

### 4. Verify Data Quality

After backfill, verify:

- Record counts match expectations
- Date ranges are complete
- No missing critical entities (closed jobs, old applications)
- Data quality scores in Silver layer

### 5. Document Backfill Execution

Keep a log of:

- When backfill was executed
- Which entities were backfilled
- Parameters used (`max_pages_per_batch`, etc.)
- Results (record counts, date ranges)
- Any issues encountered

## Troubleshooting

### Issue: Backfill Still Missing Data

**Possible Causes**:

1. `max_pages_per_batch` still too low
2. API filtering by default (check Team Tailor API documentation)
3. Data deleted in Team Tailor (privacy/GDPR policies)

**Solutions**:

- Increase `max_pages_per_batch` further
- Check Team Tailor account settings for data retention policies
- Verify API permissions allow accessing historical data

### Issue: Backfill Takes Too Long

**Solutions**:

- Increase `rate_limit_per_second` (if API allows)
- Run backfills in parallel for different entity types
- Consider processing in date-based batches

### Issue: Rate Limit Errors

**Solutions**:

- Reduce `rate_limit_per_second`
- Add delays between batches
- Contact Team Tailor support for rate limit increase

## Example: Complete Backfill Script

```bash
#!/bin/bash
# Complete Team Tailor Historical Backfill Script

# Configuration
MAX_PAGES=10000
RATE_LIMIT=2.0

# Raw Layer Backfill
echo "Starting Raw Layer Backfill..."

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_jobs_backfill \
  --ENTITY_TYPE jobs \
  --MAX_PAGES_PER_BATCH $MAX_PAGES \
  --RATE_LIMIT_PER_SECOND $RATE_LIMIT

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_applications_backfill \
  --ENTITY_TYPE applications \
  --MAX_PAGES_PER_BATCH $MAX_PAGES \
  --RATE_LIMIT_PER_SECOND $RATE_LIMIT

python jobs/raw/teamtailor_raw_job.py \
  --JOB_NAME teamtailor_raw_nps_responses_backfill \
  --ENTITY_TYPE nps_responses \
  --MAX_PAGES_PER_BATCH $MAX_PAGES \
  --RATE_LIMIT_PER_SECOND $RATE_LIMIT

# Add other entities as needed...

echo "Raw Layer Backfill Complete!"

# Bronze Layer Processing
echo "Starting Bronze Layer Processing..."
# ... bronze processing commands ...

# Silver Layer Processing
echo "Starting Silver Layer Processing..."
# ... silver processing commands ...

# Gold Layer Processing
echo "Starting Gold Layer Processing..."
# ... gold processing commands ...

echo "Backfill Complete!"
```

## Related Documentation

- [RAW.md](./RAW.md) - Raw layer extraction details
- [BRONZE.md](./BRONZE.md) - Bronze layer processing
- [SILVER.md](./SILVER.md) - Silver layer transformations
- [GOLD.md](./GOLD.md) - Gold layer KPIs
- [API_REFERENCE.md](./API_REFERENCE.md) - Team Tailor API details

---

**Last Updated**: 2025-01-20  
**Status**: Active  
**Maintained By**: Data Engineering Team
