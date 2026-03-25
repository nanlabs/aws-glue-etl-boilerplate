# Team Tailor - Athena Query Examples

**Reading Time**: ~15 minutes

## Overview

This document provides practical Athena SQL queries for analyzing Team Tailor talent data across the Medallion Architecture layers (Raw, Bronze, Silver, Gold). Each query includes a description of insights you can obtain and example use cases aligned with the Talent Data Lake KPI Foundation PRD.

**Database Names** (adjust based on environment):

* **Raw**: `nan_wl_workloads_data_lake_staging_raw_zone_ingestion`
* **Bronze**: `nan_wl_workloads_data_lake_staging_bronze_processing`
* **Silver**: `nan_wl_workloads_data_lake_staging_silver_analytics`
* **Gold**: `nan_wl_workloads_data_lake_staging_gold_reporting`

**Table Names**:

* **Raw**: `teamtailor__talent__{entity}_raw` (e.g., `teamtailor__talent__candidates_raw`)
* **Bronze**: `teamtailor__talent__{entity}_bronze` (e.g., `teamtailor__talent__candidates_bronze`)
* **Silver**: `teamtailor_{entity}` (e.g., `teamtailor_candidate_profiles`)
* **Gold**: `talent_{entity}_gold` (e.g., `talent_time_to_fill_gold`)

---

## RAW Layer Queries

The Raw layer contains unprocessed JSONL data exactly as received from Team Tailor API, partitioned by ingestion date.

### Query 1: Check Data Availability by Entity and Month

**Description**: Verify which months of data have been ingested for each entity type and count records per partition.

**Use Case**: Data quality audit, identify missing data periods, validate ingestion completeness.

```sql
-- Check data availability and record counts by partition for candidates
SELECT
    year,
    month,
    COUNT(*) as record_count,
    MIN(candidate_id) as sample_id,
    MAX(ingestion_timestamp) as latest_ingestion
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.teamtailor__talent__candidates_raw
WHERE year >= 2025
GROUP BY year, month
ORDER BY year DESC, month DESC
LIMIT 50;
```

**Expected Output**: List of year/month partitions with record counts to identify data gaps or anomalies.

---

### Query 2: Sample Raw Records for Validation

**Description**: Retrieve a sample of raw records to validate JSON structure and content.

**Use Case**: Data profiling, troubleshooting ingestion issues, understanding source data format.

```sql
-- Sample raw candidate records from most recent partition
SELECT
    candidate_id,
    email,
    first_name,
    last_name,
    source,
    created_at,
    updated_at,
    year,
    month,
    ingestion_timestamp
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.teamtailor__talent__candidates_raw
WHERE year = 2025 AND month = 11
LIMIT 20;
```

**Expected Output**: Sample of raw JSON data showing candidate information and metadata.

---

### Query 3: Identify Recent Activity by Entity Type

**Description**: Find entities with recent activity across different entity types.

**Use Case**: Monitor data freshness, identify active periods, validate API connectivity.

```sql
-- Recent applications activity
SELECT
    application_id,
    candidate_id,
    job_id,
    status,
    created_at,
    year,
    month
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.teamtailor__talent__applications_raw
WHERE year = 2025 AND month = 11
ORDER BY created_at DESC
LIMIT 50;
```

**Expected Output**: List of recently created applications ordered by creation date.

---

### Query 4: NPS Responses Validation

**Description**: Check NPS response data to validate candidate satisfaction tracking.

**Use Case**: Validate NPS data extraction, verify candidate satisfaction metrics.

```sql
-- Check NPS responses
SELECT
    nps_response_id,
    candidate_id,
    score,
    nps_category,
    created_at,
    year,
    month
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.teamtailor__talent__nps_responses_raw
WHERE year = 2025 AND month = 11
ORDER BY created_at DESC
LIMIT 50;
```

**Expected Output**: List of NPS responses with scores and candidate context.

> **⚠️ Note**: The `application_stage_transitions` entity was removed because TeamTailor's Activities API was discontinued on 2020-10-01. Use `changed_stage_at` field from applications for stage timing analysis.

---

## BRONZE Layer Queries

The Bronze layer contains structured data with calculated metrics, quality scores, and enriched fields. This is ideal for data exploration and quality analysis.

### Query 5: Data Quality Overview

**Description**: Analyze data quality scores to identify records needing attention.

**Use Case**: Data quality monitoring, identify problematic records, prioritize data cleanup.

```sql
-- Data quality score distribution for candidates
SELECT
    CASE
        WHEN data_quality_score >= 0.9 THEN 'Excellent (≥0.9)'
        WHEN data_quality_score >= 0.8 THEN 'Good (0.8-0.89)'
        WHEN data_quality_score >= 0.6 THEN 'Fair (0.6-0.79)'
        WHEN data_quality_score >= 0.4 THEN 'Poor (0.4-0.59)'
        ELSE 'Critical (<0.4)'
    END as quality_tier,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(data_quality_score), 3) as avg_score,
    SUM(CASE WHEN has_complete_name THEN 1 ELSE 0 END) as complete_name_count,
    SUM(CASE WHEN has_email THEN 1 ELSE 0 END) as email_count
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__candidates_bronze
WHERE year = 2025 AND month = 11
GROUP BY
    CASE
        WHEN data_quality_score >= 0.9 THEN 'Excellent (≥0.9)'
        WHEN data_quality_score >= 0.8 THEN 'Good (0.8-0.89)'
        WHEN data_quality_score >= 0.6 THEN 'Fair (0.6-0.79)'
        WHEN data_quality_score >= 0.4 THEN 'Poor (0.4-0.59)'
        ELSE 'Critical (<0.4)'
    END
ORDER BY quality_tier;
```

**Expected Output**: Distribution of records by quality tier with metrics for each tier.

---

### Query 6: Candidate Source Analysis

**Description**: Identify top candidate sources and their quality metrics.

**Use Case**: Source effectiveness analysis, recruitment channel optimization, budget allocation.

```sql
-- Top candidate sources by volume and quality
SELECT
    COALESCE(source, 'unknown') as source,
    COUNT(*) as candidate_count,
    ROUND(AVG(data_quality_score), 3) as avg_quality_score,
    SUM(CASE WHEN has_complete_name THEN 1 ELSE 0 END) as complete_name_count,
    SUM(CASE WHEN has_email THEN 1 ELSE 0 END) as email_count,
    SUM(CASE WHEN has_phone THEN 1 ELSE 0 END) as phone_count,
    SUM(CASE WHEN has_linkedin THEN 1 ELSE 0 END) as linkedin_count
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__candidates_bronze
WHERE year = 2025 AND month = 11
GROUP BY COALESCE(source, 'unknown')
ORDER BY candidate_count DESC
LIMIT 20;
```

**Expected Output**: Top 20 sources ranked by candidate volume with quality metrics.

---

### Query 7: Application Status Distribution

**Description**: Analyze application status distribution to understand pipeline flow.

**Use Case**: Pipeline health monitoring, identify bottlenecks, track application lifecycle.

```sql
-- Application status distribution
SELECT
    status,
    COUNT(*) as application_count,
    COUNT(DISTINCT candidate_id) as unique_candidates,
    COUNT(DISTINCT job_id) as unique_jobs,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    MIN(created_at) as earliest_application,
    MAX(created_at) as latest_application
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__applications_bronze
WHERE year = 2025 AND month = 11
GROUP BY status
ORDER BY application_count DESC;
```

**Expected Output**: Application counts by status showing pipeline distribution.

---

### Query 8: Interview Scheduling Patterns

**Description**: Analyze interview scheduling patterns including business hours and weekend interviews.

**Use Case**: Interview efficiency analysis, scheduling optimization, candidate experience insights.

```sql
-- Interview scheduling patterns
SELECT
    interview_type,
    CASE
        WHEN is_weekend THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    CASE
        WHEN is_business_hours THEN 'Business Hours'
        ELSE 'After Hours'
    END as time_type,
    COUNT(*) as interview_count,
    COUNT(DISTINCT application_id) as unique_applications,
    ROUND(AVG(EXTRACT(HOUR FROM scheduled_at)), 1) as avg_hour
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__interviews_bronze
WHERE year = 2025 AND month = 11
GROUP BY interview_type,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END,
    CASE WHEN is_business_hours THEN 'Business Hours' ELSE 'After Hours' END
ORDER BY interview_count DESC;
```

**Expected Output**: Interview scheduling patterns by type, day, and time showing distribution.

---

### Query 9: Application Stage Analysis

**Description**: Analyze current stage distribution and timing using `changed_stage_at`.

**Use Case**: Candidate journey analysis, identify stage bottlenecks, track progression patterns.

```sql
-- Current stage distribution and timing for applications
SELECT
    stage_id,
    status,
    COUNT(*) as application_count,
    COUNT(DISTINCT candidate_id) as unique_candidates,
    AVG(DATE_DIFF('day', created_at, changed_stage_at)) as avg_days_to_current_stage,
    MIN(changed_stage_at) as earliest_stage_change,
    MAX(changed_stage_at) as latest_stage_change
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__applications_bronze
WHERE year = 2025 AND month = 11
  AND changed_stage_at IS NOT NULL
GROUP BY stage_id, status
ORDER BY application_count DESC
LIMIT 50;
```

**Expected Output**: Stage distribution showing application counts and timing metrics.

> **⚠️ Note**: Detailed stage transition history is unavailable because TeamTailor's Activities API was removed on 2020-10-01. Use `changed_stage_at` for current stage timing.

---

### Query 10: Monthly Activity Trends

**Description**: Track activity trends over time to identify seasonal patterns.

**Use Case**: Seasonal pattern analysis, capacity planning, trend identification.

```sql
-- Monthly activity trends for applications
SELECT
    year,
    month,
    COUNT(*) as application_count,
    COUNT(DISTINCT candidate_id) as unique_candidates,
    COUNT(DISTINCT job_id) as unique_jobs,
    COUNT(DISTINCT CASE WHEN status = 'hired' THEN application_id END) as hires_count
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__applications_bronze
WHERE year >= 2025
GROUP BY year, month
ORDER BY year DESC, month DESC
LIMIT 24;
```

**Expected Output**: Monthly aggregated application metrics showing trends over the last 24 months.

---

### Query 11: Duplicate Detection

**Description**: Identify potential duplicate records using the record_hash field.

**Use Case**: Data quality validation, identify data issues, troubleshoot ingestion.

```sql
-- Find duplicate records by hash
SELECT
    record_hash,
    COUNT(*) as occurrence_count,
    ARRAY_AGG(DISTINCT candidate_id) as candidate_ids,
    ARRAY_AGG(DISTINCT CAST(year AS VARCHAR) || '-' || CAST(month AS VARCHAR)) as partitions
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__candidates_bronze
WHERE year >= 2025
GROUP BY record_hash
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 50;
```

**Expected Output**: List of record hashes that appear multiple times, indicating potential duplicates.

---

## SILVER Layer Queries

The Silver layer contains clean, business-ready tables optimized for Talent analytics with data quality validations and enrichment.

### Query 11: Candidate Profiles Master List

**Description**: Retrieve canonical candidate information with standardized keys and attributes.

**Use Case**: Master data for joins, candidate lookup, CRM integration.

```sql
-- Candidate dimension with latest attributes
SELECT
    candidate_id,
    email,
    first_name,
    last_name,
    phone,
    linkedin_url,
    source,
    tags,
    data_quality_score,
    has_complete_name,
    has_phone,
    has_linkedin,
    candidate_created,
    candidate_updated,
    silver_ingestion_timestamp
FROM nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_candidate_profiles
WHERE data_quality_score >= 0.8
ORDER BY candidate_updated DESC
LIMIT 100;
```

**Expected Output**: Clean candidate master list with standardized fields and quality metrics.

---

### Query 13: Job Postings Catalog

**Description**: Access standardized job posting information with keys and attributes.

**Use Case**: Job lookups, hiring planning, department analysis.

```sql
-- Job postings with status
SELECT
    job_id,
    title,
    status,
    department_id,
    description,
    internal_title,
    data_quality_score,
    is_valid,
    created_at,
    updated_at,
    silver_ingestion_timestamp
FROM nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_job_postings
WHERE is_valid = true
ORDER BY created_at DESC
LIMIT 100;
```

**Expected Output**: Complete job catalog with standardized attributes.

---

### Query 14: Application Pipeline Trends

**Description**: Analyze application pipeline trends over time with aggregated metrics.

**Use Case**: Executive dashboards, trend analysis, forecasting, pipeline health monitoring.

```sql
-- Monthly application pipeline trends
SELECT
    year,
    month,
    COUNT(DISTINCT candidate_id) as unique_candidates,
    COUNT(DISTINCT job_id) as unique_jobs,
    COUNT(*) as total_applications,
    COUNT(DISTINCT CASE WHEN status = 'hired' THEN application_id END) as hires_count,
    COUNT(DISTINCT CASE WHEN status = 'rejected' THEN application_id END) as rejections_count,
    ROUND(COUNT(DISTINCT CASE WHEN status = 'hired' THEN application_id END) * 100.0 / COUNT(*), 2) as hire_rate_pct
FROM nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_application_pipeline
WHERE year >= 2025
GROUP BY year, month
ORDER BY year DESC, month DESC
LIMIT 24;
```

**Expected Output**: Monthly aggregated application metrics showing trends over the last 24 months.

---

### Query 15: Interview Events Analysis

**Description**: Compare interview outcomes and scheduling efficiency.

**Use Case**: Interview effectiveness analysis, scheduling optimization, outcome tracking.

```sql
-- Interview events with outcomes
SELECT
    interview_type,
    status,
    COUNT(*) as interview_count,
    COUNT(DISTINCT application_id) as unique_applications,
    SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) as weekend_interviews,
    SUM(CASE WHEN is_business_hours THEN 1 ELSE 0 END) as business_hours_interviews,
    ROUND(AVG(EXTRACT(EPOCH FROM (scheduled_at - created_at)) / 86400), 1) as avg_days_to_schedule
FROM nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_interview_events
WHERE year = 2025 AND month = 11
GROUP BY interview_type, status
ORDER BY interview_count DESC;
```

**Expected Output**: Interview metrics by type and status showing scheduling patterns and outcomes.

---

### Query 16: Recruiter Performance Overview

**Description**: Identify recruiter activity and performance metrics.

**Use Case**: Recruiter productivity tracking, workload distribution, performance benchmarking.

```sql
-- Recruiter performance summary
SELECT
    user_id,
    email,
    first_name,
    last_name,
    department_id,
    data_quality_score,
    is_valid,
    silver_ingestion_timestamp
FROM nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_recruiter_performance
WHERE is_valid = true
ORDER BY email
LIMIT 100;
```

**Expected Output**: Recruiter master list with quality metrics.

---

## GOLD Layer Queries

The Gold layer contains analytics-ready tables with pre-aggregated KPIs and denormalized structures for BI and reporting. Each table is partitioned for efficient querying. Below are example queries for each Gold entity aligned with the PRD use cases.

### Query 17: Time to Fill Analysis (UC1 & UC1b)

**Description**: Analyze time-to-fill metrics by job, department, and time period. Supports both "Time to Fill" (job posting to hire) and "Time to Hire" (application to hire) use cases.

**Use Case**: UC1 - Measure Time to Fill, UC1b - Measure Time to Hire. Track hiring efficiency, identify bottlenecks, compare performance across departments.

```sql
-- Time to Fill by Department (last 90 days)
SELECT
    department_id,
    COUNT(DISTINCT job_id) as jobs_filled,
    ROUND(AVG(days_to_fill), 1) as avg_days_to_fill,
    ROUND(AVG(days_to_first_application), 1) as avg_days_to_first_application,
    ROUND(AVG(days_to_interview), 1) as avg_days_to_interview,
    MIN(days_to_fill) as min_days_to_fill,
    MAX(days_to_fill) as max_days_to_fill,
    APPROX_PERCENTILE(days_to_fill, 0.5) as median_days_to_fill
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_time_to_fill_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND final_status = 'hired'
GROUP BY department_id
ORDER BY avg_days_to_fill DESC;
```

**Expected Output**: Department-level time-to-fill metrics showing average, min, max, and median days.

---

### Query 18: Time to Fill by Job Title

**Description**: Compare time-to-fill across different job titles to identify role-specific patterns.

**Use Case**: UC1 - Measure Time to Fill. Role-specific hiring analysis, identify hard-to-fill positions.

```sql
-- Time to Fill by Job Title
SELECT
    title,
    department_id,
    COUNT(*) as filled_positions,
    ROUND(AVG(days_to_fill), 1) as avg_days_to_fill,
    ROUND(AVG(days_to_first_application), 1) as avg_days_to_first_application,
    ROUND(AVG(days_to_interview), 1) as avg_days_to_interview
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_time_to_fill_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '180' DAY
  AND final_status = 'hired'
GROUP BY title, department_id
HAVING COUNT(*) >= 3  -- Filter for jobs with meaningful data
ORDER BY avg_days_to_fill DESC
LIMIT 50;
```

**Expected Output**: Top 50 job titles ranked by average time-to-fill.

---

### Query 19: Source Effectiveness Analysis (UC5)

**Description**: Evaluate recruitment source performance based on applications, interviews, offers, and hires. Calculate conversion rates to identify best-performing sources.

**Use Case**: UC5 - Analyze Source Effectiveness. Optimize recruitment budget, identify high-quality sources, track source performance over time.

```sql
-- Source Effectiveness Summary (last 90 days)
SELECT
    source,
    SUM(applications_count) as total_applications,
    SUM(interviews_count) as total_interviews,
    SUM(offers_count) as total_offers,
    SUM(hires_count) as total_hires,
    ROUND(AVG(conversion_rate), 4) as avg_conversion_rate,
    ROUND(SUM(hires_count) * 100.0 / NULLIF(SUM(applications_count), 0), 2) as overall_conversion_pct,
    ROUND(SUM(interviews_count) * 100.0 / NULLIF(SUM(applications_count), 0), 2) as interview_rate_pct,
    ROUND(SUM(offers_count) * 100.0 / NULLIF(SUM(interviews_count), 0), 2) as offer_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_source_effectiveness_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY source
HAVING SUM(applications_count) >= 10  -- Filter for sources with meaningful volume
ORDER BY overall_conversion_pct DESC;
```

**Expected Output**: Source performance metrics ranked by conversion rate showing applications → interviews → offers → hires funnel.

---

### Query 20: Source Effectiveness by Department

**Description**: Analyze source effectiveness segmented by department to understand department-specific source preferences.

**Use Case**: UC5 - Analyze Source Effectiveness. Department-specific recruitment strategy, optimize source mix per department.

```sql
-- Source Effectiveness by Department
SELECT
    department_id,
    source,
    SUM(applications_count) as total_applications,
    SUM(hires_count) as total_hires,
    ROUND(SUM(hires_count) * 100.0 / NULLIF(SUM(applications_count), 0), 2) as conversion_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_source_effectiveness_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '180' DAY
GROUP BY department_id, source
HAVING SUM(applications_count) >= 5
ORDER BY department_id, conversion_rate_pct DESC;
```

**Expected Output**: Source performance by department showing which sources work best for each department.

---

### Query 21: Pipeline Health Analysis (UC2)

**Description**: Monitor application flow through stages, identify bottlenecks, and track stage conversion rates.

**Use Case**: UC2 - Monitor Pipeline Health. Identify bottlenecks, track stage conversion rates, monitor pipeline velocity.

```sql
-- Pipeline Health by Stage (last 30 days)
SELECT
    stage_id,
    COUNT(DISTINCT job_id) as unique_jobs,
    SUM(applications_count) as total_applications,
    ROUND(AVG(stage_duration), 1) as avg_stage_duration_days,
    MIN(stage_entry_date) as earliest_entry,
    MAX(stage_exit_date) as latest_exit,
    COUNT(DISTINCT CASE WHEN stage_exit_date IS NOT NULL THEN stage_id END) as completed_stages
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_pipeline_health_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY stage_id
ORDER BY total_applications DESC;
```

**Expected Output**: Stage-level metrics showing application counts, average duration, and completion rates.

---

### Query 22: Pipeline Bottleneck Identification

**Description**: Identify stages with longest average duration to pinpoint bottlenecks.

**Use Case**: UC2 - Monitor Pipeline Health. Bottleneck identification, process improvement prioritization.

```sql
-- Identify Pipeline Bottlenecks
SELECT
    stage_id,
    job_id,
    title,
    SUM(applications_count) as applications_in_stage,
    ROUND(AVG(stage_duration), 1) as avg_duration_days,
    MAX(stage_duration) as max_duration_days,
    MIN(stage_entry_date) as earliest_entry,
    MAX(stage_exit_date) as latest_exit
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_pipeline_health_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND stage_duration IS NOT NULL
GROUP BY stage_id, job_id, title
HAVING AVG(stage_duration) > 7  -- Filter for stages taking more than a week
ORDER BY avg_duration_days DESC
LIMIT 50;
```

**Expected Output**: Top 50 stage-job combinations with longest average duration, identifying bottlenecks.

---

### Query 23: Recruiter Performance Analysis (UC4)

**Description**: Evaluate recruiter efficiency and quality metrics including applications handled, interviews scheduled, offers extended, and hires made.

**Use Case**: UC4 - Evaluate Recruiter Performance. Track recruiter productivity, identify top performers, allocate workload effectively.

```sql
-- Recruiter Performance Summary (last 90 days)
SELECT
    user_id,
    first_name,
    last_name,
    email,
    SUM(applications_handled) as total_applications_handled,
    SUM(interviews_scheduled) as total_interviews_scheduled,
    SUM(offers_extended) as total_offers_extended,
    SUM(hires_made) as total_hires_made,
    ROUND(SUM(hires_made) * 100.0 / NULLIF(SUM(applications_handled), 0), 2) as hire_rate_pct,
    ROUND(SUM(offers_extended) * 100.0 / NULLIF(SUM(interviews_scheduled), 0), 2) as offer_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_recruiter_performance_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY user_id, first_name, last_name, email
HAVING SUM(applications_handled) >= 10  -- Filter for recruiters with meaningful activity
ORDER BY total_hires_made DESC;
```

**Expected Output**: Recruiter performance metrics ranked by total hires showing productivity and conversion rates.

---

### Query 24: Recruiter Workload Distribution

**Description**: Analyze workload distribution across recruiters to identify imbalances.

**Use Case**: UC4 - Evaluate Recruiter Performance. Workload balancing, capacity planning, identify overloaded recruiters.

```sql
-- Recruiter Workload Distribution
SELECT
    user_id,
    first_name || ' ' || last_name as recruiter_name,
    SUM(applications_handled) as total_applications,
    SUM(interviews_scheduled) as total_interviews,
    ROUND(AVG(applications_handled), 1) as avg_applications_per_day,
    COUNT(DISTINCT analysis_date) as active_days
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_recruiter_performance_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY user_id, first_name, last_name
ORDER BY total_applications DESC;
```

**Expected Output**: Recruiter workload metrics showing total activity and average daily workload.

---

### Query 25: Interview Metrics Analysis (UC3)

**Description**: Analyze interview effectiveness including pass/fail outcomes, interview-to-offer ratios, and interview timing patterns.

**Use Case**: UC3 - Evaluate Interview Effectiveness. Track interview success rates, identify interview types with best outcomes, optimize interview process.

```sql
-- Interview Metrics by Type (last 90 days)
SELECT
    interview_type,
    SUM(interviews_count) as total_interviews,
    SUM(passed_interviews) as total_passed,
    SUM(offers_extended) as total_offers,
    ROUND(SUM(passed_interviews) * 100.0 / NULLIF(SUM(interviews_count), 0), 2) as pass_rate_pct,
    ROUND(AVG(interview_to_offer_ratio), 4) as avg_offer_ratio,
    ROUND(SUM(offers_extended) * 100.0 / NULLIF(SUM(interviews_count), 0), 2) as offer_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_interview_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY interview_type
ORDER BY total_interviews DESC;
```

**Expected Output**: Interview metrics by type showing pass rates, offer ratios, and overall effectiveness.

---

### Query 26: Interview Effectiveness by Job

**Description**: Compare interview effectiveness across different job positions.

**Use Case**: UC3 - Evaluate Interview Effectiveness. Role-specific interview analysis, identify positions with interview challenges.

```sql
-- Interview Effectiveness by Job
SELECT
    job_id,
    SUM(interviews_count) as total_interviews,
    SUM(passed_interviews) as total_passed,
    SUM(offers_extended) as total_offers,
    ROUND(SUM(passed_interviews) * 100.0 / NULLIF(SUM(interviews_count), 0), 2) as pass_rate_pct,
    ROUND(AVG(interview_to_offer_ratio), 4) as avg_offer_ratio
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_interview_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '180' DAY
GROUP BY job_id
HAVING SUM(interviews_count) >= 5  -- Filter for jobs with meaningful interview data
ORDER BY pass_rate_pct DESC
LIMIT 50;
```

**Expected Output**: Top 50 jobs ranked by interview pass rate.

---

### Query 27: Candidate Journey Analysis (UC7)

**Description**: Analyze complete candidate lifecycle from application to hire/rejection, including time between steps and journey patterns.

**Use Case**: UC7 - Analyze the Candidate Journey. Track individual candidate journeys, identify process improvements, analyze candidate experience.

```sql
-- Candidate Journey Summary (last 90 days)
SELECT
    source,
    COUNT(DISTINCT candidate_id) as unique_candidates,
    COUNT(*) as total_journeys,
    ROUND(AVG(application_to_interview_days), 1) as avg_app_to_interview_days,
    ROUND(AVG(interview_to_offer_days), 1) as avg_interview_to_offer_days,
    ROUND(AVG(total_days), 1) as avg_total_days,
    COUNT(DISTINCT CASE WHEN final_status = 'hired' THEN candidate_id END) as hired_candidates,
    ROUND(COUNT(DISTINCT CASE WHEN final_status = 'hired' THEN candidate_id END) * 100.0 / COUNT(DISTINCT candidate_id), 2) as hire_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_candidate_journey_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY source
ORDER BY unique_candidates DESC;
```

**Expected Output**: Candidate journey metrics by source showing average timing and hire rates.

---

### Query 28: Candidate Journey by Department

**Description**: Compare candidate journeys across departments to identify department-specific patterns.

**Use Case**: UC7 - Analyze the Candidate Journey. Department-specific journey analysis, identify process differences.

```sql
-- Candidate Journey by Department
SELECT
    -- Note: department_id may need to be joined from job_postings table
    COUNT(DISTINCT candidate_id) as unique_candidates,
    COUNT(*) as total_journeys,
    ROUND(AVG(application_to_interview_days), 1) as avg_app_to_interview_days,
    ROUND(AVG(interview_to_offer_days), 1) as avg_interview_to_offer_days,
    ROUND(AVG(total_days), 1) as avg_total_days,
    COUNT(DISTINCT CASE WHEN final_status = 'hired' THEN candidate_id END) as hired_candidates
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_candidate_journey_gold cj
JOIN nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_application_pipeline ap
    ON cj.candidate_id = ap.candidate_id
JOIN nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_job_postings jp
    ON ap.job_id = jp.job_id
WHERE cj.analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY jp.department_id
ORDER BY unique_candidates DESC;
```

**Expected Output**: Candidate journey metrics by department showing timing patterns and hire rates.

---

### Query 29: Time to Hire vs Time to Fill Comparison

**Description**: Compare "Time to Fill" (job posting to hire) vs "Time to Hire" (application to hire) to understand the difference between these metrics.

**Use Case**: UC1 - Measure Time to Fill, UC1b - Measure Time to Hire. Understand the distinction between job-level and candidate-level metrics.

```sql
-- Compare Time to Fill vs Time to Hire
-- Note: This requires joining Time to Fill (job-level) with Candidate Journey (candidate-level)
SELECT
    jp.department_id,
    COUNT(DISTINCT ttf.job_id) as jobs_filled,
    ROUND(AVG(ttf.days_to_fill), 1) as avg_time_to_fill,
    COUNT(DISTINCT cj.candidate_id) as candidates_hired,
    ROUND(AVG(cj.total_days), 1) as avg_time_to_hire,
    ROUND(AVG(ttf.days_to_fill) - AVG(cj.total_days), 1) as avg_difference_days
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_time_to_fill_gold ttf
JOIN nan_wl_workloads_data_lake_staging_gold_reporting.talent_candidate_journey_gold cj
    ON ttf.job_id = cj.job_id
JOIN nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_job_postings jp
    ON ttf.job_id = jp.job_id
WHERE ttf.analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND ttf.final_status = 'hired'
  AND cj.final_status = 'hired'
GROUP BY jp.department_id
ORDER BY avg_time_to_fill DESC;
```

**Expected Output**: Comparison of time-to-fill (job-level) vs time-to-hire (candidate-level) by department.

---

## Utility Queries

### Query 30: Table Metadata and Partitions

**Description**: Check table structure, partitions, and data freshness.

**Use Case**: Data operations, troubleshooting, monitoring.

```sql
-- Check partition information for Bronze table
SHOW PARTITIONS nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__candidates_bronze;

-- Or get partition stats
SELECT
    "$path" as partition_path,
    year,
    month,
    COUNT(*) as record_count,
    MIN(ingestion_timestamp) as earliest_record,
    MAX(ingestion_timestamp) as latest_record
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__candidates_bronze
WHERE year >= 2025
GROUP BY "$path", year, month
ORDER BY year DESC, month DESC;
```

**Expected Output**: List of partitions with metadata useful for data operations.

---

### Query 31: Column Statistics

**Description**: Generate basic statistics for numeric columns.

**Use Case**: Data profiling, quality checks, understanding data distributions.

```sql
-- Statistics for Gold time-to-fill metrics
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT job_id) as unique_jobs,
    -- Time metrics statistics
    ROUND(AVG(days_to_fill), 1) as avg_days_to_fill,
    ROUND(STDDEV(days_to_fill), 1) as stddev_days_to_fill,
    MIN(days_to_fill) as min_days_to_fill,
    APPROX_PERCENTILE(days_to_fill, 0.5) as median_days_to_fill,
    MAX(days_to_fill) as max_days_to_fill,
    -- Application timing statistics
    ROUND(AVG(days_to_first_application), 1) as avg_days_to_first_app,
    ROUND(AVG(days_to_interview), 1) as avg_days_to_interview
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_time_to_fill_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND final_status = 'hired';
```

**Expected Output**: Statistical summary of key metrics for the latest data snapshot.

---

### Query 32: Data Freshness Check

**Description**: Verify data freshness across all layers to ensure pipelines are running correctly.

**Use Case**: Pipeline monitoring, data freshness validation, identify stale data.

```sql
-- Data freshness check across layers
SELECT
    'Raw' as layer,
    'candidates' as entity,
    MAX(ingestion_timestamp) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', MAX(ingestion_timestamp), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.teamtailor__talent__candidates_raw
UNION ALL
SELECT
    'Bronze' as layer,
    'candidates' as entity,
    MAX(bronze_ingestion_timestamp) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', MAX(bronze_ingestion_timestamp), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_bronze_processing.teamtailor__talent__candidates_bronze
UNION ALL
SELECT
    'Silver' as layer,
    'candidate_profiles' as entity,
    MAX(silver_ingestion_timestamp) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', MAX(silver_ingestion_timestamp), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_silver_analytics.teamtailor_candidate_profiles
UNION ALL
SELECT
    'Gold' as layer,
    'time_to_fill' as entity,
    MAX(analysis_date) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', CAST(MAX(analysis_date) AS TIMESTAMP), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_gold_reporting.talent_time_to_fill_gold;
```

**Expected Output**: Data freshness metrics across all layers showing how recent the data is.

---

## Query Optimization Tips

### Performance Best Practices

1. **Always filter by partition columns**:

```sql
WHERE year = 2025 AND month = 11  -- Raw/Bronze layer
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY  -- Gold layer
```

1. **Use latest partition for point-in-time queries**:

```sql
WHERE year = (SELECT MAX(year) FROM table_name)
  AND month = (SELECT MAX(month) FROM table_name WHERE year = (SELECT MAX(year) FROM table_name))
```

1. **Leverage quality flags for filtering**:

```sql
WHERE data_quality_score >= 0.8
  AND has_complete_name = true
  AND is_valid = true
```

1. **Use APPROX functions for large datasets**:

```sql
APPROX_PERCENTILE(column, 0.5) -- Faster than exact percentile
APPROX_COUNT_DISTINCT(column)  -- Faster than COUNT(DISTINCT)
```

1. **Limit result sets appropriately**:

```sql
LIMIT 100  -- Always use LIMIT for exploratory queries
```

### Common Athena Settings

Optimize query performance with these settings:

```sql
-- Increase result size for large queries
SET max_rows_to_scan = 10000000;

-- Enable query result reuse (24 hours)
SET query_results_s3_access_logs_location = 's3://bucket/query-results/';
```

---

## Related Documentation

* [Team Tailor Bronze Layer](./BRONZE.md) - Complete Bronze schema and field descriptions
* [Team Tailor Raw Layer](./RAW.md) - Raw data structure and ingestion details
* [Team Tailor Silver Layer](./SILVER.md) - Silver layer transformations and business logic
* [Team Tailor Gold Layer](./GOLD.md) - Gold layer KPIs and use cases
* [Team Tailor Overview](./README.md) - Integration overview and quick start
* [Medallion Architecture](../../MEDALLION_ARCHITECTURE.md) - Layer design principles

---

**Last Updated**: 2025-01-20  
**Query Count**: 32 queries (4 Raw, 7 Bronze, 5 Silver, 11 Gold, 4 Utility)  
**Database Layers**: Raw, Bronze, Silver, Gold  
**Environment**: Staging (`nan_wl_workloads_data_lake_staging_*`)
