# Team Tailor Gold Layer - Talent KPIs

## Overview

The Gold layer provides analytics-ready tables with pre-calculated Talent KPIs for dashboarding and reporting.

**Job**: `jobs/gold/teamtailor_gold_job.py`  
**Format**: Apache Iceberg  
**Storage**: S3 with Iceberg table format  
**Update Frequency**: Daily (after silver processing)

## KPI Tables

### 1. Time to Fill (`talent_time_to_fill_gold`)

**KPI**: Average days from job posting to offer acceptance

**Dimensions**:
- `job_id`, `title`, `status`, `department_id`

**Metrics**:
- `days_to_fill`: Days from posting to hire
- `days_to_first_application`: Days to first application
- `days_to_interview`: Days from application to interview

**Partitioning**: `analysis_date`, `job_id`

**Use Cases**:
- Track hiring efficiency by department
- Identify bottlenecks in the hiring process
- Compare time-to-fill across job types

### 2. Source Effectiveness (`talent_source_effectiveness_gold`)

**KPI**: Quality and conversion by source

**Dimensions**:
- `source`, `job_id`, `title`, `department_id`

**Metrics**:
- `applications_count`: Total applications from source
- `interviews_count`: Applications that reached interview stage
- `offers_count`: Applications that received offers
- `hires_count`: Applications that resulted in hires
- `conversion_rate`: Hires / Applications

**Partitioning**: `analysis_date`, `source`

**Use Cases**:
- Identify best-performing recruitment sources
- Optimize recruitment budget allocation
- Track source quality over time

### 3. Pipeline Health (`talent_pipeline_health_gold`)

**KPI**: Application flow through stages

**Dimensions**:
- `stage_id`, `job_id`, `title`, `department_id`

**Metrics**:
- `applications_count`: Applications in stage
- `stage_duration`: Average time in stage
- `stage_entry_date`, `stage_exit_date`

**Partitioning**: `analysis_date`, `stage_id`

**Use Cases**:
- Identify bottlenecks in application pipeline
- Track stage conversion rates
- Monitor pipeline velocity

### 4. Recruiter Performance (`talent_recruiter_performance_gold`)

**KPI**: Recruiter efficiency and quality

**Dimensions**:
- `user_id`, `email`, `first_name`, `last_name`, `department_id`

**Metrics**:
- `applications_handled`: Total applications managed
- `interviews_scheduled`: Interviews scheduled
- `offers_extended`: Offers extended
- `hires_made`: Successful hires

**Partitioning**: `analysis_date`, `user_id`

**Use Cases**:
- Track recruiter productivity
- Identify top performers
- Allocate workload effectively

### 5. Interview Metrics (`talent_interview_metrics_gold`)

**KPI**: Interview efficiency and outcomes

**Dimensions**:
- `interview_type` (nullable - NULL values are replaced with "unknown"), `application_id`, `job_id`

**Metrics**:
- `interviews_count`: Total interviews
- `passed_interviews`: Interviews that passed
- `offers_extended`: Offers after interviews
- `interview_to_offer_ratio`: Offers / Interviews

**Partitioning**: `analysis_date`, `interview_type`

**Data Quality Notes**:
- If `interview_type` is NULL in the source data, it is automatically replaced with "unknown" before aggregation
- This ensures all interviews are included in the metrics, even if the type is not specified

**Use Cases**:
- Track interview success rates
- Identify interview types with best outcomes
- Optimize interview process

### 6. Candidate Journey (`talent_candidate_journey_gold`)

**KPI**: Complete candidate lifecycle (UC1b, UC7)

**Dimensions**:
- `candidate_id`, `job_id`, `source`, `department_id`

**Metrics**:
- `application_date`: When candidate applied
- `first_interview_date`: When first interview occurred
- `offer_date`: When offer was extended
- `offer_accepted_date`: When offer was accepted
- `application_to_interview_days`: Days from application to interview (UC1b)
- `interview_to_offer_days`: Days from interview to offer (UC1b)
- `days_to_offer`: Days from application to offer (UC1b)
- `days_to_offer_acceptance`: Days from application to offer acceptance (UC1b)
- `total_days`: Total days in process
- `final_status`: Final outcome (hired/rejected)

**Partitioning**: `analysis_date`, `candidate_id`

**Use Cases**:
- UC1b - Measure Time to Hire
- UC7 - Analyze the Candidate Journey
- Track individual candidate journeys
- Identify process improvements
- Analyze candidate experience
- Measure time from application to hire (independent of job posting date)

### 7. Candidate NPS (`talent_candidate_nps_gold`)

**KPI**: Candidate Net Promoter Score metrics and segmentation (UC6)

**Dimensions**:
- `candidate_id` (required - rows with NULL candidate_id are filtered out), `job_id` (nullable), `recruiter_id` (nullable), `source` (nullable)

**Metrics**:
- `nps_responses_count`: Total number of NPS responses
- `avg_nps_score`: Average NPS score (0-10)
- `promoters_count`: Number of Promoters (score 9-10)
- `passives_count`: Number of Passives (score 7-8)
- `detractors_count`: Number of Detractors (score 0-6)
- `nps_score`: Calculated NPS ((Promoters - Detractors) / Total * 100)

**Partitioning**: `analysis_date`, `candidate_id`

**Data Quality Notes**:
- Only NPS responses with a valid `candidate_id` are included in the Gold table
- If `recruiter_id`, `job_id`, or `source` are not available (e.g., due to failed joins), they will be NULL
- The job attempts to join with `teamtailor_recruiter_performance`, `teamtailor_application_pipeline`, and `teamtailor_job_postings` to enrich the data, but these joins may fail if no matching records exist

**Use Cases**:
- UC6 - Measure Candidate NPS
- Evaluate candidate experience through satisfaction surveys
- Segmentation by recruiter, job, source, and pipeline experience
- Calculate NPS (Promoters – Detractors)
- Track post-process survey responses (accepted and rejected candidates)

> **⚠️ Deprecated**: `talent_stage_transitions_gold` table was removed because the Activities API endpoint was discontinued by TeamTailor on 2020-10-01. Stage analysis is partially covered by `talent_candidate_journey` using `changed_stage_at` field in application_pipeline.

## Query Examples

### Time to Fill by Department

```sql
SELECT
  department_id,
  AVG(days_to_fill) as avg_time_to_fill,
  COUNT(*) as jobs_filled
FROM glue_catalog.nan_gold_zone.talent_time_to_fill_gold
WHERE final_status = 'hired'
GROUP BY department_id
ORDER BY avg_time_to_fill DESC
```

### Source Effectiveness

```sql
SELECT
  source,
  SUM(applications_count) as total_applications,
  SUM(hires_count) as total_hires,
  AVG(conversion_rate) as avg_conversion_rate
FROM glue_catalog.nan_gold_zone.talent_source_effectiveness_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY source
ORDER BY avg_conversion_rate DESC
```

### Recruiter Performance

```sql
SELECT
  first_name || ' ' || last_name as recruiter_name,
  SUM(applications_handled) as total_applications,
  SUM(hires_made) as total_hires,
  CAST(SUM(hires_made) AS DOUBLE) / SUM(applications_handled) as hire_rate
FROM glue_catalog.nan_gold_zone.talent_recruiter_performance_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY user_id, first_name, last_name
ORDER BY hire_rate DESC
```

### Candidate NPS Analysis

```sql
SELECT
  recruiter_id,
  source,
  COUNT(DISTINCT candidate_id) as candidates_with_nps,
  AVG(nps_score) as avg_nps_score,
  SUM(promoters_count) as total_promoters,
  SUM(detractors_count) as total_detractors,
  SUM(nps_responses_count) as total_responses
FROM glue_catalog.nan_gold_zone.talent_candidate_nps_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY recruiter_id, source
ORDER BY avg_nps_score DESC
```

### Time to Hire Analysis (UC1b)

```sql
SELECT
  department_id,
  source,
  COUNT(*) as candidates,
  AVG(days_to_offer) as avg_days_to_offer,
  AVG(days_to_offer_acceptance) as avg_days_to_acceptance,
  AVG(application_to_interview_days) as avg_app_to_interview_days,
  AVG(interview_to_offer_days) as avg_interview_to_offer_days
FROM glue_catalog.nan_gold_zone.talent_candidate_journey_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND final_status = 'hired'
GROUP BY department_id, source
ORDER BY avg_days_to_acceptance DESC
```

### Stage Transitions Analysis (UC7)

```sql
SELECT
  department_id,
  current_stage_id,
  COUNT(*) as applications,
  SUM(CASE WHEN has_regression THEN 1 ELSE 0 END) as apps_with_regressions,
  SUM(CASE WHEN is_dropped_off THEN 1 ELSE 0 END) as dropped_off_apps,
  AVG(total_transitions) as avg_transitions,
  AVG(days_since_last_transition) as avg_days_since_transition
FROM glue_catalog.nan_gold_zone.talent_stage_transitions_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY department_id, current_stage_id
ORDER BY dropped_off_apps DESC
```

## Related Documentation

- [Silver Layer](./SILVER.md) - Source data structure
- [Source Overview](./README.md) - Complete integration overview
