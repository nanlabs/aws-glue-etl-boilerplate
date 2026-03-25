# Team Tailor Silver Layer

## Overview

The Silver layer provides clean, business-ready tables optimized for Talent analytics with data quality validations and enrichment.

**Job**: `jobs/silver/teamtailor_silver_job.py`  
**Format**: Apache Iceberg  
**Storage**: S3 with Iceberg table format  
**Update Frequency**: Daily (after bronze processing)

## Silver Tables

### 1. Candidate Profiles (`teamtailor_candidate_profiles`)

Enriched candidate data with data quality scores and completeness flags.

**Key Fields**:
- `candidate_id`, `email`, `first_name`, `last_name`
- `phone`, `linkedin_url`, `source`, `tags`
- `has_complete_name`, `has_phone`, `has_linkedin`
- `data_quality_score`, `is_valid`

**Merge Strategy**: UPSERT by `candidate_id`

### 2. Job Postings (`teamtailor_job_postings`)

Job details with status tracking and department information.

**Key Fields**:
- `job_id`, `title`, `status`, `department_id`
- `description`, `internal_title`
- `data_quality_score`, `is_valid`

**Merge Strategy**: UPSERT by `job_id`

### 3. Application Pipeline (`teamtailor_application_pipeline`)

Application journey with stages and referral tracking.

**Key Fields**:
- `application_id`, `candidate_id`, `job_id`, `status`
- `stage_id`, `changed_stage_at` (when current stage was set)
- `rejected_at`, `sourced` (boolean)
- `referred_by_id`
- `created_at`, `updated_at`
- Partitioned by `year/month`

**Merge Strategy**: Append (time-series)

> **Note**: `changed_stage_at` replaces the need for a separate stage transitions table.

### 4. Interview Events (`teamtailor_interview_events`)

Interview tracking with scheduling and outcome data.

**Key Fields**:
- `interview_id`, `application_id`, `interview_type`
- `scheduled_at`, `status`, `location`, `note`
- `is_weekend`, `is_business_hours`
- Partitioned by `year/month`

**Merge Strategy**: Append (time-series)

### 5. Recruiter Performance (`teamtailor_recruiter_performance`)

User/recruiter metrics for performance tracking.

**Key Fields**:
- `user_id`, `email`, `first_name`, `last_name`
- `role`, `department_id`
- `data_quality_score`, `is_valid`

**Merge Strategy**: UPSERT by `user_id`

### 6. Candidate NPS (`teamtailor_candidate_nps`)

Candidate NPS survey responses for UC6.

**Key Fields**:
- `nps_response_id`, `candidate_id`, `score`, `comment`
- `nps_category` (Promoter/Passive/Detractor)
- `created_at`, `updated_at`
- Partitioned by `year/month`

**Merge Strategy**: Append (time-series)

> **⚠️ Deprecated**: `application_stage_transitions` table was removed because the Activities API endpoint was discontinued by TeamTailor on 2020-10-01. Stage tracking is now done via `changed_stage_at` field in `application_pipeline`.

## Data Quality Features

### Quality Scores

- **High Quality** (0.95): Required fields present and valid
- **Medium Quality** (0.5): Some required fields missing
- **Low Quality** (< 0.5): Critical fields missing

### Validation Flags

- `is_valid`: Boolean flag indicating record validity
- `has_complete_name`: Name completeness check
- `has_phone`: Phone number presence
- `has_linkedin`: LinkedIn URL presence

### Enrichment Fields

- `is_weekend`: Weekend detection for interviews
- `is_business_hours`: Business hours detection
- `record_hash`: SHA-256 hash for change detection

## Related Documentation

- [Bronze Layer](./BRONZE.md) - Source data structure
- [Gold Layer](./GOLD.md) - KPI tables
- [Source Overview](./README.md) - Complete integration overview
