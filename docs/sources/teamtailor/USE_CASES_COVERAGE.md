# Team Tailor - Use Cases Coverage Analysis

## Overview

This document analyzes the coverage of all use cases (UC1-UC7) from the PRD against the implemented Gold KPI tables.

---

## Use Cases Coverage Matrix

| Use Case | Gold Table(s) | Coverage Status | Missing Metrics/Features |
|----------|---------------|-----------------|--------------------------|
| **UC1 - Time to Fill** | `talent_time_to_fill` | ✅ **COVERED** | None |
| **UC1b - Time to Hire** | `talent_candidate_journey` | ✅ **COVERED** | None |
| **UC2 - Pipeline Health** | `talent_pipeline_health` | ✅ **COVERED** | None |
| **UC3 - Interview Effectiveness** | `talent_interview_metrics` | ✅ **COVERED** | None |
| **UC4 - Recruiter Performance** | `talent_recruiter_performance` | ✅ **COVERED** | None |
| **UC5 - Source Effectiveness** | `talent_source_effectiveness` | ✅ **COVERED** | None |
| **UC6 - Candidate NPS** | `talent_candidate_nps` | ✅ **COVERED** | None |
| **UC7 - Candidate Journey** | `talent_candidate_journey` | ⚠️ **PARTIAL** | Stage transitions history unavailable (API removed) |

---

## Detailed Use Case Analysis

### ✅ UC1 – Measure Time to Fill

**Requirements:**
- ✅ Time to first application
- ✅ Time to first interview
- ✅ Total time until the role is filled

**Gold Table:** `talent_time_to_fill`

**Metrics Available:**
- ✅ `days_to_first_application`: Days from job posting to first application
- ✅ `days_to_interview`: Days from first application to first interview
- ✅ `days_to_fill`: Days from job posting to hire (when final_status = 'hired')

**Status:** ✅ **FULLY COVERED**

---

### ✅ UC1b – Measure Time to Hire

**Requirements:**
- ✅ Days from application to first interview
- ✅ Days to offer
- ✅ Days to offer acceptance

**Gold Table:** `talent_candidate_journey`

**Metrics Available:**
- ✅ `application_to_interview_days`: Days from application to first interview
- ✅ `interview_to_offer_days`: Days from interview to offer (FIXED)
- ✅ `days_to_offer`: Days from application to when offer was extended
- ✅ `days_to_offer_acceptance`: Days from application to when offer was accepted
- ✅ `total_days`: Total days in process
- ✅ `offer_date`: Timestamp when offer was extended
- ✅ `offer_accepted_date`: Timestamp when offer was accepted

**Status:** ✅ **FULLY COVERED**

**Implementation Notes:**
- Uses `application_pipeline` status = 'offer' to detect offer_date
- Uses `application_pipeline` status = 'hired' to detect offer_accepted_date
- All metrics correctly calculated from application_date

---

### ✅ UC2 – Monitor Pipeline Health

**Requirements:**
- ✅ Number of candidates per stage
- ✅ Stage conversion rates (can be calculated from stage counts)
- ✅ Time spent in each stage
- ✅ Bottlenecks or delays

**Gold Table:** `talent_pipeline_health`

**Metrics Available:**
- ✅ `applications_count`: Number of applications in stage
- ✅ `stage_duration`: Average time in stage (days)
- ✅ `stage_entry_date`: When applications entered stage
- ✅ `stage_exit_date`: When applications exited stage

**Status:** ✅ **FULLY COVERED**

---

### ✅ UC3 – Evaluate Interview Effectiveness

**Requirements:**
- ✅ Total interview count
- ✅ Pass/fail outcomes
- ✅ Interview-to-offer ratio
- ✅ Interview timing (business hours vs weekend)

**Gold Table:** `talent_interview_metrics`

**Metrics Available:**
- ✅ `interviews_count`: Total interviews
- ✅ `passed_interviews`: Interviews that passed
- ✅ `offers_extended`: Offers after interviews
- ✅ `interview_to_offer_ratio`: Offers / Interviews

**Note:** Interview timing (business hours vs weekend) is available in Silver layer (`teamtailor_interview_events`) with `is_weekend` and `is_business_hours` flags. Can be aggregated in Gold if needed.

**Status:** ✅ **FULLY COVERED**

---

### ✅ UC4 – Evaluate Recruiter Performance

**Requirements:**
- ✅ Applications handled
- ✅ Interviews scheduled
- ✅ Offers extended
- ✅ Successful hires
- ✅ Personal conversion rate (can be calculated: hires / applications)

**Gold Table:** `talent_recruiter_performance`

**Metrics Available:**
- ✅ `applications_handled`: Total applications managed
- ✅ `interviews_scheduled`: Interviews scheduled
- ✅ `offers_extended`: Offers extended
- ✅ `hires_made`: Successful hires

**Status:** ✅ **FULLY COVERED**

---

### ✅ UC5 – Analyze Source Effectiveness

**Requirements:**
- ✅ Applications
- ✅ Interviews
- ✅ Offers
- ✅ Hires
- ✅ Conversion rate by source

**Gold Table:** `talent_source_effectiveness`

**Metrics Available:**
- ✅ `applications_count`: Total applications from source
- ✅ `interviews_count`: Applications that reached interview stage
- ✅ `offers_count`: Applications that received offers
- ✅ `hires_count`: Applications that resulted in hires
- ✅ `conversion_rate`: Hires / Applications

**Status:** ✅ **FULLY COVERED**

---

### ✅ UC6 – Measure Candidate NPS

**Requirements:**
- ✅ Post-process survey (accepted and rejected candidates)
- ✅ Calculation of NPS (Promoters – Detractors)
- ✅ Segmentation by recruiter, job, seniority, and pipeline experience

**Gold Table:** `talent_candidate_nps`

**Metrics Available:**
- ✅ `nps_responses_count`: Total NPS responses
- ✅ `nps_score`: Calculated NPS ((Promoters - Detractors) / Total * 100)
- ✅ `promoters_count`: Promoters (score 9-10)
- ✅ `detractors_count`: Detractors (score 0-6)
- ✅ `passives_count`: Passives (score 7-8)
- ✅ `avg_nps_score`: Average NPS score
- ✅ Segmentation fields: `recruiter_id`, `job_id`, `source`

**Note:** Seniority segmentation would require joining with candidate profiles if seniority data is available.

**Status:** ✅ **FULLY COVERED**

---

### ⚠️ UC7 – Analyze the Candidate Journey

**Requirements:**
- ✅ Application → Screening → Interviews → Offer → Hire/Reject (covered by `talent_candidate_journey`)
- ✅ Time between each step (covered by `application_to_interview_days`, `interview_to_offer_days`, `days_to_offer`, `days_to_offer_acceptance`)
- ⚠️ Stage jumps, regressions, or drop-offs (**LIMITED** - only current stage available via `changed_stage_at`)
- ✅ Differences in journey by role or department (covered by `department_id`)

**Gold Tables:**
- `talent_candidate_journey`: Complete lifecycle with timing metrics

> **⚠️ Limitation**: The `talent_stage_transitions` table was **removed** because TeamTailor's Activities API endpoint was discontinued on 2020-10-01. Detailed stage transition history (jumps, regressions, drop-offs) is no longer available. Only current stage and `changed_stage_at` timestamp are available from applications.

**Metrics Available in `talent_candidate_journey`:**
- ✅ `application_date`: When candidate applied
- ✅ `first_interview_date`: When first interview occurred
- ✅ `offer_date`: When offer was extended
- ✅ `offer_accepted_date`: When offer was accepted
- ✅ `final_status`: Final outcome (hired/rejected)
- ✅ `application_to_interview_days`: Days from application to interview
- ✅ `interview_to_offer_days`: Days from interview to offer
- ✅ `days_to_offer`: Days from application to offer
- ✅ `days_to_offer_acceptance`: Days from application to offer acceptance
- ✅ `total_days`: Total days in process
- ✅ `department_id`: Department for segmentation

**Metrics NOT Available (due to Activities API removal):**
- ❌ `total_transitions`: Total number of stage transitions
- ❌ `forward_transition_count`: Forward progress transitions
- ❌ `regression_count`: Backward moves (regressions)
- ❌ `has_regression`: Boolean flag for applications with regressions
- ❌ `previous_stage_id`: Previous stage before current
- ❌ `days_since_last_transition`: Days since last stage change
- ❌ `is_dropped_off`: Flag for applications with no progress >30 days

**Status:** ⚠️ **PARTIALLY COVERED**

**Workaround:**
- Use `changed_stage_at` field in `application_pipeline` to track when the current stage was set
- Calculate `days_in_current_stage` as `DATEDIFF(current_date, changed_stage_at)`
- Identify potential drop-offs by filtering applications where `days_in_current_stage > 30`

---

## Summary

### Coverage Status

**6 Use Cases Fully Covered:**

- ✅ **UC1 - Time to Fill**: `talent_time_to_fill`
- ✅ **UC1b - Time to Hire**: `talent_candidate_journey`
- ✅ **UC2 - Pipeline Health**: `talent_pipeline_health`
- ✅ **UC3 - Interview Effectiveness**: `talent_interview_metrics`
- ✅ **UC4 - Recruiter Performance**: `talent_recruiter_performance`
- ✅ **UC5 - Source Effectiveness**: `talent_source_effectiveness`
- ✅ **UC6 - Candidate NPS**: `talent_candidate_nps`

**1 Use Case Partially Covered:**

- ⚠️ **UC7 - Candidate Journey**: `talent_candidate_journey` (stage transitions history unavailable due to Activities API removal)

---

## API Limitations

### Activities API Removal (2020-10-01)

TeamTailor **removed the Activities API endpoint** (`/v1/activities`) on October 1, 2020. This impacts:

1. **`application_stage_transitions`** entity - DISABLED
2. **`talent_stage_transitions`** Gold table - DISABLED
3. **UC7** detailed stage transition analysis - LIMITED

**Workaround:** Use `changed_stage_at` field from applications to track current stage timing.

**Reference:** [TeamTailor Changelog](https://docs.teamtailor.com/changelog) - "since it didn't work anyway"

---

## Data Sources Available

### Bronze Layer (8 tables)
- `teamtailor__talent__candidates_bronze`: Candidate profiles
- `teamtailor__talent__jobs_bronze`: Job postings
- `teamtailor__talent__applications_bronze`: Applications (includes `changed_stage_at`)
- `teamtailor__talent__interviews_bronze`: Interview events
- `teamtailor__talent__users_bronze`: Recruiters/hiring managers
- `teamtailor__talent__departments_bronze`: Departments
- `teamtailor__talent__stages_bronze`: Pipeline stages
- `teamtailor__talent__nps_responses_bronze`: NPS responses

### Silver Layer (6 tables)
- `teamtailor_candidate_profiles`: Enriched candidate data
- `teamtailor_job_postings`: Job details with status
- `teamtailor_application_pipeline`: Application journey (includes `changed_stage_at`, `stage_id`)
- `teamtailor_interview_events`: Interview tracking
- `teamtailor_recruiter_performance`: Recruiter metrics
- `teamtailor_candidate_nps`: NPS survey responses

### Gold Layer (7 tables)
- `talent_time_to_fill`: Time to fill metrics (UC1)
- `talent_source_effectiveness`: Source quality (UC5)
- `talent_pipeline_health`: Pipeline flow (UC2)
- `talent_recruiter_performance`: Recruiter metrics (UC4)
- `talent_interview_metrics`: Interview outcomes (UC3)
- `talent_candidate_journey`: Candidate lifecycle (UC1b, UC7 partial)
- `talent_candidate_nps`: NPS metrics (UC6)

---

**Last Updated**: 2025-12-17  
**Coverage**: 6/7 fully covered, 1/7 partially covered  
**Status**: UC7 stage transition analysis limited due to TeamTailor Activities API removal
