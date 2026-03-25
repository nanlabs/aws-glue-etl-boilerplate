# Apache Superset Dashboards Implementation Guide

## Overview

This guide provides step-by-step instructions for building comprehensive Talent and Technology dashboards in Apache Superset using the Gold layer KPI tables from Team Tailor and ClickUp integrations.

**Estimated Time**: 4-6 hours for complete implementation  
**Prerequisites**: Apache Superset installed and configured, access to AWS Athena/Glue Data Catalog

---

## Table of Contents

1. [Prerequisites & Setup](#prerequisites--setup)
2. [Talent Dashboard Implementation](#talent-dashboard-implementation)
3. [Technology Dashboard Implementation](#technology-dashboard-implementation)
4. [Advanced Configuration](#advanced-configuration)
5. [Troubleshooting & Best Practices](#troubleshooting--best-practices)

---

## Prerequisites & Setup

### 1. Superset Database Connection

#### Step 1.1: Create Athena Connection

1. Navigate to **Settings** → **Database Connections** → **+ Database**
2. Select **Amazon Athena** as database type
3. Configure connection:
   - **Display Name**: `AWS Athena - Data Lake`
   - **SQLAlchemy URI**: 
     ```
     awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region}.amazonaws.com/{schema_name}?s3_staging_dir=s3://{bucket}/athena-results/
     ```
   - **Example**:
     ```
     awsathena+rest://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY@athena.us-east-1.amazonaws.com/nan_gold_zone?s3_staging_dir=s3://my-bucket/athena-results/
     ```
4. Click **Test Connection** to verify
5. Click **Connect**

#### Step 1.2: Verify Database Access

Run a test query to verify access to Gold tables:

```sql
SELECT COUNT(*) as record_count
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_time_to_fill_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 7 DAY
LIMIT 1;
```

### 2. Create Datasets

For each Gold table, create a Superset dataset:

#### Talent Datasets (8 tables)

1. **talent_time_to_fill_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_time_to_fill_gold`

2. **talent_source_effectiveness_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_source_effectiveness_gold`

3. **talent_pipeline_health_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_pipeline_health_gold`

4. **talent_recruiter_performance_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_recruiter_performance_gold`

5. **talent_interview_metrics_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_interview_metrics_gold`

6. **talent_candidate_journey_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_candidate_journey_gold`

7. **talent_candidate_nps_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_candidate_nps_gold`

8. **talent_stage_transitions_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `talent_stage_transitions_gold`

#### Technology Datasets (9 tables)

1. **technology_request_metrics_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_request_metrics_gold`

2. **technology_satisfaction_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_satisfaction_gold`

3. **technology_throughput_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_throughput_gold`

4. **technology_request_status_flow_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_request_status_flow_gold`

5. **technology_team_performance_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_team_performance_gold`

6. **technology_stakeholder_metrics_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_stakeholder_metrics_gold`

7. **technology_process_metrics_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_process_metrics_gold`

8. **technology_account_metrics_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_account_metrics_gold`

9. **technology_rd_practices_metrics_gold**
   - Database: `AWS Athena - Data Lake`
   - Schema: `nan_gold_zone`
   - Table: `technology_rd_practices_metrics_gold`

#### Step 2.1: Create Dataset Steps

For each table:

1. Navigate to **Data** → **Datasets** → **+ Dataset**
2. Select the database connection
3. Select schema: `nan_gold_zone`
4. Select the table name
5. Click **Create Dataset**
6. Configure columns:
   - Mark `analysis_date` as temporal dimension
   - Mark numeric columns as metrics
   - Mark string columns as dimensions
7. Click **Save**

### 3. User Permissions

Ensure users have appropriate permissions:

1. **Admin Role**: Full access to create/edit dashboards
2. **Gamma Role**: View-only access to dashboards
3. **Custom Role**: Limited access to specific datasets

---

## Talent Dashboard Implementation

### Dashboard Overview

**Dashboard Name**: `Talent Analytics Dashboard`  
**Layout**: 4 sections, responsive grid  
**Refresh**: Daily (after Gold jobs complete)

### Section 1: Executive Summary (Top Row)

#### Chart 1.1: Average Time to Fill (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `talent_time_to_fill_gold`

**SQL Query**:
```sql
SELECT
  AVG(days_to_fill) as avg_time_to_fill
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_time_to_fill_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND final_status = 'hired'
  AND days_to_fill IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `talent_time_to_fill_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `avg_time_to_fill`
6. **Format**: Number (decimals: 1)
7. **Prefix**: None
8. **Suffix**: ` days`
9. **Subheader**: "Last 90 days"
10. **Color**: Blue (#1890FF)
11. Click **Run Query** → **Save**

#### Chart 1.2: Average Time to Hire (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `talent_candidate_journey_gold`

**SQL Query**:
```sql
SELECT
  AVG(days_to_offer_acceptance) as avg_time_to_hire
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_candidate_journey_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND final_status = 'hired'
  AND days_to_offer_acceptance IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `talent_candidate_journey_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `avg_time_to_hire`
6. **Format**: Number (decimals: 1)
7. **Suffix**: ` days`
8. **Subheader**: "Last 90 days"
9. **Color**: Green (#52C41A)
10. Click **Run Query** → **Save**

#### Chart 1.3: Overall NPS Score (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `talent_candidate_nps_gold`

**SQL Query**:
```sql
SELECT
  AVG(nps_score) as overall_nps
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_candidate_nps_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND nps_score IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number`
2. Dataset: `talent_candidate_nps_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `overall_nps`
6. **Format**: Number (decimals: 1)
7. **Suffix**: None
8. **Subheader**: "Last 90 days"
9. **Color**: Purple (#722ED1)
10. Click **Run Query** → **Save**

#### Chart 1.4: Total Active Applications (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `talent_pipeline_health_gold`

**SQL Query**:
```sql
SELECT
  SUM(applications_count) as total_applications
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_pipeline_health_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND applications_count IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `talent_pipeline_health_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `total_applications`
6. **Format**: Integer
7. **Suffix**: None
8. **Subheader**: "Last 30 days"
9. **Color**: Orange (#FA8C16)
10. Click **Run Query** → **Save**

#### Chart 1.5: Time to Fill Trend (Line Chart)

**Chart Type**: **Time Series Line Chart**  
**Dataset**: `talent_time_to_fill_gold`

**SQL Query**:
```sql
SELECT
  DATE(analysis_date) as date,
  department_id,
  AVG(days_to_fill) as avg_days_to_fill
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_time_to_fill_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 180 DAY
  AND final_status = 'hired'
  AND days_to_fill IS NOT NULL
GROUP BY DATE(analysis_date), department_id
ORDER BY date ASC
```

**Configuration Steps**:
1. Create new chart → Select **Time Series Line Chart**
2. Dataset: `talent_time_to_fill_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Time Column**: `date`
6. **Time Grain**: Day
7. **Metrics**: `avg_days_to_fill` (SUM aggregation)
8. **Series**: `department_id`
9. **X Axis Label**: "Date"
10. **Y Axis Label**: "Days to Fill"
11. **Legend**: Show (position: Right)
12. **Colors**: Use default palette
13. Click **Run Query** → **Save**

#### Chart 1.6: Source Effectiveness Funnel (Funnel Chart)

**Chart Type**: **Funnel Chart**  
**Dataset**: `talent_source_effectiveness_gold`

**SQL Query**:
```sql
SELECT
  source,
  SUM(applications_count) as applications,
  SUM(interviews_count) as interviews,
  SUM(offers_count) as offers,
  SUM(hires_count) as hires
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_source_effectiveness_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND source IS NOT NULL
GROUP BY source
ORDER BY applications DESC
LIMIT 10
```

**Configuration Steps**:
1. Create new chart → Select **Funnel Chart**
2. Dataset: `talent_source_effectiveness_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Group By**: `source`
6. **Metrics**: 
   - `applications` (label: "Applications")
   - `interviews` (label: "Interviews")
   - `offers` (label: "Offers")
   - `hires` (label: "Hires")
7. **Sort By**: `applications` (descending)
8. **Limit**: 10
9. Click **Run Query** → **Save**

### Section 2: Pipeline Health (Middle Left)

#### Chart 2.1: Pipeline Stage Distribution (Sankey Diagram)

**Chart Type**: **Sankey Diagram**  
**Dataset**: `talent_pipeline_health_gold`

**SQL Query**:
```sql
SELECT
  stage_id as source,
  LEAD(stage_id) OVER (PARTITION BY application_id ORDER BY stage_entry_date) as target,
  COUNT(*) as value
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_pipeline_health_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND stage_id IS NOT NULL
GROUP BY stage_id, LEAD(stage_id) OVER (PARTITION BY application_id ORDER BY stage_entry_date)
HAVING target IS NOT NULL
ORDER BY value DESC
```

**Configuration Steps**:
1. Create new chart → Select **Sankey Diagram**
2. Dataset: `talent_pipeline_health_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Source Column**: `source`
6. **Target Column**: `target`
7. **Value Column**: `value`
8. **Color Scheme**: Default
9. Click **Run Query** → **Save**

**Note**: If Sankey is not available, use **Stacked Bar Chart** with `stage_id` on X-axis and `applications_count` stacked.

#### Chart 2.2: Stage Conversion Rates (Bar Chart)

**Chart Type**: **Bar Chart**  
**Dataset**: `talent_pipeline_health_gold`

**SQL Query**:
```sql
WITH stage_counts AS (
  SELECT
    stage_id,
    SUM(applications_count) as total_applications,
    AVG(stage_duration) as avg_duration
  FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_pipeline_health_gold
  WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
    AND stage_id IS NOT NULL
  GROUP BY stage_id
),
conversion_rates AS (
  SELECT
    sc.stage_id,
    sc.total_applications,
    sc.avg_duration,
    LEAD(sc.total_applications) OVER (ORDER BY sc.stage_id) as next_stage_applications,
    CASE 
      WHEN LEAD(sc.total_applications) OVER (ORDER BY sc.stage_id) IS NOT NULL
      THEN (LEAD(sc.total_applications) OVER (ORDER BY sc.stage_id) * 100.0 / sc.total_applications)
      ELSE NULL
    END as conversion_rate
  FROM stage_counts sc
)
SELECT
  stage_id,
  total_applications,
  avg_duration,
  conversion_rate
FROM conversion_rates
WHERE conversion_rate IS NOT NULL
ORDER BY stage_id
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `talent_pipeline_health_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `stage_id`
6. **Y Axis**: `conversion_rate`
7. **Color**: `avg_duration` (gradient: blue to red)
8. **Sort**: By `stage_id` (ascending)
9. **X Axis Label**: "Stage"
10. **Y Axis Label**: "Conversion Rate (%)"
11. Click **Run Query** → **Save**

#### Chart 2.3: Applications by Department (Treemap)

**Chart Type**: **Treemap**  
**Dataset**: `talent_pipeline_health_gold`

**SQL Query**:
```sql
SELECT
  department_id,
  SUM(applications_count) as total_applications,
  AVG(stage_duration) as avg_stage_duration
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_pipeline_health_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND department_id IS NOT NULL
GROUP BY department_id
ORDER BY total_applications DESC
```

**Configuration Steps**:
1. Create new chart → Select **Treemap**
2. Dataset: `talent_pipeline_health_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Group By**: `department_id`
6. **Metric**: `total_applications` (SUM)
7. **Color**: `avg_stage_duration` (gradient)
8. **Label**: `department_id`
9. Click **Run Query** → **Save**

### Section 3: Recruiter & Source Performance (Middle Right)

#### Chart 3.1: Recruiter Performance Matrix (Table)

**Chart Type**: **Table**  
**Dataset**: `talent_recruiter_performance_gold`

**SQL Query**:
```sql
SELECT
  first_name || ' ' || last_name as recruiter_name,
  user_id,
  SUM(applications_handled) as total_applications,
  SUM(interviews_scheduled) as total_interviews,
  SUM(offers_extended) as total_offers,
  SUM(hires_made) as total_hires,
  CASE 
    WHEN SUM(applications_handled) > 0 
    THEN (SUM(hires_made) * 100.0 / SUM(applications_handled))
    ELSE 0
  END as hire_rate
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_recruiter_performance_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND user_id IS NOT NULL
GROUP BY user_id, first_name, last_name
HAVING SUM(applications_handled) > 0
ORDER BY hire_rate DESC
```

**Configuration Steps**:
1. Create new chart → Select **Table**
2. Dataset: `talent_recruiter_performance_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Columns**: All selected
6. **Formatting**:
   - `hire_rate`: Percentage (1 decimal)
   - `total_applications`, `total_interviews`, `total_offers`, `total_hires`: Integer
7. **Sort**: By `hire_rate` (descending)
8. **Pagination**: 25 rows per page
9. Click **Run Query** → **Save**

#### Chart 3.2: Source ROI Analysis (Bar Chart)

**Chart Type**: **Bar Chart**  
**Dataset**: `talent_source_effectiveness_gold`

**SQL Query**:
```sql
SELECT
  source,
  SUM(applications_count) as applications,
  AVG(conversion_rate) * 100 as avg_conversion_rate
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_source_effectiveness_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND source IS NOT NULL
GROUP BY source
HAVING SUM(applications_count) >= 10  -- Filter low-volume sources
ORDER BY avg_conversion_rate DESC
LIMIT 15
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `talent_source_effectiveness_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `source`
6. **Y Axis**: `avg_conversion_rate`
7. **Color**: `applications` (gradient: light to dark blue)
8. **Sort**: By `avg_conversion_rate` (descending)
9. **X Axis Label**: "Source"
10. **Y Axis Label**: "Conversion Rate (%)"
11. **Legend**: Show `applications` (secondary metric)
12. Click **Run Query** → **Save**

#### Chart 3.3: Source Volume vs Quality (Scatter Plot)

**Chart Type**: **Scatter Plot**  
**Dataset**: `talent_source_effectiveness_gold`

**SQL Query**:
```sql
SELECT
  source,
  SUM(applications_count) as applications,
  AVG(conversion_rate) * 100 as conversion_rate,
  SUM(hires_count) as hires
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_source_effectiveness_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND source IS NOT NULL
GROUP BY source
HAVING SUM(applications_count) >= 5
```

**Configuration Steps**:
1. Create new chart → Select **Scatter Plot**
2. Dataset: `talent_source_effectiveness_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `applications` (log scale)
6. **Y Axis**: `conversion_rate`
7. **Size**: `hires`
8. **Color**: `source` (categorical)
9. **Label**: `source`
10. **X Axis Label**: "Applications (log scale)"
11. **Y Axis Label**: "Conversion Rate (%)"
12. Click **Run Query** → **Save**

### Section 4: Candidate Experience & Journey (Bottom)

#### Chart 4.1: Candidate Journey Timeline (Timeline Chart)

**Chart Type**: **Time Series Bar Chart** (or **Gantt Chart** if available)  
**Dataset**: `talent_candidate_journey_gold`

**SQL Query**:
```sql
SELECT
  candidate_id,
  department_id,
  source,
  application_date,
  first_interview_date,
  offer_date,
  offer_accepted_date,
  application_to_interview_days,
  interview_to_offer_days,
  days_to_offer_acceptance,
  final_status
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_candidate_journey_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND final_status = 'hired'
  AND application_date IS NOT NULL
ORDER BY application_date DESC
LIMIT 100
```

**Configuration Steps**:
1. Create new chart → Select **Time Series Bar Chart**
2. Dataset: `talent_candidate_journey_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Time Column**: `application_date`
6. **Metrics**: 
   - `application_to_interview_days` (label: "App → Interview")
   - `interview_to_offer_days` (label: "Interview → Offer")
   - `days_to_offer_acceptance` (label: "Total Days")
7. **Group By**: `final_status`
8. **Stack**: Yes
9. **X Axis Label**: "Application Date"
10. **Y Axis Label**: "Days"
11. Click **Run Query** → **Save**

#### Chart 4.2: NPS Score Trends (Line Chart with Area)

**Chart Type**: **Time Series Line Chart**  
**Dataset**: `talent_candidate_nps_gold`

**SQL Query**:
```sql
SELECT
  DATE(analysis_date) as date,
  AVG(nps_score) as avg_nps,
  SUM(nps_responses_count) as total_responses,
  SUM(promoters_count) as promoters,
  SUM(passives_count) as passives,
  SUM(detractors_count) as detractors
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_candidate_nps_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 180 DAY
  AND nps_score IS NOT NULL
GROUP BY DATE(analysis_date)
ORDER BY date ASC
```

**Configuration Steps**:
1. Create new chart → Select **Time Series Line Chart**
2. Dataset: `talent_candidate_nps_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Time Column**: `date`
6. **Time Grain**: Day
7. **Metrics**: `avg_nps` (line), `total_responses` (area, secondary Y-axis)
8. **Breakdown**: None (or `source` for segmentation)
9. **X Axis Label**: "Date"
10. **Y Axis Label**: "NPS Score"
11. **Secondary Y-Axis**: `total_responses` (label: "Responses")
12. **Legend**: Show (position: Right)
13. Click **Run Query** → **Save**

#### Chart 4.3: Stage Transition Analysis (Network Graph)

**Chart Type**: **Sankey Diagram** (or **Network Graph** if available)  
**Dataset**: `talent_stage_transitions_gold`

**SQL Query**:
```sql
SELECT
  previous_stage_id as source,
  current_stage_id as target,
  COUNT(*) as transition_count,
  AVG(days_since_last_transition) as avg_days
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_stage_transitions_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND previous_stage_id IS NOT NULL
  AND current_stage_id IS NOT NULL
GROUP BY previous_stage_id, current_stage_id
ORDER BY transition_count DESC
```

**Configuration Steps**:
1. Create new chart → Select **Sankey Diagram**
2. Dataset: `talent_stage_transitions_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Source Column**: `source`
6. **Target Column**: `target`
7. **Value Column**: `transition_count`
8. **Color**: `avg_days` (gradient: green to red)
9. Click **Run Query** → **Save**

#### Chart 4.4: Drop-off Analysis (Bar Chart)

**Chart Type**: **Bar Chart**  
**Dataset**: `talent_stage_transitions_gold`

**SQL Query**:
```sql
SELECT
  current_stage_id,
  COUNT(*) as total_applications,
  SUM(CASE WHEN is_dropped_off THEN 1 ELSE 0 END) as dropped_off_count,
  SUM(CASE WHEN has_regression THEN 1 ELSE 0 END) as regression_count
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_stage_transitions_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND current_stage_id IS NOT NULL
GROUP BY current_stage_id
ORDER BY dropped_off_count DESC
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `talent_stage_transitions_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `current_stage_id`
6. **Y Axis**: `dropped_off_count` (primary), `regression_count` (secondary)
7. **Stack**: Yes
8. **Colors**: Red (drop-offs), Orange (regressions)
9. **X Axis Label**: "Current Stage"
10. **Y Axis Label**: "Count"
11. Click **Run Query** → **Save**

#### Chart 4.5: Interview Effectiveness (Pie + Bar Combo)

**Chart Type**: **Pie Chart** and **Bar Chart** (separate charts)  
**Dataset**: `talent_interview_metrics_gold`

**SQL Query for Pie Chart**:
```sql
SELECT
  COALESCE(interview_type, 'unknown') as interview_type,
  SUM(interviews_count) as total_interviews
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_interview_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
GROUP BY interview_type
ORDER BY total_interviews DESC
```

**SQL Query for Bar Chart**:
```sql
SELECT
  COALESCE(interview_type, 'unknown') as interview_type,
  AVG(interview_to_offer_ratio) * 100 as offer_rate
FROM nan_wl_workloads_data_lake_develop_gold_reporting.talent_interview_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND interview_to_offer_ratio IS NOT NULL
GROUP BY interview_type
ORDER BY offer_rate DESC
```

**Configuration Steps for Pie Chart**:
1. Create new chart → Select **Pie Chart**
2. Dataset: `talent_interview_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste first SQL query above
5. **Group By**: `interview_type`
6. **Metric**: `total_interviews` (SUM)
7. **Label**: `interview_type`
8. Click **Run Query** → **Save**

**Configuration Steps for Bar Chart**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `talent_interview_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste second SQL query above
5. **X Axis**: `interview_type`
6. **Y Axis**: `offer_rate`
7. **Sort**: By `offer_rate` (descending)
8. **X Axis Label**: "Interview Type"
9. **Y Axis Label**: "Offer Rate (%)"
10. Click **Run Query** → **Save**

### Talent Dashboard Filters Setup

#### Global Filters

1. Navigate to dashboard → **Edit Dashboard**
2. Click **+ Add Filter**
3. Create filters:

**Filter 1: Date Range**
- **Filter Type**: Time Range
- **Column**: `analysis_date`
- **Default**: Last 90 days
- **Apply to**: All charts

**Filter 2: Department**
- **Filter Type**: Select
- **Column**: `department_id`
- **Dataset**: `talent_pipeline_health_gold`
- **Apply to**: All charts

**Filter 3: Source**
- **Filter Type**: Select
- **Column**: `source`
- **Dataset**: `talent_source_effectiveness_gold`
- **Apply to**: Relevant charts

**Filter 4: Job**
- **Filter Type**: Select
- **Column**: `job_id`
- **Dataset**: `talent_pipeline_health_gold`
- **Apply to**: Relevant charts

---

## Technology Dashboard Implementation

### Dashboard Overview

**Dashboard Name**: `Technology Process Analytics Dashboard`  
**Layout**: 4 sections, responsive grid  
**Refresh**: Daily (after Gold jobs complete)

### Section 1: Executive Overview (Top Row)

#### Chart 1.1: Total Requests (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  SUM(request_count) as total_requests
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND request_count IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `total_requests`
6. **Format**: Integer
7. **Subheader**: "Last 30 days"
8. **Color**: Blue (#1890FF)
9. Click **Run Query** → **Save**

#### Chart 1.2: Average Resolution Days (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  AVG(avg_resolution_days) as avg_resolution
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND avg_resolution_days IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `avg_resolution`
6. **Format**: Number (decimals: 1)
7. **Suffix**: ` days`
8. **Subheader**: "Last 30 days"
9. **Color**: Green (#52C41A)
10. Click **Run Query** → **Save**

#### Chart 1.3: SLA Compliance Rate (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  AVG(sla_compliance_rate) * 100 as sla_compliance
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND sla_compliance_rate IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `sla_compliance`
6. **Format**: Number (decimals: 1)
7. **Suffix**: `%`
8. **Subheader**: "Last 30 days"
9. **Color**: Orange (#FA8C16)
10. Click **Run Query** → **Save**

#### Chart 1.4: External Satisfaction (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `technology_satisfaction_gold`

**SQL Query**:
```sql
SELECT
  AVG(external_satisfaction_score) as avg_csat
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_satisfaction_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND external_satisfaction_score IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `technology_satisfaction_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `avg_csat`
6. **Format**: Number (decimals: 1)
7. **Suffix**: `/10`
8. **Subheader**: "Last 30 days"
9. **Color**: Purple (#722ED1)
10. Click **Run Query** → **Save**

#### Chart 1.5: Internal Satisfaction (KPI Card)

**Chart Type**: **Big Number**  
**Dataset**: `technology_satisfaction_gold`

**SQL Query**:
```sql
SELECT
  AVG(internal_satisfaction_score) as avg_nps
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_satisfaction_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND internal_satisfaction_score IS NOT NULL
```

**Configuration Steps**:
1. Create new chart → Select **Big Number**
2. Dataset: `technology_satisfaction_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Metric**: `avg_nps`
6. **Format**: Number (decimals: 1)
7. **Suffix**: `/10`
8. **Subheader**: "Last 30 days"
9. **Color**: Cyan (#13C2C2)
10. Click **Run Query** → **Save**

#### Chart 1.6: Process Comparison (Grouped Bar Chart)

**Chart Type**: **Bar Chart**  
**Dataset**: `technology_process_metrics_gold`

**SQL Query**:
```sql
SELECT
  process_name,
  SUM(total_requests) as total_requests,
  SUM(resolved_requests) as resolved,
  AVG(avg_resolution_days) as avg_resolution
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_process_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND process_name IS NOT NULL
GROUP BY process_name
ORDER BY total_requests DESC
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `technology_process_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `process_name`
6. **Y Axis**: `total_requests`, `resolved`, `avg_resolution` (grouped bars)
7. **Group By**: Metrics (3 groups)
8. **X Axis Label**: "Process"
9. **Y Axis Label**: "Count / Days"
10. **Legend**: Show (position: Right)
11. Click **Run Query** → **Save**

#### Chart 1.7: Satisfaction Trends (Dual Axis Line Chart)

**Chart Type**: **Time Series Line Chart**  
**Dataset**: `technology_satisfaction_gold`

**SQL Query**:
```sql
SELECT
  DATE(analysis_date) as date,
  process_name,
  AVG(external_satisfaction_score) as external_sat,
  AVG(internal_satisfaction_score) as internal_sat
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_satisfaction_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 180 DAY
  AND external_satisfaction_score IS NOT NULL
GROUP BY DATE(analysis_date), process_name
ORDER BY date ASC
```

**Configuration Steps**:
1. Create new chart → Select **Time Series Line Chart**
2. Dataset: `technology_satisfaction_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Time Column**: `date`
6. **Time Grain**: Day
7. **Metrics**: `external_sat` (primary Y-axis), `internal_sat` (secondary Y-axis)
8. **Series**: `process_name`
9. **X Axis Label**: "Date"
10. **Y Axis Label**: "Satisfaction Score (0-10)"
11. **Legend**: Show (position: Right)
12. Click **Run Query** → **Save**

### Section 2: Request Metrics & Performance (Middle Left)

#### Chart 2.1: Request Volume by Process (Stacked Area Chart)

**Chart Type**: **Time Series Area Chart**  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  DATE(analysis_date) as date,
  process_name,
  SUM(request_count) as requests
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND process_name IS NOT NULL
GROUP BY DATE(analysis_date), process_name
ORDER BY date ASC
```

**Configuration Steps**:
1. Create new chart → Select **Time Series Area Chart**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Time Column**: `date`
6. **Time Grain**: Day
7. **Metrics**: `requests` (SUM)
8. **Stack**: `process_name`
9. **X Axis Label**: "Date"
10. **Y Axis Label**: "Request Count"
11. **Legend**: Show (position: Right)
12. Click **Run Query** → **Save**

#### Chart 2.2: Resolution Time Distribution (Box Plot)

**Chart Type**: **Box Plot** (or **Violin Plot** if available)  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  process_name,
  p50_resolution_days as median,
  p75_resolution_days as p75,
  p95_resolution_days as p95,
  avg_resolution_days as mean,
  stddev_resolution_days as stddev
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND process_name IS NOT NULL
  AND p50_resolution_days IS NOT NULL
GROUP BY process_name, p50_resolution_days, p75_resolution_days, p95_resolution_days, avg_resolution_days, stddev_resolution_days
```

**Configuration Steps**:
1. Create new chart → Select **Box Plot**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `process_name`
6. **Y Axis**: `median`, `p75`, `p95`, `mean`
7. **Whiskers**: Use `stddev` for error bars
8. **X Axis Label**: "Process"
9. **Y Axis Label**: "Resolution Days"
10. Click **Run Query** → **Save**

**Note**: If Box Plot is not available, use **Bar Chart** with error bars showing percentiles.

#### Chart 2.3: SLA Compliance Heatmap (Heatmap)

**Chart Type**: **Heatmap**  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  process_name,
  COALESCE(account, 'Unknown') as account,
  AVG(sla_compliance_rate) * 100 as compliance_rate
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND process_name IS NOT NULL
GROUP BY process_name, account
HAVING COUNT(*) >= 5  -- Filter low-volume combinations
ORDER BY process_name, compliance_rate DESC
```

**Configuration Steps**:
1. Create new chart → Select **Heatmap**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `process_name`
6. **Y Axis**: `account`
7. **Metric**: `compliance_rate`
8. **Color Scheme**: Green (high) to Red (low)
9. **X Axis Label**: "Process"
10. **Y Axis Label**: "Account"
11. Click **Run Query** → **Save**

#### Chart 2.4: Estimation Accuracy (Scatter Plot)

**Chart Type**: **Scatter Plot**  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  process_name,
  AVG(avg_time_estimate_hours) as avg_estimate,
  AVG(avg_time_logged_hours) as avg_logged,
  SUM(request_count) as request_count
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND process_name IS NOT NULL
  AND avg_time_estimate_hours IS NOT NULL
  AND avg_time_logged_hours IS NOT NULL
GROUP BY process_name
```

**Configuration Steps**:
1. Create new chart → Select **Scatter Plot**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `avg_estimate`
6. **Y Axis**: `avg_logged`
7. **Size**: `request_count`
8. **Color**: `process_name`
9. **Reference Line**: y = x (perfect estimation)
10. **X Axis Label**: "Average Estimate (hours)"
11. **Y Axis Label**: "Average Logged (hours)"
12. Click **Run Query** → **Save**

### Section 3: Team & Stakeholder Performance (Middle Right)

#### Chart 3.1: Team Performance Matrix (Table)

**Chart Type**: **Table**  
**Dataset**: `technology_team_performance_gold`

**SQL Query**:
```sql
SELECT
  team,
  process_name,
  SUM(total_requests) as total_requests,
  SUM(resolved_requests) as resolved,
  AVG(avg_resolution_days) as avg_resolution,
  AVG(sla_compliance_rate) * 100 as sla_compliance,
  AVG(avg_satisfaction_score) as satisfaction
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_team_performance_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND team IS NOT NULL
GROUP BY team, process_name
HAVING SUM(total_requests) > 0
ORDER BY avg_resolution ASC
```

**Configuration Steps**:
1. Create new chart → Select **Table**
2. Dataset: `technology_team_performance_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Columns**: All selected
6. **Formatting**:
   - `sla_compliance`: Percentage (1 decimal)
   - `satisfaction`: Number (1 decimal)
   - `avg_resolution`: Number (1 decimal)
   - `total_requests`, `resolved`: Integer
7. **Sort**: By `avg_resolution` (ascending)
8. **Pagination**: 25 rows per page
9. Click **Run Query** → **Save**

#### Chart 3.2: Team Throughput (Bar Chart)

**Chart Type**: **Bar Chart**  
**Dataset**: `technology_throughput_gold`

**SQL Query**:
```sql
SELECT
  team,
  process_name,
  AVG(throughput_rate) as avg_throughput
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_throughput_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND team IS NOT NULL
GROUP BY team, process_name
ORDER BY avg_throughput DESC
LIMIT 20
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `technology_throughput_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `team`
6. **Y Axis**: `avg_throughput`
7. **Color**: `process_name`
8. **Sort**: By `avg_throughput` (descending)
9. **X Axis Label**: "Team"
10. **Y Axis Label**: "Throughput (requests/day)"
11. **Legend**: Show (position: Right)
12. Click **Run Query** → **Save**

#### Chart 3.3: Stakeholder Area Demand (Treemap)

**Chart Type**: **Treemap**  
**Dataset**: `technology_stakeholder_metrics_gold`

**SQL Query**:
```sql
SELECT
  stakeholder_area,
  process_name,
  SUM(total_requests) as total_requests,
  AVG(avg_resolution_days) as avg_resolution
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_stakeholder_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND stakeholder_area IS NOT NULL
GROUP BY stakeholder_area, process_name
ORDER BY total_requests DESC
```

**Configuration Steps**:
1. Create new chart → Select **Treemap**
2. Dataset: `technology_stakeholder_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Group By**: `stakeholder_area`, `process_name`
6. **Metric**: `total_requests` (SUM)
7. **Color**: `avg_resolution` (gradient: green to red)
8. **Label**: `stakeholder_area`
9. Click **Run Query** → **Save**

#### Chart 3.4: Account Performance (Bar Chart)

**Chart Type**: **Bar Chart**  
**Dataset**: `technology_account_metrics_gold`

**SQL Query**:
```sql
SELECT
  account,
  process_name,
  SUM(total_requests) as total_requests,
  AVG(avg_resolution_days) as avg_resolution,
  AVG(avg_satisfaction_score) as satisfaction
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_account_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND account IS NOT NULL
GROUP BY account, process_name
ORDER BY total_requests DESC
LIMIT 20
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `technology_account_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `account`
6. **Y Axis**: `avg_resolution` (primary), `satisfaction` (secondary Y-axis)
7. **Size**: `total_requests` (bubble size)
8. **Color**: `process_name`
9. **Sort**: By `total_requests` (descending)
10. **X Axis Label**: "Account"
11. **Y Axis Label**: "Resolution Days / Satisfaction"
12. Click **Run Query** → **Save**

### Section 4: Process Deep Dive & Satisfaction (Bottom)

#### Chart 4.1: Status Flow Sankey (Sankey Diagram)

**Chart Type**: **Sankey Diagram**  
**Dataset**: `technology_request_status_flow_gold`

**SQL Query**:
```sql
SELECT
  from_status,
  to_status,
  SUM(transition_count) as transitions,
  AVG(avg_transition_days) as avg_days
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_status_flow_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND from_status IS NOT NULL
  AND to_status IS NOT NULL
GROUP BY from_status, to_status
ORDER BY transitions DESC
```

**Configuration Steps**:
1. Create new chart → Select **Sankey Diagram**
2. Dataset: `technology_request_status_flow_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Source Column**: `from_status`
6. **Target Column**: `to_status`
7. **Value Column**: `transitions`
8. **Color**: `avg_days` (gradient: green to red)
9. Click **Run Query** → **Save**

#### Chart 4.2: Satisfaction Segmentation (Stacked Bar)

**Chart Type**: **Bar Chart**  
**Dataset**: `technology_satisfaction_gold`

**SQL Query**:
```sql
SELECT
  process_name,
  COALESCE(account, 'Unknown') as account,
  SUM(external_promoters_count) as promoters,
  SUM(external_passives_count) as passives,
  SUM(external_detractors_count) as detractors
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_satisfaction_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAY
  AND process_name IS NOT NULL
GROUP BY process_name, account
ORDER BY process_name, (promoters + passives + detractors) DESC
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `technology_satisfaction_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `process_name` (or `account`)
6. **Y Axis**: `promoters`, `passives`, `detractors` (stacked)
7. **Stack**: Yes
8. **Colors**: Green (promoters), Yellow (passives), Red (detractors)
9. **X Axis Label**: "Process / Account"
10. **Y Axis Label**: "Count"
11. **Legend**: Show (position: Right)
12. Click **Run Query** → **Save**

#### Chart 4.3: R&D Practices Performance (Radar Chart)

**Chart Type**: **Radar Chart** (or **Spider Chart**)  
**Dataset**: `technology_rd_practices_metrics_gold`

**SQL Query**:
```sql
SELECT
  rd_practice,
  SUM(request_count) as requests,
  AVG(avg_resolution_days) as resolution_days,
  AVG(sla_compliance_rate) * 100 as sla_compliance,
  AVG(satisfaction_score) as satisfaction
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_rd_practices_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND rd_practice IS NOT NULL
GROUP BY rd_practice
ORDER BY requests DESC
```

**Configuration Steps**:
1. Create new chart → Select **Radar Chart**
2. Dataset: `technology_rd_practices_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **Group By**: `rd_practice`
6. **Metrics**: 
   - `requests` (normalized 0-100)
   - `resolution_days` (inverted, normalized)
   - `sla_compliance` (0-100)
   - `satisfaction` (normalized 0-100)
7. **Axes**: One per metric
8. **Legend**: Show
9. Click **Run Query** → **Save**

**Note**: If Radar Chart is not available, use **Bar Chart** with normalized metrics.

#### Chart 4.4: Architecture Responsibility Domain (Grouped Bar)

**Chart Type**: **Bar Chart**  
**Dataset**: `technology_request_metrics_gold`

**SQL Query**:
```sql
SELECT
  architecture_responsibility_domain,
  process_name,
  SUM(request_count) as requests,
  AVG(avg_resolution_days) as resolution_days
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND architecture_responsibility_domain IS NOT NULL
GROUP BY architecture_responsibility_domain, process_name
ORDER BY requests DESC
LIMIT 30
```

**Configuration Steps**:
1. Create new chart → Select **Bar Chart**
2. Dataset: `technology_request_metrics_gold`
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `architecture_responsibility_domain`
6. **Y Axis**: `requests` (primary), `resolution_days` (secondary Y-axis)
7. **Group**: `process_name`
8. **Sort**: By `requests` (descending)
9. **X Axis Label**: "Architecture Domain"
10. **Y Axis Label**: "Requests / Resolution Days"
11. **Legend**: Show (position: Right)
12. Click **Run Query** → **Save**

#### Chart 4.5: Satisfaction vs Resolution Time (Scatter Plot)

**Chart Type**: **Scatter Plot**  
**Dataset**: `technology_request_metrics_gold` + `technology_satisfaction_gold` (join)

**SQL Query**:
```sql
SELECT
  rm.process_name,
  rm.account,
  AVG(rm.avg_resolution_days) as resolution_days,
  AVG(s.external_satisfaction_score) as satisfaction,
  SUM(rm.request_count) as request_count
FROM nan_wl_workloads_data_lake_develop_gold_reporting.technology_request_metrics_gold rm
LEFT JOIN nan_wl_workloads_data_lake_develop_gold_reporting.technology_satisfaction_gold s
  ON rm.process_name = s.process_name
  AND rm.account = s.account
  AND rm.analysis_date = s.analysis_date
WHERE rm.analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND rm.process_name IS NOT NULL
  AND rm.account IS NOT NULL
GROUP BY rm.process_name, rm.account
HAVING AVG(s.external_satisfaction_score) IS NOT NULL
ORDER BY request_count DESC
```

**Configuration Steps**:
1. Create new chart → Select **Scatter Plot**
2. Dataset: `technology_request_metrics_gold` (or create custom SQL dataset)
3. **Query Mode**: Raw SQL
4. Paste SQL query above
5. **X Axis**: `resolution_days`
6. **Y Axis**: `satisfaction`
7. **Size**: `request_count`
8. **Color**: `process_name`
9. **Label**: `account`
10. **X Axis Label**: "Resolution Days"
11. **Y Axis Label**: "Satisfaction Score (0-10)"
12. Click **Run Query** → **Save**

### Technology Dashboard Filters Setup

#### Global Filters

1. Navigate to dashboard → **Edit Dashboard**
2. Click **+ Add Filter**
3. Create filters:

**Filter 1: Date Range**
- **Filter Type**: Time Range
- **Column**: `analysis_date`
- **Default**: Last 30 days
- **Apply to**: All charts

**Filter 2: Process**
- **Filter Type**: Select
- **Column**: `process_name`
- **Dataset**: `technology_process_metrics_gold`
- **Options**: Architecture, Studios, Modernization & Infrastructure
- **Apply to**: All charts

**Filter 3: Account**
- **Filter Type**: Select
- **Column**: `account`
- **Dataset**: `technology_account_metrics_gold`
- **Apply to**: Relevant charts

**Filter 4: Team**
- **Filter Type**: Select
- **Column**: `team`
- **Dataset**: `technology_team_performance_gold`
- **Apply to**: Relevant charts

**Filter 5: Stakeholder Area**
- **Filter Type**: Select
- **Column**: `stakeholder_area`
- **Dataset**: `technology_stakeholder_metrics_gold`
- **Apply to**: Relevant charts

---

### Technology Data Reference

This section documents important data characteristics discovered during pipeline analysis.

#### ClickUp Status Workflow

The ClickUp workspace uses the following status categories:

| Category | Status Values |
|----------|---------------|
| **Not Started** | Backlog, Ready to do |
| **Active** | Paused, In Progress, Ready for internal review |
| **Done** | Ready to communicate, Wont do |
| **Closed** | Closed |

**Important**: A task is considered "resolved" when it reaches `Ready to communicate`, `Wont do`, or `Closed` status. The Gold layer uses case-insensitive matching:

```python
lower(col("status")).isin(["closed", "ready to communicate", "wont do"])
```

#### Date Fields

The Bronze layer extracts multiple date fields from ClickUp:

| Field | Description | Typical Coverage |
|-------|-------------|------------------|
| `date_created` | When task was created | 100% |
| `date_updated` | Last update timestamp | 100% |
| `date_done` | When task was marked as done | ~94% |
| `date_closed` | When task reached "Closed" status | ~71% |
| `due_date` | Task deadline | Varies |

**Note**: `resolution_days` is calculated using `date_done` (not `date_closed`) because business considers a task "DONE" as soon as it can be communicated, not necessarily when it's fully closed.

#### Resolution Metrics Baseline

Based on current data analysis:

| Metric | Value |
|--------|-------|
| Average resolution time | 31.77 days |
| Tasks with resolution data | 94% |
| P50 resolution days | ~21 days |
| P75 resolution days | ~45 days |

#### CSAT Scores Reference

Customer Satisfaction (CSAT) scores from ClickUp custom fields:

| Metric | Value |
|--------|-------|
| Scale | 4-10 |
| Average external CSAT | 9.17 |
| Average internal CSAT | 5.89 |
| Tasks with external score | ~60% |
| Score distribution | 69% rated 10 |

**CSAT by Stakeholder Area** (top performers):
- Quality, Talent, Administration, Operations: 10.0
- Marketing: 9.56
- Engineering: 9.40
- Technology: 9.17

**Note**: When querying CSAT, always filter `WHERE external_satisfaction_score IS NOT NULL` to avoid skewed averages.

#### NPS Calculation and Data Model

**Understanding the satisfaction fields:**

| Field | Evaluator | Meaning |
|-------|-----------|---------|
| `internal_satisfaction_score` | Team members | How valuable/useful was working on this task |
| `external_satisfaction_score` | Requesters | Client satisfaction with the delivered result |

**Important Data Model Consideration:**

Each task has:
- ONE internal satisfaction score (collective team evaluation)
- ONE external satisfaction score (requester evaluation)
- MULTIPLE team members (`🫂 Team` custom field)
- MULTIPLE requesters (`🙋‍♂️ Requesters` custom field)

**⚠️ NPS Inflation Warning:**

The `technology_satisfaction_gold` table explodes `team_members`, `stakeholder_areas`, and `account` for multi-dimensional analysis. This causes count inflation:

| Table | Counting Method | Use Case |
|-------|-----------------|----------|
| `technology_satisfaction_gold` | Inflated (1 task counted per team member) | Multi-dimensional analysis by team/stakeholder |
| `technology_process_nps_gold` | Accurate (1 task = 1 vote) | **Executive NPS reporting** |

**Recommended queries for accurate NPS:**

```sql
-- Accurate process-level NPS (use technology_process_nps_gold)
SELECT
    process_name,
    internal_response_count as tasks_with_score,
    internal_promoters,
    internal_passives,
    internal_detractors,
    ROUND(internal_nps, 1) as internal_nps
FROM nan_gold_zone.technology_process_nps_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
ORDER BY process_name;
```

**NPS Segments:**
- **Promoters**: Score 9-10 (very satisfied)
- **Passives**: Score 7-8 (satisfied but not enthusiastic)
- **Detractors**: Score 0-6 (unsatisfied)
- **NPS Formula**: `((Promoters - Detractors) / Total) * 100`
- **NPS Range**: -100 (all detractors) to +100 (all promoters)

#### SLA Compliance

SLA compliance is calculated based on whether tasks are completed within their due date:

```sql
sla_compliant = CASE 
  WHEN date_done IS NOT NULL AND due_date IS NOT NULL 
  THEN date_done <= due_date 
  ELSE NULL 
END
```

Tasks without due dates are excluded from SLA calculations.

---

## Advanced Configuration

### Cross-Filtering Setup

Enable cross-filtering between related charts:

1. **Talent Dashboard**:
   - Click on department in Treemap → Filters all charts
   - Click on source in Source ROI → Filters recruiter performance
   - Click on stage in Pipeline Health → Filters stage transitions

2. **Technology Dashboard**:
   - Click on process in Process Comparison → Filters all charts
   - Click on account in Account Performance → Filters satisfaction charts
   - Click on team in Team Performance → Filters throughput charts

**Configuration Steps**:
1. Edit dashboard
2. Select chart → **Advanced** tab
3. Enable **Cross-filtering**
4. Select which charts should respond to this chart's selections
5. Save

### Refresh Schedules

Configure automatic refresh for dashboards:

1. Navigate to dashboard → **Settings** → **Auto-refresh**
2. Enable **Auto-refresh**
3. Set interval: **1 hour** (or **Daily** after Gold jobs complete)
4. Save

**Recommended Schedule**:
- **Talent Dashboard**: Refresh daily at 9:00 AM (after Gold jobs complete at 8:00 AM)
- **Technology Dashboard**: Refresh daily at 9:00 AM

### Caching Configuration

Optimize query performance with caching:

1. Navigate to **Settings** → **Cache**
2. Configure cache settings:
   - **Cache Timeout**: 3600 seconds (1 hour)
   - **Cache Key**: Include `analysis_date` filter
   - **Cache Type**: Redis (if available) or in-memory

**Per-Chart Cache**:
1. Edit chart → **Advanced** tab
2. Enable **Cache**
3. Set **Cache Timeout**: 3600 seconds
4. Save

### Performance Tuning

#### Query Optimization Tips

1. **Always filter by `analysis_date` first**:
   ```sql
   WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAY
   ```

2. **Use partition pruning**:
   - Iceberg tables are partitioned by `analysis_date`
   - Filtering by date improves query performance significantly

3. **Limit result sets**:
   ```sql
   LIMIT 100  -- For detail charts
   ```

4. **Aggregate at database level**:
   - Use `SUM()`, `AVG()`, `COUNT()` in SQL
   - Avoid aggregating large datasets in Superset

5. **Use materialized views** (if needed):
   - Create daily summary tables for frequently accessed metrics
   - Refresh materialized views after Gold jobs complete

#### Dashboard Performance

1. **Limit concurrent queries**: Configure Superset to limit concurrent queries per user
2. **Use async queries**: Enable async query execution for long-running queries
3. **Optimize chart count**: Keep dashboards under 20 charts for optimal performance
4. **Lazy load**: Enable lazy loading for charts below the fold

---

## Troubleshooting & Best Practices

### Common Issues

#### Issue 1: "No data returned" Error

**Symptoms**: Chart shows "No data" even though data exists in table

**Solutions**:
1. Check date filter: Ensure `analysis_date` filter includes recent dates
2. Verify NULL handling: Check if required columns have NULL values
3. Test SQL directly: Run query in Athena console to verify results
4. Check permissions: Verify user has access to database/table

#### Issue 2: Slow Query Performance

**Symptoms**: Charts take >30 seconds to load

**Solutions**:
1. Add date filter: Always filter by `analysis_date` to use partition pruning
2. Reduce date range: Use shorter time periods (30 days vs 180 days)
3. Add LIMIT clause: Limit result sets for detail charts
4. Check query plan: Use EXPLAIN in Athena to identify bottlenecks
5. Consider materialized views: Pre-aggregate data for frequently accessed metrics

#### Issue 3: Incorrect Aggregations

**Symptoms**: Chart shows wrong totals or averages

**Solutions**:
1. Check aggregation functions: Use `SUM()` for counts, `AVG()` for averages
2. Verify GROUP BY: Ensure all non-aggregated columns are in GROUP BY
3. Check NULL handling: Use `COALESCE()` or `CASE WHEN` for NULL values
4. Review metric definitions: Verify metric calculations in Superset

#### Issue 4: Filter Not Working

**Symptoms**: Dashboard filter doesn't affect charts

**Solutions**:
1. Verify column names: Ensure filter column matches chart dataset column
2. Check filter scope: Verify filter is applied to correct charts
3. Test filter values: Ensure filter has valid values (not all NULL)
4. Refresh dashboard: Clear cache and refresh

### Best Practices

#### 1. Dataset Management

- **Create datasets once**: Create datasets for all Gold tables upfront
- **Use consistent naming**: Follow naming convention: `{domain}_{table}_gold`
- **Document datasets**: Add descriptions to datasets explaining data source and update frequency
- **Set temporal columns**: Mark `analysis_date` as temporal dimension for time-based filtering

#### 2. Chart Design

- **Use appropriate chart types**: Match chart type to data (time series → line chart, categories → bar chart)
- **Limit colors**: Use 5-7 colors maximum per chart
- **Add labels**: Always include axis labels and chart titles
- **Use tooltips**: Enable tooltips with relevant information
- **Responsive design**: Ensure charts work on different screen sizes

#### 3. SQL Query Best Practices

- **Always filter by date**: Use `analysis_date >= CURRENT_DATE - INTERVAL N DAY`
- **Handle NULLs**: Use `COALESCE()` or `CASE WHEN` for NULL handling
- **Use appropriate aggregations**: `SUM()` for counts, `AVG()` for averages
- **Add comments**: Document complex queries with comments
- **Test queries**: Run queries in Athena console before creating charts
- **Optimize joins**: Use LEFT JOIN when possible, avoid CROSS JOIN

#### 4. Dashboard Organization

- **Logical grouping**: Group related charts together
- **Progressive disclosure**: Show summary first, details on demand
- **Consistent filters**: Use same filters across related charts
- **Clear titles**: Use descriptive titles for sections and charts
- **Responsive layout**: Use grid layout that adapts to screen size

#### 5. Performance Optimization

- **Cache aggressively**: Enable caching for frequently accessed charts
- **Limit date ranges**: Use shorter default date ranges (30-90 days)
- **Pre-aggregate**: Use Gold layer tables (already aggregated) instead of Silver/Bronze
- **Monitor query times**: Track query performance and optimize slow queries
- **Use async queries**: Enable async execution for long-running queries

#### 6. Maintenance

- **Regular updates**: Ensure Gold tables are updated daily
- **Monitor data quality**: Check for missing dates or NULL values
- **Review dashboards**: Periodically review and update dashboards based on user feedback
- **Document changes**: Document any changes to queries or chart configurations
- **Version control**: Export dashboard JSON for version control

### Dashboard Export/Import

#### Export Dashboard

1. Navigate to dashboard → **Settings** → **Export**
2. Download JSON file
3. Store in version control

#### Import Dashboard

1. Navigate to **Dashboards** → **+ Dashboard** → **Import**
2. Upload JSON file
3. Verify datasets are available
4. Test all charts

### Security Considerations

1. **Row-level security**: Implement row-level security if needed (filter by user/team)
2. **Column-level security**: Hide sensitive columns from certain users
3. **Dashboard permissions**: Restrict dashboard access to authorized users
4. **Data masking**: Mask sensitive data (e.g., email addresses) if required
5. **Audit logging**: Enable audit logging for dashboard access

---

## Conclusion

This guide provides comprehensive instructions for implementing Talent and Technology dashboards in Apache Superset. Follow the step-by-step instructions for each chart, configure filters and cross-filtering, and optimize for performance.

For questions or issues, refer to the Troubleshooting section or consult the Superset documentation.

**Last Updated**: 2025-06-04  
**Superset Version**: 3.0+  
**Data Source**: AWS Athena with Iceberg tables

