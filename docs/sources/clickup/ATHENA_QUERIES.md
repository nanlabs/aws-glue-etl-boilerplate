# ClickUp - Athena Query Examples

**Reading Time**: ~15 minutes

## Overview

This document provides practical Athena SQL queries for analyzing ClickUp Technology Requests data across the Medallion Architecture layers (Raw, Bronze, Silver, Gold). Each query includes a description of insights you can obtain and example use cases aligned with Technology process KPIs.

**Database Names** (adjust based on environment):

* **Raw**: `nan_wl_workloads_data_lake_staging_raw_zone_ingestion`
* **Bronze**: `nan_wl_workloads_data_lake_staging_bronze_processing`
* **Silver**: `nan_wl_workloads_data_lake_staging_silver_analytics`
* **Gold**: `nan_wl_workloads_data_lake_staging_gold_reporting`

**Table Names**:

* **Raw**: `clickup__technology__requests_raw`
* **Bronze**: `clickup__technology__requests_bronze`
* **Silver**: `technology_dim_requests`, `technology_dim_stakeholder_areas`, `technology_dim_teams`, `technology_fact_request_events`
* **Gold**: `technology_request_metrics_gold`, `technology_satisfaction_gold`, `technology_throughput_gold`, `technology_request_status_flow_gold`, `technology_team_performance_gold`, `technology_stakeholder_metrics_gold`, `technology_process_metrics_gold`, `technology_account_metrics_gold`, `technology_rd_practices_metrics_gold`

---

## RAW Layer Queries

The Raw layer contains unprocessed JSONL data exactly as received from ClickUp API v2, partitioned by ingestion date.

### Query 1: Check Data Availability by Month

**Description**: Verify which months of data have been ingested and count records per partition.

**Use Case**: Data quality audit, identify missing data periods, validate ingestion completeness.

```sql
-- Check data availability and record counts by partition
SELECT
    year,
    month,
    COUNT(*) as record_count,
    MIN(task_id) as sample_task_id,
    MAX(ingestion_ts) as latest_ingestion
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.clickup__technology__requests_raw
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
-- Sample raw task records from most recent partition
SELECT
    task_id,
    task_name,
    status,
    priority,
    list_id,
    date_created,
    date_updated,
    year,
    month,
    ingestion_ts
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.clickup__technology__requests_raw
WHERE year = 2025 AND month = 12
LIMIT 20;
```

**Expected Output**: Sample of raw JSON data showing task information and metadata.

---

### Query 3: Identify Recent Activity by Process List

**Description**: Find tasks with recent activity across different Technology process lists.

**Use Case**: Monitor data freshness, identify active periods, validate API connectivity.

```sql
-- Recent tasks activity by list
SELECT
    list_id,
    COUNT(*) as task_count,
    COUNT(DISTINCT status) as unique_statuses,
    MIN(date_created) as earliest_task,
    MAX(date_updated) as latest_update
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.clickup__technology__requests_raw
WHERE year = 2025 AND month = 12
GROUP BY list_id
ORDER BY task_count DESC;
```

**Expected Output**: Task counts by list ID showing activity distribution across Technology processes.

---

### Query 4: Task Status Distribution

**Description**: Analyze task status distribution to understand workflow state.

**Use Case**: Workflow monitoring, identify status patterns, validate status values.

```sql
-- Task status distribution
SELECT
    status,
    COUNT(*) as task_count,
    COUNT(DISTINCT list_id) as unique_lists,
    MIN(date_created) as earliest_task,
    MAX(date_updated) as latest_update
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.clickup__technology__requests_raw
WHERE year = 2025 AND month = 12
GROUP BY status
ORDER BY task_count DESC;
```

**Expected Output**: Task counts by status showing workflow distribution.

---

## BRONZE Layer Queries

The Bronze layer contains structured data with parsed fields, calculated metrics, and enriched metadata. This is ideal for data exploration and quality analysis.

### Query 5: Data Quality Overview

**Description**: Analyze request data quality by examining completeness of key fields.

**Use Case**: Data quality monitoring, identify problematic records, prioritize data cleanup.

```sql
-- Data quality overview for requests
SELECT
    CASE
        WHEN task_name IS NOT NULL
         AND status IS NOT NULL
         AND priority IS NOT NULL
         AND assignee_email IS NOT NULL THEN 'Complete'
        WHEN task_name IS NOT NULL
         AND status IS NOT NULL
         AND priority IS NOT NULL THEN 'Missing Assignee'
        WHEN task_name IS NOT NULL
         AND status IS NOT NULL THEN 'Missing Priority/Assignee'
        ELSE 'Incomplete'
    END as quality_tier,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    COUNT(CASE WHEN custom_fields_json IS NOT NULL THEN 1 END) as with_custom_fields,
    COUNT(CASE WHEN tags_json IS NOT NULL THEN 1 END) as with_tags,
    COUNT(CASE WHEN watchers_json IS NOT NULL THEN 1 END) as with_watchers
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year = 2025 AND month = 12
GROUP BY
    CASE
        WHEN task_name IS NOT NULL
         AND status IS NOT NULL
         AND priority IS NOT NULL
         AND assignee_email IS NOT NULL THEN 'Complete'
        WHEN task_name IS NOT NULL
         AND status IS NOT NULL
         AND priority IS NOT NULL THEN 'Missing Assignee'
        WHEN task_name IS NOT NULL
         AND status IS NOT NULL THEN 'Missing Priority/Assignee'
        ELSE 'Incomplete'
    END
ORDER BY record_count DESC;
```

**Expected Output**: Distribution of records by quality tier with metrics for each tier.

---

### Query 6: Process List Analysis

**Description**: Analyze requests by Technology process list (Architecture, Studios, Modernization & Infrastructure).

**Use Case**: Process-level analysis, identify process-specific patterns, track request volume by process.

```sql
-- Request analysis by Technology process list
SELECT
    list_id,
    list_name,
    folder_name,
    COUNT(*) as request_count,
    COUNT(DISTINCT status) as unique_statuses,
    COUNT(DISTINCT priority) as unique_priorities,
    COUNT(CASE WHEN date_closed IS NOT NULL THEN 1 END) as closed_count,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(time_logged_hours), 1) as avg_time_logged_hours,
    ROUND(AVG(time_estimate_hours), 1) as avg_time_estimate_hours,
    SUM(CASE WHEN sla_compliant = true THEN 1 ELSE 0 END) as sla_compliant_count
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year = 2025 AND month = 12
GROUP BY list_id, list_name, folder_name
ORDER BY request_count DESC;
```

**Expected Output**: Process-level metrics showing request volume, resolution times, and SLA compliance.

---

### Query 7: Priority Distribution Analysis

**Description**: Analyze request priority distribution and resolution patterns by priority.

**Use Case**: Priority management, identify high-priority bottlenecks, track priority-based resolution times.

```sql
-- Priority distribution and resolution analysis
SELECT
    priority,
    COUNT(*) as request_count,
    COUNT(CASE WHEN date_closed IS NOT NULL THEN 1 END) as resolved_count,
    ROUND(COUNT(CASE WHEN date_closed IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as resolution_rate_pct,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(time_logged_hours), 1) as avg_time_logged_hours,
    SUM(CASE WHEN sla_compliant = true THEN 1 ELSE 0 END) as sla_compliant_count,
    ROUND(SUM(CASE WHEN sla_compliant = true THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(CASE WHEN date_closed IS NOT NULL THEN 1 END), 0), 2) as sla_compliance_rate_pct
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year = 2025 AND month = 12
GROUP BY priority
ORDER BY
    CASE priority
        WHEN 'urgent' THEN 1
        WHEN 'high' THEN 2
        WHEN 'normal' THEN 3
        WHEN 'low' THEN 4
        ELSE 5
    END;
```

**Expected Output**: Priority-level metrics showing resolution rates, average times, and SLA compliance.

---

### Query 8: Status Distribution Analysis

**Description**: Analyze request status distribution to understand workflow flow.

**Use Case**: Workflow monitoring, identify bottlenecks, track status transitions.

```sql
-- Status distribution analysis
SELECT
    status,
    COUNT(*) as request_count,
    COUNT(DISTINCT list_id) as unique_lists,
    COUNT(DISTINCT priority) as unique_priorities,
    COUNT(CASE WHEN date_closed IS NOT NULL THEN 1 END) as closed_count,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days,
    MIN(date_created) as earliest_request,
    MAX(date_updated) as latest_update
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year = 2025 AND month = 12
GROUP BY status
ORDER BY request_count DESC;
```

**Expected Output**: Status-level metrics showing request distribution and resolution patterns.

---

### Query 9: Time Tracking Analysis

**Description**: Analyze time tracking patterns including logged vs estimated time.

**Use Case**: Time estimation accuracy, identify estimation issues, track time efficiency.

```sql
-- Time tracking analysis
SELECT
    list_id,
    list_name,
    COUNT(*) as request_count,
    COUNT(CASE WHEN time_logged_hours IS NOT NULL THEN 1 END) as with_time_logged,
    COUNT(CASE WHEN time_estimate_hours IS NOT NULL THEN 1 END) as with_time_estimate,
    ROUND(AVG(time_logged_hours), 1) as avg_time_logged_hours,
    ROUND(AVG(time_estimate_hours), 1) as avg_time_estimate_hours,
    ROUND(AVG(time_logged_hours / NULLIF(time_estimate_hours, 0)), 2) as avg_estimate_accuracy,
    COUNT(CASE WHEN time_logged_hours > time_estimate_hours * 1.5 THEN 1 END) as over_estimated_count,
    COUNT(CASE WHEN time_logged_hours < time_estimate_hours * 0.5 THEN 1 END) as under_estimated_count
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year = 2025 AND month = 12
  AND date_closed IS NOT NULL
GROUP BY list_id, list_name
ORDER BY request_count DESC;
```

**Expected Output**: Time tracking metrics showing estimation accuracy and patterns by process.

---

### Query 10: Monthly Activity Trends

**Description**: Track request activity trends over time to identify seasonal patterns.

**Use Case**: Seasonal pattern analysis, capacity planning, trend identification.

```sql
-- Monthly activity trends for requests
SELECT
    year,
    month,
    COUNT(*) as request_count,
    COUNT(DISTINCT list_id) as unique_lists,
    COUNT(DISTINCT status) as unique_statuses,
    COUNT(CASE WHEN date_closed IS NOT NULL THEN 1 END) as resolved_count,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(time_logged_hours), 1) as avg_time_logged_hours
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year >= 2025
GROUP BY year, month
ORDER BY year DESC, month DESC
LIMIT 24;
```

**Expected Output**: Monthly aggregated request metrics showing trends over the last 24 months.

---

### Query 11: SLA Compliance Analysis

**Description**: Analyze SLA compliance rates by process, priority, and status.

**Use Case**: SLA monitoring, identify compliance issues, track performance against deadlines.

```sql
-- SLA compliance analysis
SELECT
    list_id,
    list_name,
    priority,
    COUNT(*) as total_requests,
    COUNT(CASE WHEN date_closed IS NOT NULL AND due_date IS NOT NULL THEN 1 END) as with_sla_data,
    SUM(CASE WHEN sla_compliant = true THEN 1 ELSE 0 END) as sla_compliant_count,
    ROUND(SUM(CASE WHEN sla_compliant = true THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(CASE WHEN date_closed IS NOT NULL AND due_date IS NOT NULL THEN 1 END), 0), 2) as sla_compliance_rate_pct,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year = 2025 AND month = 12
  AND date_closed IS NOT NULL
GROUP BY list_id, list_name, priority
HAVING COUNT(CASE WHEN date_closed IS NOT NULL AND due_date IS NOT NULL THEN 1 END) > 0
ORDER BY list_name,
    CASE priority
        WHEN 'urgent' THEN 1
        WHEN 'high' THEN 2
        WHEN 'normal' THEN 3
        WHEN 'low' THEN 4
        ELSE 5
    END;
```

**Expected Output**: SLA compliance metrics by process and priority showing performance against deadlines.

---

## SILVER Layer Queries

The Silver layer contains clean, business-ready tables optimized for Technology analytics with data quality validations and enrichment.

### Query 12: Request Dimension Master List

**Description**: Retrieve canonical request information with standardized keys and attributes.

**Use Case**: Master data for joins, request lookup, analytics preparation.

```sql
-- Request dimension with latest attributes
SELECT
    request_id,
    request_name,
    status,
    priority,
    list_id,
    list_name,
    folder_name,
    account,
    stakeholder_areas,
    team_members,
    architecture_responsibility_domain,
    rd_practices,
    external_satisfaction_score,
    internal_satisfaction_score,
    resolution_days,
    sla_compliant,
    completeness_score,
    urgency_score,
    days_in_backlog,
    days_in_progress,
    days_in_review,
    date_created,
    date_updated,
    date_closed,
    ingestion_ts
FROM nan_wl_workloads_data_lake_staging_silver_analytics.technology_dim_requests
WHERE completeness_score >= 0.7
ORDER BY date_updated DESC
LIMIT 100;
```

**Expected Output**: Clean request master list with standardized fields and quality metrics.

---

### Query 13: Stakeholder Areas Analysis

**Description**: Analyze requests by stakeholder area to understand demand patterns.

**Use Case**: Stakeholder demand analysis, identify high-demand areas, track request distribution.

```sql
-- Stakeholder areas analysis
SELECT
    stakeholder_area,
    COUNT(DISTINCT request_id) as request_count,
    COUNT(DISTINCT list_id) as unique_lists,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(external_satisfaction_score), 1) as avg_external_satisfaction,
    ROUND(AVG(internal_satisfaction_score), 1) as avg_internal_satisfaction,
    ROUND(AVG(completeness_score), 2) as avg_completeness_score
FROM nan_wl_workloads_data_lake_staging_silver_analytics.technology_dim_stakeholder_areas sa
JOIN nan_wl_workloads_data_lake_staging_silver_analytics.technology_dim_requests r
    ON sa.request_id = r.request_id
WHERE sa.year = 2025 AND sa.month = 12
GROUP BY stakeholder_area
ORDER BY request_count DESC
LIMIT 20;
```

**Expected Output**: Top 20 stakeholder areas ranked by request volume with quality metrics.

---

### Query 14: Team Performance Analysis

**Description**: Analyze team performance metrics including request handling and satisfaction.

**Use Case**: Team productivity tracking, identify top-performing teams, workload distribution.

```sql
-- Team performance analysis
SELECT
    team_member,
    COUNT(DISTINCT request_id) as request_count,
    COUNT(DISTINCT list_id) as unique_lists,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(time_logged_hours), 1) as avg_time_logged_hours,
    ROUND(AVG(external_satisfaction_score), 1) as avg_external_satisfaction,
    ROUND(AVG(internal_satisfaction_score), 1) as avg_internal_satisfaction,
    SUM(CASE WHEN sla_compliant = true THEN 1 ELSE 0 END) as sla_compliant_count
FROM nan_wl_workloads_data_lake_staging_silver_analytics.technology_dim_teams t
JOIN nan_wl_workloads_data_lake_staging_silver_analytics.technology_dim_requests r
    ON t.request_id = r.request_id
WHERE t.year = 2025 AND t.month = 12
GROUP BY team_member
ORDER BY request_count DESC
LIMIT 30;
```

**Expected Output**: Top 30 team members ranked by request volume with performance metrics.

---

### Query 15: Request Events Timeline

**Description**: Analyze request events over time to understand lifecycle patterns.

**Use Case**: Request lifecycle analysis, identify event patterns, track status changes.

```sql
-- Request events timeline analysis
SELECT
    event_type,
    status,
    COUNT(*) as event_count,
    COUNT(DISTINCT request_id) as unique_requests,
    MIN(event_date) as earliest_event,
    MAX(event_date) as latest_event,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(external_satisfaction_score), 1) as avg_external_satisfaction
FROM nan_wl_workloads_data_lake_staging_silver_analytics.technology_fact_request_events
WHERE year = 2025 AND month = 12
GROUP BY event_type, status
ORDER BY event_count DESC;
```

**Expected Output**: Event metrics by type and status showing lifecycle patterns.

---

### Query 16: Data Quality Metrics

**Description**: Analyze data quality scores and completeness metrics.

**Use Case**: Data quality monitoring, identify quality trends, track improvement over time.

```sql
-- Data quality metrics analysis
SELECT
    CASE
        WHEN completeness_score >= 0.9 THEN 'Excellent (≥0.9)'
        WHEN completeness_score >= 0.8 THEN 'Good (0.8-0.89)'
        WHEN completeness_score >= 0.6 THEN 'Fair (0.6-0.79)'
        WHEN completeness_score >= 0.4 THEN 'Poor (0.4-0.59)'
        ELSE 'Critical (<0.4)'
    END as quality_tier,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(completeness_score), 3) as avg_completeness_score,
    ROUND(AVG(urgency_score), 3) as avg_urgency_score,
    ROUND(AVG(resolution_days), 1) as avg_resolution_days
FROM nan_wl_workloads_data_lake_staging_silver_analytics.technology_dim_requests
WHERE year = 2025 AND month = 12
GROUP BY
    CASE
        WHEN completeness_score >= 0.9 THEN 'Excellent (≥0.9)'
        WHEN completeness_score >= 0.8 THEN 'Good (0.8-0.89)'
        WHEN completeness_score >= 0.6 THEN 'Fair (0.6-0.79)'
        WHEN completeness_score >= 0.4 THEN 'Poor (0.4-0.59)'
        ELSE 'Critical (<0.4)'
    END
ORDER BY quality_tier;
```

**Expected Output**: Distribution of records by quality tier with metrics for each tier.

---

## GOLD Layer Queries

The Gold layer contains analytics-ready tables with pre-aggregated KPIs and denormalized structures for BI and reporting. Each table is partitioned for efficient querying.

### Query 17: Request Metrics Overview (Core KPI)

**Description**: Analyze aggregated request metrics by process, account, and architecture responsibility domain.

**Use Case**: Executive dashboards, process-level KPIs, account performance tracking.

```sql
-- Request metrics by Technology process (last 90 days)
SELECT
    process_name,
    COUNT(DISTINCT analysis_date) as days_with_data,
    SUM(request_count) as total_requests,
    SUM(resolved_count) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(p50_resolution_days), 1) as p50_resolution_days,
    ROUND(AVG(p95_resolution_days), 1) as p95_resolution_days,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_compliance_rate,
    ROUND(AVG(avg_external_satisfaction_score), 1) as avg_external_satisfaction,
    ROUND(AVG(avg_internal_satisfaction_score), 1) as avg_internal_satisfaction,
    ROUND(AVG(avg_completeness_score), 2) as avg_completeness_score,
    ROUND(AVG(avg_urgency_score), 2) as avg_urgency_score
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY process_name
ORDER BY total_requests DESC;
```

**Expected Output**: Process-level aggregated metrics showing volume, resolution times, SLA compliance, and satisfaction.

---

### Query 18: Request Metrics by Account

**Description**: Analyze request metrics segmented by account/client.

**Use Case**: Account-level performance tracking, identify high-value accounts, account-specific insights.

```sql
-- Request metrics by account (last 90 days)
SELECT
    account,
    process_name,
    SUM(request_count) as total_requests,
    SUM(resolved_count) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(p95_resolution_days), 1) as p95_resolution_days,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_compliance_rate,
    ROUND(AVG(avg_external_satisfaction_score), 1) as avg_external_satisfaction,
    ROUND(AVG(avg_internal_satisfaction_score), 1) as avg_internal_satisfaction
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND account IS NOT NULL
GROUP BY account, process_name
HAVING SUM(request_count) >= 5  -- Filter for accounts with meaningful volume
ORDER BY total_requests DESC
LIMIT 50;
```

**Expected Output**: Top 50 accounts ranked by request volume with performance metrics.

---

### Query 19: Request Metrics by Architecture Responsibility Domain

**Description**: Analyze request metrics segmented by architecture responsibility domain.

**Use Case**: Domain-level analysis, identify high-demand domains, track domain-specific performance.

```sql
-- Request metrics by architecture responsibility domain
SELECT
    architecture_responsibility_domain,
    process_name,
    SUM(request_count) as total_requests,
    SUM(resolved_count) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(p95_resolution_days), 1) as p95_resolution_days,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_compliance_rate,
    ROUND(AVG(avg_external_satisfaction_score), 1) as avg_external_satisfaction
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND architecture_responsibility_domain IS NOT NULL
GROUP BY architecture_responsibility_domain, process_name
HAVING SUM(request_count) >= 3
ORDER BY total_requests DESC
LIMIT 30;
```

**Expected Output**: Top 30 architecture responsibility domains ranked by request volume.

---

### Query 20: Satisfaction Metrics Analysis

**Description**: Analyze satisfaction metrics (CSAT/NPS) by process, account, and architecture responsibility domain.

**Use Case**: Customer satisfaction tracking, identify satisfaction trends, track NPS scores.

```sql
-- Satisfaction metrics by Technology process (last 90 days)
SELECT
    process_name,
    account,
    SUM(satisfaction_response_count) as total_responses,
    ROUND(AVG(external_satisfaction_score), 1) as avg_external_satisfaction,
    ROUND(AVG(internal_satisfaction_score), 1) as avg_internal_satisfaction,
    ROUND(AVG(external_nps_score), 1) as avg_external_nps,
    ROUND(AVG(internal_nps_score), 1) as avg_internal_nps,
    SUM(external_promoters_count) as total_external_promoters,
    SUM(external_detractors_count) as total_external_detractors,
    SUM(internal_promoters_count) as total_internal_promoters,
    SUM(internal_detractors_count) as total_internal_detractors
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_satisfaction_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY process_name, account
HAVING SUM(satisfaction_response_count) >= 5
ORDER BY avg_external_satisfaction DESC;
```

**Expected Output**: Satisfaction metrics by process and account showing CSAT and NPS scores.

> ⚠️ **Note**: The `technology_satisfaction_gold` table has **inflated counts** due to exploding team_members, stakeholder_areas, and account fields. For accurate NPS reporting, use `technology_process_nps_gold` instead (see Query 20b).

---

### Query 20b: Accurate NPS by Process (Recommended) ⭐

**Description**: Get accurate NPS metrics by process without explode inflation. Each task counts as ONE vote.

**Use Case**: Executive NPS reporting, process-level NPS comparison, stakeholder presentations.

```sql
-- Accurate NPS by Technology process (1 task = 1 vote)
SELECT
    process_name,
    SUM(internal_response_count) as tasks_with_internal_score,
    SUM(external_response_count) as tasks_with_external_score,
    ROUND(AVG(avg_internal_score), 1) as avg_internal_satisfaction,
    ROUND(AVG(avg_external_score), 1) as avg_external_satisfaction,
    SUM(internal_promoters) as internal_promoters,
    SUM(internal_passives) as internal_passives,
    SUM(internal_detractors) as internal_detractors,
    SUM(external_promoters) as external_promoters,
    SUM(external_passives) as external_passives,
    SUM(external_detractors) as external_detractors,
    ROUND(
        (CAST(SUM(internal_promoters) - SUM(internal_detractors) AS DOUBLE) / 
         NULLIF(SUM(internal_response_count), 0)) * 100, 1
    ) as internal_nps,
    ROUND(
        (CAST(SUM(external_promoters) - SUM(external_detractors) AS DOUBLE) / 
         NULLIF(SUM(external_response_count), 0)) * 100, 1
    ) as external_nps
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_process_nps_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY process_name
ORDER BY process_name;
```

**Expected Output**: Accurate NPS metrics by process with real task counts (not inflated).

**Understanding the difference:**

| Aspect | `technology_satisfaction_gold` | `technology_process_nps_gold` |
|--------|-------------------------------|-------------------------------|
| Granularity | By team, stakeholder, account | By process only |
| Counting | Inflated (1 task per team member) | Accurate (1 task = 1 vote) |
| Use Case | Multi-dimensional analysis | **Executive NPS reporting** |

---

### Query 21: Throughput Metrics Analysis

**Description**: Analyze throughput and processing velocity by process and account.

**Use Case**: Team throughput tracking, identify velocity trends, monitor work in progress.

```sql
-- Throughput metrics by Technology process (last 90 days)
SELECT
    process_name,
    team,
    SUM(requests_created) as total_created,
    SUM(requests_resolved) as total_resolved,
    ROUND(AVG(throughput_rate), 2) as avg_throughput_rate,
    ROUND(SUM(requests_resolved) * 100.0 / NULLIF(SUM(requests_created), 0), 2) as resolution_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_throughput_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY process_name, team
HAVING SUM(requests_created) >= 5
ORDER BY avg_throughput_rate DESC
LIMIT 30;
```

**Expected Output**: Throughput metrics by process and team showing daily created/resolved volume and throughput rate.

---

### Query 22: Status Flow Analysis

**Description**: Monitor status transitions between states to identify bottlenecks and workflow patterns.

**Use Case**: Workflow optimization, identify slow transitions, track how requests move across statuses.

```sql
-- Status flow transitions by Technology process (last 30 days)
SELECT
    process_name,
    from_status,
    to_status,
    SUM(transition_count) as total_transitions,
    ROUND(AVG(avg_transition_days), 1) as avg_transition_days
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_status_flow_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY process_name, from_status, to_status
ORDER BY process_name, total_transitions DESC;
```

**Expected Output**: Status transition counts and average days between statuses by process.

---

### Query 23: Team Performance Analysis

**Description**: Evaluate team performance metrics including request handling, resolution times, and satisfaction.

**Use Case**: Team productivity tracking, identify top-performing teams, workload distribution.

```sql
-- Team performance summary (last 90 days)
SELECT
    team,
    process_name,
    stakeholder_area,
    SUM(total_requests) as total_requests,
    SUM(resolved_requests) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(avg_satisfaction_score), 1) as avg_satisfaction_score,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_compliance_rate,
    ROUND(SUM(resolved_requests) * 100.0 / NULLIF(SUM(total_requests), 0), 2) as completion_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_team_performance_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY team, process_name, stakeholder_area
HAVING SUM(total_requests) >= 5
ORDER BY total_resolved DESC
LIMIT 30;
```

**Expected Output**: Team performance metrics ranked by completed requests showing productivity, resolution time, SLA compliance, and satisfaction.

---

### Query 24: Stakeholder Metrics Analysis

**Description**: Analyze metrics by stakeholder area to understand demand and performance patterns.

**Use Case**: Stakeholder demand analysis, identify high-demand areas, track stakeholder satisfaction.

```sql
-- Stakeholder metrics by Technology process (last 90 days)
SELECT
    stakeholder_area,
    process_name,
    SUM(total_requests) as total_requests,
    SUM(resolved_requests) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(avg_satisfaction_score), 1) as avg_satisfaction_score,
    COUNT(DISTINCT account) as unique_accounts
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_stakeholder_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY stakeholder_area, process_name
HAVING SUM(total_requests) >= 3
ORDER BY total_requests DESC
LIMIT 30;
```

**Expected Output**: Top 30 stakeholder areas ranked by request volume with resolution and satisfaction metrics.

---

### Query 25: Process Metrics Comparison

**Description**: Compare aggregated metrics across Technology processes (Architecture, Studios, Modernization & Infrastructure).

**Use Case**: Process-level comparison, executive reporting, identify process-specific patterns.

```sql
-- Process metrics comparison (last 90 days)
SELECT
    process_name,
    SUM(total_requests) as total_requests,
    SUM(resolved_requests) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(sla_compliance_rate), 2) as avg_sla_compliance_rate,
    ROUND(AVG(avg_satisfaction_score), 1) as avg_satisfaction_score,
    ROUND(SUM(resolved_requests) * 100.0 / NULLIF(SUM(total_requests), 0), 2) as resolution_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_process_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY process_name
ORDER BY total_requests DESC;
```

**Expected Output**: Process-level aggregated metrics comparing Architecture, Studios, and Modernization & Infrastructure.

---

### Query 26: Account Metrics Analysis

**Description**: Analyze metrics by account/client to understand account-level performance.

**Use Case**: Account-level performance tracking, identify high-value accounts, account-specific insights.

```sql
-- Account metrics summary (last 90 days)
SELECT
    account,
    process_name,
    SUM(total_requests) as total_requests,
    SUM(resolved_requests) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(avg_satisfaction_score), 1) as avg_satisfaction_score,
    ROUND(SUM(resolved_requests) * 100.0 / NULLIF(SUM(total_requests), 0), 2) as resolution_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_account_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY account, process_name
HAVING SUM(total_requests) >= 5
ORDER BY total_requests DESC
LIMIT 50;
```

**Expected Output**: Top 50 accounts ranked by request volume with performance metrics.

---

### Query 27: R&D Practices Metrics Analysis

**Description**: Analyze metrics by R&D Practices (Studios process only).

**Use Case**: R&D practice-level analysis, identify high-demand practices, track practice-specific performance.

```sql
-- R&D Practices metrics (last 90 days)
SELECT
    rd_practice,
    process_name,
    SUM(total_requests) as total_requests,
    SUM(resolved_requests) as total_resolved,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(SUM(resolved_requests) * 100.0 / NULLIF(SUM(total_requests), 0), 2) as resolution_rate_pct
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_rd_practices_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY rd_practice, process_name
HAVING SUM(total_requests) >= 3
ORDER BY total_requests DESC;
```

**Expected Output**: R&D practice-level metrics showing volume, resolution time, and resolution rate by practice and process.

---

### Query 28: Resolution Time Percentiles Analysis

**Description**: Analyze resolution time percentiles to identify outliers and understand distribution.

**Use Case**: Outlier detection, SLA planning, performance benchmarking.

```sql
-- Resolution time percentiles by process (last 90 days)
SELECT
    process_name,
    priority,
    COUNT(*) as record_count,
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(p50_resolution_days), 1) as p50_resolution_days,
    ROUND(AVG(p75_resolution_days), 1) as p75_resolution_days,
    ROUND(AVG(p95_resolution_days), 1) as p95_resolution_days,
    ROUND(AVG(stddev_resolution_days), 1) as avg_stddev_resolution_days,
    ROUND(AVG(p95_resolution_days) - AVG(p50_resolution_days), 1) as percentile_spread
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY process_name, priority
ORDER BY process_name,
    CASE priority
        WHEN 'urgent' THEN 1
        WHEN 'high' THEN 2
        WHEN 'normal' THEN 3
        WHEN 'low' THEN 4
        ELSE 5
    END;
```

**Expected Output**: Resolution time percentiles by process and priority showing distribution and outliers.

---

### Query 29: Satisfaction Score Distribution

**Description**: Analyze satisfaction score distribution using both average and median metrics.

**Use Case**: Satisfaction trend analysis, identify satisfaction patterns, track NPS components.

```sql
-- Satisfaction score distribution (last 90 days)
SELECT
    process_name,
    account,
    COUNT(*) as record_count,
    ROUND(AVG(avg_external_satisfaction_score), 1) as avg_external_satisfaction,
    ROUND(AVG(median_external_satisfaction_score), 1) as median_external_satisfaction,
    ROUND(AVG(avg_internal_satisfaction_score), 1) as avg_internal_satisfaction,
    ROUND(AVG(median_internal_satisfaction_score), 1) as median_internal_satisfaction,
    ROUND(AVG(avg_external_satisfaction_score) - AVG(median_external_satisfaction_score), 1) as external_score_skew,
    ROUND(AVG(avg_internal_satisfaction_score) - AVG(median_internal_satisfaction_score), 1) as internal_score_skew
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND avg_external_satisfaction_score IS NOT NULL
GROUP BY process_name, account
HAVING COUNT(*) >= 3
ORDER BY avg_external_satisfaction DESC
LIMIT 30;
```

**Expected Output**: Satisfaction score distribution showing average vs median to identify score skewness.

---

## Utility Queries

### Query 30: Table Metadata and Partitions

**Description**: Check table structure, partitions, and data freshness.

**Use Case**: Data operations, troubleshooting, monitoring.

```sql
-- Check partition information for Bronze table
SHOW PARTITIONS nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze;

-- Or get partition stats
SELECT
    year,
    month,
    COUNT(*) as record_count,
    MIN(ingestion_ts) as earliest_record,
    MAX(ingestion_ts) as latest_record
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
WHERE year >= 2025
GROUP BY year, month
ORDER BY year DESC, month DESC;
```

**Expected Output**: List of partitions with metadata useful for data operations.

---

### Query 31: Column Statistics

**Description**: Generate basic statistics for numeric columns.

**Use Case**: Data profiling, quality checks, understanding data distributions.

```sql
-- Statistics for Gold request metrics
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT process_name) as unique_processes,
    COUNT(DISTINCT account) as unique_accounts,
    -- Resolution time statistics
    ROUND(AVG(avg_resolution_days), 1) as avg_resolution_days,
    ROUND(AVG(stddev_resolution_days), 1) as avg_stddev_resolution_days,
    ROUND(AVG(p50_resolution_days), 1) as avg_p50_resolution_days,
    ROUND(AVG(p95_resolution_days), 1) as avg_p95_resolution_days,
    -- Satisfaction statistics
    ROUND(AVG(avg_external_satisfaction_score), 1) as avg_external_satisfaction,
    ROUND(AVG(avg_internal_satisfaction_score), 1) as avg_internal_satisfaction,
    -- Quality statistics
    ROUND(AVG(avg_completeness_score), 2) as avg_completeness_score,
    ROUND(AVG(avg_urgency_score), 2) as avg_urgency_score
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY;
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
    'requests' as entity,
    MAX(ingestion_ts) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', MAX(ingestion_ts), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_raw_zone_ingestion.clickup__technology__requests_raw
UNION ALL
SELECT
    'Bronze' as layer,
    'requests' as entity,
    MAX(ingestion_ts) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', MAX(ingestion_ts), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_bronze_processing.clickup__technology__requests_bronze
UNION ALL
SELECT
    'Silver' as layer,
    'dim_requests' as entity,
    MAX(ingestion_ts) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', MAX(ingestion_ts), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_silver_analytics.technology_dim_requests
UNION ALL
SELECT
    'Gold' as layer,
    'request_metrics' as entity,
    MAX(analysis_date) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    DATE_DIFF('hour', CAST(MAX(analysis_date) AS TIMESTAMP), CURRENT_TIMESTAMP) as hours_behind
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold;
```

**Expected Output**: Data freshness metrics across all layers showing how recent the data is.

---

### Query 33: Process List Mapping

**Description**: Verify process name mapping from list_id to process_name.

**Use Case**: Data validation, verify process assignments, troubleshoot mapping issues.

```sql
-- Process list mapping verification
SELECT
    list_id,
    list_name,
    COUNT(DISTINCT request_id) as request_count,
    COUNT(DISTINCT process_name) as unique_process_names
FROM nan_wl_workloads_data_lake_staging_gold_reporting.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY list_id, list_name
ORDER BY request_count DESC;
```

**Expected Output**: List ID to process name mapping verification showing request distribution.

---

## Query Optimization Tips

### Performance Best Practices

1. **Always filter by partition columns**:

```sql
WHERE year = 2025 AND month = 12  -- Raw/Bronze/Silver layer
WHERE analysis_date >= CURRENT_DATE - INTERVAL '90' DAY  -- Gold layer
```

2. **Use latest partition for point-in-time queries**:

```sql
WHERE year = (SELECT MAX(year) FROM table_name)
  AND month = (SELECT MAX(month) FROM table_name WHERE year = (SELECT MAX(year) FROM table_name))
```

3. **Leverage quality flags for filtering**:

```sql
WHERE completeness_score >= 0.7
  AND urgency_score >= 0.5
```

4. **Use APPROX functions for large datasets**:

```sql
APPROX_PERCENTILE(column, 0.5) -- Faster than exact percentile
APPROX_COUNT_DISTINCT(column)  -- Faster than COUNT(DISTINCT)
```

5. **Handle comma-separated strings properly**:

```sql
-- Split comma-separated strings for analysis
CROSS JOIN UNNEST(SPLIT(account, ', ')) AS t(account_item)
```

6. **Limit result sets appropriately**:

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

* [ClickUp Bronze Layer](./BRONZE.md) - Complete Bronze schema and field descriptions
* [ClickUp Raw Layer](./RAW.md) - Raw data structure and ingestion details
* [ClickUp Silver Layer](./SILVER.md) - Silver layer transformations and business logic
* [ClickUp Gold Layer](./GOLD.md) - Gold layer KPIs and use cases
* [ClickUp Overview](./README.md) - Integration overview and quick start
* [Medallion Architecture](../../MEDALLION_ARCHITECTURE.md) - Layer design principles

---

**Last Updated**: 2025-12-09  
**Query Count**: 33 queries (4 Raw, 7 Bronze, 5 Silver, 13 Gold, 4 Utility)  
**Database Layers**: Raw, Bronze, Silver, Gold  
**Environment**: Staging (`nan_wl_workloads_data_lake_staging_*`)
