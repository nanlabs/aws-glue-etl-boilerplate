# ClickUp Gold Layer - Technology KPIs

## Overview

The Gold layer provides analytics-ready tables with pre-calculated Technology KPIs for dashboarding and reporting.

**Job**: `jobs/gold/clickup_gold_job.py`  
**Format**: Apache Iceberg  
**Storage**: S3 with Iceberg table format  
**Update Frequency**: Daily (after silver processing)

## Technology Processes

The Gold layer supports metrics for 3 Technology processes:

1. **Architecture** (list_id: `900201093820`)
   - Architecture-related requests and consultancy
   - Metrics by Architecture Responsibility Domain

2. **Studios** (list_id: `901703323552`)
   - Learning & R&D requests
   - Metrics by R&D Practices (Cloud Data Engineering, IA & ML Engineering, etc.)

3. **Modernization & Infrastructure** (list_id: `901702911170`)
   - Infrastructure and modernization requests
   - Metrics by Architecture Responsibility Domain

## KPI Tables

### 1. `technology_request_metrics_gold`

**KPI**: Aggregated request metrics by period, process, account, and architecture responsibility domain

**Dimensions**: `analysis_date`, `process_name`, `stakeholder_area`, `team`, `priority`, `account`, `architecture_responsibility_domain`, `list_name`

**Metrics**:
- `request_count`: Total requests
- `resolved_count`: Resolved requests
- `avg_resolution_days`: Average resolution time
- `stddev_resolution_days`: Standard deviation of resolution days
- `p50_resolution_days`: Median (50th percentile) resolution days
- `p75_resolution_days`: 75th percentile resolution days
- `p95_resolution_days`: 95th percentile resolution days
- `sla_compliance_rate`: SLA compliance percentage
- `avg_time_logged_hours`: Average time logged
- `avg_time_estimate_hours`: Average time estimate
- `estimate_accuracy`: Time logged / time estimate ratio
- `avg_external_satisfaction_score`: Average external satisfaction score (2-10, only evaluated requests)
- `avg_internal_satisfaction_score`: Average internal satisfaction score (2-10, only evaluated requests)
- `median_external_satisfaction_score`: Median external satisfaction score (2-10)
- `median_internal_satisfaction_score`: Median internal satisfaction score (2-10)
- `avg_completeness_score`: Average data completeness score (0-1)
- `avg_urgency_score`: Average request urgency score (0-1)

**Partitioning**: `analysis_date`, `process_name`, `stakeholder_area`, `team`, `priority`

**Use Cases**:
- Track request volume by Technology process (Architecture, Studios, Modernization & Infrastructure)
- Monitor resolution times by account/client (including percentiles for outlier detection)
- Analyze SLA compliance by architecture responsibility domain
- Measure estimation accuracy by process
- Analyze satisfaction trends using both average and median scores
- Assess data quality using completeness and urgency scores

### 2. `technology_satisfaction_gold`

**KPI**: Satisfaction metrics (CSAT/NPS) by process, account, and architecture responsibility domain

**Dimensions**: `analysis_date`, `process_name`, `stakeholder_area`, `team`, `account`, `architecture_responsibility_domain`, `list_name`

**Metrics**:
- `external_satisfaction_score`: Average external CSAT score (2-10 scale, NULL excluded)
- `internal_satisfaction_score`: Average internal NPS score (2-10 scale, NULL excluded)
- `external_nps_score`: Calculated external NPS ((Promoters - Detractors) / Total * 100)
- `internal_nps_score`: Calculated internal NPS
- `satisfaction_response_count`: Total satisfaction responses
- `external_promoters_count`, `external_passives_count`, `external_detractors_count`
- `internal_promoters_count`, `internal_passives_count`, `internal_detractors_count`

**Partitioning**: `analysis_date`, `process_name`, `stakeholder_area`, `team`

**Use Cases**:
- Track customer satisfaction (CSAT) by Technology process
- Monitor internal team satisfaction (NPS) by account
- Identify satisfaction trends by architecture responsibility domain
- Calculate NPS scores for reporting by process

### 3. `technology_throughput_gold`

**KPI**: Throughput and processing velocity by process and account

**Dimensions**: `analysis_date`, `process_name`, `stakeholder_area`, `team`, `account`, `architecture_responsibility_domain`, `list_name`

**Metrics**:
- `requests_created`: Requests created
- `requests_resolved`: Requests resolved
- `throughput_rate`: Requests resolved per day
- `period_start`: Period start date
- `period_end`: Period end date

**Partitioning**: `analysis_date`, `process_name`

**Use Cases**:
- Measure team throughput by Technology process
- Track request velocity by account/client
- Monitor throughput by architecture responsibility domain
- Analyze daily throughput trends by process

### 4. `technology_request_status_flow_gold`

**KPI**: Status flow and transitions by process

**Dimensions**: `analysis_date`, `process_name`, `stakeholder_area`, `team`, `account`, `architecture_responsibility_domain`, `list_name`, `from_status`, `to_status`

**Metrics**:
- `transition_count`: Number of status transitions
- `avg_transition_days`: Average days between status transitions

**Partitioning**: `analysis_date`, `process_name`, `list_name`, `team`

**Use Cases**:
- Track status transitions by Technology process
- Monitor workflow bottlenecks by analyzing transition times
- Analyze status flow patterns by Architecture/Studios/Modernization
- Identify slow transitions between statuses

### 5. `technology_team_performance_gold`

**KPI**: Performance by team, process, account, and architecture responsibility domain

**Dimensions**: `analysis_date`, `process_name`, `team`, `stakeholder_area`, `account`, `architecture_responsibility_domain`, `list_name`

**Metrics**:
- `total_requests`: Total requests assigned to team
- `resolved_requests`: Requests completed
- `avg_resolution_days`: Average resolution time
- `sla_compliance_rate`: SLA compliance percentage
- `avg_satisfaction_score`: Average satisfaction score

**Partitioning**: `analysis_date`, `process_name`, `team`

**Use Cases**:
- Track team productivity by Technology process
- Monitor team performance by account/client
- Compare team efficiency by architecture responsibility domain
- Analyze team satisfaction by process

### 6. `technology_stakeholder_metrics_gold`

**KPI**: Metrics by stakeholder area, process, account, and architecture responsibility domain

**Dimensions**: `analysis_date`, `process_name`, `stakeholder_area`, `account`, `architecture_responsibility_domain`, `list_name`

**Metrics**:
- `total_requests`: Total requests submitted by area
- `resolved_requests`: Resolved requests
- `avg_resolution_days`: Average resolution time
- `avg_satisfaction_score`: Average satisfaction score

**Partitioning**: `analysis_date`, `process_name`

**Use Cases**:
- Track requests by stakeholder area and process
- Monitor resolution times by account
- Analyze satisfaction by architecture responsibility domain
- Identify high-demand areas by process

### 7. `technology_process_metrics_gold` ⭐ NEW

**KPI**: Aggregated metrics by Technology process (Architecture, Studios, Modernization & Infrastructure)

**Dimensions**: `analysis_date`, `process_name`, `list_name`

**Metrics**:
- `total_requests`: Total requests
- `resolved_requests`: Resolved requests
- `avg_resolution_days`: Average resolution time
- `sla_compliance_rate`: SLA compliance percentage
- `avg_satisfaction_score`: Average satisfaction score

**Partitioning**: `analysis_date`, `process_name`

**Use Cases**:
- Compare performance across Technology processes
- Track process-level KPIs for executive reporting
- Monitor process health and efficiency
- Identify process-specific bottlenecks

### 8. `technology_account_metrics_gold` ⭐ NEW

**KPI**: Metrics by Account/Client

**Dimensions**: `analysis_date`, `process_name`, `account`, `list_name`

**Metrics**:
- `total_requests`: Total requests for account
- `resolved_requests`: Resolved requests
- `avg_resolution_days`: Average resolution time
- `avg_satisfaction_score`: Average satisfaction score

**Partitioning**: `analysis_date`, `account`

**Use Cases**:
- Track performance by account/client
- Monitor account-level resolution times
- Analyze satisfaction by account
- Identify high-value accounts

### 9. `technology_rd_practices_metrics_gold` ⭐ NEW

**KPI**: Metrics by R&D Practices (Studios process only)

**Dimensions**: `analysis_date`, `rd_practice`

**Metrics**:
- `request_count`: Total requests for R&D practice
- `resolved_count`: Resolved requests
- `avg_resolution_days`: Average resolution time
- `sla_compliance_rate`: SLA compliance percentage
- `satisfaction_score`: Average satisfaction score
- `avg_time_logged_hours`: Average time logged
- `avg_time_estimate_hours`: Average time estimate

**R&D Practices**:
- Cloud Data Engineering
- IA & ML Engineering
- Real-Time Data Processing Engineering
- Cloud Infrastructure Engineering (DevOps, DevSecOps, ...)
- Methodologies & Project Management
- UI/UX Design
- Frontend Engineering + UI/UX Engineering
- Backend Engineering
- Quality Assurance

**Partitioning**: `analysis_date`, `rd_practice`

**Use Cases**:
- Track performance by R&D practice
- Monitor Studios process efficiency by practice
- Analyze satisfaction by practice
- Identify high-demand R&D practices

### 10. `technology_process_nps_gold` ⭐ NEW - Accurate NPS

**KPI**: Accurate NPS metrics by process (1 task = 1 vote, no explode inflation)

**Dimensions**: `analysis_date`, `process_name`, `list_name`

**Metrics**:
- `external_response_count`: Total tasks with external satisfaction score (not NULL)
- `internal_response_count`: Total tasks with internal satisfaction score (not NULL)
- `avg_external_score`: Average external satisfaction score (2-10)
- `avg_internal_score`: Average internal satisfaction score (2-10)
- `external_promoters`: Tasks with external score 9-10
- `external_passives`: Tasks with external score 7-8
- `external_detractors`: Tasks with external score 2-6
- `internal_promoters`: Tasks with internal score 9-10
- `internal_passives`: Tasks with internal score 7-8
- `internal_detractors`: Tasks with internal score 2-6
- `external_nps`: External NPS score (-100 to +100)
- `internal_nps`: Internal NPS score (-100 to +100)

**Partitioning**: `analysis_date`, `process_name`

**⚠️ Important**: This table differs from `technology_satisfaction_gold`:

| Aspect | `technology_satisfaction_gold` | `technology_process_nps_gold` |
|--------|-------------------------------|-------------------------------|
| Granularity | By team, stakeholder, account | By process only |
| Counting | Inflated by explode (1 task counted per team member) | Accurate (1 task = 1 vote) |
| Use Case | Multi-dimensional analysis | Process-level NPS reporting |

**Use Cases**:
- **Executive NPS reporting** with accurate counts
- Compare NPS across Technology processes
- Track NPS trends over time without inflation
- Validate NPS numbers for stakeholder presentations

**Understanding NPS Scores**:
- **Internal NPS**: Team's evaluation of how valuable/useful the work was
- **External NPS**: Requester/client's satisfaction with the result
- **NPS Formula**: `((Promoters - Detractors) / Total) * 100`
- **NPS Scale**: -100 (all detractors) to +100 (all promoters)

## Query Examples

### Request Metrics by Technology Process

```sql
SELECT
  process_name,
  SUM(request_count) as total_requests,
  SUM(resolved_count) as total_resolved,
  AVG(avg_resolution_days) as avg_resolution,
  AVG(sla_compliance_rate) as avg_sla_compliance
FROM glue_catalog.nan_gold_zone.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY process_name
ORDER BY total_requests DESC
```

### Satisfaction by Process and Account

```sql
SELECT
  process_name,
  account,
  AVG(external_satisfaction_score) as avg_csat,
  AVG(internal_satisfaction_score) as avg_nps,
  AVG(external_nps_score) as avg_external_nps,
  AVG(internal_nps_score) as avg_internal_nps
FROM glue_catalog.nan_gold_zone.technology_satisfaction_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAYS
  AND account IS NOT NULL
GROUP BY process_name, account
ORDER BY process_name, avg_csat DESC
```

### Team Performance by Process

```sql
SELECT
  process_name,
  team,
  stakeholder_area,
  SUM(total_requests) as total_assigned,
  SUM(resolved_requests) as total_completed,
  AVG(avg_resolution_days) as avg_resolution,
  AVG(avg_satisfaction_score) as avg_satisfaction,
  AVG(sla_compliance_rate) as avg_sla_compliance
FROM glue_catalog.nan_gold_zone.technology_team_performance_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY process_name, team, stakeholder_area
ORDER BY process_name, total_completed DESC
```

### Throughput by Process and Account

```sql
SELECT
  analysis_date,
  process_name,
  account,
  SUM(requests_created) as created,
  SUM(requests_resolved) as resolved,
  AVG(throughput_rate) as avg_throughput
FROM glue_catalog.nan_gold_zone.technology_throughput_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND account IS NOT NULL
GROUP BY analysis_date, process_name, account
ORDER BY analysis_date DESC, resolved DESC
```

### Status Flow Transitions by Process

```sql
SELECT
  process_name,
  from_status,
  to_status,
  SUM(transition_count) as total_transitions,
  AVG(avg_transition_days) as avg_transition_time
FROM glue_catalog.nan_gold_zone.technology_request_status_flow_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY process_name, from_status, to_status
ORDER BY process_name, total_transitions DESC
```

### Process-Level Comparison

```sql
SELECT
  process_name,
  SUM(total_requests) as total_requests,
  SUM(resolved_requests) as total_resolved,
  AVG(avg_resolution_days) as avg_resolution,
  AVG(sla_compliance_rate) as sla_compliance,
  AVG(avg_satisfaction_score) as satisfaction
FROM glue_catalog.nan_gold_zone.technology_process_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY process_name
ORDER BY total_requests DESC
```

### Account Performance

```sql
SELECT
  account,
  process_name,
  SUM(total_requests) as total_requests,
  AVG(avg_resolution_days) as avg_resolution,
  AVG(avg_satisfaction_score) as satisfaction
FROM glue_catalog.nan_gold_zone.technology_account_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 90 DAYS
  AND account IS NOT NULL
GROUP BY account, process_name
ORDER BY total_requests DESC
```

### R&D Practices Performance (Studios)

```sql
SELECT
  rd_practice,
  SUM(total_requests) as total_requests,
  SUM(resolved_requests) as total_resolved,
  AVG(avg_resolution_days) as avg_resolution
FROM glue_catalog.nan_gold_zone.technology_rd_practices_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY rd_practice
ORDER BY total_requests DESC
```

### Architecture Responsibility Domain Metrics

```sql
SELECT
  process_name,
  architecture_responsibility_domain,
  SUM(request_count) as total_requests,
  AVG(avg_resolution_days) as avg_resolution,
  AVG(satisfaction_score) as satisfaction
FROM glue_catalog.nan_gold_zone.technology_request_metrics_gold
WHERE analysis_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND architecture_responsibility_domain IS NOT NULL
GROUP BY process_name, architecture_responsibility_domain
ORDER BY process_name, total_requests DESC
```

## Related Documentation

- [Silver Layer](./SILVER.md) - Source data structure
- [Custom Fields Analysis](./CUSTOM_FIELDS_ANALYSIS.md) - Complete custom fields documentation
- [Source Overview](./README.md) - Complete integration overview
