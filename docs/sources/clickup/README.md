# ClickUp Integration - Technology Requests Data Pipeline

## Overview

This integration extracts data from ClickUp API v2, specifically from Technology Requests lists, to support NaNLABS' Technology process KPIs and analytics.

**Source System**: ClickUp API v2  
**Update Frequency**: Daily (configurable)  
**Data Types**: Tasks (Requests) from 3 Technology process lists  
**API Format**: REST JSON  
**Authentication**: Bearer Token (API Key)  
**Plan**: Business (100 requests/minute rate limit)

## Technology Processes

The integration supports 3 Technology processes:

1. **Architecture** (list_id: `900201093820`)
   - Architecture-related requests and consultancy
   - Metrics by Architecture Responsibility Domain

2. **Studios** (list_id: `901703323552`)
   - Learning & R&D requests
   - Metrics by R&D Practices (Cloud Data Engineering, IA & ML Engineering, etc.)

3. **Modernization & Infrastructure** (list_id: `901702911170`)
   - Infrastructure and modernization requests
   - Metrics by Architecture Responsibility Domain

## Quick Start

### Extract Raw Data

```bash
# Extract tasks from Technology process lists
make run-raw DATA_SOURCE=clickup ENTITY_TYPE=tasks \
  ARGS="--LIST_IDS 900201093820,901703323552,901702911170"
```

### Process to Bronze

```bash
make run-bronze DATA_SOURCE=clickup ENTITY_TYPE=tasks \
  ARGS="--CREATE_TABLES true"
```

### Process to Silver

```bash
make run-silver DATA_SOURCE=clickup ENTITY_TYPE=tasks \
  ARGS="--CREATE_TABLES true"
```

### Process to Gold (KPIs)

```bash
# Request metrics (by process, account, architecture responsibility domain)
make run-gold DATA_SOURCE=clickup ENTITY_TYPE=technology_request_metrics \
  ARGS="--CREATE_TABLES true"

# Satisfaction metrics (by process, account)
make run-gold DATA_SOURCE=clickup ENTITY_TYPE=technology_satisfaction \
  ARGS="--CREATE_TABLES true"

# Throughput metrics (by process, account)
make run-gold DATA_SOURCE=clickup ENTITY_TYPE=technology_throughput \
  ARGS="--CREATE_TABLES true"

# Process metrics (Architecture, Studios, Modernization & Infrastructure)
make run-gold DATA_SOURCE=clickup ENTITY_TYPE=technology_process_metrics \
  ARGS="--CREATE_TABLES true"

# Account metrics (by account/client)
make run-gold DATA_SOURCE=clickup ENTITY_TYPE=technology_account_metrics \
  ARGS="--CREATE_TABLES true"

# R&D Practices metrics (Studios only)
make run-gold DATA_SOURCE=clickup ENTITY_TYPE=technology_rd_practices_metrics \
  ARGS="--CREATE_TABLES true"
```

## Data Flow

```
ClickUp API v2 (REST JSON)
    ↓
Raw Layer (S3 JSONL)
    ↓
Bronze Layer (Iceberg - Parsed & Normalized)
    ↓
Silver Layer (Iceberg - Clean SSOT)
    ↓
Gold Layer (Iceberg - Technology KPIs)
```

## Supported Lists

### Raw Layer (3 Technology Process Lists)

| Process | List ID | Description |
|---------|---------|-------------|
| **Architecture** | `900201093820` | Architecture-related requests and consultancy |
| **Studios** | `901703323552` | Learning & R&D requests |
| **Modernization & Infrastructure** | `901702911170` | Infrastructure and modernization requests |

### Custom Fields

All lists contain common custom fields:
- **Account** (labels) - Account/client associated with request
- **External satisfaction** (emoji) - External CSAT/NPS (values 1-5, converted to 2-10 scale; 0 = not evaluated → NULL)
- **Internal satisfaction** (emoji) - Internal NPS (values 1-5, converted to 2-10 scale; 0 = not evaluated → NULL)
- **Stakeholder Area** (labels) - Stakeholder area (Talent, Engineering, etc.)
- **🙋‍♂️ Requesters** (users) - People who requested/need the work (can be multiple)
- **🫂 Team** (users) - Team members assigned to work on request (can be multiple)
- **Architecture Responsibility Domain** (labels) - Responsibility domain (all lists)

List-specific custom fields:
- **R&D Practices** (labels) - Only in Studios list (Cloud Data Engineering, IA & ML Engineering, etc.)

### Satisfaction Scores (NPS)

| Field | Who Evaluates | What It Measures |
|-------|---------------|------------------|
| **Internal Satisfaction** | Team members | How valuable/useful the work was for the team |
| **External Satisfaction** | Requesters | Client satisfaction with the delivered result |

**Important**: Each task has ONE internal score and ONE external score, regardless of how many team members or requesters are involved.

## Silver Layer (4 tables)

- **technology_dim_requests**: Main dimension table with request details, custom fields (Account, Architecture Responsibility Domain, R&D Practices)
- **technology_dim_stakeholder_areas**: Stakeholder areas dimension
- **technology_dim_teams**: Teams dimension
- **technology_fact_request_events**: Fact table with request events

## Gold Layer (10 KPI tables)

### Core Metrics Tables
- **technology_request_metrics_gold**: Aggregated request metrics by period, process, account, architecture responsibility domain
- **technology_satisfaction_gold**: Satisfaction metrics (CSAT/NPS) by process, account, team, stakeholder (⚠️ counts inflated by explode)
- **technology_throughput_gold**: Throughput and processing velocity by process, account
- **technology_request_status_flow_gold**: Status flow and transitions by process
- **technology_team_performance_gold**: Performance by team, process, account
- **technology_stakeholder_metrics_gold**: Metrics by stakeholder area, process, account

### Process-Specific Tables
- **technology_process_metrics_gold**: Aggregated metrics by Technology process (Architecture, Studios, Modernization & Infrastructure)
- **technology_account_metrics_gold**: Metrics by Account/Client
- **technology_rd_practices_metrics_gold**: Metrics by R&D Practices (Studios process only)
- **technology_process_nps_gold**: ⭐ **Accurate NPS** by process (1 task = 1 vote, no explode inflation)

### Which table to use for NPS?

| Use Case | Recommended Table |
|----------|-------------------|
| **Executive NPS reporting** | `technology_process_nps_gold` (accurate counts) |
| **NPS by team/stakeholder/account** | `technology_satisfaction_gold` (multi-dimensional, inflated) |
| **Process-level NPS comparison** | `technology_process_nps_gold` (accurate counts) |

## Documentation

- **[RAW.md](./RAW.md)** - Raw layer details, API reference, extraction
- **[BRONZE.md](./BRONZE.md)** - Bronze layer schema, transformations
- **[SILVER.md](./SILVER.md)** - Silver layer transformations, business logic
- **[GOLD.md](./GOLD.md)** - Gold layer KPIs, use cases, query examples
- **[ATHENA_QUERIES.md](./ATHENA_QUERIES.md)** - Practical Athena SQL queries for all layers (Raw, Bronze, Silver, Gold)
- **[API_REFERENCE.md](./API_REFERENCE.md)** - ClickUp API v2 details, authentication, endpoints

## Related Documentation

- [Job Development Guide](../../JOB_DEVELOPMENT.md) - Building Spark and PyShell jobs
- [Medallion Architecture](../../MEDALLION_ARCHITECTURE.md) - Complete architecture reference
