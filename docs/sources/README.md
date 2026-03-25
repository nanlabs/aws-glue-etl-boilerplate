# Data Sources

This directory contains documentation for all external data sources integrated into the data warehouse.

## 📚 Documentation Structure

Each source is organized by **Medallion Architecture layers** (4 layers):

```
source/
├── README.md          # Overview & quick start
├── RAW.md             # RAW zone: immutable landing (JSONL, CSV)
├── BRONZE.md          # BRONZE layer: parsed staging (Iceberg, normalized)
├── SILVER.md          # SILVER layer: clean SSOT (Iceberg, normalized)
├── GOLD.md            # GOLD layer: analytics-ready (Iceberg, denormalized)
├── ATHENA_QUERIES.md  # Athena queries for all layers (Raw, Bronze, Silver, Gold)
└── API_REFERENCE.md   # API documentation (if applicable)
```

> **📖 Complete Reference**: See [Medallion Architecture](../MEDALLION_ARCHITECTURE.md) for layer specifications

## 🎯 Available Sources

### Talent Management

#### [Team Tailor](./teamtailor/README.md)
Talent acquisition and recruitment platform data for NaNLABS Talent process.

**Entities**: 8 types - Candidates ⭐, Jobs, Applications, Interviews, Users, Departments, Stages, Activities

| Layer | Format | Documentation |
|-------|--------|---------------|
| **Raw** | JSONL | [RAW.md](./teamtailor/RAW.md) - 8 entities, intelligent partitioning |
| **Bronze** | Iceberg | [BRONZE.md](./teamtailor/BRONZE.md) - Parsed staging with merge strategies |
| **Silver** | Iceberg | [SILVER.md](./teamtailor/SILVER.md) - Clean SSOT tables for Talent analytics |
| **Gold** | Iceberg | [GOLD.md](./teamtailor/GOLD.md) - Talent KPI tables (time-to-fill, source effectiveness, etc.) |
| **API** | N/A | [API_REFERENCE.md](./teamtailor/API_REFERENCE.md) - Team Tailor API docs |

### Technology Operations

#### [ClickUp](./clickup/README.md)
Technology requests and task management data for NaNLABS Technology process KPIs.

**Lists**: 3 lists - Architecture Requests, Learning R&D Requests, Modernization & Infrastructure Requests

| Layer | Format | Documentation |
|-------|--------|---------------|
| **Raw** | JSONL | [RAW.md](./clickup/RAW.md) - Tasks from 3 lists with custom fields, date partitioning |
| **Bronze** | Iceberg | [BRONZE.md](./clickup/BRONZE.md) - Parsed tasks with custom fields extraction |
| **Silver** | Iceberg | [SILVER.md](./clickup/SILVER.md) - Clean SSOT tables (requests, stakeholder_areas, teams, events) |
| **Gold** | Iceberg | [GOLD.md](./clickup/GOLD.md) - Technology KPI tables (request metrics, satisfaction, throughput, etc.) |
| **API** | N/A | [API_REFERENCE.md](./clickup/API_REFERENCE.md) - ClickUp API v2 docs |

---

## 🔍 Finding Information

### By Pipeline Stage
- **Raw Zone**: Look for `RAW.md` in the source folder
- **Bronze Processing**: Look for `BRONZE.md`
- **Silver Tables**: Look for `SILVER.md`

### By Question Type
- "What data do we extract?" → `RAW.md`
- "How is it processed?" → `BRONZE.md`
- "What tables are available?" → `SILVER.md`
- "What's the API structure?" → `API_REFERENCE.md`

## 📖 Related Documentation

- [Job Development Guide](../JOB_DEVELOPMENT.md) - Building Spark and PyShell jobs
- [Medallion Architecture](../MEDALLION_ARCHITECTURE.md) - Complete architecture reference
- [Medallion Job Pattern](../MEDALLION_JOB_PATTERN.md) - MedallionJobBase API reference

## ➕ Adding New Sources

When adding a new data source:

1. Create folder: `docs/sources/your_source/`
2. Create files:
   - `README.md` - Overview & quick start
   - `RAW.md` - Extraction & ingestion
   - `BRONZE.md` - Initial processing
   - `SILVER.md` - Final tables
3. Update this index with a new entry
4. Follow existing patterns for consistency

### Template Structure

```markdown
# Source Name - Raw Zone

## Overview
- Source system
- Update frequency
- Data types

## Extraction
- Job: `jobs/raw/source_job.py`
- Method: API / SFTP / Database
- Format: JSONL / CSV / Parquet

## Storage
- S3 Path: `s3://bucket/raw/source/`
- Partitioning: year/month/day
- Format: ...

## Entities
### Entity 1
- Description
- Key fields
- Sample data

## Related
- [Bronze Processing](./BRONZE.md)
- [Source Overview](./README.md)
```

---

**Questions?** Check the source's README.md or the main [Documentation Index](../README.md).
