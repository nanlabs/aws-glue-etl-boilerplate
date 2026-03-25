# Data Warehouse Documentation

## Overview

This documentation covers the NaNLABS Data Warehouse platform built with AWS Glue 5.0 and Apache Iceberg 1.7.1, implementing a Medallion Architecture for modern data lakehouse operations.

## Quick Start

| Document | Description |
|----------|-------------|
| **[Development Guide](./DEVELOPMENT.md)** ⚡ | **Complete guide: DevContainer setup, local testing, and workflow** |
| **[Quick Reference](./QUICK_REFERENCE.md)** | ⚡ **Copy-paste commands for common tasks** |
| **[Job Development Guide](./JOB_DEVELOPMENT.md)** | Building Spark and PyShell jobs |
| **[Deployment Guide](./DEPLOYMENT.md)** | AWS deployment procedures (see ClickUp for detailed runbooks) |
| **[Architecture](./ARCHITECTURE.md)** | System architecture and design principles |
| **[Testing Guidelines](./TESTING.md)** | Testing requirements and best practices |
| **[AI Guidelines](../AGENTS.md)** | AI interaction and execution guidelines |
| **[ClickUp Workflow](../CLICKUP_WORKFLOW.md)** | Project management and task tracking |

## Core Documentation

### Architecture & Design
- **[Medallion Architecture](./MEDALLION_ARCHITECTURE.md)** ⭐ - **Complete 4-layer reference, patterns, requirements, and best practices**
- **[Architecture](./ARCHITECTURE.md)** - System architecture and AWS design principles
- **[Medallion Job Pattern](./MEDALLION_JOB_PATTERN.md)** - Building jobs with MedallionJobBase

### Job Development Guides
- **[Job Development Guide](./JOB_DEVELOPMENT.md)** - Building Spark and PyShell jobs (unified guide)
- **[Best Practices](./BEST_PRACTICES.md)** - Coding standards and guidelines

### Development & Operations
- **[Development Guide](./DEVELOPMENT.md)** ⚡ - **DevContainer setup, local testing, and workflow**
- **[Testing Guidelines](./TESTING.md)** - Testing structure and best practices
- **[Deployment Guide](./DEPLOYMENT.md)** - AWS deployment procedures (see ClickUp for detailed runbooks)
- **[Operations Guide](./OPERATIONS.md)** - Troubleshooting and monitoring (see ClickUp for detailed runbooks)

### Technical References
- **[Schema Evolution](./SCHEMA_EVOLUTION.md)** - Iceberg schema management
- **[Secrets Management](./SECRETS_MANAGEMENT.md)** - AWS Secrets Manager integration
- **[Environment Variables](./ENVIRONMENT_VARIABLES.md)** - Configuration reference
- **[AWS Glue Troubleshooting](./AWS_GLUE_TROUBLESHOOTING.md)** - Quick reference (see ClickUp for detailed runbooks)
- **[AI Guidelines](../AGENTS.md)** - AI interaction guidelines

## Data Sources

### Team Tailor
- **[Team Tailor Integration](./sources/teamtailor/README.md)** - Quick start and complete data pipeline overview
  - [Raw Zone](./sources/teamtailor/RAW.md) - API extraction
  - [Bronze Layer](./sources/teamtailor/BRONZE.md) - Structured storage and merge logic
  - [Silver Layer](./sources/teamtailor/SILVER.md) - Clean SSOT tables for Talent analytics
  - [Gold Layer](./sources/teamtailor/GOLD.md) - Talent KPI tables
  - [API Reference](./sources/teamtailor/API_REFERENCE.md) - API documentation

### ClickUp
- **[ClickUp Integration](./sources/clickup/README.md)** - Technology requests data pipeline overview
  - [Raw Zone](./sources/clickup/RAW.md) - API extraction from Technology process lists
  - [Bronze Layer](./sources/clickup/BRONZE.md) - Parsed tasks with custom fields and metadata
  - [Silver Layer](./sources/clickup/SILVER.md) - Clean SSOT tables for Technology analytics
  - [Gold Layer](./sources/clickup/GOLD.md) - Technology KPI tables (request metrics, satisfaction, throughput)
  - [API Reference](./sources/clickup/API_REFERENCE.md) - ClickUp API v2 documentation

## Technology Stack

- **Compute**: AWS Glue 5.0 (Spark 3.5.4, Python 3.11)
- **Table Format**: Apache Iceberg 1.7.1
- **Storage**: Amazon S3 with optimized layouts
- **Catalog**: AWS Glue Data Catalog with Iceberg integration
- **Orchestration**: AWS Step Functions / Airflow

## Quick Start Commands

For copy-paste command sequences (local runs, packaging, deployments), follow the **[Development Guide](./DEVELOPMENT.md)** which tracks the latest verified workflows. All development happens inside the DevContainer.

## Contributing

Please read the [Development Guide](./DEVELOPMENT.md) and [Best Practices](./BEST_PRACTICES.md) before contributing to this project.

## Support

For questions or issues:
- Check the [Operations Guide](./OPERATIONS.md) for troubleshooting
- Review the [Testing Guidelines](./TESTING.md) for testing issues
- Consult the [Architecture](./ARCHITECTURE.md) for system understanding

## License

This project is proprietary to NaNLABS.
