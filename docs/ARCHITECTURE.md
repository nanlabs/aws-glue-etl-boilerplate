# Architecture Guide

Detailed system architecture and design for the AWS Glue POC data pipeline.

## Overview

The AWS Glue POC implements a medallion architecture (Bronze/Silver/Gold) for data processing, providing a scalable and cost-effective data pipeline solution.

## High-Level Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   AWS Glue POC   │    │   Consumers     │
│                 │    │                  │    │                 │
│ • Team Tailor API│───▶│ • S3 Data Lake   │───▶│ • Athena        │
│ • External APIs │    │ • Glue Jobs      │    │ • BI Tools      │
│ • CSV Files     │    │ • Glue Catalog   │    │ • Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Data Flow Architecture

### Medallion Architecture Layers

```
Raw Data Sources
       ↓
┌─────────────────┐
│   Raw Zone      │  ← Python Shell Jobs (Ingestion)
│   (S3)          │
└─────────────────┘
       ↓
┌─────────────────┐
│   Bronze Layer  │  ← PySpark Jobs (Cleaning & Validation)
│   (S3 + Catalog)│
└─────────────────┘
       ↓
┌─────────────────┐
│   Silver Layer  │  ← PySpark Jobs (Business Logic)
│   (S3 + Catalog)│
└─────────────────┘
       ↓
┌─────────────────┐
│   Gold Layer    │  ← PySpark Jobs (Aggregation)
│   (S3 + Catalog)│
└─────────────────┘
       ↓
┌─────────────────┐
│   Analytics     │  ← Athena Queries
│   (Athena)      │
└─────────────────┘
```

## Component Architecture

### S3 Data Lake Structure

```
s3://glue-poc-data-lake-{env}-{suffix}/
├── raw-zone/                    # Raw ingested data
│   └── teamtailor/
│       ├── candidates/         # Candidate data partitioned by date
│       ├── jobs/               # Job postings data
│       └── applications/       # Application data
├── bronze/                      # Cleaned and validated data
│   └── teamtailor_talent/      # Partitioned Iceberg tables
├── silver/                      # Business logic applied
│   └── talent_dim_*/          # Talent dimension tables
├── gold/                        # Analytics-ready aggregated data
│   └── analytics/
├── jobs/                        # Glue job scripts
│   ├── raw/                    # Python Shell scripts
│   ├── bronze/                 # PySpark ETL scripts
│   ├── silver/                 # PySpark transformation scripts
│   └── utils/                  # Utility scripts
├── libs/                        # Shared Python libraries
└── athena-results/             # Query result cache
```

### Glue Jobs Architecture

#### Job Types and Responsibilities

**Python Shell Jobs (Raw Zone)**
- **Purpose**: Data ingestion from external APIs
- **Configuration**: 0.0625 DPU (cost-optimized)
- **Use Cases**: API calls, file downloads, simple transformations
- **Examples**: Team Tailor API ingestion, external data extraction

**PySpark Jobs (Bronze/Silver/Gold)**
- **Purpose**: Large-scale data processing and transformation
- **Configuration**: G.1X workers, 2+ nodes
- **Use Cases**: Data cleaning, validation, complex transformations
- **Examples**: Event processing, data quality checks, aggregations

### Glue Catalog Architecture

```
Databases:
├── {env}_raw_zone              # Raw data schemas
├── {env}_bronze_processing     # Cleaned data schemas  
├── {env}_silver_analytics      # Transformed data schemas
└── {env}_gold_analytics        # Aggregated data schemas

Tables (Auto-discovered):
├── teamtailor__talent__candidates_bronze  # Candidate data with partitions
├── teamtailor__talent__jobs_bronze        # Job postings data
├── talent_dim_candidate_profiles          # Silver candidate dimension
└── talent_time_to_fill_gold              # Gold analytics tables
```

## Infrastructure Architecture

### AWS Services Used

**Core Services**
- **AWS Glue**: ETL jobs and data catalog
- **Amazon S3**: Data lake storage
- **Amazon Athena**: SQL queries and analytics
- **CloudWatch**: Monitoring and logging

**Supporting Services**
- **IAM**: Access control and permissions
- **AWS Cost Explorer**: Cost tracking and optimization

### Network Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS Account                          │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   AWS Glue      │  │   Amazon S3     │  │  CloudWatch  │ │
│  │                 │  │                 │  │              │ │
│  │ • Jobs          │  │ • Data Lake     │  │ • Logs       │ │
│  │ • Catalog       │  │ • Scripts       │  │ • Metrics    │ │
│  │ • Crawlers      │  │ • Results       │  │ • Alarms     │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
│           │                     │                   │       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │      IAM        │  │   Amazon        │  │ Cost Explorer│ │
│  │                 │  │   Athena        │  │              │ │
│  │ • Roles         │  │                 │  │ • Budgets    │ │
│  │ • Policies      │  │ • Workgroups    │  │ • Reports    │ │
│  │ • Permissions   │  │ • Queries       │  │ • Alerts     │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Security Architecture

### IAM Role Structure

```
aws_iam_role.glue_service_role
├── AWS Managed Policies:
│   └── AWSGlueServiceRole
└── Custom Policies:
    ├── S3 Access Policy (data lake bucket)
    ├── Glue Catalog Policy (databases and tables)
    └── CloudWatch Logs Policy (job logging)
```

### Security Controls

**Data Protection**
- S3 server-side encryption (AES256)
- No public bucket access
- Bucket policies for service access only

**Access Control**
- Least privilege IAM roles
- Service-specific permissions
- No cross-account access

**Monitoring**
- CloudWatch logging for all jobs
- Cost tracking and alerting
- Resource tagging for governance

## Scalability Architecture

### Horizontal Scaling

**Glue Jobs**
- Auto-scaling worker nodes for PySpark jobs
- Configurable DPU allocation
- Job parallelization support

**S3 Storage**
- Unlimited storage capacity
- Automatic partitioning
- Lifecycle policies for cost optimization

### Vertical Scaling

**Worker Configuration**
- G.1X: 4 vCPU, 16 GB RAM (cost-optimized)
- G.2X: 8 vCPU, 32 GB RAM (performance)
- G.4X: 16 vCPU, 64 GB RAM (high-performance)

**Python Shell Configuration**
- 0.0625 DPU: Minimal cost
- 1 DPU: Standard processing

## Monitoring Architecture

### CloudWatch Integration

```
Job Execution Metrics
├── /aws-glue/jobs/{job-name}        # Application logs
├── /aws-glue/jobs/error/{job-name}  # Error logs  
└── /aws-glue/jobs/output/{job-name} # Output logs

Custom Metrics
├── Job Success Rate
├── Data Processing Volume
├── Cost per Job Run
└── Error Rate by Job Type
```

### Cost Tracking Architecture

**Tagging Strategy**
- Environment-based cost allocation
- Component-based tracking
- Team/owner identification
- Automated cleanup identification

**Cost Controls**
- S3 lifecycle policies
- Athena query limits
- Log retention policies
- Resource auto-shutdown tags

## Deployment Architecture

### Infrastructure as Code

```
terraform/
├── main.tf          # Provider and common configuration
├── variables.tf     # Input variables and validation
├── s3.tf           # Data lake and storage
├── glue.tf         # Jobs and catalog
├── iam.tf          # Permissions and roles
├── athena.tf       # Query engine (optional)
├── cloudwatch.tf   # Monitoring and logging
└── outputs.tf      # Resource references and information
```

### Environment Separation

**Resource Naming Convention**
- `{project}-{component}-{environment}`
- Example: `glue-poc-data-lake-poc-a1b2c3d4`

**Environment-Specific Configuration**
- Separate terraform.tfvars per environment
- Environment-specific resource sizing
- Different retention and lifecycle policies

## Performance Architecture

### Data Processing Optimization

**Partitioning Strategy**
- Date-based partitioning for time-series data
- Source-based partitioning for multi-source data
- Columnar storage (Parquet) for analytics

**Job Optimization**
- Incremental processing with job bookmarks
- Parallel processing for independent datasets
- Resource allocation based on data volume

### Query Performance

**Athena Optimization**
- Columnar data formats (Parquet)
- Partition pruning
- Query result caching
- Workgroup-based resource management

This architecture provides a scalable, cost-effective, and maintainable data pipeline solution suitable for proof-of-concept and production workloads.
