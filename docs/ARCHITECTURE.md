# Architecture

## Layers

- `jobs/raw/`: extraction from public/external APIs to S3 raw zone
- `jobs/bronze/`: normalization and schema shaping in Spark
- `jobs/silver/`: standardized records and data quality normalization
- `jobs/gold/`: simple business-level aggregates

## Shared libraries

- `libs/common/`: shared config, env helpers, logging, reusable utils
- `libs/pyshell/`: Python Shell base jobs for extraction
- `libs/pyspark/`: Spark session + Medallion base classes

## Deployment boundary

Runtime code lives in this repository. Infrastructure provisioning is expected to be handled by dedicated IaC under `deployment/` in future phases.
