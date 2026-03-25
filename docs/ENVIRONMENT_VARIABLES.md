# Environment Variables Configuration

This document describes the environment variables used in the NaNLABS Data Warehouse platform.

## Overview

The platform uses environment variables to configure AWS infrastructure, database names, API settings, and other runtime parameters. This approach provides flexibility across different environments (local, development, staging, production) without hardcoding values in the source code.

## Environment Files

### `.envrc` - Environment Configuration (direnv)
The `.envrc` file uses `direnv` to automatically load environment variables. It includes a `load_localstack_params()` function that reads configuration parameters from LocalStack SSM Parameter Store when `STAGE=local`.

**Configuration Variables** (set at the top of `.envrc` or via `.env`):
- `AWS_ACCOUNT_NAME` - Account name for AWS resources (e.g., `workloads-data-lake-develop`)
- `ENVIRONMENT` - Environment name (develop, staging, prod)
- `STAGE` - Stage name (local, dev, staging, prod). Use `STAGE=local` for local development

**Local Development Mode (STAGE=local)**:
- When `STAGE=local`, all services use LocalStack (S3, Glue, Secrets Manager, SSM)
- `.envrc` configures LocalStack endpoints and calls `load_localstack_params()` to load parameters from LocalStack SSM
- Set `STAGE=local` in `.envrc`, then run `direnv allow` to enable automatic loading
- Run `make prepare-localstack` to sync secrets and SSM parameters from AWS real to LocalStack
- When `STAGE` is not `local`, all services use real AWS

**`load_localstack_params()` Function**:
This function automatically loads the following parameters from LocalStack SSM Parameter Store:
- `WAREHOUSE_PATH` - Data warehouse S3 path
- `RAW_ZONE_PATH` - Raw zone S3 path
- `RAW_DATABASE_NAME` - Raw database name
- `BRONZE_DATABASE_NAME` - Bronze database name
- `SILVER_DATABASE_NAME` - Silver database name
- `GOLD_DATABASE_NAME` - Gold database name
- `TEAMTAILOR_API_BASE_URL_PARAM` - TeamTailor API base URL parameter name

**Parameter Store Path Patterns**:
- Data Lake config: `/nan-wl-{account}/data-lake/{parameter-name}`
- Direct params: `/nan-wl-{account}/{parameter-name}`

**Example**: For `workloads-data-lake-develop` account, parameters are stored as:
- `/nan-wl-workloads-data-lake-develop/data-lake/storage_bucket_name`
- `/nan-wl-workloads-data-lake-develop/data-lake/raw_database_name`
- `/nan-wl-workloads-data-lake-develop/teamtailor-api-base-url`

**How it works**:
1. `.envrc` configures LocalStack endpoints (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL_GLUE`, etc.)
2. Calls `load_localstack_params()` to read parameters from LocalStack SSM Parameter Store
3. Loads `.env` file for additional overrides
4. Use `make prepare-localstack` to sync secrets and SSM parameters from AWS real to LocalStack
5. Jobs read from LocalStack when `STAGE=local`

### `.env` - Local Development Configuration
Contains variables for local development. For AWS environments, most values are automatically loaded from Parameter Store via `.envrc`.

**Important**: Docker Compose reads `.env` directly from the filesystem (via `env_file` directive). This is **independent of direnv** - Docker Compose doesn't use direnv, it reads the file directly when containers start.

**Required Variables**:
- `STAGE=local` - **REQUIRED**: Enables local development mode with LocalStack
- `SFTP_KEY_NAME` - **REQUIRED**: Name for SFTP keys (used by docker-compose services)
- `SFTP_KEY_TYPE` - **REQUIRED**: SSH key type (ed25519, rsa, ecdsa)
- `SFTP_KEY_COMMENT` - **REQUIRED**: Comment for SSH key generation

**Optional Variables** (with defaults):
- `AWS_ACCOUNT_NAME` - Defaults to `workloads-data-lake-develop`. Determines Parameter Store path
- `LOCALSTACK_ENDPOINT` - Defaults to `http://localstack:4566`. Only needed if LocalStack runs on different endpoint
- `AWS_REGION` - Defaults to `us-east-1` for LocalStack (set automatically by Makefile). LocalStack works with any region
- `LOAD_AWS_CONFIG` - Defaults to `true`. Set to `false` to skip loading config from Parameter Store
- `SFTP_KEYS_DIR` - Defaults to `/keys` (used by docker-compose init container)
- `SFTP_USER` - Defaults to `sftpuser` (used by docker-compose sftp-server)
- `SFTP_PORT` - Defaults to `2222` (used by docker-compose sftp-server)

**Variables Automatically Configured** (when `STAGE=local`):
- `AWS_ENDPOINT_URL_S3=http://localstack:4566` (S3 uses LocalStack)
- `AWS_ENDPOINT_URL_GLUE=http://localstack:4566` (Glue uses LocalStack)
- `AWS_ENDPOINT_URL_SECRETSMANAGER=http://localstack:4566` (Secrets Manager uses LocalStack)
- `AWS_ENDPOINT_URL_SSM=http://localstack:4566` (SSM uses LocalStack)
- `AWS_PROFILE` (derived from `AWS_ACCOUNT_NAME`, used for syncing secrets from AWS real)
- **Note**: `AWS_ENDPOINT_URL` general is NOT set to avoid conflicts

**Variables Set by Makefile Targets** (`make run-raw`, `make run-bronze`, etc.):
- `STAGE=local`
- `AWS_REGION=us-east-1`
- `AWS_DEFAULT_REGION=us-east-1`
- `AWS_ACCESS_KEY_ID=test` (for LocalStack)
- `AWS_SECRET_ACCESS_KEY=test` (for LocalStack)
- All service-specific endpoints point to LocalStack

**See**: `.env.example` for a complete template with all options.

### `.env.example` - Template File
Minimal template file for local development. Copy to `.env` and customize as needed. Most configuration is loaded from AWS Parameter Store via `.envrc` which uses AWS CLI directly.

## Variable Categories

### AWS Configuration
```bash
AWS_PROFILE=your-aws-profile-name  # Auto-derived from AWS_ACCOUNT_NAME in local mode
AWS_REGION=us-east-1  # LocalStack: us-east-1 (any region works), Real AWS: us-west-2
AWS_DEFAULT_REGION=us-east-1  # Alternative to AWS_REGION
```

**Service-Specific Endpoints** (for LocalStack in local mode):
```bash
# LocalStack endpoints (set automatically when STAGE=local)
AWS_ENDPOINT_URL_S3=http://localstack:4566
AWS_ENDPOINT_URL_GLUE=http://localstack:4566
AWS_ENDPOINT_URL_SECRETSMANAGER=http://localstack:4566
AWS_ENDPOINT_URL_SSM=http://localstack:4566
```

**Important**: We do NOT use `AWS_ENDPOINT_URL` general to avoid conflicts.
Use service-specific endpoints (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL_GLUE`, etc.) instead.

**Local Development Mode**: When `STAGE=local`, the environment automatically configures:
- **LocalStack** for all services (S3, Glue, Secrets Manager, SSM) via service-specific endpoints
- **Secret Synchronization**: Run `make prepare-localstack` to sync secrets and SSM parameters from AWS real to LocalStack
- This allows jobs to run completely locally without requiring AWS credentials during execution
- Secrets are synced once at startup, then all services read from LocalStack

**Alternative**: Use `awslocal` CLI command for LocalStack access:
```bash
# Instead of: aws s3 ls
awslocal s3 ls

# awslocal automatically uses LocalStack endpoint
```

**Note**: The `create_boto3_client()` utility function automatically detects the AWS region using the following priority:
1. Explicit `region_name` parameter
2. `AWS_REGION` or `AWS_DEFAULT_REGION` environment variables
3. Automatic detection via `boto3.Session()` (works in AWS Glue using IAM role metadata)
4. Fallback to `us-east-1` (with warning)

In AWS Glue, the region is typically auto-detected from the IAM role, so setting `AWS_REGION` is optional but recommended for explicit control.

### Infrastructure Configuration
```bash
AWS_S3_BUCKET=your-data-lake-bucket
AWS_SECRETS_PREFIX=your-secrets-prefix
PROJECT_NAME=yourproject
ENVIRONMENT=develop
STAGE=dev
```

### Database Configuration
```bash
RAW_DATABASE_NAME=${PROJECT_NAME}_raw_zone
BRONZE_DATABASE_NAME=${PROJECT_NAME}_bronze_zone
SILVER_DATABASE_NAME=${PROJECT_NAME}_silver_zone
GOLD_DATABASE_NAME=${PROJECT_NAME}_gold_zone
```

### S3 Configuration
```bash
WAREHOUSE_PATH=s3://${AWS_S3_BUCKET}/${PROJECT_NAME}/
RAW_ZONE_PATH=s3://${AWS_S3_BUCKET}/raw-zone/
```

### API Configuration
```bash
# Team Tailor API Settings
TEAMTAILOR_API_BASE_URL=https://api.teamtailor.com
TEAMTAILOR_RATE_LIMIT_PER_SECOND=2.0
TEAMTAILOR_MAX_PAGES_PER_BATCH=20
TEAMTAILOR_MAX_RETRIES=3
TEAMTAILOR_BATCH_SIZE=1000
TEAMTAILOR_API_SECRET_NAME=nan-data-lake-develop/teamtailor-api
TEAMTAILOR_API_TOKEN=test-api-token-for-local-development  # For local development only

# SFTP Settings (for future data sources)
# SFTP client utilities are available in libs/common/utils/sftp_utils.py
SFTP_PORT=22
SFTP_MIN_FILE_SIZE_BYTES=1000
SFTP_ENABLE_DATA_VALIDATION=true
```

## Parameter Resolution Priority

The system uses a **three-tier parameter resolution system** with current precedence:

1. **Command Line Parameters** (highest priority) - Explicitly passed parameters
   ```bash
   python job.py --ENTITY_TYPE profiles --START_DATE 2024-01-01
   ```

2. **Environment Variables** (medium priority) - Set in environment files
   ```bash
   export RAW_ZONE_PATH=s3://bucket/raw-zone/
   export WAREHOUSE_PATH=s3://bucket/warehouse/
   export TEAMTAILOR_API_SECRET_NAME=my-secret
   ```

3. **Default Values** (lowest priority) - Sensible defaults for non-critical fields
   ```python
   api_revision: str = Field(default="2024-02-15")
   ```

4. **Required Parameters** (fail fast if missing)
   ```python
   raw_zone_path: str = Field(default_factory=lambda: _get_required_env_var("RAW_ZONE_PATH"))
   # Raises ValueError if not provided
   ```

### Job Config Parameters (Environment Variables)

#### Required Parameters (fail if not present)
- `RAW_ZONE_PATH`: Full S3 path to raw zone location
- `WAREHOUSE_PATH`: S3 path for Iceberg warehouse  
- `RAW_DATABASE_NAME`: Raw database name
- `BRONZE_DATABASE_NAME`: Bronze database name
- `TEAMTAILOR_API_SECRET_NAME`: AWS Secrets Manager secret name

#### Optional Parameters (use defaults if not present)
- `TEAMTAILOR_API_BASE_URL`: API base URL (default: "https://api.teamtailor.com")
- `TEAMTAILOR_RATE_LIMIT_PER_SECOND`: Rate limit per second (default: 2.0)
- `TEAMTAILOR_MAX_PAGES_PER_BATCH`: Max pages per batch (default: 20)
- `TEAMTAILOR_MAX_RETRIES`: Maximum retries (default: 3)
- `TEAMTAILOR_BATCH_SIZE`: Batch size for S3 writes (default: 1000)

### Job Run Parameters (Command Line)

#### Required Parameters (always passed as parameters)
- `JOB_NAME`: Job name
- `ENTITY_TYPE`: Entity type (profiles, events, campaigns, etc.)

#### Optional Parameters (passed as parameters when needed)
- `START_DATE`: Start date for extraction (YYYY-MM-DD)
- `END_DATE`: End date for extraction (YYYY-MM-DD)

### SFTP Configuration (for future data sources)

SFTP client utilities are available in `libs/common/utils/sftp_utils.py` for future data sources that require SFTP connectivity.

#### Environment Variables
- `SFTP_PORT`: SFTP port (default: 22)
- `SFTP_MIN_FILE_SIZE_BYTES`: Minimum file size (default: 1000)
- `SFTP_ENABLE_DATA_VALIDATION`: Enable data validation (default: true)

### Auto-configured Parameters (from environment variables)
- `raw_database`: From `RAW_DATABASE_NAME`
- `bronze_database`: From `BRONZE_DATABASE_NAME`
- `raw_table`: Auto-configured as `teamtailor__talent__{entity_type}_raw`
- `bronze_table`: Auto-configured as `teamtailor__talent__{entity_type}_bronze`
- `teamtailor_raw_path`: Auto-configured based on entity type
- `source_name`: Hardcoded as "teamtailor" (part of job essence)

### Optional Parameters (consumed from environment variables)
- `WAREHOUSE_S3_LOCATION`: S3 location for Iceberg warehouse
- `RAW_S3_PATH`: Complete S3 path for raw data
- `AWS_REGION`: AWS region for services (optional: auto-detected in AWS Glue)
- `ENVIRONMENT`: Environment name (develop, staging, prod)
- `STAGE`: Stage name (local, dev, staging, prod)

### 1. Setup Environment

> **💡 For Data Engineers**: Most of this is handled automatically by the DevContainer. You only need to run `direnv allow` and `make prepare-localstack` once per session. See [Development Guide](./DEVELOPMENT.md) for the simple workflow.

#### Prerequisites

- AWS CLI installed and configured (handled by DevContainer)
- AWS SSO profiles configured (for accessing Parameter Store) - **only needed for `make prepare-localstack`**
- direnv installed (optional, but recommended - handled by DevContainer)

#### For Local Development
```bash
# Copy template
cp .env.example .env

# Customize values for local development (minimal - most config comes from Parameter Store)
vim .env

# Set STAGE=local to enable LocalStack
echo "STAGE=local" >> .env

# direnv will automatically load .envrc (which loads .env and AWS SSM) when you cd into the directory
# Run 'direnv allow' to enable automatic loading
```

#### For AWS Environments (develop, staging, prod)
```bash
# Set the account name (defaults to 'workloads-data-lake-develop' if not set)
export AWS_ACCOUNT_NAME=workloads-data-lake-develop  # or staging, prod

# Ensure AWS credentials are configured
export AWS_PROFILE=nan-wl-${AWS_ACCOUNT_NAME}-terraform-execution
aws sso login --profile $AWS_PROFILE

# direnv will automatically load parameters from AWS SSM Parameter Store
# Run 'direnv allow' to enable automatic loading
```

**Note**:
- Configuration is automatically loaded from AWS SSM Parameter Store using AWS CLI
- Parameters are loaded directly via `aws ssm get-parameter` and `aws ssm get-parameters-by-path`
- For local development, `load_localstack_params()` reads from LocalStack SSM Parameter Store

### 2. Run Jobs

Jobs are executed through AWS Glue console or infrastructure automation. See [OPERATIONS.md](./OPERATIONS.md) for deployment and execution procedures.

## Required Parameters

Jobs now use a clear separation between job config and job run parameters:

### Raw Job Required Parameters
- `--job_name`: Job name
- `--entity_type`: Entity type to extract
- `--start_date`: Start date for extraction (optional for some entities)

### Bronze Job Required Parameters  
- `--job_name`: Job name
- `--entity_type`: Entity type to process

### Silver Job Required Parameters
- `--entity_type`: Entity type to process

## Environment-Specific Values

### Development Environment
```bash
PROJECT_NAME=nan
AWS_S3_BUCKET=nan-data-lake-develop-storage
AWS_SECRETS_PREFIX=nan-data-lake-develop
WAREHOUSE_PATH=s3://nan-data-lake-develop-storage/nan/
RAW_ZONE_PATH=s3://nan-data-lake-develop-storage/raw-zone/
```

### LocalStack Environment
```bash
PROJECT_NAME=nan
AWS_S3_BUCKET=local-bucket
AWS_ENDPOINT_URL_S3=http://localstack:4566
AWS_ENDPOINT_URL_GLUE=http://localstack:4566
WAREHOUSE_PATH=s3://local-bucket/nan/
RAW_ZONE_PATH=s3://local-bucket/raw-zone/
```

## Best Practices

1. **Never commit environment files** with real credentials
2. **Use .env.example** as a template for local development (copy to `.env`)
3. **Use .envrc.example** as a template for environment setup (copy to `.envrc`)
4. **Source environment** before running any scripts (direnv does this automatically)
5. **Most configuration** comes from AWS Parameter Store via direct AWS CLI calls
6. **Use consistent naming** across environments

## Parameter Structure

### Key Features

1. **Required `raw_zone_path`**: Full S3 path to raw zone location (from Terraform)
2. **Required `warehouse_path`**: S3 path for Iceberg warehouse (from Terraform)
3. **Current Precedence System**: Uses existing `_get_required_env_var()` vs `os.getenv()` pattern
4. **Job Config vs Job Run Separation**: Clear distinction between infrastructure and execution parameters

### Parameter Flow

```bash
# Job Config Parameters (Glue/Env vars):
RAW_ZONE_PATH=s3://nan-data-lake-develop-storage/raw-zone
WAREHOUSE_PATH=s3://nan-data-lake-develop-storage/
TEAMTAILOR_API_SECRET_NAME=nan-data-lake/teamtailor-api
    ↓
raw_zone_path = _get_required_env_var("RAW_ZONE_PATH")  # Required - fails if not present
warehouse_path = _get_required_env_var("WAREHOUSE_PATH")  # Required - fails if not present
api_secret_name = _get_required_env_var("TEAMTAILOR_API_SECRET_NAME")  # Required - fails if not present

# Job Run Parameters (Command line):
--JOB_NAME teamtailor_raw_candidates --ENTITY_TYPE candidates --START_DATE 2024-01-01
    ↓
job_name = "teamtailor_raw_candidates"  # Always passed as parameter
entity_type = "candidates"  # Always passed as parameter
start_date = "2024-01-01"  # Optional parameter
```

## Troubleshooting

### Common Issues

1. **Missing required parameters**: Jobs will fail with validation errors if required parameters are not provided
2. **Environment not loaded**: Run `direnv allow` to enable automatic environment loading
3. **Wrong AWS profile**: Ensure `AWS_PROFILE` is set correctly for your environment
4. **LocalStack conflicts**: Service-specific endpoints (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL_GLUE`) don't affect real AWS services

### Validation Commands
```bash
# Enable direnv automatic loading (run once per directory):
direnv allow

# Verify parameters loaded from Parameter Store
echo "AWS_S3_BUCKET: $AWS_S3_BUCKET"
echo "PROJECT_NAME: $PROJECT_NAME"
echo "ENVIRONMENT: $ENVIRONMENT"

# Test job configuration
python -c "
from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig
config = TeamTailorRawConfig(
    job_name='test',
    entity_type='candidates',
    start_date='2024-01-01'
)
print('✅ Configuration valid')
"
```

### Loading Configuration from Parameter Store

The `.envrc` file automatically loads the following parameters from AWS Parameter Store when not in local development mode:

| Environment Variable | Parameter Store Path | Description |
|---------------------|---------------------|-------------|
| `AWS_S3_BUCKET` | `/nan-data-lake-{account}/data-lake/storage_bucket_name` | S3 bucket name for data storage |
| `AWS_REGION` | `/nan-data-lake-{account}/data-lake/aws_region` | AWS region |
| `ENVIRONMENT` | `/nan-data-lake-{account}/data-lake/environment` | Environment name (develop/staging/prod) |
| `RAW_ZONE_PATH` | `/nan-data-lake-{account}/data-lake/raw_zone_path` | S3 path to raw zone |
| `WAREHOUSE_PATH` | `/nan-data-lake-{account}/data-lake/warehouse_path` | S3 path to Iceberg warehouse |
| `RAW_DATABASE_NAME` | `/nan-data-lake-{account}/data-lake/raw_database_name` | Raw database name |
| `BRONZE_DATABASE_NAME` | `/nan-data-lake-{account}/data-lake/bronze_database_name` | Bronze database name |
| `SILVER_DATABASE_NAME` | `/nan-data-lake-{account}/data-lake/silver_database_name` | Silver database name |
| `GOLD_DATABASE_NAME` | `/nan-data-lake-{account}/data-lake/gold_database_name` | Gold database name |
| `TEAMTAILOR_API_BASE_URL` | `/nan-data-lake-{account}/teamtailor-api-base-url` | Team Tailor API base URL |

**Derived Variables** (automatically set):
- `AWS_SECRETS_PREFIX` = `nan-data-lake-{account}` (derived from account name)
- `STAGE` = `ENVIRONMENT` (if not set separately)
- `GLUE_JOB_RAW_DATABASE` = `RAW_DATABASE_NAME`
- `GLUE_JOB_BRONZE_DATABASE` = `BRONZE_DATABASE_NAME`

**Default Values** (used if not in Parameter Store, can be overridden in `.env`):
- `PROJECT_NAME` = `nan`
- `GLUE_JOB_SOURCE_NAME` = `teamtailor`
- `TEAMTAILOR_RATE_LIMIT` = `2.0`
- `TEAMTAILOR_MAX_PAGES` = `20`
- `SPARK_LOCAL_DIRS` = `/tmp/spark-temp`
- `PYSPARK_PYTHON` = `python3`

**Account Selection**: The account is determined by:
1. `AWS_ACCOUNT_NAME` environment variable (highest priority)
2. `ENVIRONMENT` environment variable (mapped: local/dev → develop)
3. Default: `develop`

**Disable Auto-Loading**: To disable automatic loading from Parameter Store, set:
```bash
export LOAD_AWS_CONFIG=false
```
