# Development Guide

> **⚠️ IMPORTANT**: This project ONLY supports development with Dev Containers. All development, testing, and job execution happens inside the dev container.

## Quick Start

### Prerequisites

- **[VS Code](https://code.visualstudio.com/)** with [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers), OR
- **[Dev Container CLI](https://containers.dev/supporting#devcontainer-cli)** for non-VS Code users

### Setup (One-Time)

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd internal-data-lake-jobs
   ```

2. **Open in Dev Container**:
   - **VS Code**: Open project → Click **"Reopen in Container"** when prompted
   - **CLI**: `devcontainer up --workspace-folder .`

3. **Configure environment** (inside dev container):

   ```bash
   # Copy .envrc.example to .envrc if not already done
   cp .envrc.example .envrc

   # Enable direnv automatic environment loading
   direnv allow

   # Prepare LocalStack with secrets and SSM parameters from AWS
   make prepare-localstack
   ```

4. **Verify setup** (inside dev container):

   ```bash
   make services-status  # Check all services are running
   make validate         # Validate environment
   ```

## Development Environment

The dev container provides a complete local development environment:

| Service | URL | Purpose |
|---------|-----|---------|
| **LocalStack** | <http://localhost:4566> | AWS services simulation (S3, Glue, IAM) |
| **Jupyter Lab** | <http://localhost:8888> | Interactive development |
| **Spark UI** | <http://localhost:4040> | Spark job monitoring (when jobs run) |
| **SFTP Server** | sftp://localhost:2222 | Test file transfers |

**Important**: When working with AWS services in the dev container, use `awslocal` instead of `aws` commands. The `awslocal` command automatically points to LocalStack.

### Local Development Mode: All Services Use LocalStack

When `STAGE=local`, all services use LocalStack:

- **LocalStack** is used for:
  - S3 storage (local file system)
  - Glue Data Catalog (Derby metastore)
  - Secrets Manager (secrets synced from AWS real)
  - SSM Parameter Store (parameters synced from AWS real)
  - IAM/STS (for testing)

**Environment Configuration**: The `.envrc` file uses `direnv` to automatically load environment variables. It includes a `load_localstack_params()` function that reads configuration parameters from LocalStack SSM Parameter Store when `STAGE=local`.

**Secret Synchronization**: Secrets and SSM parameters are synced from AWS real to LocalStack. Run `make prepare-localstack` to sync manually. This script also creates S3 buckets and Glue databases in LocalStack based on SSM parameters.

**Configuration Flow**:

1. `.envrc` configures LocalStack endpoints and calls `load_localstack_params()` to load parameters from LocalStack SSM
2. Parameters loaded include: `WAREHOUSE_PATH`, `RAW_ZONE_PATH`, database names, and API base URLs
3. When `STAGE=local`, all AWS services point to LocalStack
4. When `STAGE` is not `local`, services use real AWS

**Manual Operations**:

- `make prepare-localstack` - Sync secrets and SSM parameters from AWS real to LocalStack, create S3 buckets and Glue databases
- `make clean-localstack` - Clean all LocalStack resources (secrets, SSM, S3, Glue databases)
- `make aws-login` - Login to AWS SSO for data workload accounts (temporarily unsets LocalStack endpoints; wraps `./scripts/aws-login.sh`)

**Note**:

- Run `direnv allow` first to enable automatic environment loading from `.envrc`
- If AWS credentials are not available, run `make aws-login` before `make prepare-localstack`
- The `load_localstack_params()` function in `.envrc` automatically loads required parameters from LocalStack SSM when available

## Running Jobs Locally

All jobs run inside the dev container using Makefile commands.

### Medallion Jobs by Tier (Recommended)

Use the tier-specific targets which configure LocalStack:

```bash
# Raw Tier
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--MAX_PAGES_PER_BATCH 2"

# Bronze Tier
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--CREATE_TABLES true"

# Silver Tier
make run-silver DATA_SOURCE=teamtailor ENTITY_TYPE=candidate_profiles \
  ARGS="--CREATE_TABLES true"

# Gold Tier
make run-gold DATA_SOURCE=teamtailor ENTITY_TYPE=talent_time_to_fill \
  ARGS="--CREATE_TABLES true"
```

**What these targets do**:

- Rely on `direnv` to load environment variables (including LocalStack endpoints)
- Set `STAGE=local` and unset `AWS_PROFILE` to ensure LocalStack is used
- Execute the job with proper arguments

**Prerequisites**:

- Environment variables loaded via `direnv` (run `direnv allow` once)
- Secrets and SSM parameters synced to LocalStack (run `make prepare-localstack` before first use)
- S3 buckets and Glue databases created in LocalStack (done automatically by `prepare-localstack`)

### Legacy Job Execution (Still Supported)

```bash
# PyShell Jobs
make pyshell-run JOB=jobs/raw/teamtailor_raw_job.py \
  ARGS="--ENTITY_TYPE candidates --START_DATE 2025-01-01"

# Spark Jobs
make spark-submit JOB=jobs/bronze/teamtailor_bronze_job.py \
  ARGS="--CREATE_TABLES true --SHOW_COUNTS true --ENTITY_TYPE candidates"
```

### Example: Complete Pipeline

```bash
# 1. Run Raw Job (API → S3)
make run-raw DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--MAX_PAGES_PER_BATCH 2"

# 2. Run Bronze Job (S3 → Iceberg)
make run-bronze DATA_SOURCE=teamtailor ENTITY_TYPE=candidates \
  ARGS="--CREATE_TABLES true"

# 3. Run Silver Job (Bronze → Silver)
make run-silver DATA_SOURCE=teamtailor ENTITY_TYPE=candidate_profiles \
  ARGS="--CREATE_TABLES true"

# 4. Run Gold Job (Silver → Gold)
make run-gold DATA_SOURCE=teamtailor ENTITY_TYPE=talent_time_to_fill \
  ARGS="--CREATE_TABLES true"
```

## Development Workflow

### 1. Job Development

#### Creating New Jobs

- Follow established patterns: PySpark jobs extend `MedallionJobBase`, PyShell jobs extend `PyShellJobBase`
- Create configuration classes using Pydantic
- Create both unit and integration tests

#### Job Execution Pattern

**PySpark Jobs:**

```python
from libs.pyspark import MedallionJobBase, SparkSessionFactory
from libs.common import BronzeJobConfig

class YourBronzeJob(MedallionJobBase):
    def extract(self):
        return self.read_external_table(
            database=self.config.raw_database,
            table=self.config.raw_table
        )

    def transform(self, df):
        # Your transformation logic
        return df

    def load(self, df):
        self.write_to_iceberg(
            df=df,
            database=self.config.bronze_database,
            table=self.config.bronze_table
        )

if __name__ == "__main__":
    session = SparkSessionFactory.create_session()
    config = BronzeJobConfig.from_args()
    job = YourBronzeJob(session.spark, config)
    job.run()
    session.spark.stop()
```

**PyShell Jobs:**

```python
from libs.pyshell import PyShellJobBase
from libs.common import RawJobConfig

class YourPyShellJob(PyShellJobBase):
    def extract(self):
        ...

    def load(self, data):
        ...

if __name__ == "__main__":
    config = RawJobConfig.from_args()
    job = YourPyShellJob(config)
    job.run()
```

#### Job Types

- **Spark Jobs**: For data processing with Spark (Bronze, Silver, Gold layers)
- **Python Shell Jobs**: For API calls and simple data extraction (Raw zone)
- **Utility Jobs**: For migrations, catalog exploration, etc.

### 2. Testing

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests only
make test-integration

# Run with coverage
make test-coverage

# Run linting
make lint
```

See [Testing Guidelines](./TESTING.md) for detailed testing patterns.

### 3. Code Quality

```bash
# Format code
make format

# Auto-fix formatting and imports
make autofix

# Lint code
make lint
```

### 4. Working with LocalStack

```bash
# Use awslocal instead of aws commands
awslocal s3 ls s3://local-bucket/
awslocal s3 cp file.txt s3://local-bucket/path/

# Check service status
make services-status
```

## Code Standards

### Spark Runtime Dependency

`pyspark` is installed at image build time in the `Dockerfile` (pinned to AWS Glue 5 / Spark 3.5 compatible version). Do not add `pyspark` to Pipenv—upgrade is handled via container rebuild.

### File Organization

- **Jobs**: `jobs/{layer}/{job_name}.py`
- **Libraries**: `libs/{category}/{module_name}.py`
- **Tests**: `tests/{unit|integration}/test_{job_name}.py`
- **Migrations**: `migrations/{id}_{description}.sql`

### Naming Conventions

- **Jobs**: `{source}_{entity}_{layer}_{function}.py`
- **Tables**:
  - **Raw Layer**: `{source}_{entity}_raw` (e.g., `teamtailor__talent__candidates_raw`)
  - **Bronze Layer**: `{source}_{entity}_bronze` (e.g., `teamtailor__talent__candidates_bronze`)
  - **Silver Layer**: `{source}_{business_entity}_silver` (e.g., `talent_dim_candidate_profiles`)
  - **Gold Layer**: `{business_domain}_{entity_type}` (e.g., `customer_profile`, `sales_fact`)
- **Functions**: `snake_case`
- **Classes**: `PascalCase`

**Table Naming Rules**:

- Use singular nouns for entities (`event` not `events`)
- Raw layer: `_raw` suffix
- Bronze layer: `_validated` suffix
- Silver layer: `_clean` suffix
- Gold layer: business domain prioritized, source hidden
- Maximum 63 characters for compatibility

### Documentation

- All functions must have docstrings
- Complex logic requires inline comments
- Update relevant documentation for new patterns

## Environment Configuration

### Local Development (STAGE=local)

When `STAGE=local` is set in `.envrc`, the environment automatically:

- Sets LocalStack endpoints for all AWS services (`AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL_GLUE`, `AWS_ENDPOINT_URL_SECRETSMANAGER`, `AWS_ENDPOINT_URL_SSM`)
- Uses test credentials (`AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`)
- Calls `load_localstack_params()` to load configuration parameters from LocalStack SSM Parameter Store
- All AWS services (S3, Glue, Secrets Manager, SSM) point to LocalStack

### Environment Variables

| Variable | Local | Cloud | Purpose |
|----------|-------|-------|---------|
| `STAGE` | `local` (required) | `dev/staging/prod` | Environment indicator |
| `AWS_ENDPOINT_URL_S3` | `http://localstack:4566` (auto-set) | **UNSET** | LocalStack S3 endpoint |
| `AWS_ENDPOINT_URL_GLUE` | `http://localstack:4566` (auto-set) | **UNSET** | LocalStack Glue endpoint |
| `AWS_REGION` | `us-east-1` (set by Makefile) | `us-west-2` | AWS region |
| `AWS_ACCESS_KEY_ID` | `test` (set by Makefile) | **UNSET** | Test credentials |
| `AWS_PROFILE` | Derived from `AWS_ACCOUNT_NAME` | Your profile | AWS SSO profile |
| `AWS_ACCOUNT_NAME` | `workloads-data-lake-develop` (optional) | Required | Parameter Store path |

See [Environment Variables Guide](./ENVIRONMENT_VARIABLES.md) for complete configuration details.

## Common Issues

### Dev Container Not Starting

- **VS Code**: Check Docker Desktop is running, then try "Dev Containers: Rebuild Container"
- **CLI**: Check Docker is running, then try `devcontainer rebuild --workspace-folder .`

### Services Not Running

```bash
make services-status  # Check service health
```

If LocalStack is not running:

- VS Code: Command Palette → "Dev Containers: Rebuild Container"
- CLI: `devcontainer rebuild --workspace-folder .`
- Wait 15-30 seconds for services to initialize

### Tests Failing

```bash
make services-status  # Verify LocalStack services are running
make validate         # Validate environment
```

Ensure you're inside the dev container: `hostname` should show container ID.

### Job Execution Errors

- Check Spark configuration and dependencies
- Verify environment variables: `make services-status`
- Check logs: `awslocal logs tail /aws-glue/python-jobs/output --follow`

📋 **Complete troubleshooting guide**: [ClickUp: Troubleshooting Playbook](link-to-clickup)

## Quick Reference

### Makefile Commands

| Command | Purpose |
|---------|---------|
| `make services-status` | Check all service health |
| `make prepare-localstack` | Sync secrets and SSM parameters from AWS to LocalStack |
| `make clean-localstack` | Clean all LocalStack resources |
| `make run-raw DATA_SOURCE=<source> ENTITY_TYPE=<type>` | Run raw job |
| `make run-bronze DATA_SOURCE=<source> ENTITY_TYPE=<type>` | Run bronze job |
| `make run-silver DATA_SOURCE=<source> ENTITY_TYPE=<type>` | Run silver job |
| `make run-gold DATA_SOURCE=<source> ENTITY_TYPE=<type>` | Run gold job |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests only |
| `make test-integration` | Run integration tests only |
| `make format` | Format code with black |
| `make lint` | Check code quality |
| `make notebook` | Open Jupyter Lab |

### Important Paths

| Path | Purpose |
|------|---------|
| `/tmp/metastore_db_local` | Derby metastore |
| `/tmp/spark-warehouse-local` | Hive warehouse |
| `s3://local-bucket/` | LocalStack S3 bucket |

## Deployment

📋 **Full deployment procedures**: [ClickUp: AWS Glue Deployment Runbook](link-to-clickup)

Quick checklist:

- [ ] Package artifacts: `make package`
- [ ] Verify S3 uploads
- [ ] Update job configuration
- [ ] Test job run

## Related Documentation

- [Job Development Guide](./JOB_DEVELOPMENT.md) - Building Spark and PyShell jobs
- [Testing Guidelines](./TESTING.md) - Testing requirements and patterns
- [Best Practices](./BEST_PRACTICES.md) - Coding standards
- [Architecture](./ARCHITECTURE.md) - System architecture
- [Environment Variables](./ENVIRONMENT_VARIABLES.md) - Configuration reference

---

**Last Updated**: 2025-01-XX  
**Focus**: DevContainer-only development
