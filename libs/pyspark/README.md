# PySpark Components - Hybrid Local/Glue Support

This module provides reusable PySpark components for building data warehouse jobs that work in both local development (LocalStack + Derby metastore) and AWS Glue (GlueContext + Glue Data Catalog) environments.

## 📦 Components

### 1. SparkSessionFactory
Factory for creating Spark sessions with automatic environment detection.

**Features:**
- ✅ Automatic environment detection (local vs Glue)
- ✅ Local mode: LocalStack S3A + Derby metastore + Iceberg HadoopCatalog
- ✅ Glue mode: GlueContext + Glue Data Catalog + Iceberg GlueCatalog
- ✅ Configurable via `SessionConfig`
- ✅ External table support in both environments

### 2. MedallionJobBase
Base class for Medallion Architecture jobs (Bronze, Silver, Gold) with hybrid support.

**Features:**
- ✅ Works with SparkSession (local) or GlueContext (Glue)
- ✅ Automatic context type detection
- ✅ Dual catalog support: `spark_catalog` (external tables) and `glue_catalog` (Iceberg tables)
- ✅ DataFrame-first approach (Glue v5 best practice)
- ✅ Read/write methods for both external and Iceberg tables

### 3. SessionConfig
Configuration dataclass for session factory with sensible defaults.

---

## 🚀 Quick Start

### Simple Usage (Auto-detect Environment)

```python
from libs.pyspark import SparkSessionFactory, MedallionJobBase
from libs.common import BronzeJobConfig

# Create session (auto-detects local vs Glue)
session_info = SparkSessionFactory.create_session()
spark = session_info.spark
is_local = session_info.is_local

# Load configuration
config = BronzeJobConfig.from_args()

# Create job
if session_info.is_local:
    job = MyBronzeJob(spark, config)
else:
    job = MyBronzeJob(session_info.context['glue_context'], config)

# Run job
job.run()

# Cleanup
if not session_info.is_local:
    session_info.context['job'].commit()
else:
    spark.stop()
```

### With Custom Configuration

```python
from libs.pyspark import SparkSessionFactory, SessionConfig

# Create custom configuration
session_config = SessionConfig(
    app_name="My Job",
    warehouse_path="s3://bucket/warehouse/",
    aws_region="us-west-2"
)

# Create session with config
session_info = SparkSessionFactory.create_session(session_config)
spark = session_info.spark
```

---

## 🔧 Environment Variables

### Required Environment Variables by Mode

#### Local Mode (LocalStack + Derby)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_ENDPOINT_URL_S3` | ✅ Yes | `http://localstack:4566` | LocalStack endpoint for S3 |
| `AWS_ENDPOINT_URL_GLUE` | ✅ Yes | `http://localstack:4566` | LocalStack endpoint for Glue |
| `AWS_REGION` | ✅ Yes | `us-east-1` | AWS region for LocalStack |
| `AWS_ACCESS_KEY_ID` | ✅ Yes | `test` | Test AWS access key |
| `AWS_SECRET_ACCESS_KEY` | ✅ Yes | `test` | Test AWS secret key |

**Setup for Local Mode:**
```bash
# Required environment variables
export AWS_ENDPOINT_URL_S3=http://localstack:4566
export AWS_ENDPOINT_URL_GLUE=http://localstack:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Run job (no --JOB_NAME triggers local mode)
python jobs/bronze/teamtailor_bronze_job.py --ENTITY_TYPE candidates
```

#### AWS Glue Mode (Production)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_PROFILE` | Recommended | - | AWS SSO profile for authentication |
| `AWS_REGION` | ✅ Yes | `us-west-2` | AWS region (bucket location) |
| `AWS_ENDPOINT_URL_S3` | ❌ Must NOT be set | - | **MUST BE UNSET** for Glue |
| `AWS_ENDPOINT_URL_GLUE` | ❌ Must NOT be set | - | **MUST BE UNSET** for Glue |
| `AWS_ACCESS_KEY_ID` | ❌ Must NOT be set | - | Use IAM role instead |
| `AWS_SECRET_ACCESS_KEY` | ❌ Must NOT be set | - | Use IAM role instead |

**Setup for AWS Glue Mode:**
```bash
# IMPORTANT: Unset LocalStack variables!
unset AWS_ENDPOINT_URL
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY

# Set AWS profile and region
export AWS_PROFILE=your-aws-profile
export AWS_REGION=us-west-2  # Must match S3 bucket region

# Verify AWS credentials
aws sts get-caller-identity

# Deploy and run jobs through AWS Glue console or infrastructure automation
# See OPERATIONS.md for deployment procedures
```

### Environment Variable Priority

The session factory uses the following priority:

1. **Environment Detection** (highest priority):
   - Presence of `--JOB_NAME` argument → Glue mode
   - No `--JOB_NAME` argument → Local mode

2. **AWS Credentials** (by mode):
   - **Local**: Uses `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from environment
   - **Glue**: Uses IAM role attached to Glue job (no credentials needed)

3. **S3 Endpoint**:
   - **Local**: Uses `AWS_ENDPOINT_URL_S3` (LocalStack)
   - **Glue**: Uses default AWS S3 endpoint (**must unset** `AWS_ENDPOINT_URL_S3`)

### Common Mistakes to Avoid

❌ **DON'T**: Set `AWS_ENDPOINT_URL` when running on AWS Glue
```bash
# This will cause Glue jobs to fail!
export AWS_ENDPOINT_URL_S3=http://localstack:4566
export AWS_ENDPOINT_URL_GLUE=http://localstack:4566
aws glue start-job-run --job-name my_job  # ❌ FAILS
```

✅ **DO**: Unset service-specific endpoints before running Glue jobs
```bash
# Correct way
unset AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL_GLUE
aws glue start-job-run --job-name my_job  # ✅ WORKS
```

❌ **DON'T**: Use `us-east-1` for Glue if your bucket is in `us-west-2`
```bash
# This will cause S3 301 redirect errors!
export AWS_REGION=us-east-1  # Wrong region
```

✅ **DO**: Match the AWS region to your S3 bucket location
```bash
# Check bucket region first
aws s3api get-bucket-location --bucket your-bucket
# Output: {"LocationConstraint": "us-west-2"}

# Use the correct region
export AWS_REGION=us-west-2
```

### Quick Reference

**Switch from Local to Glue:**
```bash
# Step 1: Clean environment
unset AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL_GLUE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

# Step 2: Set AWS profile and correct region
export AWS_PROFILE=your-aws-profile
export AWS_REGION=us-west-2  # Match your bucket region

# Step 3: Verify
aws sts get-caller-identity
```

**Switch from Glue to Local:**
```bash
# Step 1: Set LocalStack environment
export AWS_ENDPOINT_URL_S3=http://localstack:4566
export AWS_ENDPOINT_URL_GLUE=http://localstack:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Step 2: Unset AWS profile
unset AWS_PROFILE

# Step 3: Verify LocalStack is running
curl http://localstack:4566/_localstack/health
```

### AWS SSO Login

For AWS Glue mode with SSO:
```bash
# Login to AWS SSO
aws sso login --profile your-aws-profile

# Verify authentication
aws sts get-caller-identity --profile your-aws-profile

# Set as default profile
export AWS_PROFILE=your-aws-profile
```

---

## 📖 Detailed Usage

### SparkSessionFactory

#### Basic Usage

```python
from libs.pyspark import SparkSessionFactory

# Auto-detect environment and create session
session_info = SparkSessionFactory.create_session()

# Access components
spark = session_info.spark  # SparkSession
is_local = session_info.is_local  # Boolean: True if local, False if Glue
context = session_info.context  # Dict with environment-specific context
```

#### SessionInfo Structure

```python
SessionInfo(
    spark=<SparkSession>,      # Spark session instance
    is_local=True/False,        # Environment flag
    context={                    # Environment-specific context
        # Local mode:
        'type': 'local'

        # Glue mode:
        'type': 'glue',
        'glue_context': <GlueContext>,
        'job': <Job>
    }
)
```

#### Environment Detection

The factory automatically detects the environment:

- **Local mode**: No `--JOB_NAME` argument → Uses SparkSession + Derby + LocalStack
- **Glue mode**: `--JOB_NAME` argument present → Uses GlueContext + Glue Data Catalog

#### SessionConfig Options

```python
from libs.pyspark import SessionConfig

config = SessionConfig(
    # Basic configuration
    app_name="MyApp",                      # Spark application name
    warehouse_path="s3://bucket/",     # Iceberg warehouse location
    aws_region="us-east-1",                # AWS region

    # Local mode configuration (LocalStack)
    s3_endpoint="http://localstack:4566",  # LocalStack endpoint
    aws_access_key_id="test",              # Test credentials
    aws_secret_access_key="test",          # Test credentials

    # Hive configuration
    enable_hive_support=True,              # Enable Hive catalog
    json_serde_jars_path="/path/to/jars",  # JSON SerDe JARs

    # Derby metastore (local mode)
    derby_metastore_path="/tmp/metastore_db",
    spark_warehouse_dir="/tmp/warehouse",
    hadoop_conf_dir="/tmp/hadoop-conf",
    spark_temp_dir="/tmp/spark-temp",

    # Iceberg configuration
    iceberg_catalog_name="glue_catalog"    # Catalog name
)
```

#### Convenience Functions

```python
from libs.pyspark import create_hybrid_session, get_spark_session

# Simple session creation
session_info = create_hybrid_session(
    app_name="My Job",
    warehouse_path="s3://bucket/"
)

# Get just the Spark session (no context info)
spark = get_spark_session()
```

---

### MedallionJobBase

#### Creating a Job Class

```python
from libs.pyspark import MedallionJobBase
from libs.common import BronzeJobConfig

class MyBronzeJob(MedallionJobBase):
    """Custom Bronze job with hybrid support."""

    def __init__(self, spark_or_glue_context, config: BronzeJobConfig):
        """
        Initialize job.

        Args:
            spark_or_glue_context: Either SparkSession (local) or GlueContext (Glue)
            config: Validated Bronze job configuration
        """
        super().__init__(spark_or_glue_context, config)

    def extract(self):
        """Extract data from raw external table."""
        return self.read_external_table(
            self.config.raw_database,
            self.config.raw_table
        )

    def transform(self, df):
        """Transform data."""
        # Your transformation logic here
        from pyspark.sql.functions import col, current_timestamp

        transformed_df = df.select(
            col("id"),
            col("name"),
            current_timestamp().alias("ingestion_ts")
        )

        return transformed_df

    def load(self, df):
        """Load data to bronze Iceberg table."""
        self.write_to_iceberg(
            df,
            self.config.bronze_database,
            self.config.bronze_table
        )

    def run(self):
        """Run the complete ETL pipeline."""
        self.logger.info("Starting job...")

        # Extract
        raw_df = self.extract()

        # Transform
        bronze_df = self.transform(raw_df)

        # Load
        self.load(bronze_df)

        self.logger.info("Job completed!")
```

#### Available Methods

##### Reading Data

```python
# Read from external table (spark_catalog)
df = self.read_external_table("my_database", "my_table")

# Read from external table with filters
df = self.read_external_table(
    "my_database",
    "my_table",
    filters={"year": 2024, "month": [1, 2, 3]}
)

# Read from Iceberg table (glue_catalog)
df = self.read_iceberg_table("my_database", "my_table")

# Read from Iceberg table with filters
df = self.read_iceberg_table(
    "my_database",
    "my_table",
    filters={"year": 2024}
)
```

##### Writing Data

```python
# Write to Iceberg table (append mode - default)
self.write_to_iceberg(df, "my_database", "my_table")

# Write to Iceberg table (overwrite partitions)
self.write_to_iceberg(df, "my_database", "my_table", mode="overwrite")
```

##### Creating Tables

```python
# Create external table
self.create_external_table(
    database="raw_db",
    table="my_table",
    schema="id STRING, name STRING",
    location="s3://bucket/raw/",
    partition_columns=["year INT", "month INT"]
)

# Create Iceberg table
self.create_iceberg_table(
    database="bronze_db",
    table="my_table",
    schema="id STRING, name STRING, ts TIMESTAMP",
    partition_columns=["year", "month"]
)

# Create database
self.create_database_if_not_exists("my_database", catalog="glue_catalog")
```

##### Running Pipeline

```python
# Run complete medallion pipeline
self.run_medallion_pipeline()
# This calls extract() → transform() → load() in sequence
```

#### Context Type Detection

The base class automatically detects whether you passed a SparkSession or GlueContext:

```python
# Local mode
spark = SparkSessionFactory.create_session().spark
job = MyJob(spark, config)
# → self.is_local = True, self.spark = spark, self.glue_context = None

# Glue mode
session_info = SparkSessionFactory.create_session()
job = MyJob(session_info.context['glue_context'], config)
# → self.is_local = False, self.spark = glue_context.spark_session, self.glue_context = glue_context
```

#### Catalog Names

```python
self.hive_catalog = "spark_catalog"      # For external tables
self.iceberg_catalog = "glue_catalog"    # For Iceberg tables
```

---

## 🏗️ Architecture

### Local Mode (Development)

```
┌─────────────────────────────────────────────┐
│ SparkSession                                 │
├─────────────────────────────────────────────┤
│ Custom HADOOP_CONF_DIR                       │
│ └─ hive-site.xml (Derby config)             │
│ └─ core-site.xml, hdfs-site.xml (copied)    │
├─────────────────────────────────────────────┤
│ Hive Catalog (spark_catalog)                │
│ └─ Derby Embedded Metastore                 │
│    └─ External tables (JSON SerDe)          │
├─────────────────────────────────────────────┤
│ Iceberg Catalog (glue_catalog)              │
│ └─ HadoopCatalog                            │
│    └─ Iceberg tables on LocalStack S3       │
├─────────────────────────────────────────────┤
│ S3 Access                                    │
│ └─ S3A filesystem → LocalStack              │
│    └─ http://localstack:4566                │
└─────────────────────────────────────────────┘
```

### Glue Mode (Production)

```
┌─────────────────────────────────────────────┐
│ GlueContext                                  │
├─────────────────────────────────────────────┤
│ Hive Catalog (spark_catalog)                │
│ └─ AWS Glue Data Catalog                    │
│    └─ External tables                       │
├─────────────────────────────────────────────┤
│ Iceberg Catalog (glue_catalog)              │
│ └─ GlueCatalog                              │
│    └─ Iceberg tables on AWS S3              │
├─────────────────────────────────────────────┤
│ S3 Access                                    │
│ └─ Direct AWS S3 access                     │
└─────────────────────────────────────────────┘
```

---

## 💡 Examples

### Complete Job Example

See `jobs/bronze/teamtailor_bronze_job.py` for a complete working example.

Key sections:
1. Configuration with Pydantic
2. Session creation with factory
3. Job class extending MedallionJobBase
4. Hybrid main execution block

### Simple Example

```python
#!/usr/bin/env python3
"""Simple Bronze Job Example"""

import logging
from libs.pyspark import SparkSessionFactory, SessionConfig, MedallionJobBase
from libs.common import BronzeJobConfig
from pydantic import Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom configuration
class MyBronzeConfig(BronzeJobConfig):
    source_name: str = Field(default="my_source")
    raw_database: str = Field(default="my_raw_db")
    raw_table: str = Field(default="my_raw_table")
    teamtailor_raw_path: str = Field(default="s3://bucket/raw/")
    bronze_database: str = Field(default="my_bronze_db")
    bronze_table: str = Field(default="my_bronze_table")
    warehouse_path: str = Field(default="s3://bucket/warehouse/")

# Job class
class MyBronzeJob(MedallionJobBase):
    def extract(self):
        return self.read_external_table(self.config.raw_database, self.config.raw_table)

    def transform(self, df):
        # Your transformation logic
        return df

    def load(self, df):
        self.write_to_iceberg(df, self.config.bronze_database, self.config.bronze_table)

    def run(self):
        logger.info("Starting job...")
        df = self.extract()
        df_transformed = self.transform(df)
        self.load(df_transformed)
        logger.info("Job completed!")

# Main execution
if __name__ == "__main__":
    # Load config
    config = MyBronzeConfig.from_args()

    # Create session
    session_config = SessionConfig(
        app_name=config.job_name,
        warehouse_path=config.warehouse_path
    )
    session_info = SparkSessionFactory.create_session(session_config)

    # Create and run job
    if session_info.is_local:
        job = MyBronzeJob(session_info.spark, config)
    else:
        job = MyBronzeJob(session_info.context['glue_context'], config)

    job.run()

    # Cleanup
    if not session_info.is_local:
        session_info.context['job'].commit()
    else:
        session_info.spark.stop()
```

---

## 🔧 Technical Details

### How Environment Detection Works

```python
def detect_environment():
    """Detect environment based on command line arguments."""
    if '--JOB_NAME' in sys.argv or os.environ.get('GLUE_VERSION'):
        return 'glue'
    return 'local'
```

### How Context Type Detection Works

```python
def __init__(self, spark_or_glue_context, config):
    """Auto-detect context type."""
    if hasattr(spark_or_glue_context, 'spark_session'):
        # It's a GlueContext
        self.glue_context = spark_or_glue_context
        self.spark = spark_or_glue_context.spark_session
        self.is_local = False
    else:
        # It's a SparkSession
        self.spark = spark_or_glue_context
        self.glue_context = None
        self.is_local = True
```

### Local Mode Setup

The factory performs these steps for local mode:

1. **Custom Hadoop Configuration**
   - Creates `/tmp/hadoop-conf-local/`
   - Copies EMR configs (if available)
   - Creates custom `hive-site.xml` with Derby settings
   - Sets `HADOOP_CONF_DIR` environment variable

2. **Environment Variables**
   - `AWS_ENDPOINT_URL=http://localstack:4566`
   - `AWS_ACCESS_KEY_ID=test`
   - `AWS_SECRET_ACCESS_KEY=test`
   - `AWS_REGION=us-east-1`

3. **SparkSession Configuration**
   - S3A filesystem for LocalStack
   - Derby metastore configuration
   - Iceberg HadoopCatalog
   - JSON SerDe JARs
   - Parquet type conversion settings

### Glue Mode Setup

The factory performs these steps for Glue mode:

1. **GlueContext Initialization**
   - Gets `JOB_NAME` from arguments
   - Creates SparkContext
   - Creates GlueContext
   - Initializes Job for tracking

2. **Iceberg Configuration**
   - Configures Iceberg GlueCatalog
   - Sets warehouse location
   - Configures S3FileIO

---

## 🧪 Testing

See [LOCAL_DEVELOPMENT.md](../../docs/LOCAL_DEVELOPMENT.md) for testing instructions.

---

## 📚 See Also

- [LOCAL_DEVELOPMENT.md](../../docs/LOCAL_DEVELOPMENT.md) - Local development guide
- [BEST_PRACTICES.md](../../docs/BEST_PRACTICES.md) - Coding standards
- [ARCHITECTURE.md](../../docs/ARCHITECTURE.md) - System architecture
- [jobs/bronze/teamtailor_bronze_job.py](../../jobs/bronze/teamtailor_bronze_job.py) - Complete example

---

## 🤝 Contributing

When modifying these components:

1. Maintain backward compatibility
2. Update documentation
3. Add unit tests
4. Test in both local and Glue environments
5. Follow the established patterns

---

**Version**: 1.0.0  
**Last Updated**: 2025-10-14  
**Maintainer**: NaNLABS Data Warehouse Team
