# Libs Structure - Organization Guide

**Last Updated**: 2025-10-02

## Overview

The `libs/` directory is organized into three main modules that separate shared code from PySpark-specific and PyShell-specific components:

```
libs/
├── common/          # Shared code (both PySpark & PyShell)
├── pyspark/         # PySpark-specific code
└── pyshell/         # PyShell-specific code
```

## Directory Structure

### 📦 libs/common/ - Shared Components

Code that doesn't depend on PySpark or PyShell specifics:

```
libs/common/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── config_base.py          # Base configuration with 3-tier resolution
│   ├── database_config.py      # Database configuration utilities
│   └── job_config.py          # Job configurations (Bronze/Silver/Gold/PyShell)
├── utils/
│   ├── __init__.py
│   ├── aws.py                  # AWS boto3 client utilities (auto region detection)
│   ├── logger.py               # Logging utilities
│   └── secrets.py              # AWS Secrets Manager integration
├── teamtailor/
│   ├── __init__.py
│   └── client.py               # Team Tailor API client
└── exceptions.py               # Custom exceptions
```

**Import pattern:**
```python
from libs.common import ConfigBase, BronzeJobConfig, RawJobConfig
from libs.common.config import DatabaseConfig
from libs.common.utils import create_boto3_client, get_logger, SecretsManager
```

### ⚡ libs/pyspark/ - PySpark Components

Code that requires PySpark (uses `pyspark.sql`, `SparkSession`, etc.):

```
libs/pyspark/
├── __init__.py
├── medallion_job_base.py       # Base class for Bronze/Silver/Gold jobs
└── session_manager.py          # Spark session configuration
```

**Dependencies:**
- `pyspark.sql`
- `pyspark.conf`
- `awsglue.context`

**Import pattern:**
```python
from libs.pyspark import MedallionJobBase, SessionManager
```

### 🐚 libs/pyshell/ - PyShell Components

Code that requires PyShell libraries (uses `boto3`, `requests`, etc.):

```
libs/pyshell/
├── __init__.py
└── pyshell_job_base.py         # Base class for API ingestion jobs
```

**Dependencies:**
- `boto3`
- `requests`
- `botocore`

**Import pattern:**
```python
from libs.pyshell import PyShellJobBase
```

## Usage Examples

### PySpark Job (Bronze/Silver/Gold)

```python
"""
Bronze layer job using PySpark
"""
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Import PySpark-specific and shared components
from libs.pyspark import MedallionJobBase
from libs.common import BronzeJobConfig

class MyBronzeJob(MedallionJobBase):
    def __init__(self, glue_context, config: BronzeJobConfig):
        super().__init__(glue_context, config)

    def extract(self):
        return self.read_external_table(
            self.config.raw_database,
            self.config.raw_table
        )

    def transform(self, df):
        # PySpark DataFrame transformations
        return df

    def load(self, df):
        self.write_to_iceberg(
            df,
            self.config.bronze_database,
            self.config.bronze_table
        )

# Run job
if __name__ == "__main__":
    sc = SparkContext()
    glue_context = GlueContext(sc)
    config = BronzeJobConfig.from_args()
    job = MyBronzeJob(glue_context, config)
    job.run_medallion_pipeline()
```

### PyShell Job (Raw Zone API Ingestion)

```python
"""
Raw zone job using PyShell
"""
from pydantic import Field

# Import PyShell-specific and shared components
from libs.pyshell import PyShellJobBase
from libs.common import RawJobConfig

class MyPyShellConfig(RawJobConfig):
    # Source-specific defaults
    source_name: str = Field(default="my_api")
    s3_bucket: str = Field(default="my-bucket")
    s3_prefix: str = Field(default="raw/my_api/data")
    api_endpoint: str = Field(default="/data")

class MyPyShellJob(PyShellJobBase):
    def extract(self):
        # Fetch from API
        url = f"{self.config.api_base_url}{self.config.api_endpoint}"
        data, request_info = self.make_api_request(url, params={})
        return data, request_info

    def load(self, data):
        # Write to S3
        records, request_info = data
        self.write_to_s3(records, request_info)

# Run job
if __name__ == "__main__":
    config = MyPyShellConfig.from_args()
    job = MyPyShellJob(config)
    job.run()
```

## Benefits of This Organization

### ✅ Clear Separation of Concerns

- **common/**: Pure Python code, no external dependencies
- **pyspark/**: Only code that needs PySpark
- **pyshell/**: Only code that needs boto3/requests

### ✅ Single Package for All Jobs

- One `libs.zip` contains all three modules
- Both PySpark and PyShell jobs can use it
- Simpler deployment and maintenance

### ✅ Easy to Understand Dependencies

```python
# PySpark job knows what it needs
from libs.pyspark import MedallionJobBase  # Needs PySpark
from libs.common import BronzeJobConfig     # Pure Python

# PyShell job knows what it needs
from libs.pyshell import PyShellJobBase     # Needs boto3
from libs.common import RawJobConfig    # Pure Python
```

### ✅ Prevents Circular Dependencies

- `pyspark/` can import from `common/`
- `pyshell/` can import from `common/`
- `common/` never imports from `pyspark/` or `pyshell/`

## Module Dependencies

```
┌─────────────┐
│   common/   │  ← Pure Python, no PySpark/PyShell deps
└─────────────┘
       ↑
       │ imports
       │
   ┌───┴───┬────────┐
   │       │        │
┌──┴──┐ ┌──┴──┐  ┌─┴────┐
│pyspark│pyshell│  │jobs/ │
│  /   │  /    │  │      │
└──────┘└──────┘  └──────┘
```

## Adding New Components

### Adding to libs/common/

Use for:
- Configuration classes (no PySpark/PyShell deps)
- Utility functions (pure Python)
- Business logic (source-specific logic)
- Exceptions and validators

```python
# libs/common/my_module.py
from .config import ConfigBase

class MyUtility:
    """Pure Python utility"""
    pass
```

### Adding to libs/pyspark/

Use for:
- Classes that use `pyspark.sql.DataFrame`
- Spark session management
- PySpark-specific transformations

```python
# libs/pyspark/my_module.py
from pyspark.sql import DataFrame
from libs.common import ConfigBase

class MySparkUtil:
    """PySpark-specific utility"""
    pass
```

### Adding to libs/pyshell/

Use for:
- Classes that use `boto3`
- API clients with `requests`
- S3 operations without Spark

```python
# libs/pyshell/my_module.py
from libs.common.utils.aws import create_boto3_client
from libs.common import ConfigBase

class MyPyShellUtil:
    """PyShell-specific utility"""
    def __init__(self):
        # Use create_boto3_client for automatic region detection
        self.s3_client = create_boto3_client('s3')
        # Region is auto-detected from:
        # 1. Explicit parameter
        # 2. AWS_REGION/AWS_DEFAULT_REGION env vars
        # 3. boto3.Session() (IAM role metadata in AWS Glue)
        # 4. Fallback to us-east-1
```

## Package Contents

When you run `./scripts/package.sh`, it creates `build/libs.zip` containing:

```
libs.zip
└── libs/
    ├── __init__.py
    ├── common/           # ~30KB
    │   ├── config/
    │   ├── utils/
    │   ├── teamtailor/
    │   └── exceptions.py
    ├── pyspark/          # ~15KB
    │   ├── medallion_job_base.py
    │   └── session_manager.py
    └── pyshell/          # ~10KB
        └── pyshell_job_base.py
```

**Total size:** ~55-60KB (uncompressed)

## Import Patterns

```python
# PySpark jobs
from libs.pyspark import MedallionJobBase
from libs.common import BronzeJobConfig

# PyShell jobs
from libs.pyshell import PyShellJobBase
from libs.common import RawJobConfig
```

## Related Documentation

- [Packaging Guide](./PACKAGING.md) - How to build and deploy libs.zip
- [Job Development Guide](./JOB_DEVELOPMENT.md) - Building Spark and PyShell jobs
- [Medallion Job Pattern](./MEDALLION_JOB_PATTERN.md) - MedallionJobBase API reference

---

**Questions?** Review the examples above or check existing jobs in `jobs/bronze/` and `jobs/raw/`.
