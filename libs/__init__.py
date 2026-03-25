"""
NaNLABS Data Warehouse Libraries

This package provides reusable components for data warehouse jobs:

- libs.common: Shared utilities, configuration, and business logic
- libs.pyspark: PySpark-specific components (Medallion jobs, Spark session)
- libs.pyshell: PyShell-specific components (API ingestion, boto3 operations)

Usage:
    # PySpark jobs
    from libs.pyspark import MedallionJobBase
    from libs.common import BronzeJobConfig

    # PyShell jobs
    from libs.pyshell import PyShellJobBase
    from libs.common import RawJobConfig
"""

__version__ = "1.0.0"

__all__ = [
    "common",
    "pyspark",
    "pyshell",
]
