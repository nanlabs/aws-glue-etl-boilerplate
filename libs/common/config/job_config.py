"""
Medallion Job Configuration Classes - Simplified Declarative Structure

This module provides Pydantic-based configuration classes for Medallion architecture jobs.
All configuration classes use three-tier parameter resolution from ConfigBase.

Features:
- Type-safe configuration with Pydantic validation
- Three-tier parameter resolution (command line, environment variables, defaults)
- Layer-specific configurations (Raw, Bronze, Silver)
- Declarative approach - no hidden defaults or auto-configuration

Usage:
    from jobs.bronze.teamtailor_bronze_job import TeamTailorBronzeConfig

    config = TeamTailorBronzeConfig.from_args()
    # All parameters are validated and ready to use
    print(config.bronze_database_name)
"""

from enum import Enum
from typing import Optional

from pydantic import Field, field_validator

from ..utils.env import get_required_env_var
from .config_base import ConfigBase


class MedallionLayer(str, Enum):
    """Medallion architecture layers."""

    RAW = "raw"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class JobConfigBase(ConfigBase):
    """
    Base configuration class for all Medallion jobs with unified parameter resolution.

    Extends ConfigBase with common job parameters and automatic validation.
    All Medallion job configs should inherit from this.

    UNIFIED PARAMETER RESOLUTION (highest to lowest priority):
    1. Command line parameters (--PARAM_NAME value)
    2. Environment variables (PARAM_NAME)
    3. Default values (if available)
    4. Error if required field is missing

    Attributes:
        source_name: Name of the source system (e.g., 'teamtailor', 'salesforce')
        warehouse_path: S3 path for Iceberg warehouse (from Terraform)
        raw_zone_path: Full S3 path to raw zone location (required)
        create_tables: Whether to create/recreate tables
        show_counts: Whether to show record counts after processing
    """

    # Common fields for all jobs
    source_name: str = Field(
        default_factory=lambda: get_required_env_var("SOURCE_NAME"),
        description="Name of the source system (e.g., 'teamtailor', 'salesforce')",
    )
    warehouse_path: str = Field(
        default_factory=lambda: get_required_env_var("WAREHOUSE_PATH"),
        description="S3 path for Iceberg warehouse (from Terraform)",
    )
    raw_zone_path: str = Field(
        default_factory=lambda: get_required_env_var("RAW_ZONE_PATH"),
        description="Full S3 path to raw zone location (required)",
    )
    create_tables: bool = Field(
        default=True, description="Whether to create/recreate tables"
    )
    show_counts: bool = Field(
        default=True, description="Whether to show record counts after processing"
    )

    @field_validator("raw_zone_path")
    @classmethod
    def validate_raw_zone_path(cls, v: str) -> str:
        """Ensure raw_zone_path ends with /."""
        if not v.endswith("/"):
            v = v + "/"
        return v

    @field_validator("warehouse_path")
    @classmethod
    def validate_warehouse_path(cls, v: str) -> str:
        """Validate warehouse S3 path format."""
        if not v.startswith("s3://"):
            raise ValueError(f"warehouse_path must start with s3://, got: {v}")
        if not v.endswith("/"):
            v = v + "/"
        return v


class RawJobConfig(JobConfigBase):
    """
    Configuration for Raw layer jobs (API/SFTP extraction).

    Raw jobs extract data from external sources and store it in S3.
    These jobs run in AWS Glue Python Shell environment without Spark.

    Required Parameters:
    - SOURCE_NAME: Name of the source system
    - RAW_ZONE_PATH: Full S3 path to raw zone location (from Terraform)
    - WAREHOUSE_PATH: S3 path for Iceberg warehouse (from Terraform)

    Optional Parameters:
    - API_BASE_URL: Base URL for the API
    - API_KEY: API key for authentication
    - SFTP_HOST: SFTP server hostname
    - SFTP_USERNAME: SFTP username
    - SFTP_PASSWORD: SFTP password

    Example:
        config = RawJobConfig.from_args()
        print(config.raw_zone_path)
    """

    # API configuration
    api_base_url: Optional[str] = Field(
        default=None, description="Base URL for the API"
    )
    api_key: Optional[str] = Field(
        default=None, description="API key for authentication"
    )

    # SFTP configuration
    sftp_host: Optional[str] = Field(default=None, description="SFTP server hostname")
    sftp_username: Optional[str] = Field(default=None, description="SFTP username")
    sftp_password: Optional[str] = Field(default=None, description="SFTP password")


class BronzeJobConfig(JobConfigBase):
    """
    Configuration for Bronze layer jobs.

    Bronze jobs process data from raw to bronze layer.

    Required Parameters:
    - SOURCE_NAME: Source system name
    - RAW_DATABASE_NAME: Raw database name
    - RAW_TABLE: Raw table name
    - RAW_S3_LOCATION: S3 location for raw data
    - BRONZE_DATABASE_NAME: Bronze database name
    - BRONZE_TABLE: Bronze table name
    - WAREHOUSE_S3_LOCATION: Iceberg warehouse location

    Optional Parameters:
    - CREATE_TABLES: Create tables (default: true)
    - SHOW_COUNTS: Show record counts (default: true)

    Example:
        config = BronzeJobConfig.from_args()
        print(config.bronze_database_name)
    """

    # Raw layer configuration
    raw_database_name: str = Field(
        default_factory=lambda: get_required_env_var("RAW_DATABASE_NAME"),
        description="Raw layer database name",
    )

    raw_table_name: Optional[str] = Field(
        default=None,
        description="Raw layer table name (auto-configured if not provided)",
    )

    # Note: raw_zone_path is inherited from JobConfigBase (no need to redefine)

    # Bronze layer configuration
    bronze_database_name: str = Field(
        default_factory=lambda: get_required_env_var("BRONZE_DATABASE_NAME"),
        description="Bronze layer database name",
    )
    bronze_table_name: Optional[str] = Field(
        default=None,
        description="Bronze layer table name (auto-configured if not provided)",
    )


class SilverJobConfig(JobConfigBase):
    """
    Configuration for Silver layer jobs.

    Silver jobs process data from bronze to silver layer.

    Required Parameters:
    - SOURCE_NAME: Source system name
    - BRONZE_DATABASE_NAME: Bronze database name
    - BRONZE_TABLE: Bronze table name
    - SILVER_DATABASE_NAME: Silver database name
    - SILVER_TABLE: Silver table name
    - WAREHOUSE_PATH: Iceberg warehouse location

    Optional Parameters:
    - CREATE_TABLES: Create tables (default: true)
    - SHOW_COUNTS: Show record counts (default: true)

    Example:
        config = SilverJobConfig.from_args()
        print(config.silver_database_name)
    """

    # Bronze layer configuration (source)
    bronze_database_name: str = Field(
        ..., description="Bronze layer database name (source)"
    )
    bronze_table_name: Optional[str] = Field(
        default=None,
        description="Bronze layer table name (source, auto-configured if not provided)",
    )

    # Silver layer configuration (target)
    silver_database_name: str = Field(
        ..., description="Silver layer database name (target)"
    )
    silver_table_name: Optional[str] = Field(
        default=None,
        description="Silver layer table name (target, auto-configured if not provided)",
    )


class GoldJobConfig(JobConfigBase):
    """
    Configuration for Gold layer jobs.

    Gold jobs process data from silver to gold layer.

    Required Parameters:
    - SOURCE_NAME: Source system name
    - SILVER_DATABASE_NAME: Silver database name
    - SILVER_TABLE: Silver table name
    - GOLD_DATABASE_NAME: Gold database name
    - GOLD_TABLE: Gold table name
    - WAREHOUSE_PATH: Iceberg warehouse location

    Optional Parameters:
    - CREATE_TABLES: Create tables (default: true)
    - SHOW_COUNTS: Show record counts (default: true)

    Example:
        config = GoldJobConfig.from_args()
        print(config.gold_database_name)
    """

    # Silver layer configuration (source)
    silver_database_name: str = Field(
        ..., description="Silver layer database name (source)"
    )
    silver_table_name: Optional[str] = Field(
        default=None,
        description="Silver layer table name (source, auto-configured if not provided)",
    )

    # Gold layer configuration (target)
    gold_database_name: str = Field(
        ..., description="Gold layer database name (target)"
    )
    gold_table_name: Optional[str] = Field(
        default=None,
        description="Gold layer table name (target, auto-configured if not provided)",
    )


# ==========================================
# NOTE: Source-specific configs (TeamTailorRawConfig, etc.)
# have been moved to their respective job files for better modularity.
#
# If you need a source-specific config, find it in:
# - jobs/raw/{source}_raw_job.py
# - jobs/bronze/{source}_bronze_job.py
# - jobs/silver/{source}_silver_job.py
# ==========================================


def validate_config(config: JobConfigBase) -> None:
    """
    Validate configuration and log summary.

    Args:
        config: Configuration instance to validate

    Raises:
        ValueError: If configuration is invalid
    """
    import logging

    logger = logging.getLogger(__name__)

    # Configuration is already validated by Pydantic
    # This function provides additional custom validation if needed

    logger.info("=" * 80)
    logger.info("Job Configuration Summary")
    logger.info("=" * 80)
    logger.info(f"Job Name: {config.job_name}")
    logger.info(f"Source: {config.source_name}")
    logger.info(f"Environment: {config.environment}")
    logger.info(f"Stage: {config.stage}")
    logger.info(f"AWS Region: {config.aws_region}")
    logger.info(f"Warehouse Path: {config.warehouse_path}")
    logger.info(f"Create Tables: {config.create_tables}")
    logger.info(f"Show Counts: {config.show_counts}")

    # Source-specific jobs should perform any additional logging after calling this helper.

    # Log layer-specific configuration
    if isinstance(config, BronzeJobConfig):
        logger.info("-" * 80)
        logger.info("Bronze Layer Configuration")
        logger.info("-" * 80)
        logger.info(f"Raw Database: {config.raw_database_name}")
        logger.info(f"Raw Table: {config.raw_table_name}")
        raw_zone_location = getattr(config, "raw_zone_path", None)
        if raw_zone_location:
            logger.info(f"Raw S3 Location: {raw_zone_location}")
        logger.info(f"Bronze Database: {config.bronze_database_name}")
        logger.info(f"Bronze Table: {config.bronze_table_name}")

    elif isinstance(config, SilverJobConfig):
        logger.info("-" * 80)
        logger.info("Silver Layer Configuration")
        logger.info("-" * 80)
        logger.info(f"Bronze Database: {config.bronze_database_name}")
        logger.info(f"Bronze Table: {config.bronze_table_name}")
        logger.info(f"Silver Database: {config.silver_database_name}")
        logger.info(f"Silver Table: {config.silver_table_name}")

    elif isinstance(config, GoldJobConfig):
        logger.info("-" * 80)
        logger.info("Gold Layer Configuration")
        logger.info("-" * 80)
        logger.info(f"Silver Database: {config.silver_database_name}")
        logger.info(f"Silver Table: {config.silver_table_name}")
        logger.info(f"Gold Database: {config.gold_database_name}")
        logger.info(f"Gold Table: {config.gold_table_name}")


# Export all BASE configuration classes for easy importing
# Note: Source-specific configs (TeamTailor*, etc.) are now in their respective job files
__all__ = [
    "MedallionLayer",
    "JobConfigBase",
    "RawJobConfig",
    "BronzeJobConfig",
    "SilverJobConfig",
    "GoldJobConfig",
    "validate_config",
]
