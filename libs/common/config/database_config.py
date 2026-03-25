"""
Database configuration management for Medallion Architecture.

This module provides database and table configuration classes that support
implicit layer detection based on naming conventions and explicit configuration.
"""

import logging
from typing import Dict, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

from .job_config import MedallionLayer  # Single source of truth

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """
    Database configuration with implicit layer detection.

    This class provides a unified way to configure source and target databases
    and tables, with automatic layer detection based on naming conventions.
    """

    model_config = {"extra": "forbid", "validate_assignment": True}

    # Source configuration
    source_database: str = Field(description="Source database name")
    source_table: str = Field(description="Source table name")

    # Target configuration
    target_database: str = Field(description="Target database name")
    target_table: str = Field(description="Target table name")

    # Optional explicit layer specification
    source_layer: Optional[MedallionLayer] = Field(
        default=None,
        description="Explicit source layer (auto-detected if not specified)",
    )
    target_layer: Optional[MedallionLayer] = Field(
        default=None,
        description="Explicit target layer (auto-detected if not specified)",
    )

    # Environment and naming conventions
    environment: str = Field(
        default="develop", description="Environment name (develop, staging, prod)"
    )
    prefix: str = Field(default="nan", description="Database prefix (nan, prod, etc.)")

    @field_validator("source_database", "target_database")
    @classmethod
    def validate_database_name(cls, v: str) -> str:
        """Validate database name format."""
        if not v or not v.strip():
            raise ValueError("Database name cannot be empty")
        return v.strip()

    @field_validator("source_table", "target_table")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        """Validate table name format."""
        if not v or not v.strip():
            raise ValueError("Table name cannot be empty")
        return v.strip()

    @property
    def detected_source_layer(self) -> MedallionLayer:
        """Detect source layer from database/table naming conventions."""
        if self.source_layer:
            return self.source_layer

        # Detect from database name
        db_lower = str(self.source_database).lower()
        table_lower = str(self.source_table).lower()

        if "raw" in db_lower or "raw" in table_lower:
            return MedallionLayer.RAW
        elif "bronze" in db_lower or "bronze" in table_lower:
            return MedallionLayer.BRONZE
        elif "silver" in db_lower or "silver" in table_lower:
            return MedallionLayer.SILVER
        elif "gold" in db_lower or "gold" in table_lower:
            return MedallionLayer.GOLD
        else:
            # Default to raw if no layer indicators found
            logger.warning(
                "Could not detect source layer from %s.%s, defaulting to RAW",
                self.source_database,
                self.source_table,
            )
            return MedallionLayer.RAW

    @property
    def detected_target_layer(self) -> MedallionLayer:
        """Detect target layer from database/table naming conventions."""
        if self.target_layer:
            return self.target_layer

        # Detect from database name
        db_lower = str(self.target_database).lower()
        table_lower = str(self.target_table).lower()

        if "raw" in db_lower or "raw" in table_lower:
            return MedallionLayer.RAW
        elif "bronze" in db_lower or "bronze" in table_lower:
            return MedallionLayer.BRONZE
        elif "silver" in db_lower or "silver" in table_lower:
            return MedallionLayer.SILVER
        elif "gold" in db_lower or "gold" in table_lower:
            return MedallionLayer.GOLD
        else:
            # Default to bronze if no layer indicators found
            logger.warning(
                "Could not detect target layer from %s.%s, defaulting to BRONZE",
                self.target_database,
                self.target_table,
            )
            return MedallionLayer.BRONZE

    @property
    def source_catalog(self) -> str:
        """Get the appropriate catalog for the source layer."""
        layer = self.detected_source_layer
        if layer == MedallionLayer.RAW:
            return "spark_catalog"  # External tables
        else:
            return "glue_catalog"  # Iceberg tables

    @property
    def target_catalog(self) -> str:
        """Get the appropriate catalog for the target layer."""
        layer = self.detected_target_layer
        if layer == MedallionLayer.RAW:
            return "spark_catalog"  # External tables
        else:
            return "glue_catalog"  # Iceberg tables

    @property
    def source_full_name(self) -> str:
        """Get the full source table name with catalog."""
        return f"{self.source_catalog}.{self.source_database}.{self.source_table}"

    @property
    def target_full_name(self) -> str:
        """Get the full target table name with catalog."""
        return f"{self.target_catalog}.{self.target_database}.{self.target_table}"

    @property
    def is_cross_layer_pipeline(self) -> bool:
        """Check if this is a cross-layer pipeline (e.g., raw -> bronze)."""
        return self.detected_source_layer != self.detected_target_layer

    @property
    def pipeline_type(self) -> str:
        """Get the pipeline type based on source and target layers."""
        source = self.detected_source_layer.value
        target = self.detected_target_layer.value

        if source == target:
            return f"{source}_processing"
        else:
            return f"{source}_to_{target}"

    def get_s3_path(self, layer: Optional[MedallionLayer] = None) -> str:
        """
        Get the S3 path for a given layer.

        Args:
            layer: Layer to get path for (uses target layer if not specified)

        Returns:
            S3 path for the layer
        """
        if layer is None:
            layer = self.detected_target_layer

        base_path = f"s3://{self.environment}-data-lake-storage"

        if layer == MedallionLayer.RAW:
            return f"{base_path}/raw"
        elif layer == MedallionLayer.BRONZE:
            return f"{base_path}/bronze"
        elif layer == MedallionLayer.SILVER:
            return f"{base_path}/silver"
        elif layer == MedallionLayer.GOLD:
            return f"{base_path}/gold"
        else:
            return f"{base_path}/{layer.value}"

    def get_table_s3_path(self, layer: Optional[MedallionLayer] = None) -> str:
        """
        Get the S3 path for a specific table.

        Args:
            layer: Layer to get path for (uses target layer if not specified)

        Returns:
            S3 path for the table
        """
        base_path = self.get_s3_path(layer)
        if layer is None:
            layer = self.detected_target_layer

        if layer == MedallionLayer.RAW:
            return f"{base_path}/{self.source_table}/"
        else:
            return f"{base_path}/{self.target_table}/"

    def validate_pipeline_logic(self) -> Tuple[bool, Optional[str]]:
        """
        Validate that the pipeline makes logical sense.

        Returns:
            Tuple of (is_valid, error_message)
        """
        source_layer = self.detected_source_layer
        target_layer = self.detected_target_layer

        # Define valid layer transitions
        valid_transitions = {
            MedallionLayer.RAW: [MedallionLayer.BRONZE],
            MedallionLayer.BRONZE: [MedallionLayer.SILVER],
            MedallionLayer.SILVER: [MedallionLayer.GOLD],
            MedallionLayer.GOLD: [],  # Gold is the final layer
        }

        # Same layer processing is always valid
        if source_layer == target_layer:
            return True, None

        # Check if transition is valid
        if target_layer not in valid_transitions.get(source_layer, []):
            return (
                False,
                f"Invalid pipeline: {source_layer.value} -> {target_layer.value}. Valid transitions from {source_layer.value}: {[t.value for t in valid_transitions[source_layer]]}",
            )

        return True, None

    def get_database_creation_sql(self) -> Dict[str, str]:
        """
        Get SQL statements to create databases if they don't exist.

        Returns:
            Dictionary mapping database names to CREATE DATABASE statements
        """
        sql_statements = {}

        # Source database
        if self.detected_source_layer == MedallionLayer.RAW:
            sql_statements[self.source_database] = (
                f"CREATE DATABASE IF NOT EXISTS {self.source_database}"
            )
        else:
            sql_statements[self.source_database] = (
                f"CREATE DATABASE IF NOT EXISTS glue_catalog.{self.source_database}"
            )

        # Target database
        if self.detected_target_layer == MedallionLayer.RAW:
            sql_statements[self.target_database] = (
                f"CREATE DATABASE IF NOT EXISTS {self.target_database}"
            )
        else:
            sql_statements[self.target_database] = (
                f"CREATE DATABASE IF NOT EXISTS glue_catalog.{self.target_database}"
            )

        return sql_statements

    def log_configuration(self) -> None:
        """Log the database configuration for debugging."""
        logger.info("Database Configuration:")
        logger.info(
            "  Source: %s (layer: %s)",
            self.source_full_name,
            self.detected_source_layer.value,
        )
        logger.info(
            "  Target: %s (layer: %s)",
            self.target_full_name,
            self.detected_target_layer.value,
        )
        logger.info("  Pipeline Type: %s", self.pipeline_type)
        logger.info("  Cross-layer: %s", self.is_cross_layer_pipeline)

        # Validate pipeline logic
        is_valid, error_msg = self.validate_pipeline_logic()
        if not is_valid:
            logger.warning("Pipeline validation warning: %s", error_msg)
        else:
            logger.info("  Pipeline validation: ✅ Valid")


class DatabaseConfigBuilder:
    """
    Builder class for creating DatabaseConfig instances with common patterns.
    """

    @staticmethod
    def create_raw_to_bronze(
        source_db: str,
        source_table: str,
        target_db: str,
        target_table: str,
        environment: str = "develop",
        prefix: str = "nan",
    ) -> DatabaseConfig:
        """Create a raw to bronze pipeline configuration."""
        return DatabaseConfig(
            source_database=source_db,
            source_table=source_table,
            target_database=target_db,
            target_table=target_table,
            source_layer=MedallionLayer.RAW,
            target_layer=MedallionLayer.BRONZE,
            environment=environment,
            prefix=prefix,
        )

    @staticmethod
    def create_bronze_to_silver(
        source_db: str,
        source_table: str,
        target_db: str,
        target_table: str,
        environment: str = "develop",
        prefix: str = "nan",
    ) -> DatabaseConfig:
        """Create a bronze to silver pipeline configuration."""
        return DatabaseConfig(
            source_database=source_db,
            source_table=source_table,
            target_database=target_db,
            target_table=target_table,
            source_layer=MedallionLayer.BRONZE,
            target_layer=MedallionLayer.SILVER,
            environment=environment,
            prefix=prefix,
        )

    @staticmethod
    def create_silver_to_gold(
        source_db: str,
        source_table: str,
        target_db: str,
        target_table: str,
        environment: str = "develop",
        prefix: str = "nan",
    ) -> DatabaseConfig:
        """Create a silver to gold pipeline configuration."""
        return DatabaseConfig(
            source_database=source_db,
            source_table=source_table,
            target_database=target_db,
            target_table=target_table,
            source_layer=MedallionLayer.SILVER,
            target_layer=MedallionLayer.GOLD,
            environment=environment,
            prefix=prefix,
        )

    @staticmethod
    def create_with_auto_detection(
        source_db: str,
        source_table: str,
        target_db: str,
        target_table: str,
        environment: str = "develop",
        prefix: str = "nan",
    ) -> DatabaseConfig:
        """Create configuration with automatic layer detection."""
        return DatabaseConfig(
            source_database=source_db,
            source_table=source_table,
            target_database=target_db,
            target_table=target_table,
            environment=environment,
            prefix=prefix,
        )
