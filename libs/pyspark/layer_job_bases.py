"""
Layer-Specific Job Base Classes

This module provides layer-specific base classes for the Medallion Architecture:
- BronzeJobBase: For Bronze layer jobs (Raw → Bronze)
- SilverJobBase: For Silver layer jobs (Bronze → Silver)
- GoldJobBase: For Gold layer jobs (Silver → Gold)

Each class automatically handles database creation for its respective layer.

Usage:
    from libs.pyspark import BronzeJobBase

    class MyBronzeJob(BronzeJobBase):
        def create_tables(self):
            # Create your tables here
            pass

        def extract(self):
            # Extract from raw
            pass

        def transform(self, df):
            # Transform logic
            pass

        def load(self, df):
            # Load to bronze
            pass
"""

from .medallion_job_base import MedallionJobBase


class BronzeJobBase(MedallionJobBase):
    """
    Base class for Bronze layer jobs.

    Bronze jobs process data from Raw zone to Bronze layer:
    - Input: Raw external tables (spark_catalog)
    - Output: Bronze Iceberg tables (glue_catalog)

    Automatically creates:
    - spark_catalog.{raw_database}
    - glue_catalog.{bronze_database}
    """

    def __init__(self, spark_or_glue_context, config, **kwargs):
        """
        Initialize Bronze job.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (Glue)
            config: BronzeJobConfig instance
            **kwargs: Additional arguments passed to parent
        """
        super().__init__(spark_or_glue_context, config, layer="bronze", **kwargs)

    def _get_required_databases(self):
        """
        Get databases required for Bronze layer.

        Returns:
            List of tuples: [(catalog, database, layer_name), ...]
        """
        databases = []

        # Raw database (external tables)
        if hasattr(self.config, "raw_database_name") and self.config.raw_database_name:
            databases.append(("spark_catalog", self.config.raw_database_name, "Raw"))

        # Bronze database (Iceberg tables)
        if (
            hasattr(self.config, "bronze_database_name")
            and self.config.bronze_database_name
        ):
            databases.append(
                ("glue_catalog", self.config.bronze_database_name, "Bronze")
            )

        return databases


class SilverJobBase(MedallionJobBase):
    """
    Base class for Silver layer jobs.

    Silver jobs process data from Bronze to Silver layer:
    - Input: Bronze Iceberg tables (glue_catalog)
    - Output: Silver Iceberg tables (glue_catalog)

    Automatically creates:
    - glue_catalog.{bronze_database}
    - glue_catalog.{silver_database}
    """

    def __init__(self, spark_or_glue_context, config, **kwargs):
        """
        Initialize Silver job.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (Glue)
            config: SilverJobConfig instance
            **kwargs: Additional arguments passed to parent
        """
        super().__init__(spark_or_glue_context, config, layer="silver", **kwargs)

    def _get_required_databases(self):
        """
        Get databases required for Silver layer.

        Returns:
            List of tuples: [(catalog, database, layer_name), ...]
        """
        databases = []

        # Bronze database (input)
        if (
            hasattr(self.config, "bronze_database_name")
            and self.config.bronze_database_name
        ):
            databases.append(
                ("glue_catalog", self.config.bronze_database_name, "Bronze")
            )

        # Silver database (output)
        if (
            hasattr(self.config, "silver_database_name")
            and self.config.silver_database_name
        ):
            databases.append(
                ("glue_catalog", self.config.silver_database_name, "Silver")
            )

        return databases


class GoldJobBase(MedallionJobBase):
    """
    Base class for Gold layer jobs.

    Gold jobs process data from Silver to Gold layer:
    - Input: Silver Iceberg tables (glue_catalog)
    - Output: Gold Iceberg tables (glue_catalog)

    Automatically creates:
    - glue_catalog.{silver_database}
    - glue_catalog.{gold_database}
    """

    def __init__(self, spark_or_glue_context, config, **kwargs):
        """
        Initialize Gold job.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (Glue)
            config: GoldJobConfig instance
            **kwargs: Additional arguments passed to parent
        """
        super().__init__(spark_or_glue_context, config, layer="gold", **kwargs)

    def _get_required_databases(self):
        """
        Get databases required for Gold layer.

        Returns:
            List of tuples: [(catalog, database, layer_name), ...]
        """
        databases = []

        # Silver database (input)
        if (
            hasattr(self.config, "silver_database_name")
            and self.config.silver_database_name
        ):
            databases.append(
                ("glue_catalog", self.config.silver_database_name, "Silver")
            )

        # Gold database (output)
        if (
            hasattr(self.config, "gold_database_name")
            and self.config.gold_database_name
        ):
            databases.append(("glue_catalog", self.config.gold_database_name, "Gold"))

        return databases
