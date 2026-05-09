"""
Medallion Job Base Class - Hybrid Local/Glue Support

Base class for Medallion Architecture jobs (Bronze, Silver, Gold) with support for both
local development (LocalStack + Derby) and AWS Glue (GlueContext + Glue Data Catalog) environments.

Features:
- Hybrid support: Works with SparkSession (local) or GlueContext (Glue)
- Dual catalog support: spark_catalog (external tables) and glue_catalog (Iceberg tables)
- DataFrame-first approach (Glue v5 best practice)
- Read/write methods for both external and Iceberg tables
- Layer-specific processing patterns
- Pydantic-based configuration management
- Automatic environment detection

Usage (Glue mode):
    from libs.pyspark import MedallionJobBase
    from libs.common import BronzeJobConfig
    from awsglue.context import GlueContext

    class MyBronzeJob(MedallionJobBase):
        def __init__(self, glue_context, config: BronzeJobConfig):
            super().__init__(glue_context, config)

        def extract(self):
            return self.read_external_table(self.config.raw_database, self.config.raw_table)

        def transform(self, df):
            # Your transformation logic
            return df

        def load(self, df):
            self.write_to_iceberg(df, self.config.bronze_database, self.config.bronze_table)

Usage (Local mode):
    from libs.pyspark import SparkSessionFactory, MedallionJobBase
    from libs.common import BronzeJobConfig

    # Create local session
    session_info = SparkSessionFactory.create_session()

    class MyBronzeJob(MedallionJobBase):
        def __init__(self, spark, config: BronzeJobConfig):
            super().__init__(spark, config)

        # Same methods as Glue mode
        def extract(self):
            return self.read_external_table(self.config.raw_database, self.config.raw_table)

Usage (Hybrid - auto-detect):
    from libs.pyspark import SparkSessionFactory, MedallionJobBase

    # Auto-detect environment and create session
    session_info = SparkSessionFactory.create_session()

    # Create job (works for both local and Glue)
    if session_info.is_local:
        job = MyBronzeJob(session_info.spark, config)
    else:
        job = MyBronzeJob(session_info.context['glue_context'], config)
"""

import logging

from pyspark.sql.functions import col


class MedallionJobBase:
    """
    Base class for Medallion Architecture jobs supporting both external tables and Iceberg tables.

    This class works in both local (SparkSession) and Glue (GlueContext) environments,
    automatically detecting the context type and configuring accordingly.

    Features:
    - Hybrid support: SparkSession (local) or GlueContext (Glue)
    - Dual catalog support: spark_catalog (external tables) and glue_catalog (Iceberg tables)
    - Medallion layer patterns: Bronze, Silver, Gold processing
    - AWS Glue v5 DataFrame-first approach
    - Pydantic-based configuration management
    - Automatic environment detection
    """

    def __init__(self, spark_or_glue_context, config, layer=None, is_local=None):
        """
        Initialize the Medallion job base class with hybrid support.

        This constructor accepts either a SparkSession (local mode) or GlueContext (Glue mode)
        and automatically detects which one it is.

        Args:
            spark_or_glue_context: Either SparkSession (local) or GlueContext (Glue)
            config: Job configuration instance (BronzeJobConfig, SilverJobConfig, or GoldJobConfig)
            layer: Optional medallion layer name (bronze/silver/gold). If not provided,
                   will be inferred from config type.
            is_local: Optional flag to explicitly set local mode. If not provided,
                     will be auto-detected from context type.

        Example (Glue):
            job = MyJob(glue_context, config)

        Example (Local):
            spark = SparkSessionFactory.create_session().spark
            job = MyJob(spark, config)

        Example (Explicit):
            job = MyJob(spark, config, is_local=True)
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

        # Auto-detect context type if is_local not explicitly provided
        if is_local is None:
            # Check if it's a GlueContext (has spark_session attribute)
            # or a SparkSession (doesn't have spark_session attribute)
            if hasattr(spark_or_glue_context, "spark_session"):
                # It's a GlueContext
                self.glue_context = spark_or_glue_context
                self.spark = spark_or_glue_context.spark_session
                self.is_local = False
                self.logger.info("Detected GlueContext (Glue mode)")
            else:
                # It's a SparkSession
                self.spark = spark_or_glue_context
                self.glue_context = None
                self.is_local = True
                self.logger.info("Detected SparkSession (local mode)")
        else:
            # Explicit is_local flag provided
            self.is_local = is_local
            if is_local:
                self.spark = spark_or_glue_context
                self.glue_context = None
                self.logger.info("Initialized in local mode (explicit)")
            else:
                self.glue_context = spark_or_glue_context
                self.spark = spark_or_glue_context.spark_session
                self.logger.info("Initialized in Glue mode (explicit)")

        # Catalog names
        self.hive_catalog = "spark_catalog"  # For external tables
        self.iceberg_catalog = "glue_catalog"  # For Iceberg tables

        # Set layer - infer from config type if not provided
        if layer is None:
            from libs.common.config.job_config import (
                BronzeJobConfig,
                GoldJobConfig,
                SilverJobConfig,
            )

            if isinstance(config, BronzeJobConfig):
                self.layer = "bronze"
            elif isinstance(config, SilverJobConfig):
                self.layer = "silver"
            elif isinstance(config, GoldJobConfig):
                self.layer = "gold"
            else:
                self.layer = "unknown"
        else:
            self.layer = layer

        # Log initialization details
        self.logger.info(
            "MedallionJobBase initialized for %s layer (local=%s)",
            self.layer,
            self.is_local,
        )
        self.logger.info("Source: %s", config.source_name)
        self.logger.info("Warehouse: %s", config.warehouse_path)

        # Note: Iceberg catalog should be configured by SessionFactory or in main BEFORE creating job instance

    def _configure_iceberg_catalog(self):
        """Configure Iceberg catalog with warehouse location from config."""
        warehouse_path = self.config.warehouse_path

        self.logger.info(
            "Configuring Iceberg catalog with warehouse: %s", warehouse_path
        )

        # Iceberg catalog configuration
        self.spark.conf.set(
            f"spark.sql.catalog.{self.iceberg_catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        self.spark.conf.set(
            f"spark.sql.catalog.{self.iceberg_catalog}.warehouse", warehouse_path
        )
        self.spark.conf.set(
            f"spark.sql.catalog.{self.iceberg_catalog}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        self.spark.conf.set(
            f"spark.sql.catalog.{self.iceberg_catalog}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )

        self.logger.info("Iceberg catalog configured successfully")

    def read_external_table(self, database, table, filters=None, is_partitioned=True):
        """
        Read data from external table (spark_catalog) using DataFrame.

        Args:
            database: Database name
            table: Table name
            filters: Optional filters dict
            is_partitioned: Whether the table is partitioned (default: True)

        Returns:
            DataFrame: Data from external table (Glue v5 best practice)
        """
        try:
            full_table_name = f"{self.hive_catalog}.{database}.{table}"
            self.logger.info("Reading from external table: %s", full_table_name)

            # Repair partitions first (only for partitioned tables)
            if is_partitioned:
                self.logger.info("Repairing partitions for: %s", full_table_name)
                self.spark.sql(f"MSCK REPAIR TABLE {full_table_name}")
            else:
                self.logger.info("Skipping partition repair (table is not partitioned)")

            # Read directly as DataFrame (Glue v5 best practice)
            self.logger.info("Reading DataFrame from table")
            df = self.spark.table(full_table_name)

            # Apply filters if provided
            if filters:
                for column, value in filters.items():
                    if isinstance(value, list):
                        df = df.filter(col(column).isin(value))
                    else:
                        df = df.filter(col(column) == value)

            self.logger.info("Read %d records from external table", df.count())
            return df

        except Exception as e:
            self.logger.error(
                "Error reading external table %s.%s: %s", database, table, e
            )
            raise

    def read_iceberg_table(self, database, table, filters=None):
        """
        Read data from Iceberg table (glue_catalog) using DataFrame.

        Args:
            database: Database name
            table: Table name
            filters: Optional filters dict

        Returns:
            DataFrame: Data from Iceberg table (Glue v5 best practice)
        """
        try:
            full_table_name = f"{self.iceberg_catalog}.{database}.{table}"
            self.logger.info("Reading from Iceberg table: %s", full_table_name)

            # Read using Spark SQL
            query = f"SELECT * FROM {full_table_name}"

            # Apply filters if provided
            if filters:
                filter_conditions = []
                for column, value in filters.items():
                    if isinstance(value, list):
                        values_str = ", ".join(
                            [f"'{v}'" if isinstance(v, str) else str(v) for v in value]
                        )
                        filter_conditions.append(f"{column} IN ({values_str})")
                    else:
                        value_str = (
                            f"'{value}'" if isinstance(value, str) else str(value)
                        )
                        filter_conditions.append(f"{column} = {value_str}")

                if filter_conditions:
                    query += f" WHERE {' AND '.join(filter_conditions)}"

            df = self.spark.sql(query)

            self.logger.info("Read %d records from Iceberg table", df.count())
            return df

        except Exception as e:
            self.logger.error(
                "Error reading Iceberg table %s.%s: %s", database, table, e
            )
            raise

    def write_to_iceberg(self, df, database, table, mode="append"):
        """
        Write DataFrame to Iceberg table (Glue v5 best practice).

        Args:
            df: DataFrame to write
            database: Database name
            table: Table name
            mode: Write mode ("append", "overwrite")
        """
        try:
            if df.count() == 0:
                self.logger.warning("Empty DataFrame, skipping write")
                return

            self.logger.info(
                "Writing %d records to Iceberg table: %s.%s",
                df.count(),
                database,
                table,
            )

            # Write to Iceberg table using DataFrame API (Glue v5 best practice)
            if mode == "overwrite":
                df.writeTo(f"glue_catalog.{database}.{table}").overwritePartitions()
            else:
                df.writeTo(f"glue_catalog.{database}.{table}").append()

            self.logger.info(
                "Successfully wrote to Iceberg table: %s.%s", database, table
            )

        except Exception as e:
            self.logger.error(
                "Error writing to Iceberg table %s.%s: %s", database, table, e
            )
            raise

    def create_external_table(
        self, database, table, schema, location, partition_columns=None
    ):
        """
        Create external table in spark_catalog.

        Args:
            database: Database name
            table: Table name
            schema: Table schema
            location: S3 location
            partition_columns: Optional partition columns
        """
        try:
            full_table_name = f"{self.hive_catalog}.{database}.{table}"
            self.logger.info("Creating external table: %s", full_table_name)

            # Drop table if exists
            self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

            # Build CREATE TABLE statement
            create_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {full_table_name} (
                {schema}
            )
            """

            # Add partitioning if specified
            if partition_columns:
                partition_clause = ", ".join(partition_columns)
                create_sql += f"\nPARTITIONED BY ({partition_clause})"

            # Add SerDe and location
            create_sql += f"""
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
                'ignore.malformed.json' = 'true'
            )
            LOCATION '{location}'
            TBLPROPERTIES (
                'classification' = 'json',
                'compressionType' = 'none',
                'typeOfData' = 'file'
            )
            """

            self.spark.sql(create_sql)

            # Repair partitions if partitioned
            if partition_columns:
                self.spark.sql(f"MSCK REPAIR TABLE {full_table_name}")

            self.logger.info("External table created successfully: %s", full_table_name)

        except Exception as e:
            self.logger.error(
                "Error creating external table %s.%s: %s", database, table, e
            )
            raise

    def create_iceberg_table(self, database, table, schema, partition_columns=None):
        """
        Create Iceberg table in glue_catalog.

        Args:
            database: Database name
            table: Table name
            schema: Table schema
            partition_columns: Optional partition columns
        """
        try:
            full_table_name = f"{self.iceberg_catalog}.{database}.{table}"
            self.logger.info("Creating Iceberg table: %s", full_table_name)

            # Drop table if exists
            self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

            # Build CREATE TABLE statement
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {schema}
            )
            USING ICEBERG
            """

            # Add partitioning if specified
            if partition_columns:
                partition_clause = ", ".join(partition_columns)
                create_sql += f"\nPARTITIONED BY ({partition_clause})"

            self.spark.sql(create_sql)
            self.logger.info("Iceberg table created successfully: %s", full_table_name)

        except Exception as e:
            self.logger.error(
                "Error creating Iceberg table %s.%s: %s", database, table, e
            )
            raise

    def create_database_if_not_exists(self, database, catalog=None):
        """
        Create database if it doesn't exist.

        Args:
            database: Database name
            catalog: Catalog name (optional)
        """
        try:
            if catalog:
                full_db_name = f"{catalog}.{database}"
            else:
                # Default to Iceberg catalog
                full_db_name = f"{self.iceberg_catalog}.{database}"

            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}")
            self.logger.info("Database created/verified: %s", full_db_name)

        except Exception as e:
            self.logger.error("Error creating database %s: %s", database, e)
            raise

    # ==========================================
    # PUBLIC API - Main Entry Point
    # ==========================================

    def run(self):
        """
        Run complete ETL pipeline with full infrastructure setup.

        This is the main entry point for all jobs. It handles:
        1. Infrastructure setup (databases + tables) if config.create_tables=True
        2. ETL execution (extract → transform → load)

        Override this method for custom workflows.
        """
        try:
            self.logger.info("🚀 Starting %s layer ETL pipeline", self.layer)

            # 1. Ensure infrastructure exists
            self._ensure_infrastructure()

            # 2. Execute ETL
            self._execute_etl()

            self.logger.info("✅ %s layer pipeline completed successfully", self.layer)

        except Exception as e:
            self.logger.error("❌ %s layer pipeline failed: %s", self.layer, e)
            raise

    # ==========================================
    # INFRASTRUCTURE SETUP (INTERNAL)
    # ==========================================

    def _ensure_infrastructure(self):
        """
        Ensure all required infrastructure exists.

        This includes:
        1. Databases (automatically determined by layer)
        2. Tables (custom implementation per job)

        Controlled by config.create_tables flag.
        """
        if not hasattr(self.config, "create_tables") or not self.config.create_tables:
            self.logger.info("⏭️  Skipping infrastructure setup (create_tables=False)")
            return

        self.logger.info("🏗️  Setting up infrastructure...")

        # 1. Create databases
        self._ensure_databases()

        # 2. Create tables
        self._ensure_tables()

        self.logger.info("✅ Infrastructure ready")

    def _ensure_databases(self):
        """
        Create required databases if they don't exist.

        Databases are determined by _get_required_databases() which should be
        overridden in layer-specific base classes (BronzeJobBase, SilverJobBase, etc.)
        """
        databases = self._get_required_databases()

        if not databases:
            self.logger.warning("No databases defined for this job")
            return

        for catalog, database, layer_name in databases:
            try:
                full_db_name = f"{catalog}.{database}"
                self.logger.info(
                    f"   📦 Creating {layer_name} database: {full_db_name}"
                )
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_db_name}")
            except Exception as e:
                self.logger.error(f"Failed to create database {full_db_name}: {e}")
                raise

    def _get_required_databases(self):
        """
        Get list of required databases for this job.

        Returns list of tuples: [(catalog, database, layer_name), ...]

        This method should be overridden in layer-specific base classes:
        - BronzeJobBase: returns [(spark_catalog, raw_db), (glue_catalog, bronze_db)]
        - SilverJobBase: returns [(glue_catalog, bronze_db), (glue_catalog, silver_db)]
        - GoldJobBase: returns [(glue_catalog, silver_db), (glue_catalog, gold_db)]

        Override in subclass for custom database requirements.
        """
        self.logger.warning(
            "_get_required_databases() not implemented, no databases will be created"
        )
        return []

    def _ensure_tables(self):
        """
        Create required tables if they don't exist.

        Calls the create_tables() method which must be implemented by concrete job classes.
        """
        self.logger.info("   📋 Creating tables...")
        try:
            self.create_tables()
        except NotImplementedError:
            self.logger.warning(
                "create_tables() not implemented, skipping table creation"
            )

    # ==========================================
    # ETL EXECUTION (INTERNAL)
    # ==========================================

    def _execute_etl(self):
        """
        Execute the ETL pipeline: Extract → Transform → Load.

        This is the core data processing flow.
        """
        self.logger.info("📥 Extract → Transform → Load")

        # Extract
        extracted_data = self.extract()

        # Transform
        transformed_data = self.transform(extracted_data)

        # Load
        self.load(transformed_data)

    # ==========================================
    # ABSTRACT METHODS (MUST IMPLEMENT)
    # ==========================================

    def create_tables(self):
        """
        Create required tables for this job.

        Must be implemented by concrete job classes.

        Example:
            def create_tables(self):
                # Create raw external table
                self.spark.sql(f"CREATE EXTERNAL TABLE ...")

                # Create bronze Iceberg table
                self.spark.sql(f"CREATE TABLE ... USING ICEBERG")
        """
        raise NotImplementedError("Subclasses must implement create_tables() method")

    def extract(self):
        """
        Extract data from source.

        Must be implemented by concrete job classes.

        Returns:
            DataFrame: Extracted data
        """
        raise NotImplementedError("Subclasses must implement extract() method")

    def transform(self, data):
        """
        Transform data according to layer requirements.

        Must be implemented by concrete job classes.

        Args:
            data: Input DataFrame from extract()

        Returns:
            DataFrame: Transformed data
        """
        raise NotImplementedError("Subclasses must implement transform() method")

    def load(self, data):
        """
        Load data to target table.

        Must be implemented by concrete job classes.

        Args:
            data: Transformed DataFrame from transform()
        """
        raise NotImplementedError("Subclasses must implement load() method")

    # ==========================================
    # LEGACY METHODS (DEPRECATED)
    # ==========================================

    def run_medallion_pipeline(self):
        """
        DEPRECATED: Use run() instead.

        This method is kept for backward compatibility but will be removed in future versions.
        """
        self.logger.warning("run_medallion_pipeline() is deprecated, use run() instead")
        self.run()
