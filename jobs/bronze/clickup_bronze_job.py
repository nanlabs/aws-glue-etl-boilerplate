"""
ClickUp Bronze Pipeline Job - Technology Requests

This job processes ClickUp data from raw to bronze layer with support for both
local development (LocalStack + Derby) and AWS Glue (GlueContext + Glue Data Catalog).

Features:
- ✅ Hybrid support: Works locally with LocalStack and in AWS Glue
- ✅ AWS Glue v5 DataFrame-first approach
- ✅ BronzeJobBase with encapsulated infrastructure setup
- ✅ Bronze layer processing: raw → bronze
- ✅ Pydantic-based configuration with automatic parameter resolution
- ✅ ClickUpBronzeConfig with ClickUp-specific defaults
- ✅ Custom fields parsing (emoji fields for satisfaction)
- ✅ Time-based partitioning by date_created

Local Usage (auto-detected, no --JOB_NAME):
  python jobs/bronze/clickup_bronze_job.py

AWS Glue Usage (auto-detected with --JOB_NAME):
  python jobs/bronze/clickup_bronze_job.py --JOB_NAME clickup_bronze_requests

Minimal Usage (using defaults):
  python jobs/bronze/clickup_bronze_job.py --JOB_NAME clickup_bronze_requests

Full Usage (overriding defaults):
  python jobs/bronze/clickup_bronze_job.py \\
    --JOB_NAME clickup_bronze_requests \\
    --RAW_DATABASE_NAME nan_develop_raw_zone_ingestion \\
    --RAW_TABLE_NAME clickup__technology__requests_raw \\
    --BRONZE_DATABASE_NAME nan_develop_bronze_zone_ingestion \\
    --BRONZE_TABLE_NAME clickup__technology__requests_bronze \\
    --WAREHOUSE_PATH s3://bucket/nan/

Required Parameters (Glue mode only):
- JOB_NAME: AWS Glue job name (triggers Glue mode)

Optional Parameters (with ClickUp defaults):
- SOURCE_NAME: Source system name (default: 'clickup')
- RAW_DATABASE_NAME: Raw layer database name (default: 'nan_develop_raw_zone_ingestion')
- RAW_TABLE_NAME: Raw layer table name (default: 'clickup__technology__requests_raw')
- BRONZE_DATABASE_NAME: Bronze layer database name (default: 'nan_develop_bronze_zone_ingestion')
- BRONZE_TABLE_NAME: Bronze layer table name (default: 'clickup__technology__requests_bronze')
- WAREHOUSE_PATH: S3 location for Iceberg warehouse (default: develop bucket)
- CREATE_TABLES: Whether to create tables (default: true)
- SHOW_COUNTS: Whether to show record counts (default: true)
- ENVIRONMENT: Environment (develop/staging/prod, default: develop)
- STAGE: Stage (local/dev/staging/prod, default: local)
- LOG_LEVEL: Logging level (default: INFO)
"""

import logging
from typing import Optional

from pydantic import Field

from libs.common import BronzeJobConfig, setup_logging
from libs.pyspark import BronzeJobBase, SessionConfig, SparkSessionFactory

# Configure logging
logger = logging.getLogger(__name__)


# ==========================================
# CLICKUP BRONZE CONFIGURATION
# ==========================================
class ClickUpBronzeConfig(BronzeJobConfig):
    """
    ClickUp-specific Bronze configuration.

    Extends BronzeJobConfig with ClickUp-specific fields.
    Auto-configures table names and paths.
    """

    # Source name with default value
    source_name: str = Field(
        default="clickup",
        description="Source system name (ClickUp)",
    )

    # Auto-configured fields (set in model_post_init)
    raw_table_name: Optional[str] = Field(
        default=None,
        description="Raw table name (auto-configured: clickup__technology__requests_raw)",
    )
    bronze_table_name: Optional[str] = Field(
        default=None,
        description="Bronze table name (auto-configured: clickup__technology__requests_bronze)",
    )
    clickup_raw_path: Optional[str] = Field(
        default=None,
        description="S3 path for ClickUp raw data (auto-configured: raw_zone_path + clickup/tasks/)",
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure table names and paths if not provided."""
        if not self.raw_table_name:
            self.raw_table_name = "clickup__technology__requests_raw"
        if not self.bronze_table_name:
            self.bronze_table_name = "clickup__technology__requests_bronze"
        # Auto-configure clickup_raw_path: raw_zone_path + clickup/tasks/
        if not self.clickup_raw_path:
            self.clickup_raw_path = f"{self.raw_zone_path}clickup/tasks/"


# ==========================================
# CLICKUP BRONZE JOB
# ==========================================
class ClickUpBronzeJob(BronzeJobBase):
    """
    ClickUp bronze layer processing job.

    Processes ClickUp tasks from raw to bronze layer:
    - Parses JSON payloads
    - Extracts key fields (task_id, name, status, dates, etc.)
    - Extracts custom fields (emoji fields for satisfaction)
    - Parses arrays (labels, users)
    - Calculates derived fields (resolution time, SLA compliance)
    - Partitions by year/month based on date_created
    """

    def __init__(self, spark_or_glue_context, config: ClickUpBronzeConfig):
        """
        Initialize ClickUp bronze job.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (AWS Glue)
            config: ClickUpBronzeConfig instance
        """
        super().__init__(spark_or_glue_context, config)
        self.config: ClickUpBronzeConfig = config  # Type hint for IDE support

        logger.info("Initialized ClickUp Bronze job")
        logger.info(
            f"  Raw table: {self.config.raw_database_name}.{self.config.raw_table_name}"
        )
        logger.info(
            f"  Bronze table: {self.config.bronze_database_name}.{self.config.bronze_table_name}"
        )

    def extract(self):
        """Extract data from raw zone external table using DataFrame (Glue v5 best practice)."""
        self.logger.info(
            f"Extracting from {self.config.raw_database_name}.{self.config.raw_table_name}"
        )
        # Use read_external_table which automatically does MSCK REPAIR TABLE for partitioned tables
        df = self.read_external_table(
            self.config.raw_database_name,
            self.config.raw_table_name,
            is_partitioned=True,  # ClickUp raw tables are partitioned by year/month
        )

        # Filter out null payloads
        df = df.filter(df.payload.isNotNull())

        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records from raw table")

        return df

    def transform(self, df):
        """
        Transform raw ClickUp tasks to bronze format.

        Extracts:
        - Task fields (id, name, status, priority, dates)
        - Custom fields (emoji fields for satisfaction)
        - Arrays (assignees, labels, custom field values)
        - Calculated fields (resolution_days, sla_compliance)
        """
        from pyspark.sql.functions import (
            coalesce,
            col,
            current_timestamp,
            datediff,
            from_unixtime,
            get_json_object,
            lit,
            lower,
        )
        from pyspark.sql.functions import month as spark_month
        from pyspark.sql.functions import to_timestamp, when
        from pyspark.sql.functions import year as spark_year

        logger.info("Transforming ClickUp tasks to bronze format")

        # ClickUp task structure:
        # - id: task ID
        # - name: task name
        # - status: status object with status field
        # - date_created: timestamp in milliseconds
        # - date_updated: timestamp in milliseconds
        # - date_closed: timestamp in milliseconds (nullable)
        # - due_date: timestamp in milliseconds (nullable)
        # - assignees: array of user objects
        # - tags: array of tag objects
        # - custom_fields: array of custom field objects
        # - time_spent: time logged in milliseconds (nullable)
        # - time_estimate: time estimate in milliseconds (nullable)
        # - priority: priority object with priority field

        # Define status categories based on ClickUp workflow
        # Not started: BACKLOG, READY TO DO
        # Active: PAUSED, IN PROGRESS, READY FOR INTERNAL REVIEW
        # Done: READY TO COMMUNICATE, WONT DO (considered DONE for business purposes)
        # Closed: CLOSED
        DONE_STATUSES = [
            "ready to communicate",
            "wont do",
            "closed",
        ]

        bronze_df = df.select(
            # Key fields - extract from ClickUp task payload
            get_json_object(col("payload"), "$.id").alias("task_id"),
            get_json_object(col("payload"), "$.name").alias("task_name"),
            get_json_object(col("payload"), "$.status.status").alias("status"),
            get_json_object(col("payload"), "$.priority.priority").alias("priority"),
            # Dates - ClickUp uses milliseconds timestamps
            # Use from_unixtime instead of to_timestamp to avoid scientific notation issues
            to_timestamp(
                from_unixtime(
                    get_json_object(col("payload"), "$.date_created").cast("bigint") / 1000
                )
            ).alias("date_created"),
            to_timestamp(
                from_unixtime(
                    get_json_object(col("payload"), "$.date_updated").cast("bigint") / 1000
                )
            ).alias("date_updated"),
            # date_done: ClickUp's date_done - when task was marked as done/closed
            # This is set when status changes to DONE or CLOSED
            when(
                get_json_object(col("payload"), "$.date_done").isNotNull(),
                to_timestamp(
                    from_unixtime(
                        get_json_object(col("payload"), "$.date_done").cast("bigint") / 1000
                    )
                ),
            ).alias("date_done"),
            # date_closed: ClickUp's date_closed - only set when status is literally "Closed"
            when(
                get_json_object(col("payload"), "$.date_closed").isNotNull(),
                to_timestamp(
                    from_unixtime(
                        get_json_object(col("payload"), "$.date_closed").cast("bigint") / 1000
                    )
                ),
            ).alias("date_closed"),
            when(
                get_json_object(col("payload"), "$.due_date").isNotNull(),
                to_timestamp(
                    from_unixtime(
                        get_json_object(col("payload"), "$.due_date").cast("bigint") / 1000
                    )
                ),
            ).alias("due_date"),
            # Assignees - extract first assignee email (can be expanded later)
            get_json_object(col("payload"), "$.assignees[0].email").alias(
                "assignee_email"
            ),
            get_json_object(col("payload"), "$.assignees[0].username").alias(
                "assignee_username"
            ),
            # List metadata
            get_json_object(col("payload"), "$._metadata.list_id").alias("list_id"),
            get_json_object(col("payload"), "$.list.name").alias("list_name"),
            get_json_object(col("payload"), "$.folder.name").alias("folder_name"),
            get_json_object(col("payload"), "$.space.id").alias("space_id"),
            # Tags - extract as JSON array (will be parsed in Silver)
            get_json_object(col("payload"), "$.tags").alias("tags_json"),
            # Watchers - extract as JSON array
            get_json_object(col("payload"), "$.watchers").alias("watchers_json"),
            # Dependencies - extract as JSON array
            get_json_object(col("payload"), "$.dependencies").alias(
                "dependencies_json"
            ),
            # Checklists - extract as JSON array
            get_json_object(col("payload"), "$.checklists").alias("checklists_json"),
            # Custom fields - extract raw array for parsing in Silver layer
            # Full parsing with UDFs will be done in Silver for better flexibility
            get_json_object(col("payload"), "$.custom_fields").alias(
                "custom_fields_json"
            ),
            # Time tracking - ClickUp uses milliseconds
            when(
                get_json_object(col("payload"), "$.time_spent").isNotNull(),
                (
                    get_json_object(col("payload"), "$.time_spent").cast("bigint")
                    / 3600000.0
                ).cast("double"),
            ).alias("time_logged_hours"),
            when(
                get_json_object(col("payload"), "$.time_estimate").isNotNull(),
                (
                    get_json_object(col("payload"), "$.time_estimate").cast("bigint")
                    / 3600000.0
                ).cast("double"),
            ).alias("time_estimate_hours"),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
            # Partitioning columns (from raw or computed)
            # Use from_unixtime for consistent timestamp parsing
            coalesce(
                col("year"),
                spark_year(
                    to_timestamp(
                        from_unixtime(
                            get_json_object(col("payload"), "$.date_created").cast("bigint") / 1000
                        )
                    )
                ),
            ).alias("year"),
            coalesce(
                col("month"),
                spark_month(
                    to_timestamp(
                        from_unixtime(
                            get_json_object(col("payload"), "$.date_created").cast("bigint") / 1000
                        )
                    )
                ),
            ).alias("month"),
        )

        # Calculate derived fields
        # Resolution days: date_created to date_done (when task was marked as done/closed)
        # date_done is the actual completion date from ClickUp API
        bronze_df = bronze_df.withColumn(
            "resolution_days",
            when(
                col("date_done").isNotNull(),
                datediff(col("date_done"), col("date_created")),
            ).otherwise(None),
        )

        # SLA compliance: check if date_done <= due_date (if both exist)
        # Uses date_done as the completion date for SLA calculation
        bronze_df = bronze_df.withColumn(
            "sla_compliant",
            when(
                (col("date_done").isNotNull()) & (col("due_date").isNotNull()),
                col("date_done") <= col("due_date"),
            ).otherwise(None),
        )

        # Add status category for easier filtering
        # Not started: backlog, ready to do
        # Active: paused, in progress, ready for internal review
        # Done: ready to communicate, wont do
        # Closed: closed
        bronze_df = bronze_df.withColumn(
            "status_category",
            when(
                lower(col("status")).isin(["backlog", "ready to do"]),
                lit("not_started"),
            )
            .when(
                lower(col("status")).isin(
                    ["paused", "in progress", "ready for internal review"]
                ),
                lit("active"),
            )
            .when(
                lower(col("status")).isin(["ready to communicate", "wont do"]),
                lit("done"),
            )
            .when(lower(col("status")) == "closed", lit("closed"))
            .otherwise(lit("unknown")),
        )

        # Add is_done flag (business definition: ready to communicate, wont do, closed)
        bronze_df = bronze_df.withColumn(
            "is_done",
            lower(col("status")).isin(
                ["ready to communicate", "wont do", "closed"]
            ),
        )

        # Custom fields parsing will be done in Silver layer with UDFs
        # For now, we preserve the raw custom_fields JSON array

        # Fallback partitioning if year/month are null
        bronze_df = bronze_df.withColumn(
            "year",
            when(col("year").isNull(), spark_year(current_timestamp())).otherwise(
                col("year")
            ),
        )
        bronze_df = bronze_df.withColumn(
            "month",
            when(col("month").isNull(), spark_month(current_timestamp())).otherwise(
                col("month")
            ),
        )

        record_count = bronze_df.count()
        logger.info(f"Transformed {record_count} tasks to bronze format")
        return bronze_df

    def _extract_custom_field_value(self, payload_col, field_name: str):
        """
        Extract value from custom field by name.

        ClickUp custom fields structure:
        {
          "custom_fields": [
            {
              "id": "...",
              "name": "field name",
              "value": "value" or {"value": "value"} or [{"value": "value"}]
            }
          ]
        }

        Note: Full parsing of custom fields will be done in Silver layer with UDFs.
        For Bronze, we extract the raw custom_fields array for later processing.
        """
        from pyspark.sql.functions import get_json_object

        # Extract the custom_fields array - parsing will be done in Silver layer
        # This allows us to preserve all custom field data for later processing
        return get_json_object(payload_col, "$.custom_fields")

    def _extract_custom_field_labels(self, payload_col, field_name: str):
        """
        Extract labels from custom field by name.

        Note: Full parsing will be done in Silver layer with UDFs.
        """
        from pyspark.sql.functions import get_json_object

        # Return custom_fields array for later parsing
        return get_json_object(payload_col, "$.custom_fields")

    def _extract_custom_field_users(self, payload_col, field_name: str):
        """
        Extract users from custom field by name.

        Note: Full parsing will be done in Silver layer with UDFs.
        """
        from pyspark.sql.functions import get_json_object

        # Return custom_fields array for later parsing
        return get_json_object(payload_col, "$.custom_fields")

    def _emoji_to_score(self, emoji_col):
        """
        Map emoji to satisfaction score (2-10).

        Note: This method is DEPRECATED. ClickUp API returns numeric values (1-5),
        not emoji characters. Silver layer handles the conversion: 1-5 → 2-10.

        IMPORTANT: A value of 0 means "not evaluated", NOT a score of 0.
        The minimum valid score is 1 star (maps to score 2).

        Mapping (legacy - not used):
        - 😍 = 10 (love)
        - 😊 = 8 (happy)
        - 😐 = 6 (neutral)
        - 😞 = 4 (sad)
        - 😢 = 2 (very sad)
        - 0 = None (not evaluated)

        Args:
            emoji_col: Spark Column containing emoji string

        Returns:
            Spark Column with satisfaction score
        """
        from pyspark.sql.functions import when

        return (
            when(emoji_col == "😍", 10)
            .when(emoji_col == "😊", 8)
            .when(emoji_col == "😐", 5)
            .when(emoji_col == "😞", 2)
            .when(emoji_col == "😢", 1)
            .otherwise(None)
        )

    def load(self, df) -> None:
        """
        Load DataFrame to Iceberg bronze table.

        Uses append strategy (time-based partitioning by year/month).
        """
        # Skip loading if DataFrame is empty
        if df.count() == 0:
            logger.info("Empty DataFrame, skipping load operation")
            return

        logger.info("Loading ClickUp tasks to bronze...")
        logger.info(
            f"  Target: {self.config.bronze_database_name}.{self.config.bronze_table_name}"
        )

        # Use append strategy (time-based data)
        self.write_to_iceberg(
            df,
            self.config.bronze_database_name,
            self.config.bronze_table_name,
            mode="append",
        )

        logger.info("✅ Successfully loaded ClickUp tasks to bronze")

    def create_tables(self):
        """
        Create external table for JSON Lines and bronze Iceberg table.

        Creates both:
        - Raw external table (spark_catalog) pointing to S3
        - Bronze Iceberg table (glue_catalog)
        """
        is_partitioned = True  # ClickUp tasks are partitioned by year/month

        logger.info("Creating tables for ClickUp technology requests")
        logger.info(f"  Partitioning: {is_partitioned}")

        # ========================================
        # CREATE RAW EXTERNAL TABLE (spark_catalog)
        # ========================================
        logger.info(
            "Creating raw external table: %s.%s",
            self.config.raw_database_name,
            self.config.raw_table_name,
        )

        self.spark.sql(
            f"DROP TABLE IF EXISTS spark_catalog.{self.config.raw_database_name}.{self.config.raw_table_name}"
        )

        # Base table definition
        # Note: metadata is stored as JSON STRING to preserve valid JSON format
        raw_table_ddl = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS spark_catalog.{self.config.raw_database_name}.{self.config.raw_table_name} (
          payload STRING,
          metadata STRING
        )
        PARTITIONED BY (
          year INT,
          month INT
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'true'
        )
        LOCATION '{self.config.clickup_raw_path}'
        TBLPROPERTIES (
          'classification' = 'json',
          'compressionType' = 'none',
          'typeOfData' = 'file'
        )
        """

        self.spark.sql(raw_table_ddl)
        logger.info(
            f"  ✓ Raw table created: spark_catalog.{self.config.raw_database_name}.{self.config.raw_table_name}"
        )

        # ========================================
        # CREATE BRONZE ICEBERG TABLE (glue_catalog)
        # ========================================
        logger.info(
            "Creating bronze Iceberg table: %s.%s",
            self.config.bronze_database_name,
            self.config.bronze_table_name,
        )

        # Drop table if exists
        # Note: After creating spark_catalog table, Spark may have context issues
        # Wrapping in try/except to handle gracefully
        try:
            self.spark.sql(
                f"DROP TABLE IF EXISTS glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
            )
        except Exception as e:
            # If DROP fails due to catalog context issues, log and continue
            # CREATE TABLE IF NOT EXISTS will handle the case where table already exists
            logger.debug(
                f"Could not drop bronze table (may not exist or catalog context issue): {str(e)}"
            )

        # Get bronze schema DDL
        bronze_schema_ddl = self._get_bronze_schema_ddl()

        # Build Iceberg DDL
        # Use IF NOT EXISTS in case DROP failed due to catalog context issues
        bronze_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name} (
          {bronze_schema_ddl}
        )
        USING ICEBERG
        PARTITIONED BY (year, month)
        """

        self.spark.sql(bronze_table_ddl)
        logger.debug(f"bronze_table_ddl: {bronze_table_ddl}")
        logger.info(
            f"  ✓ Bronze table created: glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
        )

        logger.info("✅ Tables created successfully")

    def _get_bronze_schema_ddl(self) -> str:
        """
        Get DDL schema for Bronze table (columns only).

        Returns:
            str: DDL schema definition (without CREATE TABLE part)
        """
        # Schema matches the transform() output
        schema_fields = [
            "task_id STRING",
            "task_name STRING",
            "status STRING",
            "priority STRING",
            "date_created TIMESTAMP",
            "date_updated TIMESTAMP",
            "date_done TIMESTAMP",
            "date_closed TIMESTAMP",
            "due_date TIMESTAMP",
            "assignee_email STRING",
            "assignee_username STRING",
            "list_id STRING",
            "list_name STRING",
            "folder_name STRING",
            "space_id STRING",
            "tags_json STRING",
            "watchers_json STRING",
            "dependencies_json STRING",
            "checklists_json STRING",
            "custom_fields_json STRING",
            "time_logged_hours DOUBLE",
            "time_estimate_hours DOUBLE",
            "source STRING",
            "ingestion_ts TIMESTAMP",
            "payload STRING",
            "metadata STRING",
            "resolution_days INT",
            "sla_compliant BOOLEAN",
            "status_category STRING",
            "is_done BOOLEAN",
            "year INT",
            "month INT",
        ]

        return ",\n          ".join(schema_fields)


# ==========================================
# MAIN EXECUTION (HYBRID LOCAL/GLUE)
# ==========================================
if __name__ == "__main__":
    try:
        setup_logging()
        # Step 1: Load configuration using ClickUpBronzeConfig
        # Parameters are resolved in this order:
        # Configuration resolution (highest to lowest priority):
        #   1. Workflow Run Properties (if in Glue Workflow)
        #   2. Command line arguments (--PARAM_NAME)
        #   3. Environment variables (PARAM_NAME)
        #   4. ClickUp-specific defaults (defined in ClickUpBronzeConfig)
        logger.info("Loading ClickUp Bronze configuration...")
        config = ClickUpBronzeConfig.from_args()

        # Reconfigure logging with config value (in case it was set via Workflow Properties)
        # This ensures the final log level matches the resolved config
        logging.getLogger().setLevel(getattr(logging, config.log_level))

        # Log complete configuration with sources
        config.log_configuration()

        # Step 2: Create hybrid session (auto-detects local vs Glue)
        logger.info("🚀 Initializing Spark session (hybrid mode)...")
        session_config = SessionConfig(
            app_name=config.job_name,
            warehouse_path=config.warehouse_path,
            aws_region=config.aws_region,
        )
        session_info = SparkSessionFactory.create_session(session_config)
        spark = session_info.spark
        is_local = session_info.is_local
        context = session_info.context

        logger.info(f"✅ Session created (local={is_local})")

        # Step 3: Create Bronze Job instance with hybrid support
        if is_local:
            bronze_job = ClickUpBronzeJob(spark, config)
        else:
            bronze_job = ClickUpBronzeJob(context["glue_context"], config)

        # Step 4: Run Bronze Job pipeline
        # Note: run() handles infrastructure setup (databases + tables) automatically
        logger.info(f"🔄 Running {config.source_name} Bronze job...")
        bronze_job.run()

        # Step 5: Show counts if requested
        if config.show_counts:
            logger.info("📊 Showing record counts...")
            spark.sql(
                f"SELECT COUNT(*) as raw_count FROM spark_catalog.{config.raw_database_name}.{config.raw_table_name}"
            ).show()
            spark.sql(
                f"SELECT COUNT(*) as bronze_count FROM glue_catalog.{config.bronze_database_name}.{config.bronze_table_name}"
            ).show()

        # Step 6: Commit job (Glue only)
        if not is_local:
            context["job"].commit()
            logger.info(
                f"✅ {config.source_name} Bronze Pipeline completed successfully (Glue)!"
            )
        else:
            spark.stop()
            logger.info(
                f"✅ {config.source_name} Bronze Pipeline completed successfully (Local)!"
            )

    except Exception as e:
        logger.error(f"❌ Job failed with error: {e}", exc_info=True)
        raise
