"""
ClickUp Silver Pipeline Job - Technology Requests

This job processes ClickUp data from bronze to silver layer with support for both
local development (LocalStack + Derby) and AWS Glue (GlueContext + Glue Data Catalog).

Features:
- ✅ Hybrid support: Works locally with LocalStack and in AWS Glue
- ✅ AWS Glue v5 DataFrame-first approach
- ✅ SilverJobBase with encapsulated infrastructure setup
- ✅ Silver layer processing: bronze → silver
- ✅ Pydantic-based configuration with automatic parameter resolution
- ✅ ClickUpSilverConfig with ClickUp-specific defaults
- ✅ Custom fields parsing with UDFs
- ✅ Multiple Silver tables: dimensions and facts
- ✅ Data quality validation and normalization

Local Usage (auto-detected, no --JOB_NAME):
  python jobs/silver/clickup_silver_job.py

AWS Glue Usage (auto-detected with --JOB_NAME):
  python jobs/silver/clickup_silver_job.py --JOB_NAME clickup_silver_requests

Minimal Usage (using defaults):
  python jobs/silver/clickup_silver_job.py --JOB_NAME clickup_silver_requests

Full Usage (overriding defaults):
  python jobs/silver/clickup_silver_job.py \\
    --JOB_NAME clickup_silver_requests \\
    --BRONZE_DATABASE nan_develop_bronze_zone_ingestion \\
    --BRONZE_TABLE clickup__technology__requests_bronze \\
    --SILVER_DATABASE nan_develop_silver_analytics \\
    --WAREHOUSE_PATH s3://bucket/nan/
"""

import json
import logging
from typing import Optional

from pydantic import Field

from libs.common import SilverJobConfig, setup_logging
from libs.pyspark import SessionConfig, SilverJobBase, SparkSessionFactory

# Configure logging
logger = logging.getLogger(__name__)


# ==========================================
# CLICKUP SILVER CONFIGURATION
# ==========================================
class ClickUpSilverConfig(SilverJobConfig):
    """
    ClickUp-specific Silver configuration.

    Extends SilverJobConfig with ClickUp-specific fields.
    Auto-configures table names and paths.
    """

    # Source name with default value
    source_name: str = Field(
        default="clickup",
        description="Source system name (ClickUp)",
    )

    # Auto-configured fields
    bronze_table_name: Optional[str] = Field(
        default=None,
        description="Bronze table name (auto-configured: clickup__technology__requests_bronze)",
    )
    silver_table_name: Optional[str] = Field(
        default=None,
        description="Silver table name (auto-configured: technology_dim_requests)",
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure table names if not provided."""
        if not self.bronze_table_name:
            self.bronze_table_name = "clickup__technology__requests_bronze"
        if not self.silver_table_name:
            self.silver_table_name = "technology_dim_requests"


# ==========================================
# CLICKUP SILVER JOB
# ==========================================
class ClickUpSilverJob(SilverJobBase):
    """
    ClickUp silver layer processing job.

    Processes ClickUp tasks from bronze to silver layer:
    - Parses custom fields with UDFs
    - Creates dimension tables (requests, stakeholder_areas, teams)
    - Creates fact table (request_events)
    - Validates and cleans satisfaction data
    - Normalizes names and applies business rules
    """

    def __init__(self, spark_or_glue_context, config: ClickUpSilverConfig):
        """
        Initialize ClickUp silver job.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (AWS Glue)
            config: ClickUpSilverConfig instance
        """
        super().__init__(spark_or_glue_context, config)
        self.config: ClickUpSilverConfig = config  # Type hint for IDE support

        logger.info("Initialized ClickUp Silver job")
        logger.info(
            f"  Bronze table: {self.config.bronze_database_name}.{self.config.bronze_table_name}"
        )
        logger.info(f"  Silver database: {self.config.silver_database_name}")

    def extract(self):
        """Extract data from bronze layer."""
        self.logger.info("Extracting ClickUp tasks from bronze table")

        # Use read_iceberg_table which handles catalog correctly
        df = self.read_iceberg_table(
            self.config.bronze_database_name,
            self.config.bronze_table_name,
        )

        # Filter out null task_ids
        df = df.filter(df.task_id.isNotNull())
        record_count = df.count()
        logger.info(f"Extracted {record_count} records from bronze table")

        return df

    def transform(self, df):
        """
        Transform bronze data to silver format.

        Creates multiple tables:
        1. technology_dim_requests - Main dimension table
        2. technology_dim_stakeholder_areas - Stakeholder areas dimension
        3. technology_dim_teams - Teams dimension
        4. technology_fact_request_events - Fact table with events
        """
        from pyspark.sql.functions import (
            col,
            current_date,
            current_timestamp,
            datediff,
            greatest,
            least,
            lit,
            size,
            udf,
            when,
        )
        from pyspark.sql.types import ArrayType, IntegerType, StringType

        logger.info("Transforming ClickUp tasks to silver format")

        if df.count() == 0:
            logger.warning("Empty DataFrame, skipping transformation")
            return df

        # Register UDFs for custom field parsing
        @udf(returnType=StringType())
        def extract_custom_field_value(custom_fields_json, field_name):
            """Extract value from custom field by name."""
            if not custom_fields_json:
                return None
            try:
                custom_fields = (
                    json.loads(custom_fields_json)
                    if isinstance(custom_fields_json, str)
                    else custom_fields_json
                )
                if not isinstance(custom_fields, list):
                    return None
                for field in custom_fields:
                    if field.get("name") == field_name:
                        value = field.get("value")
                        if isinstance(value, dict):
                            return value.get("value")
                        elif isinstance(value, list) and len(value) > 0:
                            return (
                                value[0].get("value")
                                if isinstance(value[0], dict)
                                else str(value[0])
                            )
                        else:
                            return str(value) if value is not None else None
                return None
            except (json.JSONDecodeError, TypeError, AttributeError):
                return None

        @udf(returnType=ArrayType(StringType()))
        def extract_custom_field_labels(custom_fields_json, field_name):
            """
            Extract labels from custom field by name.

            ClickUp label fields return arrays of label IDs (strings).
            We extract the IDs and attempt to map them to names using type_config.options
            if available. If mapping is not available, we return the IDs.
            """
            if not custom_fields_json:
                return []
            try:
                custom_fields = (
                    json.loads(custom_fields_json)
                    if isinstance(custom_fields_json, str)
                    else custom_fields_json
                )
                if not isinstance(custom_fields, list):
                    return []
                for field in custom_fields:
                    if field.get("name") == field_name:
                        value = field.get("value")
                        type_config = field.get("type_config", {})
                        options = type_config.get("options", [])

                        # Create mapping from label ID to label name
                        id_to_name = {}
                        for option in options:
                            option_id = option.get("id")
                            option_label = option.get("label")
                            if option_id and option_label:
                                id_to_name[option_id] = option_label

                        labels = []
                        if isinstance(value, list):
                            # Value is array of label IDs
                            for item in value:
                                if isinstance(item, dict):
                                    # If item is an object, try to get name or id
                                    label_name = (
                                        item.get("name")
                                        or item.get("label")
                                        or item.get("id")
                                    )
                                    if label_name:
                                        # Map ID to name if mapping exists
                                        labels.append(
                                            id_to_name.get(label_name, label_name)
                                        )
                                else:
                                    # Item is a string (label ID)
                                    label_id = str(item)
                                    # Map ID to name if mapping exists
                                    labels.append(id_to_name.get(label_id, label_id))
                        elif isinstance(value, dict):
                            # Single label object
                            label_name = (
                                value.get("name")
                                or value.get("label")
                                or value.get("id")
                            )
                            if label_name:
                                labels.append(id_to_name.get(label_name, label_name))
                        elif value:
                            # Single value (label ID as string)
                            label_id = str(value)
                            labels.append(id_to_name.get(label_id, label_id))
                        return labels
                return []
            except (json.JSONDecodeError, TypeError, AttributeError):
                return []

        @udf(returnType=ArrayType(StringType()))
        def extract_custom_field_users(custom_fields_json, field_name):
            """Extract user emails from custom field by name."""
            if not custom_fields_json:
                return []
            try:
                custom_fields = (
                    json.loads(custom_fields_json)
                    if isinstance(custom_fields_json, str)
                    else custom_fields_json
                )
                if not isinstance(custom_fields, list):
                    return []
                for field in custom_fields:
                    if field.get("name") == field_name:
                        value = field.get("value")
                        users = []
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict):
                                    user_email = item.get("email") or item.get(
                                        "username"
                                    )
                                    if user_email:
                                        users.append(str(user_email))
                                else:
                                    users.append(str(item))
                        elif isinstance(value, dict):
                            user_email = value.get("email") or value.get("username")
                            if user_email:
                                users.append(str(user_email))
                        return users
                return []
            except (json.JSONDecodeError, TypeError, AttributeError):
                return []

        @udf(returnType=IntegerType())
        def emoji_value_to_score(emoji_value_str):
            """
            Convert emoji field numeric value (1-5) to satisfaction score (2-10).

            ClickUp emoji fields return numeric values (1-5), not emoji characters.
            We map 1-5 to 2-10 scale by multiplying by 2.

            IMPORTANT: Value of 0 means "not evaluated" in ClickUp, not a valid score.
            The minimum valid score is 1 star. Values of 0 or None return None.
            """
            if not emoji_value_str:
                return None
            try:
                value = int(str(emoji_value_str).strip())
                # Value 0 means "not evaluated", return None
                # Map valid values 1-5 to 2-10 scale (multiply by 2)
                if 1 <= value <= 5:
                    return value * 2
                return None
            except (ValueError, TypeError, AttributeError):
                return None

        # Parse custom fields with corrected field names
        # Note: ClickUp emoji fields return numeric values (1-5), not emoji characters
        # Note: Use "Account" field, ignore "Client" if it exists (obsolete field)
        df_with_custom_fields = (
            df.withColumn(
                "account",
                extract_custom_field_labels(col("custom_fields_json"), lit("Account")),
            )
            .withColumn(
                "external_satisfaction_value",
                extract_custom_field_value(
                    col("custom_fields_json"),
                    lit("external value (customer satisfaction)"),
                ),
            )
            .withColumn(
                "internal_satisfaction_value",
                extract_custom_field_value(
                    col("custom_fields_json"), lit("internal value")
                ),
            )
            .withColumn(
                "stakeholder_areas",
                extract_custom_field_labels(
                    col("custom_fields_json"), lit("🏠 Stakeholder Area")
                ),
            )
            .withColumn(
                "requesters",
                extract_custom_field_users(
                    col("custom_fields_json"), lit("🙋‍♂️ Requesters")
                ),
            )
            .withColumn(
                "team_members",
                extract_custom_field_users(col("custom_fields_json"), lit("🫂 Team")),
            )
            .withColumn(
                "architecture_responsibility_domain",
                extract_custom_field_labels(
                    col("custom_fields_json"),
                    lit("📑 Architecture Responsibility Domain"),
                ),
            )
            .withColumn(
                "rd_practices",
                extract_custom_field_labels(
                    col("custom_fields_json"), lit("🤖 R&D Practices")
                ),
            )
        )

        # Normalize satisfaction values: treat 0 as NULL (not evaluated)
        # In ClickUp emoji fields, 0 means "not evaluated", valid values are 1-5
        df_with_custom_fields = df_with_custom_fields.withColumn(
            "external_satisfaction_value",
            when(
                (col("external_satisfaction_value").isNotNull())
                & (col("external_satisfaction_value") != "0")
                & (col("external_satisfaction_value") != ""),
                col("external_satisfaction_value"),
            ).otherwise(lit(None)),
        ).withColumn(
            "internal_satisfaction_value",
            when(
                (col("internal_satisfaction_value").isNotNull())
                & (col("internal_satisfaction_value") != "0")
                & (col("internal_satisfaction_value") != ""),
                col("internal_satisfaction_value"),
            ).otherwise(lit(None)),
        )

        # Calculate satisfaction scores from numeric values (1-5 → 2-10)
        df_with_scores = df_with_custom_fields.withColumn(
            "external_satisfaction_score",
            emoji_value_to_score(col("external_satisfaction_value")),
        ).withColumn(
            "internal_satisfaction_score",
            emoji_value_to_score(col("internal_satisfaction_value")),
        )

        # Add temporal derived fields based on ClickUp status workflow:
        # Not started: BACKLOG, READY TO DO
        # Active: PAUSED, IN PROGRESS, READY FOR INTERNAL REVIEW
        # Done: READY TO COMMUNICATE, WONT DO
        # Closed: CLOSED
        from pyspark.sql.functions import lower

        df_with_scores = (
            df_with_scores.withColumn(
                "days_in_backlog",
                when(
                    (col("status").isNotNull())
                    & (lower(col("status")).isin(["backlog", "ready to do"])),
                    datediff(current_date(), col("date_created")),
                ).otherwise(None),
            )
            .withColumn(
                "days_in_progress",
                when(
                    (col("status").isNotNull())
                    & (
                        lower(col("status")).isin(
                            ["paused", "in progress", "ready for internal review"]
                        )
                    ),
                    datediff(current_date(), col("date_updated")),
                ).otherwise(None),
            )
            .withColumn(
                "days_in_review",
                when(
                    (col("status").isNotNull())
                    & (lower(col("status")) == "ready for internal review"),
                    datediff(current_date(), col("date_updated")),
                ).otherwise(None),
            )
        )

        # Calculate completeness score (0-1): based on filled fields
        # Note: account and stakeholder_areas are arrays before concat_ws, so we check size
        df_with_scores = df_with_scores.withColumn(
            "completeness_score",
            (
                when(col("task_name").isNotNull(), 1).otherwise(0)
                + when(col("status").isNotNull(), 1).otherwise(0)
                + when(col("priority").isNotNull(), 1).otherwise(0)
                + when(col("assignee_email").isNotNull(), 1).otherwise(0)
                + when(
                    (col("account").isNotNull()) & (size(col("account")) > 0), 1
                ).otherwise(0)
                + when(
                    (col("stakeholder_areas").isNotNull())
                    & (size(col("stakeholder_areas")) > 0),
                    1,
                ).otherwise(0)
            ).cast("double")
            / lit(6.0),
        )

        # Calculate urgency score (0-1): based on priority + due_date proximity
        df_with_scores = df_with_scores.withColumn(
            "urgency_score",
            (
                when(
                    col("priority").isNotNull(),
                    when(col("priority") == "urgent", lit(0.5))
                    .when(col("priority") == "high", lit(0.375))
                    .when(col("priority") == "normal", lit(0.25))
                    .when(col("priority") == "low", lit(0.125))
                    .otherwise(lit(0.0)),
                ).otherwise(lit(0.0))
                + when(
                    (col("due_date").isNotNull())
                    & (col("due_date") >= current_timestamp()),
                    least(
                        lit(0.5),
                        greatest(
                            lit(0.0),
                            lit(0.5)
                            - (
                                datediff(col("due_date"), current_timestamp())
                                / lit(60.0)
                            ),
                        ),
                    ),
                ).otherwise(lit(0.0))
            ),
        )

        # Create main dimension table: technology_dim_requests
        from pyspark.sql.functions import concat_ws

        requests_dim = df_with_scores.select(
            col("task_id").alias("request_id"),
            col("task_name").alias("request_name"),
            col("status"),
            col("priority"),
            col("date_created"),
            col("date_updated"),
            col("date_done"),  # When task was marked as done (from ClickUp API)
            col("date_closed"),  # When task was marked as closed (only for "Closed" status)
            col("due_date"),
            col("assignee_email").alias("assignee"),
            col("list_id"),
            col("list_name"),  # From Bronze enrichment
            col("folder_name"),  # From Bronze enrichment
            col("space_id"),  # From Bronze enrichment
            concat_ws(", ", col("account")).alias(
                "account"
            ),  # Convert ARRAY<STRING> to STRING
            col("external_satisfaction_value"),
            col("internal_satisfaction_value"),
            col("external_satisfaction_score"),
            col("internal_satisfaction_score"),
            col("time_logged_hours"),
            col("time_estimate_hours"),
            col("resolution_days"),
            col("sla_compliant"),
            concat_ws(", ", col("stakeholder_areas")).alias(
                "stakeholder_areas"
            ),  # Convert ARRAY<STRING> to STRING
            concat_ws(", ", col("requesters")).alias(
                "requesters"
            ),  # Convert ARRAY<STRING> to STRING
            concat_ws(", ", col("team_members")).alias(
                "team_members"
            ),  # Convert ARRAY<STRING> to STRING
            concat_ws(", ", col("architecture_responsibility_domain")).alias(
                "architecture_responsibility_domain"
            ),  # Convert ARRAY<STRING> to STRING
            concat_ws(", ", col("rd_practices")).alias(
                "rd_practices"
            ),  # Convert ARRAY<STRING> to STRING
            col("days_in_backlog"),
            col("days_in_progress"),
            col("days_in_review"),
            col("completeness_score"),
            col("urgency_score"),
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            col("year"),
            col("month"),
        )

        logger.info(f"Created requests dimension with {requests_dim.count()} records")

        # Store transformed data for load method
        self.requests_dim = requests_dim
        self.df_with_scores = df_with_scores

        return requests_dim

    def load(self, df) -> None:
        """
        Load DataFrames to Iceberg silver tables.

        Creates multiple tables:
        1. technology_dim_requests - Main dimension
        2. technology_dim_stakeholder_areas - Stakeholder areas dimension
        3. technology_dim_teams - Teams dimension
        4. technology_fact_request_events - Fact table
        """
        from pyspark.sql.functions import col, current_timestamp, explode, lit

        logger.info("Loading ClickUp data to silver tables...")

        # 1. Load main dimension table: technology_dim_requests
        if df.count() > 0:
            logger.info("Loading technology_dim_requests...")
            self.write_to_iceberg(
                df,
                self.config.silver_database_name,
                "technology_dim_requests",
                mode="append",
            )
            logger.info("✅ Loaded technology_dim_requests")

        # 2. Create stakeholder areas dimension (explode from requests)
        if hasattr(self, "df_with_scores"):
            from pyspark.sql.functions import explode, lower, trim

            stakeholder_areas_df = self.df_with_scores.select(
                explode(col("stakeholder_areas")).alias("stakeholder_area"),
                col("task_id").alias("request_id"),
                col("year"),
                col("month"),
            ).filter(col("stakeholder_area").isNotNull())

            if stakeholder_areas_df.count() > 0:
                # Normalize stakeholder area names
                stakeholder_areas_df = (
                    stakeholder_areas_df.withColumn(
                        "stakeholder_area_normalized",
                        trim(lower(col("stakeholder_area"))),
                    )
                    .select(
                        col("stakeholder_area_normalized").alias("stakeholder_area"),
                        col("request_id"),
                        col("year"),
                        col("month"),
                    )
                    .distinct()
                )

                logger.info("Loading technology_dim_stakeholder_areas...")
                self.write_to_iceberg(
                    stakeholder_areas_df,
                    self.config.silver_database_name,
                    "technology_dim_stakeholder_areas",
                    mode="append",
                )
                logger.info("✅ Loaded technology_dim_stakeholder_areas")

        # 3. Create teams dimension (explode from requests)
        if hasattr(self, "df_with_scores"):
            from pyspark.sql.functions import explode, lower, trim

            teams_df = self.df_with_scores.select(
                explode(col("team_members")).alias("team_member"),
                col("task_id").alias("request_id"),
                col("year"),
                col("month"),
            ).filter(col("team_member").isNotNull())

            if teams_df.count() > 0:
                # Normalize team member names
                teams_df = (
                    teams_df.withColumn(
                        "team_member_normalized", trim(lower(col("team_member")))
                    )
                    .select(
                        col("team_member_normalized").alias("team_member"),
                        col("request_id"),
                        col("year"),
                        col("month"),
                    )
                    .distinct()
                )

                logger.info("Loading technology_dim_teams...")
                self.write_to_iceberg(
                    teams_df,
                    self.config.silver_database_name,
                    "technology_dim_teams",
                    mode="append",
                )
                logger.info("✅ Loaded technology_dim_teams")

        # 4. Create fact table: technology_fact_request_events
        if hasattr(self, "df_with_scores"):
            fact_df = self.df_with_scores.select(
                col("task_id").alias("request_id"),
                col("date_created").alias("event_date"),
                lit("created").alias("event_type"),
                col("status"),
                col("priority"),
                col("list_id"),
                col("external_satisfaction_score"),
                col("internal_satisfaction_score"),
                col("resolution_days"),
                col("sla_compliant"),
                col("time_logged_hours"),
                col("time_estimate_hours"),
                lit(self.config.source_name).alias("source"),
                current_timestamp().alias("ingestion_ts"),
                col("year"),
                col("month"),
            )

            # Add update events
            update_fact_df = self.df_with_scores.filter(
                col("date_updated").isNotNull()
            ).select(
                col("task_id").alias("request_id"),
                col("date_updated").alias("event_date"),
                lit("updated").alias("event_type"),
                col("status"),
                col("priority"),
                col("list_id"),
                col("external_satisfaction_score"),
                col("internal_satisfaction_score"),
                col("resolution_days"),
                col("sla_compliant"),
                col("time_logged_hours"),
                col("time_estimate_hours"),
                lit(self.config.source_name).alias("source"),
                current_timestamp().alias("ingestion_ts"),
                col("year"),
                col("month"),
            )

            # Combine events
            all_events_df = fact_df.union(update_fact_df)

            if all_events_df.count() > 0:
                logger.info("Loading technology_fact_request_events...")
                self.write_to_iceberg(
                    all_events_df,
                    self.config.silver_database_name,
                    "technology_fact_request_events",
                    mode="append",
                )
                logger.info("✅ Loaded technology_fact_request_events")

        logger.info("✅ Successfully loaded all silver tables")

    def create_tables(self):
        """
        Create Silver Iceberg tables.

        Creates 4 tables:
        1. technology_dim_requests - Main dimension
        2. technology_dim_stakeholder_areas - Stakeholder areas dimension
        3. technology_dim_teams - Teams dimension
        4. technology_fact_request_events - Fact table
        """
        is_partitioned = True  # All tables are partitioned by year/month

        logger.info("Creating Silver tables for ClickUp technology requests")
        logger.info(f"  Partitioning: {is_partitioned}")

        # 1. Create technology_dim_requests
        logger.info("Creating technology_dim_requests...")
        self.spark.sql(
            f"DROP TABLE IF EXISTS glue_catalog.{self.config.silver_database_name}.technology_dim_requests"
        )
        requests_ddl = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.silver_database_name}.technology_dim_requests (
          request_id STRING NOT NULL,
          request_name STRING,
          status STRING,
          priority STRING,
          date_created TIMESTAMP,
          date_updated TIMESTAMP,
          date_done TIMESTAMP,
          date_closed TIMESTAMP,
          due_date TIMESTAMP,
          assignee STRING,
          list_id STRING,
          list_name STRING,
          folder_name STRING,
          space_id STRING,
          account STRING,
          external_satisfaction_value STRING,
          internal_satisfaction_value STRING,
          external_satisfaction_score INT,
          internal_satisfaction_score INT,
          time_logged_hours DOUBLE,
          time_estimate_hours DOUBLE,
          resolution_days INT,
          sla_compliant BOOLEAN,
          stakeholder_areas STRING,
          requesters STRING,
          team_members STRING,
          architecture_responsibility_domain STRING,
          rd_practices STRING,
          days_in_backlog INT,
          days_in_progress INT,
          days_in_review INT,
          completeness_score DOUBLE,
          urgency_score DOUBLE,
          source STRING,
          ingestion_ts TIMESTAMP,
          year INT,
          month INT
        )
        USING ICEBERG
        """
        if is_partitioned:
            requests_ddl += "\n        PARTITIONED BY (year, month)"
        self.spark.sql(requests_ddl)
        logger.info("  ✓ Created technology_dim_requests")

        # 2. Create technology_dim_stakeholder_areas
        logger.info("Creating technology_dim_stakeholder_areas...")
        self.spark.sql(
            f"DROP TABLE IF EXISTS glue_catalog.{self.config.silver_database_name}.technology_dim_stakeholder_areas"
        )
        stakeholder_areas_ddl = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.silver_database_name}.technology_dim_stakeholder_areas (
          stakeholder_area STRING NOT NULL,
          request_id STRING NOT NULL,
          year INT,
          month INT
        )
        USING ICEBERG
        """
        if is_partitioned:
            stakeholder_areas_ddl += "\n        PARTITIONED BY (year, month)"
        self.spark.sql(stakeholder_areas_ddl)
        logger.info("  ✓ Created technology_dim_stakeholder_areas")

        # 3. Create technology_dim_teams
        logger.info("Creating technology_dim_teams...")
        self.spark.sql(
            f"DROP TABLE IF EXISTS glue_catalog.{self.config.silver_database_name}.technology_dim_teams"
        )
        teams_ddl = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.silver_database_name}.technology_dim_teams (
          team_member STRING NOT NULL,
          request_id STRING NOT NULL,
          year INT,
          month INT
        )
        USING ICEBERG
        """
        if is_partitioned:
            teams_ddl += "\n        PARTITIONED BY (year, month)"
        self.spark.sql(teams_ddl)
        logger.info("  ✓ Created technology_dim_teams")

        # 4. Create technology_fact_request_events
        logger.info("Creating technology_fact_request_events...")
        self.spark.sql(
            f"DROP TABLE IF EXISTS glue_catalog.{self.config.silver_database_name}.technology_fact_request_events"
        )
        fact_ddl = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.silver_database_name}.technology_fact_request_events (
          request_id STRING NOT NULL,
          event_date TIMESTAMP,
          event_type STRING NOT NULL,
          status STRING,
          priority STRING,
          list_id STRING,
          external_satisfaction_score INT,
          internal_satisfaction_score INT,
          resolution_days INT,
          sla_compliant BOOLEAN,
          time_logged_hours DOUBLE,
          time_estimate_hours DOUBLE,
          source STRING,
          ingestion_ts TIMESTAMP,
          year INT,
          month INT
        )
        USING ICEBERG
        """
        if is_partitioned:
            fact_ddl += "\n        PARTITIONED BY (year, month)"
        self.spark.sql(fact_ddl)
        logger.info("  ✓ Created technology_fact_request_events")

        logger.info("✅ All Silver tables created successfully")


# ==========================================
# MAIN EXECUTION (HYBRID LOCAL/GLUE)
# ==========================================
if __name__ == "__main__":
    try:
        setup_logging()
        logger.info("Loading ClickUp Silver configuration...")
        config = ClickUpSilverConfig.from_args()

        # Reconfigure logging with config value
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

        # Step 3: Create Silver Job instance with hybrid support
        if is_local:
            silver_job = ClickUpSilverJob(spark, config)
        else:
            silver_job = ClickUpSilverJob(context["glue_context"], config)

        # Step 4: Run Silver Job pipeline
        logger.info(f"🔄 Running {config.source_name} Silver job...")
        silver_job.run()

        # Step 5: Show counts if requested
        if config.show_counts:
            logger.info("📊 Showing record counts...")
            spark.sql(
                f"SELECT COUNT(*) as bronze_count FROM glue_catalog.{config.bronze_database_name}.{config.bronze_table_name}"
            ).show()
            spark.sql(
                f"SELECT COUNT(*) as silver_count FROM glue_catalog.{config.silver_database_name}.{config.silver_table_name}"
            ).show()

        # Step 6: Commit job (Glue only)
        if not is_local:
            context["job"].commit()
            logger.info(
                f"✅ {config.source_name} Silver Pipeline completed successfully (Glue)!"
            )
        else:
            spark.stop()
            logger.info(
                f"✅ {config.source_name} Silver Pipeline completed successfully (Local)!"
            )

    except Exception as e:
        logger.error(f"❌ Job failed with error: {e}", exc_info=True)
        raise
