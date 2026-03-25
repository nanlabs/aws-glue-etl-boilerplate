"""
Team Tailor Gold Pipeline Job - Talent KPIs

This job processes Team Tailor data from silver to gold layer with support for both
local development (LocalStack + Derby) and AWS Glue (GlueContext + Glue Data Catalog).

Features:
- ✅ Hybrid support: Works locally with LocalStack and in AWS Glue
- ✅ AWS Glue v5 DataFrame-first approach
- ✅ GoldJobBase with encapsulated infrastructure setup
- ✅ Gold layer processing: silver → gold
- ✅ Pydantic-based configuration with automatic parameter resolution
- ✅ Talent-focused KPIs: 8 specialized analytics tables
- ✅ Pre-calculated KPIs: time-to-fill, source effectiveness, pipeline health, etc.

Local Usage (auto-detected, no --JOB_NAME):
  python jobs/gold/teamtailor_gold_job.py --ENTITY_TYPE talent_time_to_fill

AWS Glue Usage (auto-detected with --JOB_NAME):
  python jobs/gold/teamtailor_gold_job.py --JOB_NAME teamtailor_gold_pipeline --ENTITY_TYPE talent_time_to_fill

Minimal Usage (using defaults):
  python jobs/gold/teamtailor_gold_job.py --ENTITY_TYPE talent_time_to_fill

Full Usage (overriding defaults):
  python jobs/gold/teamtailor_gold_job.py \\
    --JOB_NAME teamtailor_gold_pipeline \\
    --ENTITY_TYPE talent_time_to_fill \\
    --SILVER_DATABASE_NAME nan_develop_silver_analytics \\
    --GOLD_DATABASE_NAME nan_develop_gold_analytics \\
    --WAREHOUSE_PATH s3://bucket/nan/

Required Parameters:
- ENTITY_TYPE: Gold entity type (talent_time_to_fill, talent_source_effectiveness, etc.)

Optional Parameters (with Team Tailor defaults):
- SILVER_DATABASE_NAME: Silver layer database name (default: from environment)
- SILVER_TABLE_NAME: Silver layer table name (auto-configured from entity_type)
- GOLD_DATABASE_NAME: Gold layer database name (default: from environment)
- GOLD_TABLE_NAME: Gold layer table name (auto-configured from entity_type)
- WAREHOUSE_PATH: S3 location for Iceberg warehouse (default: from environment)
- CREATE_TABLES: Whether to create tables (default: true)
- SHOW_COUNTS: Whether to show record counts (default: true)
- LOG_LEVEL: Logging level (default: INFO)

Environment Detection:
- Local mode: No --JOB_NAME argument → SparkSession + Derby metastore + LocalStack S3A
- Glue mode: --JOB_NAME argument present → GlueContext + Glue Data Catalog + AWS S3
"""

import logging
from typing import ClassVar, Optional

from pydantic import Field

from libs.common import GoldJobConfig, setup_logging
from libs.pyspark import GoldJobBase, SessionConfig, SparkSessionFactory

# Configure logging
logger = logging.getLogger(__name__)

# ==========================================
# TEAM TAILOR GOLD ENTITIES CONFIGURATION
# ==========================================

TEAMTAILOR_GOLD_ENTITIES = {
    "talent_time_to_fill": {
        "silver_tables": [
            "teamtailor_job_postings",
            "teamtailor_application_pipeline",
            "teamtailor_interview_events",
        ],
        "partition_cols": ["analysis_date", "job_id"],
        "description": "Average days from job posting to offer acceptance",
        "domain": "talent",
    },
    "talent_source_effectiveness": {
        "silver_tables": [
            "teamtailor_candidate_profiles",
            "teamtailor_application_pipeline",
            "teamtailor_job_postings",
        ],
        "partition_cols": ["analysis_date", "source"],
        "description": "Quality and conversion by source",
        "domain": "talent",
    },
    "talent_pipeline_health": {
        "silver_tables": [
            "teamtailor_application_pipeline",
            "teamtailor_job_postings",
        ],
        "partition_cols": ["analysis_date", "stage_id"],
        "description": "Application flow through stages",
        "domain": "talent",
    },
    "talent_recruiter_performance": {
        "silver_tables": [
            "teamtailor_recruiter_performance",
            "teamtailor_application_pipeline",
            "teamtailor_interview_events",
        ],
        "partition_cols": ["analysis_date", "user_id"],
        "description": "Recruiter efficiency and quality",
        "domain": "talent",
    },
    "talent_interview_metrics": {
        "silver_tables": [
            "teamtailor_interview_events",
            "teamtailor_application_pipeline",
        ],
        "partition_cols": ["analysis_date", "interview_type"],
        "description": "Interview efficiency and outcomes",
        "domain": "talent",
    },
    "talent_candidate_journey": {
        "silver_tables": [
            "teamtailor_candidate_profiles",
            "teamtailor_application_pipeline",
            "teamtailor_interview_events",
            "teamtailor_job_postings",
        ],
        "partition_cols": ["analysis_date", "candidate_id"],
        "description": "Complete candidate lifecycle",
        "domain": "talent",
    },
    "talent_candidate_nps": {
        "silver_tables": [
            "teamtailor_candidate_nps",
            "teamtailor_candidate_profiles",
            "teamtailor_application_pipeline",
            "teamtailor_recruiter_performance",
            "teamtailor_job_postings",
        ],
        "partition_cols": ["analysis_date", "candidate_id"],
        "description": "Candidate NPS metrics and segmentation (UC6)",
        "domain": "talent",
    },
    # DISABLED: talent_stage_transitions
    # Depends on application_stage_transitions which uses the Activities API
    # that was REMOVED by TeamTailor on 2020-10-01
    # Stage transition analysis can be done with changed_stage_at in application_pipeline
    # "talent_stage_transitions": {
    #     "silver_tables": [
    #         "teamtailor_application_stage_transitions",
    #         "teamtailor_application_pipeline",
    #         "teamtailor_job_postings",
    #     ],
    #     "partition_cols": ["analysis_date", "application_id"],
    #     "description": "Stage transition analysis: jumps, regressions, drop-offs (UC7)",
    #     "domain": "talent",
    # },
}

# ==========================================
# TEAM TAILOR GOLD CONFIGURATION
# ==========================================


class TeamTailorGoldConfig(GoldJobConfig):
    """
    Team Tailor-specific Gold configuration.

    Extends GoldJobConfig with Team Tailor-specific fields.
    Auto-configures silver_table_name and gold_table_name based on entity_type.

    Parameter Resolution (highest to lowest priority):
    1. Workflow Run Properties (if running in Glue Workflow)
    2. CLI: --ENTITY_TYPE talent_time_to_fill
    3. ENV: ENTITY_TYPE=talent_time_to_fill
    4. Error (entity_type is required)
    """

    # Source name with default value
    source_name: ClassVar[str] = "teamtailor"

    # Required fields - ConfigBase resolves: Workflow → CLI → ENV → Error
    entity_type: str = Field(
        ...,
        description="Team Tailor Gold entity type (talent_time_to_fill, talent_source_effectiveness, etc.)",
    )

    # Auto-configured fields
    silver_table_name: Optional[str] = Field(
        default=None, description="Silver table name (auto-configured if not provided)"
    )
    gold_table_name: Optional[str] = Field(
        default=None, description="Gold table name (auto-configured if not provided)"
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure table names based on entity_type."""
        # Validate entity_type
        if self.entity_type not in TEAMTAILOR_GOLD_ENTITIES:
            valid_entities = ", ".join(TEAMTAILOR_GOLD_ENTITIES.keys())
            raise ValueError(
                f"Invalid entity_type '{self.entity_type}'. "
                f"Valid options: {valid_entities}"
            )

        # Auto-configure gold_table_name (target) based on entity_type
        if not self.gold_table_name:
            self.gold_table_name = f"{self.entity_type}_gold"

        # Note: silver_table_name is not auto-configured as gold jobs may read from multiple silver tables
        # The job will handle reading from multiple tables based on entity_config


# ==========================================
# TEAM TAILOR GOLD JOB
# ==========================================


class TeamTailorGoldJob(GoldJobBase):
    """
    Team Tailor Gold Pipeline Job.

    Processes Team Tailor data from Silver to Gold layer with specialized analytics tables
    for Talent KPIs: time-to-fill, source effectiveness, pipeline health, recruiter performance,
    interview metrics, and candidate journey.
    """

    def __init__(self, spark_or_glue_context, config: TeamTailorGoldConfig):
        """
        Initialize Team Tailor Gold Job with Pydantic configuration.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (Glue)
            config: Validated TeamTailorGoldConfig instance (all parameters auto-resolved)
        """
        # Validate entity_type before initializing
        if config.entity_type not in TEAMTAILOR_GOLD_ENTITIES:
            valid_entities = ", ".join(TEAMTAILOR_GOLD_ENTITIES.keys())
            raise ValueError(
                f"Invalid entity_type '{config.entity_type}'. "
                f"Valid options: {valid_entities}"
            )

        super().__init__(spark_or_glue_context, config)
        self.entity_type = config.entity_type
        self.entity_config = TEAMTAILOR_GOLD_ENTITIES[self.entity_type]

        logger.info(
            f"Initialized Team Tailor Gold job for entity: {config.entity_type}"
        )

    def extract(self):
        """Extract data from Silver layer tables."""
        logger.info(f"Extracting {self.entity_type} data from Silver layer")

        # Get silver tables for this entity
        silver_tables = self.entity_config["silver_tables"]

        # Read all tables first to understand their schemas
        table_dfs = {}
        for table_name in silver_tables:
            source_table = (
                f"glue_catalog.{self.config.silver_database_name}.{table_name}"
            )
            logger.info(f"Reading silver table: {source_table}")
            table_dfs[table_name] = self.spark.sql(f"SELECT * FROM {source_table}")
            logger.debug(f"  Columns in {table_name}: {table_dfs[table_name].columns}")

        # Determine join strategy based on table relationships
        # TeamTailor relationships:
        # - application_pipeline connects candidates (candidate_id) and jobs (job_id)
        # - interview_events connects to applications (application_id)
        # - candidate_profiles only has candidate_id
        # - job_postings only has job_id

        # Start with the primary table
        primary_table = silver_tables[0]
        primary_df = table_dfs[primary_table]

        # Join remaining tables using proper relationships
        if len(silver_tables) > 1:
            for table_name in silver_tables[1:]:
                logger.info(f"Joining with silver table: {table_name}")
                join_df = table_dfs[table_name]

                # Determine join key and strategy based on table relationships
                join_key, join_strategy = self._determine_join_strategy(
                    primary_df, join_df, primary_table, table_name
                )

                if (
                    join_key
                    and join_key in primary_df.columns
                    and join_key in join_df.columns
                ):
                    # Create table prefix for column aliasing to avoid conflicts
                    table_prefix = self._get_table_prefix(table_name)

                    # Select columns from join_df with aliases for conflicting columns
                    existing_cols = set(primary_df.columns)
                    join_cols = []

                    for col_name in join_df.columns:
                        if col_name == join_key:
                            # Keep join key without alias
                            join_cols.append(col_name)
                        elif col_name not in existing_cols:
                            # New column, no alias needed
                            join_cols.append(col_name)
                        else:
                            # Column conflict - add table prefix alias
                            aliased_name = f"{table_prefix}_{col_name}"
                            join_cols.append(f"{col_name} as {aliased_name}")
                            logger.debug(
                                f"  Aliasing conflicting column '{col_name}' as '{aliased_name}'"
                            )

                    if join_cols:
                        # Build select statement with proper aliasing
                        from pyspark.sql.functions import col

                        select_exprs = []
                        for col_expr in join_cols:
                            if " as " in col_expr.lower():
                                # Parse alias expression (handle case variations)
                                parts = (
                                    col_expr.split(" as ", 1)
                                    if " as " in col_expr
                                    else col_expr.split(" AS ", 1)
                                )
                                if len(parts) == 2:
                                    select_exprs.append(
                                        col(parts[0].strip()).alias(parts[1].strip())
                                    )
                                else:
                                    select_exprs.append(col(col_expr.strip()))
                            else:
                                select_exprs.append(col(col_expr.strip()))

                        join_df_selected = join_df.select(*select_exprs)
                        primary_df = primary_df.join(
                            join_df_selected, on=join_key, how=join_strategy
                        )
                        logger.info(
                            f"  ✓ Joined on '{join_key}' using {join_strategy} join"
                        )
                    else:
                        logger.warning(f"  ⚠ No new columns to add from {table_name}")
                else:
                    logger.warning(
                        f"  ⚠ Join key '{join_key}' not found in both tables. "
                        f"Primary columns: {primary_df.columns}, "
                        f"Join columns: {join_df.columns}. Skipping join."
                    )

        # Log extraction results
        record_count = primary_df.count()
        logger.info(f"Extracted {record_count} records from Silver layer")
        logger.debug(f"Final columns: {primary_df.columns}")

        return primary_df

    def _determine_join_strategy(self, primary_df, join_df, primary_table, join_table):
        """
        Determine the join key and strategy based on table relationships.

        Returns:
            tuple: (join_key, join_type) or (None, None) if no valid join found
        """
        primary_cols = set(primary_df.columns)
        join_cols = set(join_df.columns)

        # Define table relationship mappings
        # Format: (table_pattern, available_keys, preferred_join_key)
        table_relationships = {
            "candidate_profiles": {
                "keys": ["candidate_id"],
                "joins_with": {
                    "application": ("candidate_id", "left"),
                    "job": None,  # No direct relationship
                },
            },
            "job_postings": {
                "keys": ["job_id"],
                "joins_with": {
                    "application": ("job_id", "left"),
                    "candidate": None,  # No direct relationship
                },
            },
            "application_pipeline": {
                "keys": ["application_id", "candidate_id", "job_id"],
                "joins_with": {
                    "candidate": ("candidate_id", "left"),
                    "job": ("job_id", "left"),
                    "interview": ("application_id", "left"),
                },
            },
            "interview_events": {
                "keys": ["interview_id", "application_id"],
                "joins_with": {
                    "application": ("application_id", "left"),
                },
            },
            "recruiter_performance": {
                "keys": ["user_id"],
                "joins_with": {
                    "application": None,  # May need to check if user_id exists in applications
                },
            },
        }

        # Find matching table pattern
        primary_pattern = None
        join_pattern = None

        for pattern in table_relationships.keys():
            if pattern in primary_table.lower():
                primary_pattern = pattern
            if pattern in join_table.lower():
                join_pattern = pattern

        # Try to find join key based on relationships
        if primary_pattern and join_pattern:
            primary_rel = table_relationships.get(primary_pattern, {})
            join_rel = table_relationships.get(join_pattern, {})

            # Check if primary table can join with join table
            if "joins_with" in primary_rel:
                for key_pattern, join_info in primary_rel["joins_with"].items():
                    if key_pattern in join_table.lower() and join_info:
                        join_key, join_type = join_info
                        if join_key in primary_cols and join_key in join_cols:
                            return join_key, join_type

            # Check reverse: if join table can join with primary table
            if "joins_with" in join_rel:
                for key_pattern, join_info in join_rel["joins_with"].items():
                    if key_pattern in primary_table.lower() and join_info:
                        join_key, join_type = join_info
                        if join_key in primary_cols and join_key in join_cols:
                            return join_key, join_type

        # Fallback: try common join keys
        common_keys = [
            "candidate_id",
            "job_id",
            "application_id",
            "interview_id",
            "user_id",
        ]
        for key in common_keys:
            if key in primary_cols and key in join_cols:
                logger.info(f"  Using fallback join key: {key}")
                return key, "left"

        return None, None

    def _get_table_prefix(self, table_name):
        """Get a short prefix for table name to use in column aliases."""
        prefixes = {
            "candidate_profiles": "candidate",
            "job_postings": "job",
            "application_pipeline": "app",
            "interview_events": "interview",
            "recruiter_performance": "recruiter",
        }

        for key, prefix in prefixes.items():
            if key in table_name.lower():
                return prefix

        # Fallback: use first part of table name
        return table_name.split("_")[0] if "_" in table_name else table_name[:8]

    def transform(self, silver_df):
        """Transform Silver data to Gold format with KPI calculations."""
        logger.info(f"Transforming {self.entity_type} data to Gold format")

        # Get transformer based on entity type
        transformer = self._get_transformer()
        gold_df = transformer(silver_df)

        # Log transformation results
        record_count = gold_df.count()
        logger.info(f"Transformed {record_count} records to Gold format")

        return gold_df

    def load(self, gold_df):
        """Load Gold data to target table."""
        logger.info(f"Loading {self.entity_type} data to Gold layer")

        # Get target table name
        target_table = f"glue_catalog.{self.config.gold_database_name}.{self.config.gold_table_name}"

        # Gold tables use UPSERT strategy (replace existing data)
        self._load_upsert(gold_df, target_table)

        # Log load results
        record_count = gold_df.count()
        logger.info(f"Loaded {record_count} records to {target_table}")

    def _get_transformer(self):
        """Get the appropriate transformer function for the entity type."""
        if self.entity_type == "talent_time_to_fill":
            return self._transform_time_to_fill
        elif self.entity_type == "talent_source_effectiveness":
            return self._transform_source_effectiveness
        elif self.entity_type == "talent_pipeline_health":
            return self._transform_pipeline_health
        elif self.entity_type == "talent_recruiter_performance":
            return self._transform_recruiter_performance
        elif self.entity_type == "talent_interview_metrics":
            return self._transform_interview_metrics
        elif self.entity_type == "talent_candidate_journey":
            return self._transform_candidate_journey
        elif self.entity_type == "talent_candidate_nps":
            return self._transform_candidate_nps
        elif self.entity_type == "talent_stage_transitions":
            return self._transform_stage_transitions
        else:
            raise ValueError(
                f"No transformer available for entity type: {self.entity_type}"
            )

    def _transform_time_to_fill(self, silver_df):
        """Transform to time-to-fill KPI table."""
        from pyspark.sql.functions import (
            coalesce,
            col,
            current_timestamp,
            datediff,
            lit,
        )
        from pyspark.sql.functions import max as spark_max
        from pyspark.sql.functions import min as spark_min
        from pyspark.sql.functions import when

        # Map column names from joined tables
        # job_postings.created_at -> job_posted_date
        # application_pipeline.created_at -> first_application_date
        # interview_events.scheduled_at -> first_interview_date
        # application_pipeline.status -> final_status
        # Map column names from joined tables (handle aliases and optional columns)
        # After JOINs, columns may have table prefixes (e.g., job_created_at, app_created_at)
        available_cols = set(silver_df.columns)

        # Build coalesce expressions with available columns
        # Collect all possible column names for each field
        job_created_cols = []
        if "job_created_at" in available_cols:
            job_created_cols.append(col("job_created_at"))
        if "created_at" in available_cols:
            job_created_cols.append(col("created_at"))
        job_created_col = coalesce(*job_created_cols) if job_created_cols else lit(None)

        app_created_cols = []
        if "app_created_at" in available_cols:
            app_created_cols.append(col("app_created_at"))
        if "created_at" in available_cols:
            app_created_cols.append(col("created_at"))
        app_created_col = coalesce(*app_created_cols) if app_created_cols else lit(None)

        interview_scheduled_cols = []
        if "interview_scheduled_at" in available_cols:
            interview_scheduled_cols.append(col("interview_scheduled_at"))
        if "scheduled_at" in available_cols:
            interview_scheduled_cols.append(col("scheduled_at"))
        interview_scheduled_col = (
            coalesce(*interview_scheduled_cols)
            if interview_scheduled_cols
            else lit(None)
        )

        final_status_cols = []
        if "app_status" in available_cols:
            final_status_cols.append(col("app_status"))
        if "application_status" in available_cols:
            final_status_cols.append(col("application_status"))
        if "status" in available_cols:
            final_status_cols.append(col("status"))
        final_status_col = (
            coalesce(*final_status_cols) if final_status_cols else lit(None)
        )

        # Calculate time-to-fill metrics
        # Group by job_id and aggregate from joined tables
        # Include custom fields from job_postings if available
        available_cols = set(silver_df.columns)

        # Build aggregation expressions list
        agg_exprs = [
            spark_min(job_created_col).alias("job_posted_date"),
            spark_min(app_created_col).alias("first_application_date"),
            spark_min(interview_scheduled_col).alias("first_interview_date"),
            spark_max(final_status_col).alias("final_status"),
            spark_max(col("title")).alias("title"),
            spark_max(col("status")).alias("status"),  # Job status
            spark_max(col("department_id")).alias("department_id"),
        ]

        # Add custom fields if available
        for cf_col in [
            "custom_field_account",
            "custom_field_employment_type",
            "custom_field_budget",
        ]:
            if cf_col in available_cols:
                agg_exprs.append(spark_max(col(cf_col)).alias(cf_col))

        gold_df = (
            silver_df.groupBy("job_id")
            .agg(*agg_exprs)
            .withColumn(
                "days_to_fill",
                when(
                    col("final_status") == "hired",
                    datediff(col("first_application_date"), col("job_posted_date")),
                ).otherwise(None),
            )
            .withColumn(
                "days_to_first_application",
                when(
                    col("first_application_date").isNotNull(),
                    datediff(col("first_application_date"), col("job_posted_date")),
                ).otherwise(None),
            )
            .withColumn(
                "days_to_interview",
                when(
                    col("first_interview_date").isNotNull()
                    & col("first_application_date").isNotNull(),
                    datediff(
                        col("first_interview_date"), col("first_application_date")
                    ),
                ).otherwise(None),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _transform_source_effectiveness(self, silver_df):
        """Transform to source effectiveness KPI table."""
        from pyspark.sql.functions import coalesce, col, count, current_timestamp, lit
        from pyspark.sql.functions import max as spark_max
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        available_cols = set(silver_df.columns)

        # Build coalesce expression for source (ensure it's never NULL)
        source_cols = []
        if "source" in available_cols:
            source_cols.append(col("source"))
        source_col = (
            coalesce(*source_cols, lit("unknown")) if source_cols else lit("unknown")
        )

        # Build coalesce expression for status
        status_cols = []
        if "app_status" in available_cols:
            status_cols.append(col("app_status"))
        if "application_status" in available_cols:
            status_cols.append(col("application_status"))
        if "status" in available_cols:
            status_cols.append(col("status"))
        status_col = coalesce(*status_cols) if status_cols else lit(None)

        # Calculate source effectiveness metrics
        # First ensure source is never NULL, then group by
        silver_df_with_source = silver_df.withColumn("source", source_col)

        # Include custom fields from candidate_profiles if available
        available_cols = set(silver_df_with_source.columns)
        group_by_cols = ["source", "job_id", "title", "department_id"]

        # Add custom field role if available for segmentation
        agg_exprs = [
            count("*").alias("applications_count"),
            spark_sum(when(status_col == "interview", 1).otherwise(0)).alias(
                "interviews_count"
            ),
            spark_sum(when(status_col == "offer", 1).otherwise(0)).alias(
                "offers_count"
            ),
            spark_sum(when(status_col == "hired", 1).otherwise(0)).alias("hires_count"),
        ]

        if "custom_field_role" in available_cols:
            group_by_cols.append("custom_field_role")
        elif "candidate_custom_field_role" in available_cols:
            agg_exprs.append(
                spark_max(col("candidate_custom_field_role")).alias("custom_field_role")
            )

        gold_df = (
            silver_df_with_source.groupBy(*group_by_cols)
            .agg(*agg_exprs)
            .withColumn(
                "conversion_rate",
                when(
                    col("applications_count") > 0,
                    col("hires_count") / col("applications_count"),
                ).otherwise(0.0),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _transform_pipeline_health(self, silver_df):
        """Transform to pipeline health KPI table."""
        from pyspark.sql.functions import col, count, current_timestamp, datediff, lit
        from pyspark.sql.functions import max as spark_max
        from pyspark.sql.functions import min as spark_min

        # Calculate pipeline health metrics
        # Include custom fields from job_postings for segmentation
        available_cols = set(silver_df.columns)
        group_by_cols = ["stage_id", "job_id", "title", "department_id"]

        # Build aggregation expressions
        agg_exprs = [
            count("*").alias("applications_count"),
            spark_min("created_at").alias("stage_entry_date"),
            spark_max("updated_at").alias("stage_exit_date"),
        ]

        # Add custom fields for segmentation if available
        if "custom_field_account" in available_cols:
            group_by_cols.append("custom_field_account")
        elif "job_custom_field_account" in available_cols:
            agg_exprs.append(
                spark_max(col("job_custom_field_account")).alias("custom_field_account")
            )

        if "custom_field_employment_type" in available_cols:
            group_by_cols.append("custom_field_employment_type")
        elif "job_custom_field_employment_type" in available_cols:
            agg_exprs.append(
                spark_max(col("job_custom_field_employment_type")).alias(
                    "custom_field_employment_type"
                )
            )

        gold_df = (
            silver_df.groupBy(*group_by_cols)
            .agg(*agg_exprs)
            .withColumn(
                "stage_duration",
                datediff(col("stage_exit_date"), col("stage_entry_date")),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _transform_recruiter_performance(self, silver_df):
        """Transform to recruiter performance KPI table."""
        from pyspark.sql.functions import coalesce, col, count, current_timestamp, lit
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        available_cols = set(silver_df.columns)

        # Build coalesce expression for interview scheduled date
        interview_scheduled_cols = []
        if "interview_scheduled_at" in available_cols:
            interview_scheduled_cols.append(col("interview_scheduled_at"))
        if "scheduled_at" in available_cols:
            interview_scheduled_cols.append(col("scheduled_at"))
        interview_scheduled_col = (
            coalesce(*interview_scheduled_cols)
            if interview_scheduled_cols
            else lit(None)
        )

        # Build coalesce expression for status
        status_cols = []
        if "app_status" in available_cols:
            status_cols.append(col("app_status"))
        if "application_status" in available_cols:
            status_cols.append(col("application_status"))
        if "status" in available_cols:
            status_cols.append(col("status"))
        status_col = coalesce(*status_cols) if status_cols else lit(None)

        # Calculate recruiter performance metrics
        gold_df = (
            silver_df.groupBy(
                "user_id", "email", "first_name", "last_name", "department_id"
            )
            .agg(
                count("*").alias("applications_handled"),
                spark_sum(
                    when(interview_scheduled_col.isNotNull(), 1).otherwise(0)
                ).alias("interviews_scheduled"),
                spark_sum(when(status_col == "offer", 1).otherwise(0)).alias(
                    "offers_extended"
                ),
                spark_sum(when(status_col == "hired", 1).otherwise(0)).alias(
                    "hires_made"
                ),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _transform_interview_metrics(self, silver_df):
        """Transform to interview metrics KPI table."""
        from pyspark.sql.functions import coalesce, col, count, current_timestamp, lit
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        # Handle null interview_type values by filling with "unknown"
        # This prevents NullPointerException when grouping
        silver_df_with_type = silver_df.withColumn(
            "interview_type",
            coalesce(col("interview_type"), lit("unknown")),
        )

        # Calculate interview metrics
        gold_df = (
            silver_df_with_type.groupBy("interview_type", "application_id", "job_id")
            .agg(
                count("*").alias("interviews_count"),
                spark_sum(when(col("status") == "passed", 1).otherwise(0)).alias(
                    "passed_interviews"
                ),
                spark_sum(when(col("status") == "offer", 1).otherwise(0)).alias(
                    "offers_extended"
                ),
            )
            .withColumn(
                "interview_to_offer_ratio",
                when(
                    col("interviews_count") > 0,
                    col("offers_extended") / col("interviews_count"),
                ).otherwise(0.0),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _transform_candidate_journey(self, silver_df):
        """
        Transform to candidate journey KPI table (UC7).

        Includes metrics for UC1b (Time to Hire):
        - Days from application to first interview
        - Days to offer
        - Days to offer acceptance
        """
        from pyspark.sql.functions import (
            coalesce,
            col,
            current_timestamp,
            datediff,
            lit,
        )
        from pyspark.sql.functions import max as spark_max
        from pyspark.sql.functions import min as spark_min
        from pyspark.sql.functions import when

        # Map column names from joined tables (handle aliases and optional columns)
        available_cols = set(silver_df.columns)

        # Build coalesce expressions with available columns
        # Collect all possible column names for each field
        app_created_cols = []
        if "app_created_at" in available_cols:
            app_created_cols.append(col("app_created_at"))
        if "created_at" in available_cols:
            app_created_cols.append(col("created_at"))
        app_created_col = coalesce(*app_created_cols) if app_created_cols else lit(None)

        app_updated_cols = []
        if "app_updated_at" in available_cols:
            app_updated_cols.append(col("app_updated_at"))
        if "updated_at" in available_cols:
            app_updated_cols.append(col("updated_at"))
        app_updated_col = coalesce(*app_updated_cols) if app_updated_cols else lit(None)

        interview_scheduled_cols = []
        if "interview_scheduled_at" in available_cols:
            interview_scheduled_cols.append(col("interview_scheduled_at"))
        if "scheduled_at" in available_cols:
            interview_scheduled_cols.append(col("scheduled_at"))
        interview_scheduled_col = (
            coalesce(*interview_scheduled_cols)
            if interview_scheduled_cols
            else lit(None)
        )

        final_status_cols = []
        if "app_status" in available_cols:
            final_status_cols.append(col("app_status"))
        if "application_status" in available_cols:
            final_status_cols.append(col("application_status"))
        if "status" in available_cols:
            final_status_cols.append(col("status"))
        final_status_col = (
            coalesce(*final_status_cols) if final_status_cols else lit(None)
        )

        # Department ID from job_postings
        department_id_cols = []
        if "job_department_id" in available_cols:
            department_id_cols.append(col("job_department_id"))
        if "department_id" in available_cols:
            department_id_cols.append(col("department_id"))
        department_id_col = (
            coalesce(*department_id_cols) if department_id_cols else lit(None)
        )

        # Calculate candidate journey metrics
        # For offer dates, we need to detect when status = 'offer' and when it changes to 'hired'
        # We'll use the updated_at timestamp when status = 'offer' as offer_date
        # And when final_status = 'hired' and there was an offer, use that timestamp as offer_accepted_date

        # First, identify status column to use for filtering
        status_col_for_filter = None
        if "app_status" in available_cols:
            status_col_for_filter = col("app_status")
        elif "application_status" in available_cols:
            status_col_for_filter = col("application_status")
        elif "status" in available_cols:
            status_col_for_filter = col("status")

        # Aggregate to get key dates and statuses
        agg_exprs = [
            spark_min(app_created_col).alias("application_date"),
            spark_min(interview_scheduled_col).alias("first_interview_date"),
            spark_max(final_status_col).alias("final_status"),
            spark_max(department_id_col).alias("department_id"),
        ]

        # Add offer_date aggregation if status column exists
        if status_col_for_filter is not None:
            # Get offer_date: min updated_at when status = 'offer'
            agg_exprs.append(
                spark_min(
                    when(status_col_for_filter == "offer", app_updated_col)
                ).alias("offer_date")
            )
            # Get offer_accepted_date: min updated_at when status = 'hired'
            # (this represents when offer was accepted, assuming offer came before hired)
            agg_exprs.append(
                spark_min(
                    when(status_col_for_filter == "hired", app_updated_col)
                ).alias("offer_accepted_date")
            )
        else:
            # Fallback: use None if status column not available
            agg_exprs.append(lit(None).cast("timestamp").alias("offer_date"))
            agg_exprs.append(lit(None).cast("timestamp").alias("offer_accepted_date"))

        gold_df = (
            silver_df.groupBy("candidate_id", "job_id", "source")
            .agg(*agg_exprs)
            .withColumn(
                "application_to_interview_days",
                when(
                    col("first_interview_date").isNotNull()
                    & col("application_date").isNotNull(),
                    datediff(col("first_interview_date"), col("application_date")),
                ).otherwise(None),
            )
            .withColumn(
                "interview_to_offer_days",
                when(
                    col("offer_date").isNotNull()
                    & col("first_interview_date").isNotNull(),
                    datediff(col("offer_date"), col("first_interview_date")),
                ).otherwise(None),
            )
            .withColumn(
                "days_to_offer",
                when(
                    col("offer_date").isNotNull() & col("application_date").isNotNull(),
                    datediff(col("offer_date"), col("application_date")),
                ).otherwise(None),
            )
            .withColumn(
                "days_to_offer_acceptance",
                when(
                    col("offer_accepted_date").isNotNull()
                    & col("application_date").isNotNull(),
                    datediff(col("offer_accepted_date"), col("application_date")),
                ).otherwise(None),
            )
            .withColumn(
                "total_days",
                when(
                    col("application_date").isNotNull(),
                    datediff(current_timestamp(), col("application_date")),
                ).otherwise(None),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _transform_candidate_nps(self, silver_df):
        """Transform to candidate NPS KPI table (UC6)."""
        from pyspark.sql.functions import avg as spark_avg
        from pyspark.sql.functions import coalesce, col
        from pyspark.sql.functions import count as spark_count
        from pyspark.sql.functions import current_timestamp, lit
        from pyspark.sql.functions import max as spark_max
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        # Map column names from joined tables (handle aliases and optional columns)
        available_cols = set(silver_df.columns)

        # NPS responses table columns
        nps_score_col = col("score") if "score" in available_cols else lit(None)
        nps_category_col = (
            col("nps_category") if "nps_category" in available_cols else lit(None)
        )

        # Build candidate_id column with coalesce
        candidate_id_col = coalesce(
            col("candidate_id") if "candidate_id" in available_cols else lit(None),
            (
                col("nps_candidate_id")
                if "nps_candidate_id" in available_cols
                else lit(None)
            ),
        )

        # Filter out rows where candidate_id is NULL (required by Gold schema NOT NULL constraint)
        # Add candidate_id column first, then filter
        silver_df_with_candidate_id = silver_df.withColumn(
            "candidate_id_for_grouping", candidate_id_col
        ).filter(col("candidate_id_for_grouping").isNotNull())

        # Build aggregation expressions for enrichment fields
        # These need to be aggregated since we're grouping by candidate_id only
        agg_exprs = [
            # NPS metrics
            spark_count("*").alias("nps_responses_count"),
            spark_avg(nps_score_col).alias("avg_nps_score"),
            spark_sum(when(nps_category_col == "Promoter", 1).otherwise(0)).alias(
                "promoters_count"
            ),
            spark_sum(when(nps_category_col == "Passive", 1).otherwise(0)).alias(
                "passives_count"
            ),
            spark_sum(when(nps_category_col == "Detractor", 1).otherwise(0)).alias(
                "detractors_count"
            ),
        ]

        # Add enrichment fields from joins (aggregate them since candidate can have multiple jobs/applications)
        # Always ensure these columns exist, even if NULL (required by Gold schema)
        if "job_id" in available_cols:
            agg_exprs.append(spark_max(col("job_id")).alias("job_id"))
        elif "app_job_id" in available_cols:
            agg_exprs.append(spark_max(col("app_job_id")).alias("job_id"))
        else:
            # job_id is optional in schema, but we'll add it as NULL if not available
            agg_exprs.append(lit(None).cast("string").alias("job_id"))

        if "user_id" in available_cols:
            agg_exprs.append(spark_max(col("user_id")).alias("recruiter_id"))
        elif "recruiter_user_id" in available_cols:
            agg_exprs.append(spark_max(col("recruiter_user_id")).alias("recruiter_id"))
        else:
            # recruiter_id is required in schema, add as NULL if join failed
            agg_exprs.append(lit(None).cast("string").alias("recruiter_id"))

        if "source" in available_cols:
            agg_exprs.append(spark_max(col("source")).alias("source"))
        elif "candidate_source" in available_cols:
            agg_exprs.append(spark_max(col("candidate_source")).alias("source"))
        else:
            # source is optional in schema, but we'll add it as NULL if not available
            agg_exprs.append(lit(None).cast("string").alias("source"))

        # Calculate NPS metrics grouped by candidate and enriched with context
        # UC6 requirements: NPS calculation, segmentation by recruiter, job, seniority, pipeline experience
        gold_df = (
            silver_df_with_candidate_id.groupBy(
                col("candidate_id_for_grouping").alias("candidate_id")
            )
            .agg(*agg_exprs)
            .withColumn(
                "nps_score",
                when(
                    col("nps_responses_count") > 0,
                    (
                        (col("promoters_count") - col("detractors_count"))
                        * 100.0
                        / col("nps_responses_count")
                    ),
                ).otherwise(None),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _transform_stage_transitions(self, silver_df):
        """
        Transform to stage transitions KPI table (UC7).

        Analyzes stage transitions to detect:
        - Stage jumps (forward skips)
        - Regressions (backward moves)
        - Drop-offs (no progress after threshold)
        """
        from pyspark.sql.functions import coalesce, col
        from pyspark.sql.functions import count as spark_count
        from pyspark.sql.functions import current_timestamp, datediff, lit
        from pyspark.sql.functions import max as spark_max
        from pyspark.sql.functions import min as spark_min
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        # Map column names from joined tables
        available_cols = set(silver_df.columns)

        # Stage transition columns
        stage_id_col = col("stage_id") if "stage_id" in available_cols else lit(None)
        previous_stage_id_col = (
            col("previous_stage_id")
            if "previous_stage_id" in available_cols
            else lit(None)
        )
        # transition_type_col is defined but not currently used
        # transition_type_col = (
        #     col("transition_type") if "transition_type" in available_cols else lit(None)
        # )
        created_at_cols = []
        if "created_at" in available_cols:
            created_at_cols.append(col("created_at"))
        if "transition_created_at" in available_cols:
            created_at_cols.append(col("transition_created_at"))
        created_at_col = coalesce(*created_at_cols) if created_at_cols else lit(None)

        # Application and job context
        application_id_col = (
            col("application_id") if "application_id" in available_cols else lit(None)
        )
        job_id_cols = []
        if "job_id" in available_cols:
            job_id_cols.append(col("job_id"))
        if "app_job_id" in available_cols:
            job_id_cols.append(col("app_job_id"))
        job_id_col = coalesce(*job_id_cols) if job_id_cols else lit(None)

        department_id_cols = []
        if "job_department_id" in available_cols:
            department_id_cols.append(col("job_department_id"))
        if "department_id" in available_cols:
            department_id_cols.append(col("department_id"))
        department_id_col = (
            coalesce(*department_id_cols) if department_id_cols else lit(None)
        )

        # Calculate stage transition metrics grouped by application
        # Detect jumps (forward skips), regressions (backward moves), and drop-offs
        gold_df = (
            silver_df.groupBy(application_id_col.alias("application_id"))
            .agg(
                spark_count("*").alias("total_transitions"),
                spark_min(created_at_col).alias("first_transition_date"),
                spark_max(created_at_col).alias("last_transition_date"),
                spark_max(stage_id_col).alias("current_stage_id"),
                spark_max(previous_stage_id_col).alias("previous_stage_id"),
                # Count transitions where previous_stage exists (indicates a change)
                # Note: To detect true regressions vs forward moves, we would need stage ordering
                # For now, we track all transitions where previous_stage != current_stage
                spark_sum(
                    when(
                        (previous_stage_id_col.isNotNull())
                        & (previous_stage_id_col != stage_id_col),
                        1,
                    ).otherwise(0)
                ).alias("transition_count"),
                # Count potential regressions: when we see a stage_id that appeared before
                # This is a simplified heuristic - full detection requires stage ordering
                spark_sum(
                    when(
                        (previous_stage_id_col.isNotNull())
                        & (previous_stage_id_col != stage_id_col),
                        1,
                    ).otherwise(0)
                ).alias("regression_count"),
                # Get job and department context
                spark_max(job_id_col).alias("job_id"),
                spark_max(department_id_col).alias("department_id"),
            )
            .withColumn(
                "has_regression",
                when(col("regression_count") > 0, True).otherwise(False),
            )
            .withColumn(
                "days_since_last_transition",
                when(
                    col("last_transition_date").isNotNull(),
                    datediff(current_timestamp(), col("last_transition_date")),
                ).otherwise(None),
            )
            .withColumn(
                "is_dropped_off",
                # Consider dropped off if no transition in last 30 days and not in final stage
                when(
                    (col("days_since_last_transition") > 30)
                    & (col("current_stage_id").isNotNull()),
                    True,
                ).otherwise(False),
            )
            .withColumn("analysis_date", current_timestamp().cast("date"))
            .withColumn("source_system", lit("teamtailor"))
        )

        return gold_df

    def _load_upsert(self, gold_df, target_table: str):
        """Load data using UPSERT strategy (replace partition)."""
        # For Gold tables, we typically replace the entire partition
        # This is a simplified version - in production, you might want partition-based replacement
        self.write_to_iceberg(
            gold_df,
            self.config.gold_database_name,
            self.config.gold_table_name,
        )

    def create_tables(self):
        """Create Gold Iceberg table."""
        entity_config = self.entity_config
        is_partitioned = len(entity_config["partition_cols"]) > 0

        logger.info(f"Creating Gold table for entity: {self.config.entity_type}")
        logger.info(f"  Partitioning: {is_partitioned}")

        # Drop table if exists
        self.spark.sql(
            f"DROP TABLE IF EXISTS glue_catalog.{self.config.gold_database_name}.{self.config.gold_table_name}"
        )

        # Get entity-specific schema
        gold_schema_ddl = self._get_gold_schema_ddl()

        # Build Iceberg DDL
        gold_table_ddl = f"""
        CREATE TABLE glue_catalog.{self.config.gold_database_name}.{self.config.gold_table_name} (
          {gold_schema_ddl}
        )
        USING ICEBERG
        """

        # Add partitioning if needed
        if is_partitioned:
            partition_cols = ", ".join(entity_config["partition_cols"])
            gold_table_ddl += f"\n        PARTITIONED BY ({partition_cols})"

        self.spark.sql(gold_table_ddl)
        logger.debug(f"gold_table_ddl: {gold_table_ddl}")
        logger.info(
            f"  ✓ Gold table created: glue_catalog.{self.config.gold_database_name}.{self.config.gold_table_name}"
        )

    def _get_gold_schema_ddl(self) -> str:
        """Get DDL schema for Gold table (columns only)."""
        entity_type = self.config.entity_type

        if entity_type == "talent_time_to_fill":
            return """
job_id STRING NOT NULL,
title STRING,
status STRING,
department_id STRING,
custom_field_account STRING,
custom_field_employment_type STRING,
custom_field_budget STRING,
job_posted_date TIMESTAMP,
first_application_date TIMESTAMP,
first_interview_date TIMESTAMP,
final_status STRING,
days_to_fill INT,
days_to_first_application INT,
days_to_interview INT,
analysis_date DATE,
source_system STRING"""
        elif entity_type == "talent_source_effectiveness":
            return """
source STRING NOT NULL,
job_id STRING,
title STRING,
department_id STRING,
custom_field_role STRING,
applications_count BIGINT,
interviews_count BIGINT,
offers_count BIGINT,
hires_count BIGINT,
conversion_rate DOUBLE,
analysis_date DATE,
source_system STRING"""
        elif entity_type == "talent_pipeline_health":
            return """
stage_id STRING,
job_id STRING,
title STRING,
department_id STRING,
custom_field_account STRING,
custom_field_employment_type STRING,
applications_count BIGINT,
stage_entry_date TIMESTAMP,
stage_exit_date TIMESTAMP,
stage_duration INT,
analysis_date DATE,
source_system STRING"""
        elif entity_type == "talent_recruiter_performance":
            return """
user_id STRING NOT NULL,
email STRING,
first_name STRING,
last_name STRING,
department_id STRING,
applications_handled BIGINT,
interviews_scheduled BIGINT,
offers_extended BIGINT,
hires_made BIGINT,
analysis_date DATE,
source_system STRING"""
        elif entity_type == "talent_interview_metrics":
            return """
interview_type STRING,
application_id STRING,
job_id STRING,
interviews_count BIGINT,
passed_interviews BIGINT,
offers_extended BIGINT,
interview_to_offer_ratio DOUBLE,
analysis_date DATE,
source_system STRING"""
        elif entity_type == "talent_candidate_journey":
            return """
candidate_id STRING NOT NULL,
job_id STRING,
source STRING,
department_id STRING,
application_date TIMESTAMP,
first_interview_date TIMESTAMP,
offer_date TIMESTAMP,
offer_accepted_date TIMESTAMP,
final_status STRING,
application_to_interview_days INT,
interview_to_offer_days INT,
days_to_offer INT,
days_to_offer_acceptance INT,
total_days INT,
analysis_date DATE,
source_system STRING"""
        elif entity_type == "talent_candidate_nps":
            return """
candidate_id STRING NOT NULL,
job_id STRING,
recruiter_id STRING,
source STRING,
nps_responses_count BIGINT,
avg_nps_score DOUBLE,
promoters_count BIGINT,
passives_count BIGINT,
detractors_count BIGINT,
nps_score DOUBLE,
analysis_date DATE,
source_system STRING"""
        elif entity_type == "talent_stage_transitions":
            return """
application_id STRING NOT NULL,
job_id STRING,
department_id STRING,
current_stage_id STRING,
previous_stage_id STRING,
total_transitions BIGINT,
transition_count BIGINT,
regression_count BIGINT,
has_regression BOOLEAN,
first_transition_date TIMESTAMP,
last_transition_date TIMESTAMP,
days_since_last_transition INT,
is_dropped_off BOOLEAN,
analysis_date DATE,
source_system STRING"""
        else:
            raise ValueError(f"Unknown entity_type: {entity_type}")


# ==========================================
# MAIN EXECUTION (HYBRID LOCAL/GLUE)
# ==========================================
if __name__ == "__main__":
    try:
        setup_logging()
        logger.info("Loading Team Tailor Gold configuration...")
        config = TeamTailorGoldConfig.from_args()

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

        # Step 3: Create Gold Job instance with hybrid support
        if is_local:
            gold_job = TeamTailorGoldJob(spark, config)
        else:
            gold_job = TeamTailorGoldJob(context["glue_context"], config)

        # Step 4: Run Gold Job pipeline
        logger.info(f"🔄 Running {config.source_name} Gold job...")
        gold_job.run()

        # Step 5: Show counts if requested
        if config.show_counts:
            logger.info("📊 Showing record counts...")

            # Show Gold table count (always available)
            try:
                gold_count_df = spark.sql(
                    f"SELECT COUNT(*) as gold_count FROM glue_catalog.{config.gold_database_name}.{config.gold_table_name}"
                )
                gold_count_df.show()
            except Exception as e:
                logger.warning(f"Could not show gold count: {e}")

            # Show Silver table counts (may have multiple tables)
            # Note: silver_table_name is None for Gold jobs as they read from multiple Silver tables
            try:
                entity_config = TEAMTAILOR_GOLD_ENTITIES.get(config.entity_type, {})
                silver_tables = entity_config.get("silver_tables", [])

                if silver_tables:
                    logger.info(f"Silver tables used: {', '.join(silver_tables)}")
                    for table_name in silver_tables:
                        try:
                            silver_count_df = spark.sql(
                                f"SELECT COUNT(*) as {table_name}_count FROM glue_catalog.{config.silver_database_name}.{table_name}"
                            )
                            silver_count_df.show()
                        except Exception as e:
                            logger.warning(
                                f"Could not show count for {table_name}: {e}"
                            )
                else:
                    logger.info("No silver tables configured for this entity type")
            except Exception as e:
                logger.warning(f"Could not show silver counts: {e}")

        # Step 6: Commit job (Glue only)
        if not is_local:
            context["job"].commit()
            logger.info(
                f"✅ {config.source_name} Gold Pipeline completed successfully (Glue)!"
            )
        else:
            spark.stop()
            logger.info(
                f"✅ {config.source_name} Gold Pipeline completed successfully (Local)!"
            )

    except Exception as e:
        logger.error(f"❌ Job failed with error: {e}", exc_info=True)
        raise
