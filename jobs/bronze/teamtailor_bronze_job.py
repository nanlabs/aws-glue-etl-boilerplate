"""
Team Tailor Bronze Pipeline Job - Hybrid Local/Glue Support

This job processes Team Tailor data from raw to bronze layer with support for both
local development (LocalStack + Derby) and AWS Glue (GlueContext + Glue Data Catalog).

Features:
- ✅ Hybrid support: Works locally with LocalStack and in AWS Glue
- ✅ AWS Glue v5 DataFrame-first approach
- ✅ BronzeJobBase with encapsulated infrastructure setup
- ✅ Bronze layer processing: raw → bronze
- ✅ Pydantic-based configuration with automatic parameter resolution
- ✅ TeamTailorBronzeConfig with Team Tailor-specific defaults
- ✅ Four-tier parameter resolution: workflow properties, job parameters, environment variables, defaults
- ✅ External table support in both environments (Derby/Glue Data Catalog)
- ✅ Multi-entity support: 11 entity types with intelligent partitioning and merge strategies

Local Usage (auto-detected, no --JOB_NAME):
  python jobs/bronze/teamtailor_bronze_job.py

AWS Glue Usage (auto-detected with --JOB_NAME):
  python jobs/bronze/teamtailor_bronze_job.py --JOB_NAME teamtailor_bronze_pipeline

Minimal Usage (using defaults):
  python jobs/bronze/teamtailor_bronze_job.py --JOB_NAME teamtailor_bronze_pipeline

Full Usage (overriding defaults):
  python jobs/bronze/teamtailor_bronze_job.py \\
    --JOB_NAME teamtailor_bronze_pipeline \\
    --ENTITY_TYPE candidates \\
    --RAW_DATABASE_NAME nan_develop_raw_zone_ingestion \\
    --RAW_TABLE_NAME teamtailor__talent__candidates_raw \\
    --RAW_ZONE_PATH s3://bucket/nan/raw/ \\
    --BRONZE_DATABASE_NAME nan_develop_bronze_zone_ingestion \\
    --BRONZE_TABLE_NAME teamtailor__talent__candidates_bronze \\
    --WAREHOUSE_PATH s3://bucket/nan/

Required Parameters (Glue mode only):
- JOB_NAME: AWS Glue job name (triggers Glue mode)

Optional Parameters (with Team Tailor defaults):
- SOURCE_NAME: Source system name (default: 'teamtailor')
- ENTITY_TYPE: Entity type (candidates, jobs, applications, interviews, users, departments, stages, application_stage_transitions, nps_responses)
- RAW_DATABASE_NAME: Raw layer database name (default: 'nan_develop_raw_zone_ingestion')
- RAW_TABLE_NAME: Raw layer table name (auto-configured from entity_type)
- BRONZE_DATABASE_NAME: Bronze layer database name (default: 'nan_develop_bronze_zone_ingestion')
- BRONZE_TABLE_NAME: Bronze layer table name (auto-configured from entity_type)
- WAREHOUSE_PATH: S3 location for Iceberg warehouse (default: develop bucket)
- CREATE_TABLES: Whether to create tables (default: true)
- SHOW_COUNTS: Whether to show record counts (default: true)
- ENVIRONMENT: Environment (develop/staging/prod, default: develop)
- STAGE: Stage (local/dev/staging/prod, default: local)
- LOG_LEVEL: Logging level (default: INFO)

Environment Detection:
- Local mode: No --JOB_NAME argument → SparkSession + Derby metastore + LocalStack S3A
- Glue mode: --JOB_NAME argument present → GlueContext + Glue Data Catalog + AWS S3
"""

import logging
from typing import Optional

from pydantic import Field

from libs.common import BronzeJobConfig, setup_logging
from libs.pyspark import BronzeJobBase, SessionConfig, SparkSessionFactory

# Configure logging
logger = logging.getLogger(__name__)

# ==========================================
# TEAM TAILOR BRONZE ENTITIES CONFIGURATION
# ==========================================
TEAMTAILOR_BRONZE_ENTITIES = {
    "candidates": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "merge_by_id",
        "raw_table_name": "teamtailor__talent__candidates_raw",
        "bronze_table_name": "teamtailor__talent__candidates_bronze",
        "merge_key": "candidate_id",
        "key_fields": ["candidate_id", "email", "first_name", "last_name"],
        "description": "Candidate profiles and information",
    },
    "jobs": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "merge_by_id",
        "raw_table_name": "teamtailor__talent__jobs_raw",
        "bronze_table_name": "teamtailor__talent__jobs_bronze",
        "merge_key": "job_id",
        "key_fields": ["job_id", "title", "status", "created_at"],
        "description": "Job postings and positions",
    },
    "applications": {
        "partitioned": True,
        "partition_cols": ["year", "month"],
        "merge_strategy": "append",
        "raw_table_name": "teamtailor__talent__applications_raw",
        "bronze_table_name": "teamtailor__talent__applications_bronze",
        "merge_key": None,
        "key_fields": [
            "application_id",
            "candidate_id",
            "job_id",
            "status",
            "created_at",
        ],
        "description": "Candidate applications to jobs",
    },
    "interviews": {
        "partitioned": True,
        "partition_cols": ["year", "month"],
        "merge_strategy": "append",
        "raw_table_name": "teamtailor__talent__interviews_raw",
        "bronze_table_name": "teamtailor__talent__interviews_bronze",
        "merge_key": None,
        "key_fields": [
            "interview_id",
            "application_id",
            "interview_type",
            "scheduled_at",
        ],
        "description": "Interview events and outcomes",
    },
    "users": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "merge_by_id",
        "raw_table_name": "teamtailor__talent__users_raw",
        "bronze_table_name": "teamtailor__talent__users_bronze",
        "merge_key": "user_id",
        "key_fields": ["user_id", "email", "first_name", "last_name"],
        "description": "Recruiters and hiring managers",
    },
    "departments": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "merge_by_id",
        "raw_table_name": "teamtailor__talent__departments_raw",
        "bronze_table_name": "teamtailor__talent__departments_bronze",
        "merge_key": "department_id",
        "key_fields": ["department_id", "name"],
        "description": "Department structure",
    },
    "stages": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "merge_by_id",
        "raw_table_name": "teamtailor__talent__stages_raw",
        "bronze_table_name": "teamtailor__talent__stages_bronze",
        "merge_key": "stage_id",
        "key_fields": ["stage_id", "name", "job_id"],
        "description": "Application stages",
    },
    # DISABLED: application_stage_transitions
    # The Activities API endpoint was REMOVED by TeamTailor on 2020-10-01
    # Use changed_stage_at field in applications instead
    # "application_stage_transitions": {
    #     "partitioned": True,
    #     "partition_cols": ["year", "month"],
    #     "merge_strategy": "append",
    #     "raw_table_name": "teamtailor__talent__application_stage_transitions_raw",
    #     "bronze_table_name": "teamtailor__talent__application_stage_transitions_bronze",
    #     "merge_key": None,
    #     "key_fields": ["activity_id", "application_id", "activity_type", "created_at"],
    #     "description": "Stage transition activities for applications (Time to Hire tracking)",
    # },
    "nps_responses": {
        "partitioned": True,
        "partition_cols": ["year", "month"],
        "merge_strategy": "append",
        "raw_table_name": "teamtailor__talent__nps_responses_raw",
        "bronze_table_name": "teamtailor__talent__nps_responses_bronze",
        "merge_key": None,
        "key_fields": ["nps_response_id", "candidate_id", "score", "created_at"],
        "description": "NPS (Net Promoter Score) survey responses from candidates",
    },
}


# ==========================================
# TEAM TAILOR BRONZE CONFIGURATION
# ==========================================
class TeamTailorBronzeConfig(BronzeJobConfig):
    """
    Team Tailor-specific Bronze configuration.

    Extends BronzeJobConfig with Team Tailor-specific fields.
    Auto-configures table names and paths based on entity_type.

    Parameter Resolution (highest to lowest priority):
    1. Workflow Run Properties (if running in Glue Workflow)
    2. CLI: --ENTITY_TYPE candidates
    3. ENV: ENTITY_TYPE=candidates
    4. Default values (if available)
    """

    # Source name with default value
    source_name: str = Field(
        default="teamtailor",
        description="Source system name (Team Tailor)",
    )

    # Required fields - ConfigBase resolves: CLI → ENV → Error
    entity_type: str = Field(
        ...,
        description=(
            "Team Tailor entity type: candidates, jobs, applications, interviews, users, departments, stages, "
            "application_stage_transitions, nps_responses"
        ),
    )

    # Auto-configured field
    teamtailor_raw_path: Optional[str] = Field(
        default=None,
        description="Computed S3 path for Team Tailor raw data (auto-configured in model_post_init)",
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure table names and paths based on entity_type."""
        # Auto-configure raw_table_name and bronze_table_name if not provided
        # Use entity config if available, otherwise use default pattern
        if not hasattr(self, "raw_table_name") or self.raw_table_name is None:
            # Check if entity config has explicit table name
            entity_config = TEAMTAILOR_BRONZE_ENTITIES.get(self.entity_type, {})
            if "raw_table_name" in entity_config:
                self.raw_table_name = entity_config["raw_table_name"]
            else:
                # Default pattern for entities without explicit config
                self.raw_table_name = f"teamtailor__talent__{self.entity_type}_raw"

        if not hasattr(self, "bronze_table_name") or self.bronze_table_name is None:
            # Check if entity config has explicit table name
            entity_config = TEAMTAILOR_BRONZE_ENTITIES.get(self.entity_type, {})
            if "bronze_table_name" in entity_config:
                self.bronze_table_name = entity_config["bronze_table_name"]
            else:
                # Default pattern for entities without explicit config
                self.bronze_table_name = (
                    f"teamtailor__talent__{self.entity_type}_bronze"
                )

        # Auto-configure teamtailor_raw_path: raw_zone_path + teamtailor/{entity_type}/
        # Example: s3://bucket/raw-zone/ + teamtailor/candidates/ = s3://bucket/raw-zone/teamtailor/candidates/
        self.teamtailor_raw_path = f"{self.raw_zone_path}teamtailor/{self.entity_type}/"


# ==========================================
# TEAM TAILOR BRONZE JOB
# ==========================================
class TeamTailorBronzeJob(BronzeJobBase):
    """
    Team Tailor Bronze Job using BronzeJobBase with Pydantic configuration.

    This job processes Team Tailor data from raw to bronze layer with validated configuration.
    Uses TeamTailorBronzeConfig for configuration management.
    """

    def __init__(self, spark_or_glue_context, config: TeamTailorBronzeConfig):
        """
        Initialize Team Tailor Bronze Job with Pydantic configuration.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (Glue)
            config: Validated TeamTailorBronzeConfig instance (all parameters auto-resolved)
        """
        # Validate entity_type before initializing
        if config.entity_type not in TEAMTAILOR_BRONZE_ENTITIES:
            valid_entities = ", ".join(TEAMTAILOR_BRONZE_ENTITIES.keys())
            raise ValueError(
                f"Invalid entity_type '{config.entity_type}'. "
                f"Valid options: {valid_entities}"
            )

        super().__init__(spark_or_glue_context, config)
        self.entity_type = config.entity_type
        self.entity_config = TEAMTAILOR_BRONZE_ENTITIES[self.entity_type]

        self.logger.info(
            f"TeamTailorBronzeJob initialized for source: {config.source_name}, entity: {self.entity_type}"
        )

    @property
    def is_partitioned(self) -> bool:
        """Check if entity supports partitioning."""
        return self.entity_config["partitioned"]

    def extract(self):
        """Extract data from raw external table using DataFrame (Glue v5 best practice)."""
        self.logger.info(
            f"Extracting from {self.config.raw_database_name}.{self.config.raw_table_name}"
        )
        return self.read_external_table(
            self.config.raw_database_name,
            self.config.raw_table_name,
            is_partitioned=self.is_partitioned,
        )

    def transform(self, df):
        """
        Transform raw data to Bronze schema (multi-entity aware).

        Dispatches to entity-specific transformation methods based on entity_type.
        Each entity has its own transformation logic for key field extraction from JSON API format.
        """
        entity_type = self.config.entity_type
        self.logger.info(f"Transforming {entity_type} data...")

        if df.count() == 0:
            self.logger.warning("Empty DataFrame, skipping transformation")
            return df

        # Dispatch to entity-specific transformation
        if entity_type == "candidates":
            return self._transform_candidates(df)
        elif entity_type == "jobs":
            return self._transform_jobs(df)
        elif entity_type == "applications":
            return self._transform_applications(df)
        elif entity_type == "interviews":
            return self._transform_interviews(df)
        elif entity_type == "users":
            return self._transform_users(df)
        elif entity_type == "departments":
            return self._transform_departments(df)
        elif entity_type == "stages":
            return self._transform_stages(df)
        elif entity_type == "application_stage_transitions":
            return self._transform_activities(df)
        elif entity_type == "nps_responses":
            return self._transform_nps_responses(df)
        else:
            raise ValueError(f"Unknown entity_type: {entity_type}")

    def _transform_candidates(self, df):
        """Transform candidates data (snapshot, no partitions)."""
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            desc,
            get_json_object,
            lit,
            row_number,
        )
        from pyspark.sql.window import Window

        # Extract fields from JSON API format (id, type, attributes, relationships)
        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("candidate_id"),
            get_json_object(col("payload"), "$.attributes.email").alias("email"),
            get_json_object(col("payload"), "$.attributes['first-name']").alias(
                "first_name"
            ),
            get_json_object(col("payload"), "$.attributes['last-name']").alias(
                "last_name"
            ),
            get_json_object(col("payload"), "$.attributes['created-at']")
            .cast("timestamp")
            .alias("candidate_created"),
            get_json_object(col("payload"), "$.attributes['updated-at']")
            .cast("timestamp")
            .alias("candidate_updated"),
            # Custom fields - extract included_custom_field_values array
            get_json_object(col("payload"), "$.included_custom_field_values").alias(
                "custom_field_values_json"
            ),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
        )

        # Deduplicate by candidate_id, keeping the most recent record
        window_spec = Window.partitionBy("candidate_id").orderBy(
            desc("candidate_updated")
        )
        bronze_df = bronze_df.withColumn("row_num", row_number().over(window_spec))
        bronze_df = bronze_df.filter(col("row_num") == 1).drop("row_num")

        self.logger.info(
            f"Transformed {bronze_df.count()} candidates (after deduplication)"
        )
        return bronze_df

    def _transform_jobs(self, df):
        """Transform jobs data (snapshot, no partitions)."""
        from pyspark.sql.functions import col, current_timestamp, get_json_object, lit

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("job_id"),
            get_json_object(col("payload"), "$.attributes.title").alias("title"),
            get_json_object(col("payload"), "$.attributes.status").alias("status"),
            get_json_object(col("payload"), "$.attributes['created-at']")
            .cast("timestamp")
            .alias("created_at"),
            get_json_object(col("payload"), "$.attributes['updated-at']")
            .cast("timestamp")
            .alias("updated_at"),
            # Custom fields - extract included_custom_field_values array
            get_json_object(col("payload"), "$.included_custom_field_values").alias(
                "custom_field_values_json"
            ),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
        )

        self.logger.info(f"Transformed {bronze_df.count()} jobs")
        return bronze_df

    def _transform_applications(self, df):
        """Transform applications data (partitioned by year/month)."""
        from pyspark.sql.functions import col, current_timestamp, get_json_object, lit, when
        from pyspark.sql.functions import month as spark_month
        from pyspark.sql.functions import to_timestamp
        from pyspark.sql.functions import year as spark_year

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("application_id"),
            get_json_object(col("payload"), "$.relationships.candidate.data.id").alias(
                "candidate_id"
            ),
            get_json_object(col("payload"), "$.relationships.job.data.id").alias(
                "job_id"
            ),
            # Stage relationship - current stage of the application
            get_json_object(col("payload"), "$.relationships.stage.data.id").alias(
                "stage_id"
            ),
            get_json_object(col("payload"), "$.attributes.status").alias("status"),
            # Key timestamps for Time to Hire calculations
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['created-at']")
            ).alias("created_at"),
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['updated-at']")
            ).alias("updated_at"),
            # changed-stage-at: When application moved to current stage (key for Time in Stage)
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['changed-stage-at']")
            ).alias("changed_stage_at"),
            # rejected-at: When application was rejected (key for Time to Reject)
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['rejected-at']")
            ).alias("rejected_at"),
            # sourced: Whether candidate was sourced vs applied (key for Source Effectiveness)
            when(
                get_json_object(col("payload"), "$.attributes.sourced") == "true",
                lit(True)
            ).otherwise(lit(False)).alias("sourced"),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
            # Partitioning columns (from raw or computed)
            spark_year(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['created-at']")
                )
            ).alias("year"),
            spark_month(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['created-at']")
                )
            ).alias("month"),
        )

        # Fallback to ingestion timestamp if created_at is null
        from pyspark.sql.functions import when

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

        self.logger.info(f"Transformed {bronze_df.count()} applications")
        return bronze_df

    def _transform_interviews(self, df):
        """Transform interviews data (partitioned by year/month)."""
        from pyspark.sql.functions import col, current_timestamp, get_json_object, lit
        from pyspark.sql.functions import month as spark_month
        from pyspark.sql.functions import to_timestamp
        from pyspark.sql.functions import year as spark_year

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("interview_id"),
            get_json_object(
                col("payload"), "$.relationships.application.data.id"
            ).alias("application_id"),
            get_json_object(col("payload"), "$.attributes['interview-type']").alias(
                "interview_type"
            ),
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['scheduled-at']")
            ).alias("scheduled_at"),
            get_json_object(col("payload"), "$.attributes.status").alias("status"),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
            # Partitioning columns (from raw or computed)
            spark_year(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['scheduled-at']")
                )
            ).alias("year"),
            spark_month(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['scheduled-at']")
                )
            ).alias("month"),
        )

        # Fallback to ingestion timestamp if scheduled_at is null
        from pyspark.sql.functions import when

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

        self.logger.info(f"Transformed {bronze_df.count()} interviews")
        return bronze_df

    def _transform_users(self, df):
        """Transform users data (snapshot, no partitions)."""
        from pyspark.sql.functions import col, current_timestamp, get_json_object, lit

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("user_id"),
            get_json_object(col("payload"), "$.attributes.email").alias("email"),
            get_json_object(col("payload"), "$.attributes['first-name']").alias(
                "first_name"
            ),
            get_json_object(col("payload"), "$.attributes['last-name']").alias(
                "last_name"
            ),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
        )

        self.logger.info(f"Transformed {bronze_df.count()} users")
        return bronze_df

    def _transform_departments(self, df):
        """Transform departments data (snapshot, no partitions)."""
        from pyspark.sql.functions import col, current_timestamp, get_json_object, lit

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("department_id"),
            get_json_object(col("payload"), "$.attributes.name").alias("name"),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
        )

        self.logger.info(f"Transformed {bronze_df.count()} departments")
        return bronze_df

    def _transform_stages(self, df):
        """Transform stages data (snapshot, no partitions)."""
        from pyspark.sql.functions import col, current_timestamp, get_json_object, lit

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("stage_id"),
            get_json_object(col("payload"), "$.attributes.name").alias("name"),
            get_json_object(col("payload"), "$.relationships.job.data.id").alias(
                "job_id"
            ),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
        )

        self.logger.info(f"Transformed {bronze_df.count()} stages")
        return bronze_df

    def _transform_activities(self, df):
        """
        Transform activities data (partitioned by year/month).

        Handles all activity entity types:
        - application_stage_transitions: Stage transition activities for applications (Time to Hire tracking)
        """
        from pyspark.sql.functions import (
            coalesce,
            col,
            current_timestamp,
            get_json_object,
            lit,
        )
        from pyspark.sql.functions import month as spark_month
        from pyspark.sql.functions import to_timestamp, when
        from pyspark.sql.functions import year as spark_year

        # Extract parent resource info from meta (if fetched as relationship)
        # Fallback to relationships if meta not present
        parent_candidate_id = coalesce(
            get_json_object(col("payload"), "$.meta.parent_id"),
            get_json_object(col("payload"), "$.relationships.candidate.data.id"),
            get_json_object(col("payload"), "$.relationships.candidate.data.id"),
        )

        parent_application_id = coalesce(
            get_json_object(col("payload"), "$.relationships.application.data.id"),
            get_json_object(col("payload"), "$.relationships.job-application.data.id"),
        )

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("activity_id"),
            # Parent resource IDs (candidate or application)
            parent_candidate_id.alias("candidate_id"),
            parent_application_id.alias("application_id"),
            # Activity details
            get_json_object(col("payload"), "$.attributes['activity-type']").alias(
                "activity_type"
            ),
            get_json_object(col("payload"), "$.attributes.note").alias("note"),
            get_json_object(col("payload"), "$.attributes.subject").alias("subject"),
            get_json_object(col("payload"), "$.attributes.body").alias("body"),
            # Timestamps
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['created-at']")
            ).alias("created_at"),
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['updated-at']")
            ).alias("updated_at"),
            # Stage transition specific fields (if present)
            get_json_object(col("payload"), "$.relationships.stage.data.id").alias(
                "stage_id"
            ),
            get_json_object(
                col("payload"), "$.relationships['previous-stage'].data.id"
            ).alias("previous_stage_id"),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
            # Partitioning columns (from raw or computed)
            spark_year(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['created-at']")
                )
            ).alias("year"),
            spark_month(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['created-at']")
                )
            ).alias("month"),
        )

        # Fallback to ingestion timestamp if created_at is null
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

        self.logger.info(f"Transformed {bronze_df.count()} {self.entity_type}")
        return bronze_df

    def _transform_nps_responses(self, df):
        """
        Transform NPS responses data (partitioned by year/month).

        Extracts NPS survey response data including score, candidate relationship,
        and timestamps for Candidate NPS use case (UC6).
        """
        from pyspark.sql.functions import col, current_timestamp, get_json_object, lit
        from pyspark.sql.functions import month as spark_month
        from pyspark.sql.functions import to_timestamp, when
        from pyspark.sql.functions import year as spark_year

        bronze_df = df.select(
            # Key fields - extract from JSON API payload
            get_json_object(col("payload"), "$.id").alias("nps_response_id"),
            # Candidate relationship
            get_json_object(col("payload"), "$.relationships.candidate.data.id").alias(
                "candidate_id"
            ),
            # NPS score and details (cast to INT)
            get_json_object(col("payload"), "$.attributes.score")
            .cast("int")
            .alias("score"),
            get_json_object(col("payload"), "$.attributes.comment").alias("comment"),
            # Timestamps
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['created-at']")
            ).alias("created_at"),
            to_timestamp(
                get_json_object(col("payload"), "$.attributes['updated-at']")
            ).alias("updated_at"),
            # Source and ingestion metadata
            lit(self.config.source_name).alias("source"),
            current_timestamp().alias("ingestion_ts"),
            # Raw data (full payload + metadata)
            col("payload"),
            col("metadata"),
            # Partitioning columns (from created_at or computed)
            spark_year(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['created-at']")
                )
            ).alias("year"),
            spark_month(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['created-at']")
                )
            ).alias("month"),
        )

        # Fallback to ingestion timestamp if created_at is null
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

        self.logger.info(f"Transformed {bronze_df.count()} nps_responses")
        return bronze_df

    def load(self, df) -> None:
        """
        Load DataFrame to Iceberg bronze table (multi-entity aware).

        Uses entity-specific merge strategy:
        - Time-based entities (applications, interviews, application_stage_transitions, nps_responses): append
        - Snapshot entities (candidates, jobs, users, departments, stages): merge by ID (UPSERT)
        """
        # Skip loading if DataFrame is empty
        if df.count() == 0:
            self.logger.info("Empty DataFrame, skipping load operation")
            return

        entity_config = self.entity_config
        merge_strategy = entity_config["merge_strategy"]

        self.logger.info(f"Loading {self.config.entity_type} to bronze...")
        self.logger.info(f"  Strategy: {merge_strategy}")
        self.logger.info(
            f"  Target: {self.config.bronze_database_name}.{self.config.bronze_table_name}"
        )

        if merge_strategy == "append":
            # Time-based entities: simple append (no dedup)
            self.write_to_iceberg(
                df, self.config.bronze_database_name, self.config.bronze_table_name
            )
            self.logger.info(f"  ✓ Appended {df.count()} records")

        elif merge_strategy == "merge_by_id":
            # Snapshots: UPSERT by merge_key
            merge_key = entity_config["merge_key"]
            self._merge_to_bronze(df, merge_key)
            self.logger.info(f"  ✓ Merged {df.count()} records (key: {merge_key})")

        else:
            raise ValueError(f"Unknown merge_strategy: {merge_strategy}")

    def _merge_to_bronze(self, df, merge_key: str) -> None:
        """
        Merge DataFrame to Bronze table using MERGE INTO (UPSERT).

        For snapshot entities (candidates, jobs, etc.), we need to:
        1. Check if record exists (by merge_key)
        2. If exists: UPDATE
        3. If not exists: INSERT

        Args:
            df: DataFrame to merge
            merge_key: Column name to use for matching (e.g., "candidate_id", "job_id")
        """
        database = self.config.bronze_database_name
        table = self.config.bronze_table_name
        temp_view = f"{table}_temp"

        # Register DataFrame as temp view
        df.createOrReplaceTempView(temp_view)

        # Build MERGE INTO statement
        # Get all columns except merge_key for UPDATE clause
        all_columns = df.columns
        update_cols = [c for c in all_columns if c != merge_key]

        # Build SET clause for UPDATE (quote column names to avoid reserved word conflicts)
        set_clause = ", ".join([f"target.`{col}` = src.`{col}`" for col in update_cols])

        # Build INSERT columns and values (quote column names)
        insert_cols = ", ".join([f"`{c}`" for c in all_columns])
        insert_vals = ", ".join([f"src.`{col}`" for col in all_columns])

        merge_sql = f"""
        MERGE INTO glue_catalog.{database}.{table} AS target
        USING {temp_view} AS src
        ON target.{merge_key} = src.{merge_key}
        WHEN MATCHED THEN
          UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals})
        """

        self.logger.info(f"Executing MERGE INTO for {table} (key: {merge_key})")
        self.logger.debug(merge_sql)
        self.spark.sql(merge_sql)
        self.logger.info("  ✓ MERGE completed successfully")

    def _get_bronze_schema_ddl(self) -> str:
        """
        Get Bronze table schema DDL based on entity type.

        Returns normalized Bronze schema with:
        - Entity-specific key fields
        - Full payload (JSON string)
        - Metadata string
        - Ingestion timestamp
        - Conditional partitions (year/month for time-based entities)

        Returns:
            str: DDL schema definition (without CREATE TABLE part)
        """
        entity_type = self.entity_type
        is_partitioned = self.is_partitioned

        # Base fields (common to all entities)
        # Note: metadata is stored as JSON STRING to preserve valid JSON format
        base_fields = [
            "source STRING",
            "ingestion_ts TIMESTAMP",
            "payload STRING",
            "metadata STRING",
        ]

        # Entity-specific key fields
        if entity_type == "candidates":
            key_fields = [
                "candidate_id STRING",
                "email STRING",
                "first_name STRING",
                "last_name STRING",
                "candidate_created TIMESTAMP",
                "candidate_updated TIMESTAMP",
                "custom_field_values_json STRING",
            ]
        elif entity_type == "jobs":
            key_fields = [
                "job_id STRING",
                "title STRING",
                "status STRING",
                "created_at TIMESTAMP",
                "updated_at TIMESTAMP",
                "custom_field_values_json STRING",
            ]
        elif entity_type == "applications":
            key_fields = [
                "application_id STRING",
                "candidate_id STRING",
                "job_id STRING",
                "stage_id STRING",
                "status STRING",
                "created_at TIMESTAMP",
                "updated_at TIMESTAMP",
                "changed_stage_at TIMESTAMP",
                "rejected_at TIMESTAMP",
                "sourced BOOLEAN",
            ]
        elif entity_type == "interviews":
            key_fields = [
                "interview_id STRING",
                "application_id STRING",
                "interview_type STRING",
                "scheduled_at TIMESTAMP",
                "status STRING",
            ]
        elif entity_type == "users":
            key_fields = [
                "user_id STRING",
                "email STRING",
                "first_name STRING",
                "last_name STRING",
            ]
        elif entity_type == "departments":
            key_fields = ["department_id STRING", "name STRING"]
        elif entity_type == "stages":
            key_fields = ["stage_id STRING", "name STRING", "job_id STRING"]
        elif entity_type == "application_stage_transitions":
            key_fields = [
                "activity_id STRING",
                "candidate_id STRING",
                "application_id STRING",
                "activity_type STRING",
                "stage_id STRING",
                "previous_stage_id STRING",
                "created_at TIMESTAMP",
                "updated_at TIMESTAMP",
            ]
        elif entity_type == "nps_responses":
            key_fields = [
                "nps_response_id STRING",
                "candidate_id STRING",
                "score INT",
                "comment STRING",
                "created_at TIMESTAMP",
                "updated_at TIMESTAMP",
            ]
        else:
            raise ValueError(f"Unknown entity_type: {entity_type}")

        # Combine all fields
        all_fields = key_fields + base_fields

        # Add partition fields if needed
        if is_partitioned:
            all_fields.extend(["year INT", "month INT"])

        # Join with proper formatting
        return ",\n          ".join(all_fields)

    def create_tables(self):
        """
        Create external table for JSON Lines and bronze Iceberg table.

        Multi-entity aware: supports both partitioned (applications, interviews, application_stage_transitions, nps_responses)
        and flat (snapshots) tables.
        Uses configuration from self.config to create both raw external table
        and bronze Iceberg table with proper schemas and partitioning.
        """
        entity_config = self.entity_config
        is_partitioned = self.is_partitioned

        self.logger.info(f"Creating tables for entity: {self.config.entity_type}")
        self.logger.info(f"  Partitioning: {is_partitioned}")

        # ========================================
        # CREATE RAW EXTERNAL TABLE (spark_catalog)
        # ========================================
        self.logger.info(
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
        """

        # Add partitioning if detected in S3
        if is_partitioned:
            partition_clause = "PARTITIONED BY (\n          year INT,\n          month INT\n        )\n        "
        else:
            partition_clause = ""

        # Complete DDL
        raw_table_ddl += (
            partition_clause
            + f"""
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'true'
        )
        LOCATION '{self.config.teamtailor_raw_path}'
        TBLPROPERTIES (
          'classification' = 'json',
          'compressionType' = 'none',
          'typeOfData' = 'file'
        )
        """
        )

        self.spark.sql(raw_table_ddl)
        self.logger.info(
            f"  ✓ Raw table created: spark_catalog.{self.config.raw_database_name}.{self.config.raw_table_name}"
        )

        # ========================================
        # CREATE BRONZE ICEBERG TABLE (glue_catalog)
        # ========================================
        self.logger.info(
            "Creating bronze Iceberg table: %s.%s",
            self.config.bronze_database_name,
            self.config.bronze_table_name,
        )

        self.spark.sql(
            f"DROP TABLE IF EXISTS glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
        )

        # Get entity-specific schema
        bronze_schema_ddl = self._get_bronze_schema_ddl()

        # Build Iceberg DDL
        bronze_table_ddl = f"""
        CREATE TABLE glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name} (
          {bronze_schema_ddl}
        )
        USING ICEBERG
        """

        # Add partitioning if needed
        if is_partitioned:
            partition_cols = ", ".join(entity_config["partition_cols"])
            bronze_table_ddl += f"\n        PARTITIONED BY ({partition_cols})"

        self.spark.sql(bronze_table_ddl)
        self.logger.debug(f"bronze_table_ddl: {bronze_table_ddl}")
        self.logger.info(
            f"  ✓ Bronze table created: glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
        )

        self.logger.info("✅ Tables created successfully")


# ==========================================
# MAIN EXECUTION (HYBRID LOCAL/GLUE)
# ==========================================
if __name__ == "__main__":
    try:
        setup_logging()
        # Step 1: Load configuration using TeamTailorBronzeConfig
        # Parameters are resolved in this order:
        # Configuration resolution (highest to lowest priority):
        #   1. Workflow Run Properties (if in Glue Workflow)
        #   2. Command line arguments (--PARAM_NAME)
        #   3. Environment variables (PARAM_NAME)
        #   4. Team Tailor-specific defaults (defined in TeamTailorBronzeConfig)
        logger.info("Loading Team Tailor Bronze configuration...")
        config = TeamTailorBronzeConfig.from_args()

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
            bronze_job = TeamTailorBronzeJob(spark, config)
        else:
            bronze_job = TeamTailorBronzeJob(context["glue_context"], config)

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
