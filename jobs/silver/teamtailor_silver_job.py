"""
Team Tailor Silver Pipeline Job - Hybrid Local/Glue Support

This job processes Team Tailor data from bronze to silver layer with support for both
local development (LocalStack + Derby) and AWS Glue (GlueContext + Glue Data Catalog).

Features:
- ✅ Hybrid support: Works locally with LocalStack and in AWS Glue
- ✅ AWS Glue v5 DataFrame-first approach
- ✅ SilverJobBase with encapsulated infrastructure setup
- ✅ Silver layer processing: bronze → silver
- ✅ Pydantic-based configuration with automatic parameter resolution
- ✅ TeamTailorSilverConfig with Team Tailor-specific defaults
- ✅ Four-tier parameter resolution: workflow properties, job parameters, environment variables, defaults
- ✅ External table support in both environments (Derby/Glue Data Catalog)
- ✅ 7 specialized Silver tables for Talent analytics

Local Usage (auto-detected, no --JOB_NAME):
  python jobs/silver/teamtailor_silver_job.py

AWS Glue Usage (auto-detected with --JOB_NAME):
  python jobs/silver/teamtailor_silver_job.py --JOB_NAME teamtailor_silver_pipeline

Minimal Usage (using defaults):
  python jobs/silver/teamtailor_silver_job.py --JOB_NAME teamtailor_silver_pipeline

Full Usage (overriding defaults):
  python jobs/silver/teamtailor_silver_job.py \\
    --JOB_NAME teamtailor_silver_pipeline \\
    --ENTITY_TYPE candidate_profiles \\
    --BRONZE_DATABASE nan_develop_bronze_zone_ingestion \\
    --BRONZE_TABLE teamtailor__talent__candidates_bronze \\
    --SILVER_DATABASE nan_develop_silver_analytics \\
    --SILVER_TABLE teamtailor_candidate_profiles \\
    --WAREHOUSE_S3_LOCATION s3://bucket/nan/

Required Parameters (Glue mode only):
- JOB_NAME: AWS Glue job name (triggers Glue mode)

Optional Parameters (with Team Tailor defaults):
- ENTITY_TYPE: Silver entity type (default: 'candidate_profiles')
- BRONZE_DATABASE: Bronze layer database name (default: 'nan_develop_bronze_zone_ingestion')
- BRONZE_TABLE: Bronze layer table name (auto-configured from entity_type)
- SILVER_DATABASE: Silver layer database name (default: 'nan_develop_silver_analytics')
- SILVER_TABLE: Silver layer table name (auto-configured from entity_type)
- WAREHOUSE_S3_LOCATION: S3 location for Iceberg warehouse (default: develop bucket)
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

from libs.common import SilverJobConfig, setup_logging
from libs.pyspark import SessionConfig, SilverJobBase, SparkSessionFactory

# Configure logging
logger = logging.getLogger(__name__)

# ==========================================
# TEAM TAILOR SILVER ENTITIES CONFIGURATION
# ==========================================
TEAMTAILOR_SILVER_ENTITIES = {
    "candidate_profiles": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "upsert",
        "bronze_table_suffix": "teamtailor__talent__candidates_bronze",
        "silver_table_suffix": "teamtailor_candidate_profiles",
        "merge_key": "candidate_id",
        "key_fields": ["candidate_id", "email", "first_name", "last_name"],
        "description": "Enriched candidate profile data",
    },
    "job_postings": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "upsert",
        "bronze_table_suffix": "teamtailor__talent__jobs_bronze",
        "silver_table_suffix": "teamtailor_job_postings",
        "merge_key": "job_id",
        "key_fields": ["job_id", "title", "status", "created_at"],
        "description": "Job postings with status tracking",
    },
    "application_pipeline": {
        "partitioned": True,
        "partition_cols": ["year", "month"],
        "merge_strategy": "append",
        "bronze_table_suffix": "teamtailor__talent__applications_bronze",
        "silver_table_suffix": "teamtailor_application_pipeline",
        "merge_key": None,
        "key_fields": [
            "application_id",
            "candidate_id",
            "job_id",
            "status",
            "created_at",
        ],
        "description": "Application journey with stages",
    },
    "interview_events": {
        "partitioned": True,
        "partition_cols": ["year", "month"],
        "merge_strategy": "append",
        "bronze_table_suffix": "teamtailor__talent__interviews_bronze",
        "silver_table_suffix": "teamtailor_interview_events",
        "merge_key": None,
        "key_fields": [
            "interview_id",
            "application_id",
            "interview_type",
            "scheduled_at",
        ],
        "description": "Interview tracking and outcomes",
    },
    "recruiter_performance": {
        "partitioned": False,
        "partition_cols": [],
        "merge_strategy": "upsert",
        "bronze_table_suffix": "teamtailor__talent__users_bronze",
        "silver_table_suffix": "teamtailor_recruiter_performance",
        "merge_key": "user_id",
        "key_fields": ["user_id", "email", "first_name", "last_name"],
        "description": "Recruiter/hiring manager metrics",
    },
    "candidate_nps": {
        "partitioned": True,
        "partition_cols": ["year", "month"],
        "merge_strategy": "append",
        "bronze_table_suffix": "teamtailor__talent__nps_responses_bronze",
        "silver_table_suffix": "teamtailor_candidate_nps",
        "merge_key": None,
        "key_fields": ["nps_response_id", "candidate_id", "score", "created_at"],
        "description": "Candidate NPS survey responses for UC6",
    },
    # DISABLED: application_stage_transitions
    # The Activities API endpoint was REMOVED by TeamTailor on 2020-10-01
    # Use changed_stage_at field in application_pipeline instead
    # "application_stage_transitions": {
    #     "partitioned": True,
    #     "partition_cols": ["year", "month"],
    #     "merge_strategy": "append",
    #     "bronze_table_suffix": "teamtailor__talent__application_stage_transitions_bronze",
    #     "silver_table_suffix": "teamtailor_application_stage_transitions",
    #     "merge_key": None,
    #     "key_fields": ["activity_id", "application_id", "stage_id", "created_at"],
    #     "description": "Application stage transitions for UC7 (stage jumps, regressions)",
    # },
}


# ==========================================
# TEAM TAILOR SILVER CONFIGURATION
# ==========================================
class TeamTailorSilverConfig(SilverJobConfig):
    """
    Team Tailor-specific Silver configuration.

    Extends SilverJobConfig with Team Tailor-specific fields.
    Auto-configures bronze_table_name and silver_table_name based on entity_type.

    Parameter Resolution (highest to lowest priority):
    1. Workflow Run Properties (if running in Glue Workflow)
    2. CLI: --ENTITY_TYPE candidate_profiles
    3. ENV: ENTITY_TYPE=candidate_profiles
    4. Default: candidate_profiles
    """

    # Source name with default value
    source_name: str = Field(
        default="teamtailor",
        description="Source system name (Team Tailor)",
    )

    # Optional with default - ConfigBase resolves: Workflow → CLI → ENV → Default
    entity_type: str = Field(
        default="candidate_profiles",
        description="Team Tailor Silver entity type (candidate_profiles, job_postings, application_pipeline, interview_events, recruiter_performance, candidate_nps, application_stage_transitions)",
    )

    # Auto-configured fields
    bronze_table_name: Optional[str] = Field(
        default=None, description="Bronze table name (auto-configured if not provided)"
    )
    silver_table_name: Optional[str] = Field(
        default=None, description="Silver table name (auto-configured if not provided)"
    )
    drop_and_recreate: bool = Field(
        default=True, description="If True, drop target Silver table before creating it"
    )
    enable_enrichment: bool = Field(
        default=True,
        description="Enable data enrichment (business hours, weekends, etc.)",
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure table names based on entity_type."""
        # Auto-configure bronze_table_name (source) from entity config
        if not self.bronze_table_name:
            if self.entity_type in TEAMTAILOR_SILVER_ENTITIES:
                self.bronze_table_name = TEAMTAILOR_SILVER_ENTITIES[
                    self.entity_type
                ].get("bronze_table_suffix")
            else:
                # Fallback: default to candidates_bronze table
                self.bronze_table_name = "teamtailor__talent__candidates_bronze"

        # Auto-configure silver_table_name (target) based on entity_type
        if not self.silver_table_name:
            if self.entity_type in TEAMTAILOR_SILVER_ENTITIES:
                self.silver_table_name = TEAMTAILOR_SILVER_ENTITIES[
                    self.entity_type
                ].get("silver_table_suffix")
            else:
                # Fallback: construct from entity_type
                entity_type_str = str(self.entity_type)
                self.silver_table_name = f"teamtailor_{entity_type_str}"


# ==========================================
# TEAM TAILOR SILVER JOB
# ==========================================
class TeamTailorSilverJob(SilverJobBase):
    """
    Team Tailor Silver Pipeline Job.

    Processes Team Tailor data from Bronze to Silver layer with specialized tables
    for Talent analytics.
    Uses TeamTailorSilverConfig for configuration management.
    """

    def __init__(self, spark_or_glue_context, config: TeamTailorSilverConfig):
        """
        Initialize Team Tailor Silver Job with Pydantic configuration.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (Glue)
            config: Validated TeamTailorSilverConfig instance (all parameters auto-resolved)
        """
        # Validate entity_type before initializing
        if config.entity_type not in TEAMTAILOR_SILVER_ENTITIES:
            valid_entities = ", ".join(TEAMTAILOR_SILVER_ENTITIES.keys())
            raise ValueError(
                f"Invalid entity_type '{config.entity_type}'. "
                f"Valid options: {valid_entities}"
            )

        super().__init__(spark_or_glue_context, config)
        self.entity_type = config.entity_type
        self.entity_config = TEAMTAILOR_SILVER_ENTITIES[self.entity_type]

        logger.info(
            f"Initialized Team Tailor Silver job for entity: {config.entity_type}"
        )

    def extract(self):
        """Extract data from Bronze layer."""
        logger.info(f"Extracting {self.config.entity_type} data from Bronze layer")

        # Build source table path
        source_table = f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"

        # Simple query for all entities
        query = f"SELECT * FROM {source_table}"
        logger.info(f"Extracting with query: {query}")
        bronze_df = self.spark.sql(query)

        # Log extraction results
        record_count = bronze_df.count()
        logger.info(f"Extracted {record_count} records from Bronze layer")

        return bronze_df

    def transform(self, bronze_df):
        """Transform Bronze data to Silver format."""
        logger.info(f"Transforming {self.config.entity_type} data to Silver format")

        # Register UDFs for custom field extraction
        self._register_custom_field_udfs()

        # Get transformer based on entity type
        transformer = self._get_transformer()
        silver_df = transformer(bronze_df)

        # Log transformation results
        record_count = silver_df.count()
        logger.info(f"Transformed {record_count} records to Silver format")

        return silver_df

    def _register_custom_field_udfs(self):
        """Register UDFs for extracting custom fields from TeamTailor custom-field-values."""
        import json

        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        @udf(returnType=StringType())
        def extract_custom_field_value(custom_fields_json, field_type_pattern=None):
            """
            Extract value from custom field by field-type pattern.

            Args:
                custom_fields_json: JSON string of custom-field-values array
                field_type_pattern: Pattern to match field-type (e.g., "CustomField::Select", "CustomField::MultiSelect")

            Returns:
                String representation of the value (JSON string for arrays)
            """
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
                    attrs = field.get("attributes", {})
                    field_type = attrs.get("field-type", "")
                    value = attrs.get("value")

                    # If field_type_pattern specified, match it
                    if field_type_pattern:
                        if field_type_pattern in field_type:
                            if isinstance(value, list):
                                return json.dumps(
                                    value
                                )  # Return as JSON string for arrays
                            return str(value) if value is not None else None
                    else:
                        # Return first non-null value
                        if value is not None:
                            if isinstance(value, list):
                                return json.dumps(value)
                            return str(value)

                return None
            except (json.JSONDecodeError, TypeError, AttributeError) as e:
                logger.debug(f"Error parsing custom fields: {e}")
                return None

        @udf(returnType=StringType())
        def extract_custom_field_by_type(custom_fields_json, field_type):
            """
            Extract custom field value by exact field-type match.

            Args:
                custom_fields_json: JSON string of custom-field-values array
                field_type: Exact field-type to match (e.g., "CustomField::Select", "CustomField::Number")

            Returns:
                String representation of the value
            """
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
                    attrs = field.get("attributes", {})
                    if attrs.get("field-type") == field_type:
                        value = attrs.get("value")
                        if value is not None:
                            if isinstance(value, list):
                                return json.dumps(value)
                            return str(value)

                return None
            except (json.JSONDecodeError, TypeError, AttributeError):
                return None

        # Register UDFs
        self.spark.udf.register(
            "extract_custom_field_value", extract_custom_field_value
        )
        self.spark.udf.register(
            "extract_custom_field_by_type", extract_custom_field_by_type
        )

        logger.debug("Registered custom field extraction UDFs")

    def load(self, silver_df):
        """Load Silver data to target table."""
        logger.info(f"Loading {self.config.entity_type} data to Silver layer")

        # Get target table name
        target_table = f"glue_catalog.{self.config.silver_database_name}.{self.config.silver_table_name}"

        # Get merge strategy
        merge_strategy = self.entity_config["merge_strategy"]

        # Load data based on merge strategy
        if merge_strategy == "append":
            self._load_append(silver_df, target_table)
        elif merge_strategy == "upsert":
            self._load_upsert(silver_df, target_table)
        else:
            raise ValueError(f"Unknown merge strategy: {merge_strategy}")

        # Log load results
        record_count = silver_df.count()
        logger.info(f"Loaded {record_count} records to {target_table}")

    def _get_transformer(self):
        """Get the appropriate transformer function for the entity type."""
        if self.config.entity_type == "candidate_profiles":
            return self._transform_candidate_profiles
        elif self.config.entity_type == "job_postings":
            return self._transform_job_postings
        elif self.config.entity_type == "application_pipeline":
            return self._transform_application_pipeline
        elif self.config.entity_type == "interview_events":
            return self._transform_interview_events
        elif self.config.entity_type == "recruiter_performance":
            return self._transform_recruiter_performance
        elif self.config.entity_type == "candidate_nps":
            return self._transform_candidate_nps
        elif self.config.entity_type == "application_stage_transitions":
            return self._transform_application_stage_transitions
        else:
            raise ValueError(
                f"No transformer available for entity type: {self.config.entity_type}"
            )

    def _transform_candidate_profiles(self, bronze_df):
        """Transform candidate data to Silver format."""
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            expr,
            get_json_object,
            lit,
            when,
        )

        # Base transformation
        silver_df = bronze_df.select(
            col("candidate_id"),
            col("email"),
            col("first_name"),
            col("last_name"),
            col("candidate_created"),
            col("candidate_updated"),
            col("custom_field_values_json"),  # Include custom fields
            col("ingestion_ts").alias("bronze_ingestion_timestamp"),
            col("payload"),
            col("metadata"),
        )

        # Extract additional fields from payload (JSON API format)
        silver_df = (
            silver_df.withColumn(
                "phone", get_json_object(col("payload"), "$.attributes.phone")
            )
            .withColumn(
                "linkedin_url",
                get_json_object(col("payload"), "$.attributes['linkedin-url']"),
            )
            .withColumn(
                "source",
                get_json_object(col("payload"), "$.attributes.source"),
            )
            .withColumn(
                "tags",
                get_json_object(col("payload"), "$.attributes.tags"),
            )
        )

        # Extract custom fields (if available)
        # Example: Extract first MultiSelect custom field (common for role/skill fields)

        silver_df = silver_df.withColumn(
            "custom_field_role",
            expr(
                "extract_custom_field_by_type(custom_field_values_json, 'CustomField::MultiSelect')"
            ),
        )

        # Add data quality fields
        silver_df = silver_df.withColumn(
            "data_quality_score",
            when(
                col("candidate_id").isNotNull() & col("email").isNotNull(), 0.95
            ).otherwise(0.5),
        ).withColumn(
            "is_valid",
            when(
                col("candidate_id").isNotNull() & col("email").isNotNull(), True
            ).otherwise(False),
        )

        # Add enrichment fields
        if self.config.enable_enrichment:
            silver_df = (
                silver_df.withColumn(
                    "has_complete_name",
                    when(
                        col("first_name").isNotNull() & col("last_name").isNotNull(),
                        True,
                    ).otherwise(False),
                )
                .withColumn(
                    "has_phone",
                    when(col("phone").isNotNull(), True).otherwise(False),
                )
                .withColumn(
                    "has_linkedin",
                    when(col("linkedin_url").isNotNull(), True).otherwise(False),
                )
            )

        # Add metadata
        silver_df = (
            silver_df.withColumn("silver_ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("teamtailor"))
            .withColumn("etl_job_name", lit(self.config.job_name))
            .withColumn("etl_run_id", lit("local"))  # Simplified for now
            .withColumn("etl_run_timestamp", current_timestamp())
            .withColumn(
                "bronze_table_name",
                lit(
                    f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
                ),
            )
            .withColumn("bronze_partition_year", lit(None).cast("int"))
            .withColumn("bronze_partition_month", lit(None).cast("int"))
            .withColumn(
                "record_hash",
                expr(
                    "sha2(concat_ws('||', coalesce(candidate_id,''), coalesce(email,''), cast(candidate_created as string)), 256)"
                ),
            )
        )

        # Remove raw blobs
        silver_df = silver_df.drop("payload", "metadata")

        return silver_df

    def _transform_job_postings(self, bronze_df):
        """Transform job data to Silver format."""
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            expr,
            get_json_object,
            lit,
            when,
        )

        # Base transformation
        silver_df = bronze_df.select(
            col("job_id"),
            col("title"),
            col("status"),
            col("created_at"),
            col("updated_at"),
            col("custom_field_values_json"),  # Include custom fields
            col("ingestion_ts").alias("bronze_ingestion_timestamp"),
            col("payload"),
            col("metadata"),
        )

        # Extract additional fields from payload
        silver_df = (
            silver_df.withColumn(
                "department_id",
                get_json_object(col("payload"), "$.relationships.department.data.id"),
            )
            .withColumn(
                "description",
                get_json_object(col("payload"), "$.attributes.description"),
            )
            .withColumn(
                "internal_title",
                get_json_object(col("payload"), "$.attributes['internal-title']"),
            )
        )

        # Extract custom fields (if available)
        # Jobs commonly have custom fields like: Account, Employment Type, Deal Type, Budget, etc.

        silver_df = (
            silver_df.withColumn(
                "custom_field_account",
                expr(
                    "extract_custom_field_by_type(custom_field_values_json, 'CustomField::Select')"
                ),
            )
            .withColumn(
                "custom_field_employment_type",
                expr(
                    "extract_custom_field_by_type(custom_field_values_json, 'CustomField::MultiSelect')"
                ),
            )
            .withColumn(
                "custom_field_budget",
                expr(
                    "extract_custom_field_by_type(custom_field_values_json, 'CustomField::Number')"
                ),
            )
            .withColumn(
                "custom_fields_json",
                col("custom_field_values_json"),  # Keep full JSON for future extraction
            )
        )

        # Add data quality fields
        silver_df = silver_df.withColumn(
            "data_quality_score",
            when(col("job_id").isNotNull() & col("title").isNotNull(), 0.95).otherwise(
                0.5
            ),
        ).withColumn(
            "is_valid",
            when(col("job_id").isNotNull() & col("title").isNotNull(), True).otherwise(
                False
            ),
        )

        # Add metadata
        silver_df = (
            silver_df.withColumn("silver_ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("teamtailor"))
            .withColumn("etl_job_name", lit(self.config.job_name))
            .withColumn("etl_run_id", lit("local"))
            .withColumn("etl_run_timestamp", current_timestamp())
            .withColumn(
                "bronze_table_name",
                lit(
                    f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
                ),
            )
            .withColumn("bronze_partition_year", lit(None).cast("int"))
            .withColumn("bronze_partition_month", lit(None).cast("int"))
            .withColumn(
                "record_hash",
                expr(
                    "sha2(concat_ws('||', coalesce(job_id,''), coalesce(title,''), cast(created_at as string)), 256)"
                ),
            )
        )

        # Remove raw blobs
        silver_df = silver_df.drop("payload", "metadata")

        return silver_df

    def _transform_application_pipeline(self, bronze_df):
        """Transform application data to Silver format."""
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            datediff,
            expr,
            get_json_object,
            lit,
            when,
        )

        # Check which columns exist in bronze (new columns may not exist in old data)
        available_cols = set(bronze_df.columns)

        # Base transformation with new key fields for Time to Hire
        select_cols = [
            col("application_id"),
            col("candidate_id"),
            col("job_id"),
            col("status"),
            col("created_at"),
            col("updated_at"),
            col("ingestion_ts").alias("bronze_ingestion_timestamp"),
            col("payload"),
            col("metadata"),
            col("year"),
            col("month"),
        ]

        # Add new fields if available in Bronze, otherwise extract from payload
        if "stage_id" in available_cols:
            select_cols.append(col("stage_id"))
        if "changed_stage_at" in available_cols:
            select_cols.append(col("changed_stage_at"))
        if "rejected_at" in available_cols:
            select_cols.append(col("rejected_at"))
        if "sourced" in available_cols:
            select_cols.append(col("sourced"))

        silver_df = bronze_df.select(*select_cols)

        # Extract additional fields from payload (fallback for missing columns)
        if "stage_id" not in available_cols:
            silver_df = silver_df.withColumn(
                "stage_id",
                get_json_object(col("payload"), "$.relationships.stage.data.id"),
            )
        if "changed_stage_at" not in available_cols:
            silver_df = silver_df.withColumn(
                "changed_stage_at",
                get_json_object(col("payload"), "$.attributes['changed-stage-at']").cast("timestamp"),
            )
        if "rejected_at" not in available_cols:
            silver_df = silver_df.withColumn(
                "rejected_at",
                get_json_object(col("payload"), "$.attributes['rejected-at']").cast("timestamp"),
            )
        if "sourced" not in available_cols:
            silver_df = silver_df.withColumn(
                "sourced",
                get_json_object(col("payload"), "$.attributes.sourced"),
            )

        # Extract referred_by_id
        silver_df = silver_df.withColumn(
            "referred_by_id",
            get_json_object(col("payload"), "$.relationships['referred-by'].data.id"),
        )

        # Calculate Time in Current Stage (days since changed_stage_at)
        silver_df = silver_df.withColumn(
            "days_in_current_stage",
            when(
                col("changed_stage_at").isNotNull(),
                datediff(current_timestamp(), col("changed_stage_at")),
            ).otherwise(None),
        )

        # Add data quality fields
        silver_df = silver_df.withColumn(
            "data_quality_score",
            when(
                col("application_id").isNotNull()
                & col("candidate_id").isNotNull()
                & col("job_id").isNotNull(),
                0.95,
            ).otherwise(0.5),
        ).withColumn(
            "is_valid",
            when(
                col("application_id").isNotNull()
                & col("candidate_id").isNotNull()
                & col("job_id").isNotNull(),
                True,
            ).otherwise(False),
        )

        # Add metadata
        silver_df = (
            silver_df.withColumn("silver_ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("teamtailor"))
            .withColumn("etl_job_name", lit(self.config.job_name))
            .withColumn("etl_run_id", lit("local"))
            .withColumn("etl_run_timestamp", current_timestamp())
            .withColumn(
                "bronze_table_name",
                lit(
                    f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
                ),
            )
            .withColumn("bronze_partition_year", col("year"))
            .withColumn("bronze_partition_month", col("month"))
            .withColumn(
                "record_hash",
                expr(
                    "sha2(concat_ws('||', coalesce(application_id,''), coalesce(candidate_id,''), coalesce(job_id,''), cast(created_at as string)), 256)"
                ),
            )
        )

        # Remove raw blobs
        silver_df = silver_df.drop("payload", "metadata")

        return silver_df

    def _transform_interview_events(self, bronze_df):
        """Transform interview data to Silver format."""
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            expr,
            get_json_object,
            lit,
            when,
        )

        # Base transformation
        silver_df = bronze_df.select(
            col("interview_id"),
            col("application_id"),
            col("interview_type"),
            col("scheduled_at"),
            col("status"),
            col("ingestion_ts").alias("bronze_ingestion_timestamp"),
            col("payload"),
            col("metadata"),
            col("year"),
            col("month"),
        )

        # Extract additional fields from payload
        silver_df = (
            silver_df.withColumn(
                "interviewer_ids",
                get_json_object(col("payload"), "$.relationships.interviewers.data"),
            )
            .withColumn(
                "location",
                get_json_object(col("payload"), "$.attributes.location"),
            )
            .withColumn(
                "note",
                get_json_object(col("payload"), "$.attributes.note"),
            )
        )

        # Add data quality fields
        silver_df = silver_df.withColumn(
            "data_quality_score",
            when(
                col("interview_id").isNotNull()
                & col("application_id").isNotNull()
                & col("scheduled_at").isNotNull(),
                0.95,
            ).otherwise(0.5),
        ).withColumn(
            "is_valid",
            when(
                col("interview_id").isNotNull()
                & col("application_id").isNotNull()
                & col("scheduled_at").isNotNull(),
                True,
            ).otherwise(False),
        )

        # Add enrichment fields
        if self.config.enable_enrichment:
            silver_df = silver_df.withColumn(
                "is_weekend",
                when(
                    expr("EXTRACT(DAYOFWEEK FROM scheduled_at) IN (1, 7)"), True
                ).otherwise(False),
            ).withColumn(
                "is_business_hours",
                when(
                    expr("EXTRACT(HOUR FROM scheduled_at) BETWEEN 9 AND 17"),
                    True,
                ).otherwise(False),
            )

        # Add metadata
        silver_df = (
            silver_df.withColumn("silver_ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("teamtailor"))
            .withColumn("etl_job_name", lit(self.config.job_name))
            .withColumn("etl_run_id", lit("local"))
            .withColumn("etl_run_timestamp", current_timestamp())
            .withColumn(
                "bronze_table_name",
                lit(
                    f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
                ),
            )
            .withColumn("bronze_partition_year", col("year"))
            .withColumn("bronze_partition_month", col("month"))
            .withColumn(
                "record_hash",
                expr(
                    "sha2(concat_ws('||', coalesce(interview_id,''), coalesce(application_id,''), cast(scheduled_at as string)), 256)"
                ),
            )
        )

        # Remove raw blobs
        silver_df = silver_df.drop("payload", "metadata")

        return silver_df

    def _transform_recruiter_performance(self, bronze_df):
        """Transform user/recruiter data to Silver format."""
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            expr,
            get_json_object,
            lit,
            when,
        )

        # Base transformation
        silver_df = bronze_df.select(
            col("user_id"),
            col("email"),
            col("first_name"),
            col("last_name"),
            col("ingestion_ts").alias("bronze_ingestion_timestamp"),
            col("payload"),
            col("metadata"),
        )

        # Extract additional fields from payload
        silver_df = silver_df.withColumn(
            "role",
            get_json_object(col("payload"), "$.attributes.role"),
        ).withColumn(
            "department_id",
            get_json_object(col("payload"), "$.relationships.department.data.id"),
        )

        # Add data quality fields
        silver_df = silver_df.withColumn(
            "data_quality_score",
            when(col("user_id").isNotNull() & col("email").isNotNull(), 0.95).otherwise(
                0.5
            ),
        ).withColumn(
            "is_valid",
            when(col("user_id").isNotNull() & col("email").isNotNull(), True).otherwise(
                False
            ),
        )

        # Add metadata
        silver_df = (
            silver_df.withColumn("silver_ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("teamtailor"))
            .withColumn("etl_job_name", lit(self.config.job_name))
            .withColumn("etl_run_id", lit("local"))
            .withColumn("etl_run_timestamp", current_timestamp())
            .withColumn(
                "bronze_table_name",
                lit(
                    f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
                ),
            )
            .withColumn("bronze_partition_year", lit(None).cast("int"))
            .withColumn("bronze_partition_month", lit(None).cast("int"))
            .withColumn(
                "record_hash",
                expr(
                    "sha2(concat_ws('||', coalesce(user_id,''), coalesce(email,'')), 256)"
                ),
            )
        )

        # Remove raw blobs
        silver_df = silver_df.drop("payload", "metadata")

        return silver_df

    def _transform_candidate_nps(self, bronze_df):
        """Transform NPS responses data to Silver format."""
        from pyspark.sql.functions import col, current_timestamp, expr, lit, when

        # Base transformation
        silver_df = bronze_df.select(
            col("nps_response_id"),
            col("candidate_id"),
            col("score"),
            col("comment"),
            col("created_at"),
            col("updated_at"),
            col("ingestion_ts").alias("bronze_ingestion_timestamp"),
            col("year"),
            col("month"),
            col("payload"),
            col("metadata"),
        )

        # Calculate NPS category (Promoter: 9-10, Passive: 7-8, Detractor: 0-6)
        silver_df = silver_df.withColumn(
            "nps_category",
            when(col("score").between(9, 10), "Promoter")
            .when(col("score").between(7, 8), "Passive")
            .when(col("score").between(0, 6), "Detractor")
            .otherwise("Unknown"),
        )

        # Add data quality fields
        silver_df = silver_df.withColumn(
            "data_quality_score",
            when(
                col("nps_response_id").isNotNull()
                & col("candidate_id").isNotNull()
                & col("score").isNotNull(),
                0.95,
            ).otherwise(0.5),
        ).withColumn(
            "is_valid",
            when(
                col("nps_response_id").isNotNull()
                & col("candidate_id").isNotNull()
                & col("score").isNotNull(),
                True,
            ).otherwise(False),
        )

        # Add metadata
        silver_df = (
            silver_df.withColumn("silver_ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("teamtailor"))
            .withColumn("etl_job_name", lit(self.config.job_name))
            .withColumn("etl_run_id", lit("local"))
            .withColumn("etl_run_timestamp", current_timestamp())
            .withColumn(
                "bronze_table_name",
                lit(
                    f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
                ),
            )
            .withColumn("bronze_partition_year", col("year"))
            .withColumn("bronze_partition_month", col("month"))
            .withColumn(
                "record_hash",
                expr(
                    "sha2(concat_ws('||', coalesce(nps_response_id,''), coalesce(candidate_id,''), coalesce(cast(score as string),'')), 256)"
                ),
            )
        )

        # Remove raw blobs
        silver_df = silver_df.drop("payload", "metadata")

        return silver_df

    def _transform_application_stage_transitions(self, bronze_df):
        """Transform application stage transitions data to Silver format."""
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            expr,
            get_json_object,
            lit,
            to_timestamp,
            when,
        )

        # Check if columns exist, otherwise extract from payload
        available_cols = set(bronze_df.columns)

        # Build select list with fallback to payload extraction
        select_cols = [
            col("activity_id"),
            col("application_id"),
            col("candidate_id"),
        ]

        # Extract stage_id from payload if column doesn't exist
        if "stage_id" in available_cols:
            select_cols.append(col("stage_id"))
        else:
            select_cols.append(
                get_json_object(col("payload"), "$.relationships.stage.data.id").alias(
                    "stage_id"
                )
            )

        # Extract previous_stage_id from payload if column doesn't exist
        if "previous_stage_id" in available_cols:
            select_cols.append(col("previous_stage_id"))
        else:
            select_cols.append(
                get_json_object(
                    col("payload"), "$.relationships['previous-stage'].data.id"
                ).alias("previous_stage_id")
            )

        # Add activity_type and created_at (should always exist)
        select_cols.extend(
            [
                col("activity_type"),
                col("created_at"),
            ]
        )

        # Extract updated_at from payload if column doesn't exist
        if "updated_at" in available_cols:
            select_cols.append(col("updated_at"))
        else:
            select_cols.append(
                to_timestamp(
                    get_json_object(col("payload"), "$.attributes['updated-at']")
                ).alias("updated_at")
            )

        # Add remaining columns
        select_cols.extend(
            [
                col("ingestion_ts").alias("bronze_ingestion_timestamp"),
                col("year"),
                col("month"),
                col("payload"),
                col("metadata"),
            ]
        )

        # Base transformation
        silver_df = bronze_df.select(*select_cols)

        # Determine transition type: forward (normal progression), backward (regression), or jump (skip stages)
        # Note: This is a simplified version - full analysis would require stage ordering
        silver_df = silver_df.withColumn(
            "transition_type",
            when(col("previous_stage_id").isNull(), "initial")
            .when(col("previous_stage_id") != col("stage_id"), "transition")
            .otherwise("same_stage"),
        )

        # Add data quality fields
        silver_df = silver_df.withColumn(
            "data_quality_score",
            when(
                col("activity_id").isNotNull()
                & col("application_id").isNotNull()
                & col("stage_id").isNotNull(),
                0.95,
            ).otherwise(0.5),
        ).withColumn(
            "is_valid",
            when(
                col("activity_id").isNotNull()
                & col("application_id").isNotNull()
                & col("stage_id").isNotNull(),
                True,
            ).otherwise(False),
        )

        # Add metadata
        silver_df = (
            silver_df.withColumn("silver_ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("teamtailor"))
            .withColumn("etl_job_name", lit(self.config.job_name))
            .withColumn("etl_run_id", lit("local"))
            .withColumn("etl_run_timestamp", current_timestamp())
            .withColumn(
                "bronze_table_name",
                lit(
                    f"glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name}"
                ),
            )
            .withColumn("bronze_partition_year", col("year"))
            .withColumn("bronze_partition_month", col("month"))
            .withColumn(
                "record_hash",
                expr(
                    "sha2(concat_ws('||', coalesce(activity_id,''), coalesce(application_id,''), coalesce(stage_id,'')), 256)"
                ),
            )
        )

        # Remove raw blobs
        silver_df = silver_df.drop("payload", "metadata")

        return silver_df

    def _load_append(self, silver_df, target_table: str):
        """Load data using append strategy."""
        self.write_to_iceberg(
            silver_df,
            self.config.silver_database_name,
            self.config.silver_table_name,
        )

    def _load_upsert(self, silver_df, target_table: str):
        """Load data using UPSERT strategy."""
        database = self.config.silver_database_name
        table = self.config.silver_table_name
        merge_key = self.entity_config["merge_key"]
        temp_view = f"{table}_temp"

        # Register DataFrame as temp view
        silver_df.createOrReplaceTempView(temp_view)

        # Build MERGE INTO statement
        all_columns = silver_df.columns
        update_cols = [c for c in all_columns if c != merge_key]

        set_clause = ", ".join([f"target.`{col}` = src.`{col}`" for col in update_cols])
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

        logger.info(f"Executing MERGE INTO for {table} (key: {merge_key})")
        logger.debug(merge_sql)
        self.spark.sql(merge_sql)
        logger.info("  ✓ MERGE completed successfully")

    def create_tables(self):
        """Create Silver Iceberg table."""
        entity_config = self.entity_config
        is_partitioned = self.entity_config["partitioned"]

        logger.info(f"Creating Silver table for entity: {self.config.entity_type}")
        logger.info(f"  Partitioning: {is_partitioned}")

        # Drop table if exists and drop_and_recreate is True
        if self.config.drop_and_recreate:
            self.spark.sql(
                f"DROP TABLE IF EXISTS glue_catalog.{self.config.silver_database_name}.{self.config.silver_table_name}"
            )

        # Get entity-specific schema
        silver_schema_ddl = self._get_silver_schema_ddl()

        # Build Iceberg DDL
        silver_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.silver_database_name}.{self.config.silver_table_name} (
          {silver_schema_ddl}
        )
        USING ICEBERG
        """

        # Add partitioning if needed
        if is_partitioned:
            partition_cols = ", ".join(entity_config["partition_cols"])
            silver_table_ddl += f"\n        PARTITIONED BY ({partition_cols})"

        self.spark.sql(silver_table_ddl)
        logger.debug(f"silver_table_ddl: {silver_table_ddl}")
        logger.info(
            f"  ✓ Silver table created: glue_catalog.{self.config.silver_database_name}.{self.config.silver_table_name}"
        )

    def _get_silver_schema_ddl(self) -> str:
        """Get DDL schema for Silver table (columns only)."""
        entity_type = self.config.entity_type

        if entity_type == "candidate_profiles":
            return """
candidate_id STRING NOT NULL,
email STRING,
first_name STRING,
last_name STRING,
phone STRING,
linkedin_url STRING,
source STRING,
tags STRING,
custom_field_values_json STRING,
custom_field_role STRING,
data_quality_score DOUBLE,
is_valid BOOLEAN,
has_complete_name BOOLEAN,
has_phone BOOLEAN,
has_linkedin BOOLEAN,
candidate_created TIMESTAMP,
candidate_updated TIMESTAMP,
bronze_ingestion_timestamp TIMESTAMP,
silver_ingestion_timestamp TIMESTAMP,
source_system STRING,
etl_job_name STRING,
etl_run_id STRING,
etl_run_timestamp TIMESTAMP,
bronze_table_name STRING,
bronze_partition_year INT,
bronze_partition_month INT,
record_hash STRING"""
        elif entity_type == "job_postings":
            return """
job_id STRING NOT NULL,
title STRING,
status STRING,
department_id STRING,
description STRING,
internal_title STRING,
custom_field_values_json STRING,
custom_field_account STRING,
custom_field_employment_type STRING,
custom_field_budget STRING,
custom_fields_json STRING,
data_quality_score DOUBLE,
is_valid BOOLEAN,
created_at TIMESTAMP,
updated_at TIMESTAMP,
bronze_ingestion_timestamp TIMESTAMP,
silver_ingestion_timestamp TIMESTAMP,
source_system STRING,
etl_job_name STRING,
etl_run_id STRING,
etl_run_timestamp TIMESTAMP,
bronze_table_name STRING,
bronze_partition_year INT,
bronze_partition_month INT,
record_hash STRING"""
        elif entity_type == "application_pipeline":
            return """
application_id STRING NOT NULL,
candidate_id STRING,
job_id STRING,
status STRING,
stage_id STRING,
changed_stage_at TIMESTAMP,
rejected_at TIMESTAMP,
sourced BOOLEAN,
referred_by_id STRING,
days_in_current_stage INT,
data_quality_score DOUBLE,
is_valid BOOLEAN,
created_at TIMESTAMP,
updated_at TIMESTAMP,
bronze_ingestion_timestamp TIMESTAMP,
silver_ingestion_timestamp TIMESTAMP,
source_system STRING,
etl_job_name STRING,
etl_run_id STRING,
etl_run_timestamp TIMESTAMP,
bronze_table_name STRING,
bronze_partition_year INT,
bronze_partition_month INT,
record_hash STRING,
year INT,
month INT"""
        elif entity_type == "interview_events":
            return """
interview_id STRING NOT NULL,
application_id STRING,
interview_type STRING,
scheduled_at TIMESTAMP,
status STRING,
interviewer_ids STRING,
location STRING,
note STRING,
data_quality_score DOUBLE,
is_valid BOOLEAN,
is_weekend BOOLEAN,
is_business_hours BOOLEAN,
bronze_ingestion_timestamp TIMESTAMP,
silver_ingestion_timestamp TIMESTAMP,
source_system STRING,
etl_job_name STRING,
etl_run_id STRING,
etl_run_timestamp TIMESTAMP,
bronze_table_name STRING,
bronze_partition_year INT,
bronze_partition_month INT,
record_hash STRING,
year INT,
month INT"""
        elif entity_type == "recruiter_performance":
            return """
user_id STRING NOT NULL,
email STRING,
first_name STRING,
last_name STRING,
role STRING,
department_id STRING,
data_quality_score DOUBLE,
is_valid BOOLEAN,
bronze_ingestion_timestamp TIMESTAMP,
silver_ingestion_timestamp TIMESTAMP,
source_system STRING,
etl_job_name STRING,
etl_run_id STRING,
etl_run_timestamp TIMESTAMP,
bronze_table_name STRING,
bronze_partition_year INT,
bronze_partition_month INT,
record_hash STRING"""
        elif entity_type == "candidate_nps":
            return """
nps_response_id STRING NOT NULL,
candidate_id STRING,
score INT,
comment STRING,
nps_category STRING,
created_at TIMESTAMP,
updated_at TIMESTAMP,
data_quality_score DOUBLE,
is_valid BOOLEAN,
bronze_ingestion_timestamp TIMESTAMP,
silver_ingestion_timestamp TIMESTAMP,
source_system STRING,
etl_job_name STRING,
etl_run_id STRING,
etl_run_timestamp TIMESTAMP,
bronze_table_name STRING,
bronze_partition_year INT,
bronze_partition_month INT,
record_hash STRING,
year INT,
month INT"""
        elif entity_type == "application_stage_transitions":
            return """
activity_id STRING NOT NULL,
application_id STRING,
candidate_id STRING,
stage_id STRING,
previous_stage_id STRING,
activity_type STRING,
transition_type STRING,
created_at TIMESTAMP,
updated_at TIMESTAMP,
data_quality_score DOUBLE,
is_valid BOOLEAN,
bronze_ingestion_timestamp TIMESTAMP,
silver_ingestion_timestamp TIMESTAMP,
source_system STRING,
etl_job_name STRING,
etl_run_id STRING,
etl_run_timestamp TIMESTAMP,
bronze_table_name STRING,
bronze_partition_year INT,
bronze_partition_month INT,
record_hash STRING,
year INT,
month INT"""
        else:
            raise ValueError(f"Unknown entity_type: {entity_type}")


# ==========================================
# MAIN EXECUTION (HYBRID LOCAL/GLUE)
# ==========================================
if __name__ == "__main__":
    try:
        setup_logging()
        logger.info("Loading Team Tailor Silver configuration...")
        config = TeamTailorSilverConfig.from_args()

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
            silver_job = TeamTailorSilverJob(spark, config)
        else:
            silver_job = TeamTailorSilverJob(context["glue_context"], config)

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
