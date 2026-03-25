"""
ClickUp Gold Pipeline Job - Technology KPIs

This job processes ClickUp data from silver to gold layer with support for both
local development (LocalStack + Derby) and AWS Glue (GlueContext + Glue Data Catalog).

Features:
- ✅ Hybrid support: Works locally with LocalStack and in AWS Glue
- ✅ AWS Glue v5 DataFrame-first approach
- ✅ GoldJobBase with encapsulated infrastructure setup
- ✅ Gold layer processing: silver → gold
- ✅ Pydantic-based configuration with automatic parameter resolution
- ✅ Technology-focused KPIs: 6 specialized analytics tables
- ✅ Pre-calculated KPIs: request metrics, satisfaction, throughput, status flow, team performance, stakeholder metrics

Local Usage (auto-detected, no --JOB_NAME):
  python jobs/gold/clickup_gold_job.py --ENTITY_TYPE technology_request_metrics

AWS Glue Usage (auto-detected with --JOB_NAME):
  python jobs/gold/clickup_gold_job.py --JOB_NAME clickup_gold_pipeline --ENTITY_TYPE technology_request_metrics

Minimal Usage (using defaults):
  python jobs/gold/clickup_gold_job.py --ENTITY_TYPE technology_request_metrics

Full Usage (overriding defaults):
  python jobs/gold/clickup_gold_job.py \\
    --JOB_NAME clickup_gold_pipeline \\
    --ENTITY_TYPE technology_request_metrics \\
    --SILVER_DATABASE_NAME nan_develop_silver_analytics \\
    --GOLD_DATABASE_NAME nan_develop_gold_analytics \\
    --WAREHOUSE_PATH s3://bucket/nan/

Required Parameters:
- ENTITY_TYPE: Gold entity type (technology_request_metrics, technology_satisfaction, etc.)

Optional Parameters (with ClickUp defaults):
- SILVER_DATABASE_NAME: Silver layer database name (default: from environment)
- GOLD_DATABASE_NAME: Gold layer database name (default: from environment)
- WAREHOUSE_PATH: S3 location for Iceberg warehouse (default: from environment)
- CREATE_TABLES: Whether to create tables (default: true)
- SHOW_COUNTS: Whether to show record counts (default: true)
- LOG_LEVEL: Logging level (default: INFO)
"""

import logging
from typing import ClassVar, Optional

from pydantic import Field

from libs.common import GoldJobConfig, setup_logging
from libs.pyspark import GoldJobBase, SessionConfig, SparkSessionFactory

# Configure logging
logger = logging.getLogger(__name__)

# ==========================================
# CLICKUP GOLD ENTITIES CONFIGURATION
# ==========================================

CLICKUP_GOLD_ENTITIES = {
    "technology_request_metrics": {
        "silver_tables": ["technology_dim_requests"],
        "partition_cols": [
            "analysis_date",
            "process_name",
            "stakeholder_area",
            "team",
            "priority",
        ],
        "description": "Aggregated request metrics by period, process, account, and architecture responsibility domain",
        "domain": "technology",
    },
    "technology_satisfaction": {
        "silver_tables": ["technology_dim_requests"],
        "partition_cols": ["analysis_date", "process_name"],
        "description": "Satisfaction metrics (CSAT/NPS) by process, account, and architecture responsibility domain",
        "domain": "technology",
    },
    "technology_throughput": {
        "silver_tables": ["technology_dim_requests", "technology_fact_request_events"],
        "partition_cols": [
            "analysis_date",
            "process_name",
        ],
        "description": "Throughput and processing velocity by process and account",
        "domain": "technology",
    },
    "technology_request_status_flow": {
        "silver_tables": ["technology_dim_requests", "technology_fact_request_events"],
        "partition_cols": [
            "analysis_date",
            "process_name",
            "list_name",
            "team",
        ],
        "description": "Status flow and transitions by process",
        "domain": "technology",
    },
    "technology_team_performance": {
        "silver_tables": ["technology_dim_requests", "technology_dim_teams"],
        "partition_cols": ["analysis_date", "process_name", "team"],
        "description": "Performance by team, process, account, and architecture responsibility domain",
        "domain": "technology",
    },
    "technology_stakeholder_metrics": {
        "silver_tables": [
            "technology_dim_requests",
            "technology_dim_stakeholder_areas",
        ],
        "partition_cols": [
            "analysis_date",
            "process_name",
        ],
        "description": "Metrics by stakeholder area, process, account, and architecture responsibility domain",
        "domain": "technology",
    },
    "technology_process_metrics": {
        "silver_tables": ["technology_dim_requests"],
        "partition_cols": ["analysis_date", "process_name"],
        "description": "Metrics by Technology process (Architecture, Studios, Modernization & Infrastructure)",
        "domain": "technology",
    },
    "technology_account_metrics": {
        "silver_tables": ["technology_dim_requests"],
        "partition_cols": ["analysis_date", "account"],
        "description": "Metrics by Account/Client",
        "domain": "technology",
    },
    "technology_rd_practices_metrics": {
        "silver_tables": ["technology_dim_requests"],
        "partition_cols": ["analysis_date", "rd_practice"],
        "description": "Metrics by R&D Practices (Studios process only)",
        "domain": "technology",
    },
    "technology_process_nps": {
        "silver_tables": ["technology_dim_requests"],
        "partition_cols": ["analysis_date", "process_name"],
        "description": "Accurate NPS metrics by process (1 task = 1 vote, no explode inflation)",
        "domain": "technology",
    },
}

# ==========================================
# CLICKUP GOLD CONFIGURATION
# ==========================================


class ClickUpGoldConfig(GoldJobConfig):
    """
    ClickUp-specific Gold configuration.

    Extends GoldJobConfig with ClickUp-specific fields.
    Auto-configures gold_table_name based on entity_type.
    """

    # Source name with default value
    source_name: ClassVar[str] = "clickup"

    # Required fields - ConfigBase resolves: Workflow → CLI → ENV → Error
    entity_type: str = Field(
        ...,
        description="ClickUp Gold entity type (technology_request_metrics, technology_satisfaction, etc.)",
    )

    # Auto-configured fields
    gold_table_name: Optional[str] = Field(
        default=None, description="Gold table name (auto-configured if not provided)"
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure table names based on entity_type."""
        # Validate entity_type
        if self.entity_type not in CLICKUP_GOLD_ENTITIES:
            valid_entities = ", ".join(CLICKUP_GOLD_ENTITIES.keys())
            raise ValueError(
                f"Invalid entity_type '{self.entity_type}'. "
                f"Valid options: {valid_entities}"
            )

        # Auto-configure gold_table_name (target) based on entity_type
        if not self.gold_table_name:
            self.gold_table_name = f"{self.entity_type}_gold"


# ==========================================
# CLICKUP GOLD JOB
# ==========================================


class ClickUpGoldJob(GoldJobBase):
    """
    ClickUp Gold Pipeline Job.

    Processes ClickUp data from Silver to Gold layer with specialized analytics tables
    for Technology KPIs: request metrics, satisfaction, throughput, status flow,
    team performance, and stakeholder metrics.

    Supports 3 Technology processes:
    - Architecture (list_id: 900201093820)
    - Studios / Learning & R&D (list_id: 901703323552)
    - Modernization & Infrastructure (list_id: 901702911170)
    """

    # Mapping of ClickUp list IDs to Technology process names
    LIST_ID_TO_PROCESS = {
        "900201093820": "Architecture",
        "901703323552": "Studios",  # Learning & R&D
        "901702911170": "Modernization & Infrastructure",
    }

    def __init__(self, spark_or_glue_context, config: ClickUpGoldConfig):
        """
        Initialize ClickUp Gold Job with Pydantic configuration.

        Args:
            spark_or_glue_context: SparkSession (local) or GlueContext (Glue)
            config: Validated ClickUpGoldConfig instance (all parameters auto-resolved)
        """
        # Validate entity_type before initializing
        if config.entity_type not in CLICKUP_GOLD_ENTITIES:
            valid_entities = ", ".join(CLICKUP_GOLD_ENTITIES.keys())
            raise ValueError(
                f"Invalid entity_type '{config.entity_type}'. "
                f"Valid options: {valid_entities}"
            )

        super().__init__(spark_or_glue_context, config)
        self.entity_type = config.entity_type
        self.entity_config = CLICKUP_GOLD_ENTITIES[self.entity_type]

        logger.info(f"Initialized ClickUp Gold job for entity: {config.entity_type}")

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

        # For most ClickUp gold tables, we primarily use technology_dim_requests
        # Join with other tables as needed
        primary_table = silver_tables[0]
        primary_df = table_dfs[primary_table]

        # Join with additional tables if needed
        if len(silver_tables) > 1:
            for table_name in silver_tables[1:]:
                logger.info(f"Joining with silver table: {table_name}")
                join_df = table_dfs[table_name]

                # Determine join key
                join_key = self._determine_join_key(
                    primary_df, join_df, primary_table, table_name
                )

                if (
                    join_key
                    and join_key in primary_df.columns
                    and join_key in join_df.columns
                ):
                    # Remove ambiguous columns from join_df if they exist in primary table
                    # We only want these columns from the primary table: list_id, priority, resolution_days, status
                    ambiguous_columns = [
                        "list_id",
                        "priority",
                        "resolution_days",
                        "status",
                    ]
                    join_columns = [
                        c for c in join_df.columns if c not in ambiguous_columns
                    ]
                    removed_cols = [
                        c for c in ambiguous_columns if c in join_df.columns
                    ]
                    if removed_cols:
                        logger.debug(
                            f"  Removing {removed_cols} from {table_name} to avoid ambiguity"
                        )
                        join_df = join_df.select(*join_columns)

                    # Simple inner join on request_id
                    primary_df = primary_df.join(join_df, on=join_key, how="left")
                    logger.info(f"  Joined on key: {join_key}")
                else:
                    logger.warning(
                        f"  Could not determine join key, skipping join with {table_name}"
                    )

        # Map list_id to process_name
        primary_df = self._map_list_id_to_process(primary_df)

        logger.info(f"Extracted data with {primary_df.count()} records")
        return primary_df

    def _determine_join_key(self, df1, df2, table1_name, table2_name):
        """Determine join key between two DataFrames."""
        # Common join keys for ClickUp tables
        common_keys = ["request_id", "task_id"]
        for key in common_keys:
            if key in df1.columns and key in df2.columns:
                return key
        return None

    def _map_list_id_to_process(self, df):
        """
        Map list_id to process_name using LIST_ID_TO_PROCESS mapping.

        Args:
            df: DataFrame with list_id column (should be unambiguous after removing it from joined tables)

        Returns:
            DataFrame with process_name column added
        """
        from pyspark.sql.functions import col, when

        # After removing list_id from joined tables in extract(), there should be no ambiguity
        list_id_col = col("list_id")

        # Create mapping expression
        process_expr = list_id_col
        for list_id, process_name in self.LIST_ID_TO_PROCESS.items():
            process_expr = when(list_id_col == list_id, process_name).otherwise(
                process_expr
            )
        process_expr = when(
            process_expr.isin(list(self.LIST_ID_TO_PROCESS.values())), process_expr
        ).otherwise("Unknown")

        return df.withColumn("process_name", process_expr)

    def transform(self, df):
        """
        Transform silver data to gold format with KPI calculations.

        Dispatches to entity-specific transformation methods.
        """
        entity_type = self.entity_type
        logger.info(f"Transforming {entity_type} data...")

        if df.count() == 0:
            logger.warning("Empty DataFrame, skipping transformation")
            return df

        # Dispatch to entity-specific transformation
        if entity_type == "technology_request_metrics":
            return self._transform_request_metrics(df)
        elif entity_type == "technology_satisfaction":
            return self._transform_satisfaction(df)
        elif entity_type == "technology_throughput":
            return self._transform_throughput(df)
        elif entity_type == "technology_request_status_flow":
            return self._transform_status_flow(df)
        elif entity_type == "technology_team_performance":
            return self._transform_team_performance(df)
        elif entity_type == "technology_stakeholder_metrics":
            return self._transform_stakeholder_metrics(df)
        elif entity_type == "technology_process_metrics":
            return self._transform_process_metrics(df)
        elif entity_type == "technology_account_metrics":
            return self._transform_account_metrics(df)
        elif entity_type == "technology_rd_practices_metrics":
            return self._transform_rd_practices_metrics(df)
        elif entity_type == "technology_process_nps":
            return self._transform_process_nps(df)
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")

    def _transform_request_metrics(self, df):
        """Transform to request metrics gold table."""
        from pyspark.sql.functions import (
            array,
            avg,
            col,
            count,
            current_date,
            explode,
            lit,
            lower,
            split,
        )
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import trim, when

        logger.info("Transforming to technology_request_metrics_gold")

        # Aggregate by stakeholder_area, team, priority, process_name, account, architecture_responsibility_domain
        # Split comma-separated strings and explode (handle null/empty strings safely)

        df_exploded = df.select(
            explode(
                when(
                    (col("stakeholder_areas").isNotNull())
                    & (col("stakeholder_areas") != ""),
                    split(trim(col("stakeholder_areas")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("stakeholder_area"),
            explode(
                when(
                    (col("team_members").isNotNull()) & (col("team_members") != ""),
                    split(trim(col("team_members")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("team"),
            explode(
                when(
                    (col("account").isNotNull()) & (col("account") != ""),
                    split(trim(col("account")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("account"),
            explode(
                when(
                    (col("architecture_responsibility_domain").isNotNull())
                    & (col("architecture_responsibility_domain") != ""),
                    split(trim(col("architecture_responsibility_domain")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("architecture_responsibility_domain"),
            col("priority"),
            col("process_name"),
            col("list_id").alias("list_name"),
            col("status"),
            col("resolution_days"),
            col("sla_compliant"),
            col("time_logged_hours"),
            col("time_estimate_hours"),
            col("external_satisfaction_score"),
            col("internal_satisfaction_score"),
            col("completeness_score"),
            col("urgency_score"),
            col("date_created"),
            col("year"),
            col("month"),
        ).filter(
            (col("stakeholder_area").isNotNull() & (col("stakeholder_area") != ""))
            | (col("team").isNotNull() & (col("team") != ""))
        )

        # Aggregate metrics by multiple dimensions including process_name and account
        from pyspark.sql.functions import percentile_approx, stddev

        # Define resolved status values (using lower() for case-insensitive matching)
        # Business rule: DONE = closed, ready to communicate, wont do
        resolved_statuses = ["closed", "ready to communicate", "wont do"]

        metrics_df = df_exploded.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("stakeholder_area"),
            col("team"),
            col("priority"),
            col("account"),
            col("architecture_responsibility_domain"),
            col("list_name"),
        ).agg(
            count("*").alias("request_count"),
            spark_sum(
                when(lower(col("status")).isin(resolved_statuses), 1).otherwise(0)
            ).alias("resolved_count"),
            avg(col("resolution_days")).alias("avg_resolution_days"),
            stddev(col("resolution_days")).alias("stddev_resolution_days"),
            percentile_approx(col("resolution_days"), 0.5).alias("p50_resolution_days"),
            percentile_approx(col("resolution_days"), 0.75).alias(
                "p75_resolution_days"
            ),
            percentile_approx(col("resolution_days"), 0.95).alias(
                "p95_resolution_days"
            ),
            avg(when(col("sla_compliant"), 1.0).otherwise(0.0)).alias(
                "sla_compliance_rate"
            ),
            avg(col("time_logged_hours")).alias("avg_time_logged_hours"),
            avg(col("time_estimate_hours")).alias("avg_time_estimate_hours"),
            avg(
                when(
                    (col("time_estimate_hours").isNotNull())
                    & (col("time_logged_hours").isNotNull())
                    & (col("time_estimate_hours") > 0),
                    col("time_logged_hours") / col("time_estimate_hours"),
                ).otherwise(None)
            ).alias("estimate_accuracy"),
            avg(col("external_satisfaction_score")).alias(
                "avg_external_satisfaction_score"
            ),
            avg(col("internal_satisfaction_score")).alias(
                "avg_internal_satisfaction_score"
            ),
            percentile_approx(col("external_satisfaction_score"), 0.5).alias(
                "median_external_satisfaction_score"
            ),
            percentile_approx(col("internal_satisfaction_score"), 0.5).alias(
                "median_internal_satisfaction_score"
            ),
            avg(col("completeness_score")).alias("avg_completeness_score"),
            avg(col("urgency_score")).alias("avg_urgency_score"),
        )

        logger.info(f"Transformed to request metrics with {metrics_df.count()} records")
        return metrics_df

    def _transform_satisfaction(self, df):
        """Transform to satisfaction gold table."""
        from pyspark.sql.functions import (
            array,
            avg,
            col,
            count,
            countDistinct,
            current_date,
            explode,
            lit,
            split,
        )
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import trim, when

        logger.info("Transforming to technology_satisfaction_gold")

        # Explode stakeholder_areas, team_members, account, architecture_responsibility_domain
        # Handle null/empty arrays safely

        df_exploded = df.select(
            col("request_id"),  # Keep request_id to count unique tasks
            explode(
                when(
                    (col("stakeholder_areas").isNotNull())
                    & (col("stakeholder_areas") != ""),
                    split(trim(col("stakeholder_areas")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("stakeholder_area"),
            explode(
                when(
                    (col("team_members").isNotNull()) & (col("team_members") != ""),
                    split(trim(col("team_members")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("team"),
            explode(
                when(
                    (col("account").isNotNull()) & (col("account") != ""),
                    split(trim(col("account")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("account"),
            explode(
                when(
                    (col("architecture_responsibility_domain").isNotNull())
                    & (col("architecture_responsibility_domain") != ""),
                    split(trim(col("architecture_responsibility_domain")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("architecture_responsibility_domain"),
            col("process_name"),
            col("list_id").alias("list_name"),
            col("external_satisfaction_score"),
            col("internal_satisfaction_score"),
            col("external_satisfaction_value"),
            col("internal_satisfaction_value"),
        ).filter(
            (
                (col("external_satisfaction_score").isNotNull())
                | (col("internal_satisfaction_score").isNotNull())
            )
            & (
                (col("stakeholder_area").isNotNull() & (col("stakeholder_area") != ""))
                | (col("team").isNotNull() & (col("team") != ""))
            )
        )

        # Calculate NPS segments
        df_with_segments = (
            df_exploded.withColumn(
                "external_promoter",
                when(
                    (col("external_satisfaction_score") >= 9)
                    & (col("external_satisfaction_score") <= 10),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "external_passive",
                when(
                    (col("external_satisfaction_score") >= 7)
                    & (col("external_satisfaction_score") <= 8),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "external_detractor",
                # Detractors: scores 2-6 (minimum valid score is 2, not 0)
                # NULL scores are not counted as detractors
                when(
                    (col("external_satisfaction_score").isNotNull())
                    & (col("external_satisfaction_score") <= 6),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "internal_promoter",
                when(
                    (col("internal_satisfaction_score") >= 9)
                    & (col("internal_satisfaction_score") <= 10),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "internal_passive",
                when(
                    (col("internal_satisfaction_score") >= 7)
                    & (col("internal_satisfaction_score") <= 8),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "internal_detractor",
                # Detractors: scores 2-6 (minimum valid score is 2, not 0)
                # NULL scores are not counted as detractors
                when(
                    (col("internal_satisfaction_score").isNotNull())
                    & (col("internal_satisfaction_score") <= 6),
                    1,
                ).otherwise(0),
            )
        )

        # Aggregate satisfaction metrics by process_name, account, and architecture_responsibility_domain
        # Count unique tasks (request_id) for each NPS segment to avoid inflation from explode
        satisfaction_df = df_with_segments.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("stakeholder_area"),
            col("team"),
            col("account"),
            col("architecture_responsibility_domain"),
            col("list_name"),
        ).agg(
            avg(col("external_satisfaction_score")).alias(
                "external_satisfaction_score"
            ),
            avg(col("internal_satisfaction_score")).alias(
                "internal_satisfaction_score"
            ),
            # Count unique tasks per NPS segment (not rows from explode)
            countDistinct(
                when(col("external_promoter") == 1, col("request_id"))
            ).alias("external_promoters_count"),
            countDistinct(
                when(col("external_passive") == 1, col("request_id"))
            ).alias("external_passives_count"),
            countDistinct(
                when(col("external_detractor") == 1, col("request_id"))
            ).alias("external_detractors_count"),
            countDistinct(
                when(col("internal_promoter") == 1, col("request_id"))
            ).alias("internal_promoters_count"),
            countDistinct(
                when(col("internal_passive") == 1, col("request_id"))
            ).alias("internal_passives_count"),
            countDistinct(
                when(col("internal_detractor") == 1, col("request_id"))
            ).alias("internal_detractors_count"),
            countDistinct(col("request_id")).alias("satisfaction_response_count"),
        )

        # Calculate NPS scores
        satisfaction_df = satisfaction_df.withColumn(
            "external_nps_score",
            when(
                col("satisfaction_response_count") > 0,
                (
                    (col("external_promoters_count") - col("external_detractors_count"))
                    / col("satisfaction_response_count")
                )
                * 100,
            ).otherwise(None),
        ).withColumn(
            "internal_nps_score",
            when(
                col("satisfaction_response_count") > 0,
                (
                    (col("internal_promoters_count") - col("internal_detractors_count"))
                    / col("satisfaction_response_count")
                )
                * 100,
            ).otherwise(None),
        )

        logger.info(
            f"Transformed to satisfaction metrics with {satisfaction_df.count()} records"
        )
        return satisfaction_df

    def _transform_throughput(self, df):
        """Transform to throughput gold table."""
        from pyspark.sql.functions import array, col, explode, lit, split
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import to_date, trim, when

        logger.info("Transforming to technology_throughput_gold")

        # Read fact table for events
        fact_table = f"glue_catalog.{self.config.silver_database_name}.technology_fact_request_events"
        events_df = self.spark.sql(f"SELECT * FROM {fact_table}")

        # Explode stakeholder_areas, team_members, account, architecture_responsibility_domain from requests
        # Use request_id from primary table (technology_dim_requests uses request_id, not task_id)
        requests_exploded = df.select(
            col("request_id"),
            col("list_id").alias("list_name"),
            explode(
                when(
                    (col("stakeholder_areas").isNotNull())
                    & (col("stakeholder_areas") != ""),
                    split(trim(col("stakeholder_areas")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("stakeholder_area"),
            explode(
                when(
                    (col("team_members").isNotNull()) & (col("team_members") != ""),
                    split(trim(col("team_members")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("team"),
            explode(
                when(
                    (col("account").isNotNull()) & (col("account") != ""),
                    split(trim(col("account")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("account"),
            explode(
                when(
                    (col("architecture_responsibility_domain").isNotNull())
                    & (col("architecture_responsibility_domain") != ""),
                    split(trim(col("architecture_responsibility_domain")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("architecture_responsibility_domain"),
            col("process_name"),
            col("date_created"),
            col("date_closed"),
            col("resolution_days"),
            col("status"),
        ).filter(
            (col("stakeholder_area").isNotNull() & (col("stakeholder_area") != ""))
            | (col("team").isNotNull() & (col("team") != ""))
        )

        # Join with events - use qualified column names to avoid ambiguity
        throughput_df = requests_exploded.join(
            events_df.select(
                col("request_id"),
                col("event_date"),
                col("event_type"),
            ),
            on="request_id",
            how="left",
        )

        # Aggregate by day, process_name, account
        # Include list_name and architecture_responsibility_domain from requests_exploded
        # Use coalesce to default to date_created when event_date is NULL
        from pyspark.sql.functions import coalesce, current_date

        throughput_agg = throughput_df.groupBy(
            coalesce(
                to_date(col("event_date")),
                to_date(col("date_created")),
                current_date(),
            ).alias("analysis_date"),
            col("process_name"),
            col("list_name"),
            col("stakeholder_area"),
            col("team"),
            col("account"),
            col("architecture_responsibility_domain"),
        ).agg(
            spark_sum(when(col("event_type") == "created", 1).otherwise(0)).alias(
                "requests_created"
            ),
            spark_sum(when(col("date_closed").isNotNull(), 1).otherwise(0)).alias(
                "requests_resolved"
            ),
        )

        # Add period_start and period_end (same as analysis_date for daily aggregation)
        # Calculate throughput rate (resolved per day)
        throughput_final = (
            throughput_agg.withColumn("period_start", col("analysis_date"))
            .withColumn("period_end", col("analysis_date"))
            .withColumn("throughput_rate", col("requests_resolved"))
        )

        logger.info(
            f"Transformed to throughput metrics with {throughput_final.count()} records"
        )
        return throughput_final

    def _transform_status_flow(self, df):
        """Transform to status flow gold table."""
        from pyspark.sql.functions import (
            array,
            avg,
            col,
            count,
            current_date,
            datediff,
            explode,
            lag,
            lit,
            split,
            trim,
            when,
        )
        from pyspark.sql.window import Window

        logger.info("Transforming to technology_request_status_flow_gold")

        # Read fact table for events
        fact_table = f"glue_catalog.{self.config.silver_database_name}.technology_fact_request_events"
        events_df = self.spark.sql(f"SELECT * FROM {fact_table}")

        # Explode team_members, stakeholder_areas, account, architecture_responsibility_domain
        # Use request_id from primary table (technology_dim_requests uses request_id, not task_id)
        requests_exploded = df.select(
            col("request_id"),
            col("list_id").alias("list_name"),
            col("process_name"),
            explode(
                when(
                    (col("team_members").isNotNull()) & (col("team_members") != ""),
                    split(trim(col("team_members")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("team"),
            explode(
                when(
                    (col("stakeholder_areas").isNotNull())
                    & (col("stakeholder_areas") != ""),
                    split(trim(col("stakeholder_areas")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("stakeholder_area"),
            explode(
                when(
                    (col("account").isNotNull()) & (col("account") != ""),
                    split(trim(col("account")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("account"),
            explode(
                when(
                    (col("architecture_responsibility_domain").isNotNull())
                    & (col("architecture_responsibility_domain") != ""),
                    split(trim(col("architecture_responsibility_domain")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("architecture_responsibility_domain"),
        ).filter(col("team").isNotNull() & (col("team") != ""))

        # Join with events and order by event_date to track transitions
        status_flow_df = requests_exploded.join(
            events_df.select(
                col("request_id"),
                col("event_date"),
                col("status"),
            ),
            on="request_id",
            how="left",
        )

        # Use window function to get previous status (from_status) and current status (to_status)
        window_spec = Window.partitionBy("request_id").orderBy("event_date")
        status_transitions = (
            status_flow_df.withColumn(
                "from_status", lag(col("status"), 1).over(window_spec)
            )
            .withColumn("to_status", col("status"))
            .withColumn("prev_event_date", lag(col("event_date"), 1).over(window_spec))
            .filter(
                col("from_status").isNotNull()
                & (col("from_status") != col("to_status"))
            )
            .withColumn(
                "transition_days",
                when(
                    col("prev_event_date").isNotNull(),
                    datediff(col("event_date"), col("prev_event_date")),
                ).otherwise(None),
            )
        )

        # Aggregate transitions by process_name, team, etc.
        status_agg = status_transitions.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("list_name"),
            col("team"),
            col("stakeholder_area"),
            col("account"),
            col("architecture_responsibility_domain"),
            col("from_status"),
            col("to_status"),
        ).agg(
            count("*").alias("transition_count"),
            avg(col("transition_days")).alias("avg_transition_days"),
        )

        logger.info(
            f"Transformed to status flow metrics with {status_agg.count()} records"
        )
        return status_agg

    def _transform_team_performance(self, df):
        """Transform to team performance gold table."""
        from pyspark.sql.functions import (
            array,
            avg,
            coalesce,
            col,
            count,
            current_date,
            explode,
            lit,
            lower,
            split,
        )
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import trim, when

        logger.info("Transforming to technology_team_performance_gold")

        # Explode stakeholder_areas, team_members, account, architecture_responsibility_domain
        # Note: Some lists may not have all custom fields, so we handle NULLs gracefully
        team_df = df.select(
            explode(
                when(
                    (col("team_members").isNotNull()) & (col("team_members") != ""),
                    split(trim(col("team_members")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("team"),
            explode(
                when(
                    (col("stakeholder_areas").isNotNull())
                    & (col("stakeholder_areas") != ""),
                    split(trim(col("stakeholder_areas")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("stakeholder_area"),
            explode(
                when(
                    (col("account").isNotNull()) & (col("account") != ""),
                    split(trim(col("account")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("account"),
            explode(
                when(
                    (col("architecture_responsibility_domain").isNotNull())
                    & (col("architecture_responsibility_domain") != ""),
                    split(trim(col("architecture_responsibility_domain")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("architecture_responsibility_domain"),
            col("process_name"),
            col("list_id").alias("list_name"),
            col("status"),
            col("resolution_days"),
            col("external_satisfaction_score"),
            col("internal_satisfaction_score"),
            col("time_logged_hours"),
            col("time_estimate_hours"),
            col("sla_compliant"),
        ).filter(
            (
                # Only filter by team (required), allow other fields to be NULL
                col("team").isNotNull() & (col("team") != "")
            )
        )

        # Aggregate team performance by process_name, account, architecture_responsibility_domain
        # Include stakeholder_area in groupBy but allow NULLs
        team_perf = team_df.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("team"),
            col("stakeholder_area"),  # Allow NULLs - not all lists have this field
            col("account"),
            col("architecture_responsibility_domain"),
            col("list_name"),
        ).agg(
            count("*").alias("total_requests"),
            spark_sum(
                when(lower(col("status")).isin(["closed", "ready to communicate", "wont do"]), 1).otherwise(0)
            ).alias("resolved_requests"),
            avg(col("resolution_days")).alias("avg_resolution_days"),
            avg(when(col("sla_compliant"), 1.0).otherwise(0.0)).alias(
                "sla_compliance_rate"
            ),
            avg(
                coalesce(
                    col("external_satisfaction_score"),
                    col("internal_satisfaction_score"),
                )
            ).alias("avg_satisfaction_score"),
        )

        logger.info(
            f"Transformed to team performance metrics with {team_perf.count()} records"
        )
        return team_perf

    def _transform_stakeholder_metrics(self, df):
        """Transform to stakeholder metrics gold table."""
        from pyspark.sql.functions import (
            array,
            avg,
            coalesce,
            col,
            count,
            current_date,
            explode,
            lit,
            split,
            trim,
            lower,
            when,
        )

        logger.info("Transforming to technology_stakeholder_metrics_gold")

        from pyspark.sql.functions import sum as spark_sum

        # Explode stakeholder_areas, account, architecture_responsibility_domain
        stakeholder_df = df.select(
            explode(
                when(
                    (col("stakeholder_areas").isNotNull())
                    & (col("stakeholder_areas") != ""),
                    split(trim(col("stakeholder_areas")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("stakeholder_area"),
            explode(
                when(
                    (col("account").isNotNull()) & (col("account") != ""),
                    split(trim(col("account")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("account"),
            explode(
                when(
                    (col("architecture_responsibility_domain").isNotNull())
                    & (col("architecture_responsibility_domain") != ""),
                    split(trim(col("architecture_responsibility_domain")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("architecture_responsibility_domain"),
            col("process_name"),
            col("list_id").alias("list_name"),
            col("priority"),
            col("status"),
            col("resolution_days"),
            col("external_satisfaction_score"),
            col("internal_satisfaction_score"),
            explode(
                when(
                    (col("team_members").isNotNull()) & (col("team_members") != ""),
                    split(trim(col("team_members")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("team"),
        ).filter(col("stakeholder_area").isNotNull() & (col("stakeholder_area") != ""))

        # Aggregate stakeholder metrics by process_name, account, architecture_responsibility_domain
        stakeholder_metrics = stakeholder_df.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("stakeholder_area"),
            col("account"),
            col("architecture_responsibility_domain"),
            col("list_name"),
        ).agg(
            count("*").alias("total_requests"),
            spark_sum(
                when(lower(col("status")).isin(["closed", "ready to communicate", "wont do"]), 1).otherwise(0)
            ).alias("resolved_requests"),
            avg(col("resolution_days")).alias("avg_resolution_days"),
            avg(
                coalesce(
                    col("external_satisfaction_score"),
                    col("internal_satisfaction_score"),
                )
            ).alias("avg_satisfaction_score"),
        )

        logger.info(
            f"Transformed to stakeholder metrics with {stakeholder_metrics.count()} records"
        )
        return stakeholder_metrics

    def _transform_process_metrics(self, df):
        """Transform to process metrics gold table (Architecture, Studios, Modernization & Infrastructure)."""
        from pyspark.sql.functions import avg, coalesce, col, count, current_date, lower
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        logger.info("Transforming to technology_process_metrics_gold")

        # Aggregate metrics by process_name
        process_metrics = df.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("list_id").alias("list_name"),
        ).agg(
            count("*").alias("total_requests"),
            spark_sum(
                when(lower(col("status")).isin(["closed", "ready to communicate", "wont do"]), 1).otherwise(0)
            ).alias("resolved_requests"),
            avg(col("resolution_days")).alias("avg_resolution_days"),
            avg(when(col("sla_compliant"), 1.0).otherwise(0.0)).alias(
                "sla_compliance_rate"
            ),
            avg(
                coalesce(
                    col("external_satisfaction_score"),
                    col("internal_satisfaction_score"),
                )
            ).alias("avg_satisfaction_score"),
        )

        logger.info(
            f"Transformed to process metrics with {process_metrics.count()} records"
        )
        return process_metrics

    def _transform_account_metrics(self, df):
        """Transform to account metrics gold table."""
        from pyspark.sql.functions import (
            array,
            avg,
            coalesce,
            col,
            count,
            current_date,
            explode,
            lit,
            lower,
            split,
        )
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import trim, when

        logger.info("Transforming to technology_account_metrics_gold")

        # Explode account array
        account_df = df.select(
            explode(
                when(
                    (col("account").isNotNull()) & (col("account") != ""),
                    split(trim(col("account")), ",\\s*"),
                ).otherwise(array(lit(None)).cast("array<string>"))
            ).alias("account"),
            col("process_name"),
            col("list_id").alias("list_name"),
            col("status"),
            col("resolution_days"),
            col("external_satisfaction_score"),
            col("internal_satisfaction_score"),
            col("sla_compliant"),
        ).filter(col("account").isNotNull() & (col("account") != ""))

        # Aggregate metrics by account and process
        account_metrics = account_df.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("account"),
            col("list_name"),
        ).agg(
            count("*").alias("total_requests"),
            spark_sum(
                when(lower(col("status")).isin(["closed", "ready to communicate", "wont do"]), 1).otherwise(0)
            ).alias("resolved_requests"),
            avg(col("resolution_days")).alias("avg_resolution_days"),
            avg(
                coalesce(
                    col("external_satisfaction_score"),
                    col("internal_satisfaction_score"),
                )
            ).alias("avg_satisfaction_score"),
        )

        logger.info(
            f"Transformed to account metrics with {account_metrics.count()} records"
        )
        return account_metrics

    def _transform_rd_practices_metrics(self, df):
        """Transform to R&D Practices metrics gold table (Studios process only)."""
        from pyspark.sql.functions import (
            array,
            avg,
            col,
            count,
            current_date,
            explode,
            lit,
            lower,
            split,
        )
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import trim, when

        logger.info("Transforming to technology_rd_practices_metrics_gold")

        # Filter only Studios process and explode rd_practices
        rd_practices_df = (
            df.filter(col("process_name") == "Studios")
            .select(
                explode(
                    when(
                        (col("rd_practices").isNotNull()) & (col("rd_practices") != ""),
                        split(trim(col("rd_practices")), ",\\s*"),
                    ).otherwise(array(lit(None)).cast("array<string>"))
                ).alias("rd_practice"),
                col("process_name"),
                col("list_id").alias("list_name"),
                col("status"),
                col("resolution_days"),
                col("external_satisfaction_score"),
                col("internal_satisfaction_score"),
                col("time_logged_hours"),
                col("time_estimate_hours"),
                col("sla_compliant"),
            )
            .filter(col("rd_practice").isNotNull() & (col("rd_practice") != ""))
        )

        # Aggregate metrics by R&D practice
        rd_practices_metrics = rd_practices_df.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("rd_practice"),
            col("list_name"),
        ).agg(
            count("*").alias("total_requests"),
            spark_sum(
                when(lower(col("status")).isin(["closed", "ready to communicate", "wont do"]), 1).otherwise(0)
            ).alias("resolved_requests"),
            avg(col("resolution_days")).alias("avg_resolution_days"),
        )

        logger.info(
            f"Transformed to R&D practices metrics with {rd_practices_metrics.count()} records"
        )
        return rd_practices_metrics

    def _transform_process_nps(self, df):
        """
        Transform to accurate NPS metrics by process.

        IMPORTANT: This table calculates NPS correctly without explode inflation.
        Each task counts as ONE vote, regardless of how many team members or requesters.

        NPS Segments (2-10 scale, 0 = not evaluated → excluded):
        - Promoters: 9-10 (very satisfied, likely to recommend)
        - Passives: 7-8 (satisfied but not enthusiastic)
        - Detractors: 2-6 (unsatisfied, may damage reputation)

        NPS Formula: ((promoters - detractors) / total) * 100

        Fields:
        - internal_satisfaction_score: Team's evaluation of how valuable the work was
        - external_satisfaction_score: Requester/client's satisfaction with the result

        Note: The scale used in ClickUp is 2,4,6,8,10 (mapped from emoji ratings 1-5).
        A value of 0 in the source means "not evaluated" and is treated as NULL.
        """
        from pyspark.sql.functions import (
            avg,
            col,
            count,
            countDistinct,
            current_date,
        )
        from pyspark.sql.functions import sum as spark_sum
        from pyspark.sql.functions import when

        logger.info("Transforming to technology_process_nps_gold (accurate NPS)")

        # Filter tasks with satisfaction scores (no explode - 1 task = 1 vote)
        df_with_scores = df.filter(
            (col("external_satisfaction_score").isNotNull())
            | (col("internal_satisfaction_score").isNotNull())
        ).select(
            col("request_id"),
            col("process_name"),
            col("list_id").alias("list_name"),
            col("external_satisfaction_score"),
            col("internal_satisfaction_score"),
        )

        # Calculate NPS segments for each task (1 task = 1 vote)
        df_with_segments = (
            df_with_scores
            # External NPS segments
            .withColumn(
                "external_promoter",
                when(col("external_satisfaction_score") >= 9, 1).otherwise(0),
            )
            .withColumn(
                "external_passive",
                when(
                    (col("external_satisfaction_score") >= 7)
                    & (col("external_satisfaction_score") < 9),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "external_detractor",
                when(
                    (col("external_satisfaction_score").isNotNull())
                    & (col("external_satisfaction_score") < 7),
                    1,
                ).otherwise(0),
            )
            # Internal NPS segments
            .withColumn(
                "internal_promoter",
                when(col("internal_satisfaction_score") >= 9, 1).otherwise(0),
            )
            .withColumn(
                "internal_passive",
                when(
                    (col("internal_satisfaction_score") >= 7)
                    & (col("internal_satisfaction_score") < 9),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "internal_detractor",
                when(
                    (col("internal_satisfaction_score").isNotNull())
                    & (col("internal_satisfaction_score") < 7),
                    1,
                ).otherwise(0),
            )
        )

        # Aggregate by process (NO explode = accurate counts)
        nps_df = df_with_segments.groupBy(
            current_date().alias("analysis_date"),
            col("process_name"),
            col("list_name"),
        ).agg(
            # Total unique tasks with each score type
            countDistinct(
                when(col("external_satisfaction_score").isNotNull(), col("request_id"))
            ).alias("external_response_count"),
            countDistinct(
                when(col("internal_satisfaction_score").isNotNull(), col("request_id"))
            ).alias("internal_response_count"),
            # Average scores
            avg(col("external_satisfaction_score")).alias("avg_external_score"),
            avg(col("internal_satisfaction_score")).alias("avg_internal_score"),
            # External NPS segments (sum because 1 task = 1 vote)
            spark_sum(col("external_promoter")).alias("external_promoters"),
            spark_sum(col("external_passive")).alias("external_passives"),
            spark_sum(col("external_detractor")).alias("external_detractors"),
            # Internal NPS segments
            spark_sum(col("internal_promoter")).alias("internal_promoters"),
            spark_sum(col("internal_passive")).alias("internal_passives"),
            spark_sum(col("internal_detractor")).alias("internal_detractors"),
        )

        # Calculate NPS scores
        nps_df = (
            nps_df.withColumn(
                "external_nps",
                when(
                    col("external_response_count") > 0,
                    (
                        (col("external_promoters") - col("external_detractors"))
                        / col("external_response_count")
                    )
                    * 100,
                ).otherwise(None),
            )
            .withColumn(
                "internal_nps",
                when(
                    col("internal_response_count") > 0,
                    (
                        (col("internal_promoters") - col("internal_detractors"))
                        / col("internal_response_count")
                    )
                    * 100,
                ).otherwise(None),
            )
        )

        logger.info(
            f"Transformed to process NPS metrics with {nps_df.count()} records"
        )
        return nps_df

    def load(self, df) -> None:
        """
        Load DataFrame to Iceberg gold table.

        Uses append mode with partitioning.
        """
        if df.count() == 0:
            logger.info("Empty DataFrame, skipping load operation")
            return

        logger.info("Loading ClickUp data to gold table...")
        logger.info(
            f"  Target: {self.config.gold_database_name}.{self.config.gold_table_name}"
        )

        self.write_to_iceberg(
            df,
            self.config.gold_database_name,
            self.config.gold_table_name,
            mode="append",
        )

        logger.info("✅ Successfully loaded ClickUp data to gold table")

    def create_tables(self):
        """
        Create Gold Iceberg table.

        Creates table based on entity_type configuration.
        """
        entity_config = self.entity_config
        is_partitioned = len(entity_config.get("partition_cols", [])) > 0

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
            partition_cols = ", ".join(
                entity_config.get("partition_cols", ["analysis_date"])
            )
            gold_table_ddl += f"\n        PARTITIONED BY ({partition_cols})"

        self.spark.sql(gold_table_ddl)
        logger.debug(f"gold_table_ddl: {gold_table_ddl}")
        logger.info(
            f"  ✓ Gold table created: glue_catalog.{self.config.gold_database_name}.{self.config.gold_table_name}"
        )

    def _get_gold_schema_ddl(self) -> str:
        """
        Get DDL schema for Gold table (columns only) based on entity_type.

        Returns:
            str: DDL schema definition (without CREATE TABLE part)
        """
        entity_type = self.entity_type

        # Define schemas for each entity type based on transform output
        schemas = {
            "technology_request_metrics": [
                "analysis_date DATE",
                "process_name STRING",
                "stakeholder_area STRING",
                "team STRING",
                "priority STRING",
                "account STRING",
                "architecture_responsibility_domain STRING",
                "list_name STRING",
                "request_count BIGINT",
                "resolved_count BIGINT",
                "avg_resolution_days DOUBLE",
                "stddev_resolution_days DOUBLE",
                "p50_resolution_days DOUBLE",
                "p75_resolution_days DOUBLE",
                "p95_resolution_days DOUBLE",
                "sla_compliance_rate DOUBLE",
                "avg_time_logged_hours DOUBLE",
                "avg_time_estimate_hours DOUBLE",
                "estimate_accuracy DOUBLE",
                "avg_external_satisfaction_score DOUBLE",
                "avg_internal_satisfaction_score DOUBLE",
                "median_external_satisfaction_score DOUBLE",
                "median_internal_satisfaction_score DOUBLE",
                "avg_completeness_score DOUBLE",
                "avg_urgency_score DOUBLE",
            ],
            "technology_satisfaction": [
                "analysis_date DATE",
                "process_name STRING",
                "stakeholder_area STRING",
                "team STRING",
                "account STRING",
                "architecture_responsibility_domain STRING",
                "list_name STRING",
                "external_satisfaction_score DOUBLE",
                "internal_satisfaction_score DOUBLE",
                "external_promoters_count BIGINT",
                "external_passives_count BIGINT",
                "external_detractors_count BIGINT",
                "internal_promoters_count BIGINT",
                "internal_passives_count BIGINT",
                "internal_detractors_count BIGINT",
                "satisfaction_response_count BIGINT",
                "external_nps_score DOUBLE",
                "internal_nps_score DOUBLE",
            ],
            "technology_throughput": [
                "analysis_date DATE",
                "process_name STRING",
                "stakeholder_area STRING",
                "team STRING",
                "account STRING",
                "architecture_responsibility_domain STRING",
                "list_name STRING",
                "period_start DATE",
                "period_end DATE",
                "requests_created BIGINT",
                "requests_resolved BIGINT",
                "throughput_rate DOUBLE",
            ],
            "technology_request_status_flow": [
                "analysis_date DATE",
                "process_name STRING",
                "stakeholder_area STRING",
                "team STRING",
                "account STRING",
                "architecture_responsibility_domain STRING",
                "list_name STRING",
                "from_status STRING",
                "to_status STRING",
                "transition_count BIGINT",
                "avg_transition_days DOUBLE",
            ],
            "technology_team_performance": [
                "analysis_date DATE",
                "process_name STRING",
                "team STRING",
                "stakeholder_area STRING",
                "account STRING",
                "architecture_responsibility_domain STRING",
                "list_name STRING",
                "total_requests BIGINT",
                "resolved_requests BIGINT",
                "avg_resolution_days DOUBLE",
                "sla_compliance_rate DOUBLE",
                "avg_satisfaction_score DOUBLE",
            ],
            "technology_stakeholder_metrics": [
                "analysis_date DATE",
                "process_name STRING",
                "stakeholder_area STRING",
                "account STRING",
                "architecture_responsibility_domain STRING",
                "list_name STRING",
                "total_requests BIGINT",
                "resolved_requests BIGINT",
                "avg_resolution_days DOUBLE",
                "avg_satisfaction_score DOUBLE",
            ],
            "technology_process_metrics": [
                "analysis_date DATE",
                "process_name STRING",
                "list_name STRING",
                "total_requests BIGINT",
                "resolved_requests BIGINT",
                "avg_resolution_days DOUBLE",
                "sla_compliance_rate DOUBLE",
                "avg_satisfaction_score DOUBLE",
            ],
            "technology_account_metrics": [
                "analysis_date DATE",
                "process_name STRING",
                "account STRING",
                "list_name STRING",
                "total_requests BIGINT",
                "resolved_requests BIGINT",
                "avg_resolution_days DOUBLE",
                "avg_satisfaction_score DOUBLE",
            ],
            "technology_rd_practices_metrics": [
                "analysis_date DATE",
                "process_name STRING",
                "rd_practice STRING",
                "list_name STRING",
                "total_requests BIGINT",
                "resolved_requests BIGINT",
                "avg_resolution_days DOUBLE",
            ],
            "technology_process_nps": [
                "analysis_date DATE",
                "process_name STRING",
                "list_name STRING",
                "external_response_count BIGINT",
                "internal_response_count BIGINT",
                "avg_external_score DOUBLE",
                "avg_internal_score DOUBLE",
                "external_promoters BIGINT",
                "external_passives BIGINT",
                "external_detractors BIGINT",
                "internal_promoters BIGINT",
                "internal_passives BIGINT",
                "internal_detractors BIGINT",
                "external_nps DOUBLE",
                "internal_nps DOUBLE",
            ],
        }

        if entity_type not in schemas:
            raise ValueError(
                f"Unknown entity_type: {entity_type}. Cannot generate schema."
            )

        return ",\n          ".join(schemas[entity_type])


# ==========================================
# MAIN EXECUTION (HYBRID LOCAL/GLUE)
# ==========================================
if __name__ == "__main__":
    try:
        setup_logging()
        logger.info("Loading ClickUp Gold configuration...")
        config = ClickUpGoldConfig.from_args()

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
            gold_job = ClickUpGoldJob(spark, config)
        else:
            gold_job = ClickUpGoldJob(context["glue_context"], config)

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
