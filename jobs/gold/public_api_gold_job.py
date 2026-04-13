"""
Public API Gold job.

Builds a simple aggregate table from Silver records.
"""

import logging

from pydantic import Field
from pyspark.sql.functions import count, current_timestamp, lit

from libs.common import GoldJobConfig, setup_logging
from libs.pyspark import GoldJobBase, SparkSessionFactory

setup_logging()
logger = logging.getLogger(__name__)


class PublicApiGoldConfig(GoldJobConfig):
    source_name: str = Field(default="public_api")
    entity_type: str = Field(default="posts")

    def model_post_init(self, __context) -> None:
        if not self.silver_table_name:
            self.silver_table_name = f"{self.source_name}_{self.entity_type}_silver"
        if not self.gold_table_name:
            self.gold_table_name = f"{self.source_name}_{self.entity_type}_gold"


class PublicApiGoldJob(GoldJobBase):
    def extract(self):
        return self.read_iceberg_table(
            database=self.config.silver_database_name,
            table=self.config.silver_table_name,
        )

    def transform(self, data):
        return data.agg(count("record_id").alias("records_total")).withColumn(
            "entity_type", lit(self.config.entity_type)
        ).withColumn("computed_at", current_timestamp())

    def load(self, data):
        self.write_to_iceberg(
            data,
            database=self.config.gold_database_name,
            table=self.config.gold_table_name,
            mode="append",
        )

    def create_tables(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.gold_database_name}.{self.config.gold_table_name} (
                records_total BIGINT,
                entity_type STRING,
                computed_at TIMESTAMP
            )
            USING iceberg
            """
        )


if __name__ == "__main__":
    session_info = SparkSessionFactory.create_session()
    config = PublicApiGoldConfig.from_args()
    context = (
        session_info.spark
        if session_info.is_local
        else session_info.context["glue_context"]
    )
    PublicApiGoldJob(context, config).run()
    session_info.spark.stop()
