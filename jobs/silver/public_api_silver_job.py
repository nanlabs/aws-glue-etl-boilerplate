"""
Public API Silver job.

Reads Bronze table and applies lightweight quality/typing normalization for Silver.
"""

import logging

from pydantic import Field
from pyspark.sql.functions import col, current_timestamp, length, sha2

from libs.common import SilverJobConfig, setup_logging
from libs.pyspark import SilverJobBase, SparkSessionFactory

setup_logging()
logger = logging.getLogger(__name__)


class PublicApiSilverConfig(SilverJobConfig):
    source_name: str = Field(default="public_api")
    entity_type: str = Field(default="posts")

    def model_post_init(self, __context) -> None:
        if not self.bronze_table_name:
            self.bronze_table_name = f"{self.source_name}_{self.entity_type}_bronze"
        if not self.silver_table_name:
            self.silver_table_name = f"{self.source_name}_{self.entity_type}_silver"


class PublicApiSilverJob(SilverJobBase):
    def extract(self):
        return self.read_iceberg_table(
            database=self.config.bronze_database_name,
            table=self.config.bronze_table_name,
        )

    def transform(self, data):
        return (
            data.filter(col("record_id").isNotNull())
            .withColumn("payload_size", length(col("payload_json")))
            .withColumn("payload_hash", sha2(col("payload_json"), 256))
            .withColumn("standardized_at", current_timestamp())
        )

    def load(self, data):
        self.write_to_iceberg(
            data,
            database=self.config.silver_database_name,
            table=self.config.silver_table_name,
            mode="append",
        )

    def create_tables(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.silver_database_name}.{self.config.silver_table_name} (
                record_id STRING,
                payload_json STRING,
                payload_size INT,
                payload_hash STRING,
                processed_at TIMESTAMP,
                standardized_at TIMESTAMP
            )
            USING iceberg
            """
        )


if __name__ == "__main__":
    session_info = SparkSessionFactory.create_session()
    config = PublicApiSilverConfig.from_args()
    context = (
        session_info.spark
        if session_info.is_local
        else session_info.context["glue_context"]
    )
    PublicApiSilverJob(context, config).run()
    session_info.spark.stop()
