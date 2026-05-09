"""
Public API Bronze job.

Reads raw JSON payloads from S3 and persists normalized records into Bronze Iceberg.
"""

import logging

from pydantic import Field
from pyspark.sql.functions import col, current_timestamp, to_json

from libs.common import BronzeJobConfig, setup_logging
from libs.pyspark import BronzeJobBase, SparkSessionFactory

setup_logging()
logger = logging.getLogger(__name__)


class PublicApiBronzeConfig(BronzeJobConfig):
    source_name: str = Field(default="public_api")
    entity_type: str = Field(default="posts")

    def model_post_init(self, __context) -> None:
        if not self.raw_table_name:
            self.raw_table_name = f"{self.source_name}_{self.entity_type}_raw"
        if not self.bronze_table_name:
            self.bronze_table_name = f"{self.source_name}_{self.entity_type}_bronze"


class PublicApiBronzeJob(BronzeJobBase):
    config: PublicApiBronzeConfig

    def extract(self):
        input_path = (
            f"{self.config.raw_zone_path.rstrip('/')}/{self.config.source_name}/{self.config.entity_type}/"
        )
        return self.spark.read.json(input_path)

    def transform(self, data):
        return data.select(
            col("payload.id").cast("string").alias("record_id"),
            to_json(col("payload")).alias("payload_json"),
            current_timestamp().alias("processed_at"),
        )

    def load(self, data):
        self.write_to_iceberg(
            data,
            database=self.config.bronze_database_name,
            table=self.config.bronze_table_name,
            mode="append",
        )

    def create_tables(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS glue_catalog.{self.config.bronze_database_name}.{self.config.bronze_table_name} (
                record_id STRING,
                payload_json STRING,
                processed_at TIMESTAMP
            )
            USING iceberg
            """
        )


if __name__ == "__main__":
    session_info = SparkSessionFactory.create_session()
    config = PublicApiBronzeConfig.from_args()
    context = (
        session_info.spark
        if session_info.is_local
        else session_info.context["glue_context"]
    )
    PublicApiBronzeJob(context, config).run()
    session_info.spark.stop()
