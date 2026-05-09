#!/usr/bin/env bash

set -euo pipefail

SOURCE_NAME="${1:-${SOURCE:-}}"
ENTITY_TYPE="${2:-${ENTITY_TYPE:-entity}}"

if [[ -z "${SOURCE_NAME}" ]]; then
  echo "Usage: make scaffold-source SOURCE=<source_name> [ENTITY_TYPE=<entity_type>]"
  exit 1
fi

if [[ ! "${SOURCE_NAME}" =~ ^[a-z][a-z0-9_]*$ ]]; then
  echo "SOURCE must be snake_case (example: crm_api)"
  exit 1
fi

if [[ ! "${ENTITY_TYPE}" =~ ^[a-z][a-z0-9_]*$ ]]; then
  echo "ENTITY_TYPE must be snake_case (example: customers)"
  exit 1
fi

to_pascal_case() {
  local input="$1"
  local output=""
  local part

  IFS='_' read -ra parts <<< "$input"
  for part in "${parts[@]}"; do
    output+="${part^}"
  done
  echo "$output"
}

CLASS_PREFIX="$(to_pascal_case "${SOURCE_NAME}")"

RAW_FILE="jobs/raw/${SOURCE_NAME}_raw_job.py"
BRONZE_FILE="jobs/bronze/${SOURCE_NAME}_bronze_job.py"
SILVER_FILE="jobs/silver/${SOURCE_NAME}_silver_job.py"
GOLD_FILE="jobs/gold/${SOURCE_NAME}_gold_job.py"
TEST_FILE="tests/unit/jobs/test_${SOURCE_NAME}_jobs.py"

for file in "$RAW_FILE" "$BRONZE_FILE" "$SILVER_FILE" "$GOLD_FILE" "$TEST_FILE"; do
  if [[ -f "$file" ]]; then
    echo "Refusing to overwrite existing file: $file"
    exit 1
  fi
done

cat > "$RAW_FILE" <<EOF
"""
${CLASS_PREFIX} Raw ingestion job.

TODO: replace extraction logic with your real source implementation.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pydantic import Field

from libs.common import RawJobConfig, setup_logging
from libs.pyshell import PyShellJobBase

setup_logging()
logger = logging.getLogger(__name__)


class ${CLASS_PREFIX}RawConfig(RawJobConfig):
    source_name: str = Field(default="${SOURCE_NAME}")
    entity_type: str = Field(default="${ENTITY_TYPE}")
    api_base_url: str = Field(default="https://jsonplaceholder.typicode.com")
    api_endpoint: str = Field(default="/${ENTITY_TYPE}")
    max_retries: int = Field(default=3)
    batch_size: int = Field(default=500)
    rate_limit_per_second: float = Field(default=5.0)
    raw_write_path: Optional[str] = Field(default=None)

    def model_post_init(self, __context) -> None:
        self.raw_write_path = (
            f"{self.raw_zone_path.rstrip('/')}/{self.source_name}/{self.entity_type}/"
        )


class ${CLASS_PREFIX}RawJob(PyShellJobBase):
    def __init__(self, config: ${CLASS_PREFIX}RawConfig):
        super().__init__(config)

    def extract(self) -> Tuple[List[Dict], List[Dict]]:
        endpoint = self.config.api_endpoint
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"

        url = f"{self.config.api_base_url.rstrip('/')}{endpoint}"
        response_data, request_info = self.make_api_request(url=url)

        if isinstance(response_data, list):
            records = response_data
        elif isinstance(response_data, dict):
            records = response_data.get("data", [response_data])
        else:
            records = []

        return records, [request_info]

    def transform(
        self, data: Tuple[List[Dict], List[Dict]]
    ) -> Tuple[List[Dict], List[Dict]]:
        records, request_info = data
        now = datetime.utcnow()
        for record in records:
            record.setdefault("ingested_at", now.isoformat() + "Z")
            record.setdefault("source", self.config.source_name)
            record.setdefault("entity_type", self.config.entity_type)
        return records, request_info

    def load(self, data: Tuple[List[Dict], List[Dict]]) -> None:
        records, request_info = data
        self.write_to_s3(records, request_info_list=request_info)


if __name__ == "__main__":
    config = ${CLASS_PREFIX}RawConfig.from_args()
    logger.info("Starting raw extraction for %s", config.entity_type)
    ${CLASS_PREFIX}RawJob(config).run()
EOF

cat > "$BRONZE_FILE" <<EOF
"""
${CLASS_PREFIX} Bronze job.

Reads raw JSON payloads from S3 and persists normalized records into Bronze Iceberg.
"""

import logging

from pydantic import Field
from pyspark.sql.functions import col, current_timestamp, to_json

from libs.common import BronzeJobConfig, setup_logging
from libs.pyspark import BronzeJobBase, SparkSessionFactory

setup_logging()
logger = logging.getLogger(__name__)


class ${CLASS_PREFIX}BronzeConfig(BronzeJobConfig):
    source_name: str = Field(default="${SOURCE_NAME}")
    entity_type: str = Field(default="${ENTITY_TYPE}")

    def model_post_init(self, __context) -> None:
        if not self.raw_table_name:
            self.raw_table_name = f"{self.source_name}_{self.entity_type}_raw"
        if not self.bronze_table_name:
            self.bronze_table_name = f"{self.source_name}_{self.entity_type}_bronze"


class ${CLASS_PREFIX}BronzeJob(BronzeJobBase):
    config: ${CLASS_PREFIX}BronzeConfig

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
    config = ${CLASS_PREFIX}BronzeConfig.from_args()
    context = (
        session_info.spark
        if session_info.is_local
        else session_info.context["glue_context"]
    )
    ${CLASS_PREFIX}BronzeJob(context, config).run()
    session_info.spark.stop()
EOF

cat > "$SILVER_FILE" <<EOF
"""
${CLASS_PREFIX} Silver job.

Reads Bronze table and applies lightweight quality/typing normalization for Silver.
"""

import logging

from pydantic import Field
from pyspark.sql.functions import col, current_timestamp, length, sha2

from libs.common import SilverJobConfig, setup_logging
from libs.pyspark import SilverJobBase, SparkSessionFactory

setup_logging()
logger = logging.getLogger(__name__)


class ${CLASS_PREFIX}SilverConfig(SilverJobConfig):
    source_name: str = Field(default="${SOURCE_NAME}")
    entity_type: str = Field(default="${ENTITY_TYPE}")

    def model_post_init(self, __context) -> None:
        if not self.bronze_table_name:
            self.bronze_table_name = f"{self.source_name}_{self.entity_type}_bronze"
        if not self.silver_table_name:
            self.silver_table_name = f"{self.source_name}_{self.entity_type}_silver"


class ${CLASS_PREFIX}SilverJob(SilverJobBase):
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
    config = ${CLASS_PREFIX}SilverConfig.from_args()
    context = (
        session_info.spark
        if session_info.is_local
        else session_info.context["glue_context"]
    )
    ${CLASS_PREFIX}SilverJob(context, config).run()
    session_info.spark.stop()
EOF

cat > "$GOLD_FILE" <<EOF
"""
${CLASS_PREFIX} Gold job.

Builds a simple aggregate table from Silver records.
"""

import logging

from pydantic import Field
from pyspark.sql.functions import count, current_timestamp, lit

from libs.common import GoldJobConfig, setup_logging
from libs.pyspark import GoldJobBase, SparkSessionFactory

setup_logging()
logger = logging.getLogger(__name__)


class ${CLASS_PREFIX}GoldConfig(GoldJobConfig):
    source_name: str = Field(default="${SOURCE_NAME}")
    entity_type: str = Field(default="${ENTITY_TYPE}")

    def model_post_init(self, __context) -> None:
        if not self.silver_table_name:
            self.silver_table_name = f"{self.source_name}_{self.entity_type}_silver"
        if not self.gold_table_name:
            self.gold_table_name = f"{self.source_name}_{self.entity_type}_gold"


class ${CLASS_PREFIX}GoldJob(GoldJobBase):
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
    config = ${CLASS_PREFIX}GoldConfig.from_args()
    context = (
        session_info.spark
        if session_info.is_local
        else session_info.context["glue_context"]
    )
    ${CLASS_PREFIX}GoldJob(context, config).run()
    session_info.spark.stop()
EOF

cat > "$TEST_FILE" <<EOF
"""Unit tests for ${SOURCE_NAME} medallion job configs."""

from unittest.mock import patch

from jobs.bronze.${SOURCE_NAME}_bronze_job import ${CLASS_PREFIX}BronzeConfig
from jobs.gold.${SOURCE_NAME}_gold_job import ${CLASS_PREFIX}GoldConfig
from jobs.raw.${SOURCE_NAME}_raw_job import ${CLASS_PREFIX}RawConfig
from jobs.silver.${SOURCE_NAME}_silver_job import ${CLASS_PREFIX}SilverConfig

TEST_ENV = {
    "SOURCE_NAME": "${SOURCE_NAME}",
    "ENTITY_TYPE": "${ENTITY_TYPE}",
    "WAREHOUSE_PATH": "s3://test-bucket/warehouse/",
    "RAW_ZONE_PATH": "s3://test-bucket/raw/",
    "RAW_DATABASE_NAME": "raw_zone",
    "BRONZE_DATABASE_NAME": "bronze_zone",
    "SILVER_DATABASE_NAME": "silver_zone",
    "GOLD_DATABASE_NAME": "gold_zone",
    "ENVIRONMENT": "develop",
    "STAGE": "local",
    "AWS_REGION": "us-east-1",
}


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_${SOURCE_NAME}_raw_config_sets_raw_write_path():
    config = ${CLASS_PREFIX}RawConfig(job_name="raw_job")
    assert (
        config.raw_write_path
        == "s3://test-bucket/raw/${SOURCE_NAME}/${ENTITY_TYPE}/"
    )


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_${SOURCE_NAME}_bronze_config_auto_table_names():
    config = ${CLASS_PREFIX}BronzeConfig(job_name="bronze_job")
    assert config.raw_table_name == "${SOURCE_NAME}_${ENTITY_TYPE}_raw"
    assert config.bronze_table_name == "${SOURCE_NAME}_${ENTITY_TYPE}_bronze"


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_${SOURCE_NAME}_silver_config_auto_table_names():
    config = ${CLASS_PREFIX}SilverConfig(
        job_name="silver_job",
        bronze_database_name="bronze_zone",
        silver_database_name="silver_zone",
    )
    assert config.bronze_table_name == "${SOURCE_NAME}_${ENTITY_TYPE}_bronze"
    assert config.silver_table_name == "${SOURCE_NAME}_${ENTITY_TYPE}_silver"


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_${SOURCE_NAME}_gold_config_auto_table_names():
    config = ${CLASS_PREFIX}GoldConfig(
        job_name="gold_job",
        silver_database_name="silver_zone",
        gold_database_name="gold_zone",
    )
    assert config.silver_table_name == "${SOURCE_NAME}_${ENTITY_TYPE}_silver"
    assert config.gold_table_name == "${SOURCE_NAME}_${ENTITY_TYPE}_gold"
EOF

echo "✅ Scaffold created"
echo "   - $RAW_FILE"
echo "   - $BRONZE_FILE"
echo "   - $SILVER_FILE"
echo "   - $GOLD_FILE"
echo "   - $TEST_FILE"
echo ""
echo "Next steps:"
echo "1) Implement source-specific extraction/transformation logic"
echo "2) Add integration tests in tests/integration/"
echo "3) Run: make test-unit && make lint && make type-check"
