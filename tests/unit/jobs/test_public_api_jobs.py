"""Unit tests for generic public_api medallion job configs and transforms."""

from unittest.mock import patch


from jobs.bronze.public_api_bronze_job import PublicApiBronzeConfig
from jobs.gold.public_api_gold_job import PublicApiGoldConfig, PublicApiGoldJob
from jobs.raw.public_api_raw_job import PublicApiRawConfig, PublicApiRawJob
from jobs.silver.public_api_silver_job import PublicApiSilverConfig

TEST_ENV = {
    "SOURCE_NAME": "public_api",
    "ENTITY_TYPE": "posts",
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
def test_public_api_raw_config_sets_raw_write_path():
    config = PublicApiRawConfig(job_name="raw_job")
    assert config.raw_write_path == "s3://test-bucket/raw/public_api/posts/"


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_public_api_bronze_config_auto_table_names():
    config = PublicApiBronzeConfig(job_name="bronze_job")
    assert config.raw_table_name == "public_api_posts_raw"
    assert config.bronze_table_name == "public_api_posts_bronze"


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_public_api_silver_config_auto_table_names():
    config = PublicApiSilverConfig(
        job_name="silver_job",
        bronze_database_name="bronze_zone",
        silver_database_name="silver_zone",
    )
    assert config.bronze_table_name == "public_api_posts_bronze"
    assert config.silver_table_name == "public_api_posts_silver"


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_public_api_gold_config_auto_table_names():
    config = PublicApiGoldConfig(
        job_name="gold_job",
        silver_database_name="silver_zone",
        gold_database_name="gold_zone",
    )
    assert config.silver_table_name == "public_api_posts_silver"
    assert config.gold_table_name == "public_api_posts_gold"


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_public_api_raw_transform_adds_metadata_defaults():
    job = PublicApiRawJob.__new__(PublicApiRawJob)
    job.config = PublicApiRawConfig(job_name="raw_job")

    records = [{"id": 1, "title": "hello"}]
    request_info = [{"url": "https://example.test/posts"}]

    transformed_records, transformed_request_info = job.transform((records, request_info))

    assert transformed_request_info == request_info
    assert transformed_records[0]["source"] == "public_api"
    assert transformed_records[0]["entity_type"] == "posts"
    assert transformed_records[0]["ingested_at"].endswith("Z")


@patch.dict("os.environ", TEST_ENV, clear=False)
def test_public_api_gold_transform_builds_aggregation_pipeline():
    from unittest.mock import Mock

    config = PublicApiGoldConfig(
        job_name="gold_job",
        silver_database_name="silver_zone",
        gold_database_name="gold_zone",
    )

    job = PublicApiGoldJob.__new__(PublicApiGoldJob)
    job.config = config

    mock_df = Mock()
    mock_after_agg = Mock()
    mock_after_first_with_column = Mock()

    mock_df.agg.return_value = mock_after_agg
    mock_after_agg.withColumn.return_value = mock_after_first_with_column
    mock_after_first_with_column.withColumn.return_value = "final_df"

    fake_count_expr = Mock()
    fake_count_expr.alias.return_value = "count_expr"

    with patch("jobs.gold.public_api_gold_job.count", return_value=fake_count_expr), patch(
        "jobs.gold.public_api_gold_job.lit", return_value="lit_expr"
    ), patch(
        "jobs.gold.public_api_gold_job.current_timestamp", return_value="ts_expr"
    ):
        result = job.transform(mock_df)

    assert result == "final_df"
    mock_df.agg.assert_called_once_with("count_expr")
    mock_after_agg.withColumn.assert_called_once_with("entity_type", "lit_expr")
    mock_after_first_with_column.withColumn.assert_called_once_with(
        "computed_at", "ts_expr"
    )
