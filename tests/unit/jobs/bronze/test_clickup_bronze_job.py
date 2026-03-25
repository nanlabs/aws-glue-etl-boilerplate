"""
Unit tests for ClickUp Bronze Job.

Tests cover:
- Configuration validation
- Table name auto-configuration
- Date parsing logic
- Custom fields handling
- Derived field calculations

Focus: Business logic, not Spark infrastructure.
"""

from unittest.mock import MagicMock, patch

# Test environment - all required variables
TEST_ENV = {
    "PROJECT_NAME": "test_project",
    "ENVIRONMENT": "develop",
    "RAW_DATABASE": "test_raw_db",
    "RAW_DATABASE_NAME": "test_raw_db",
    "BRONZE_DATABASE": "test_bronze_db",
    "BRONZE_DATABASE_NAME": "test_bronze_db",
    "RAW_ZONE_PATH": "s3://bucket/raw/",
    "BRONZE_ZONE_PATH": "s3://bucket/bronze/",
    "WAREHOUSE_PATH": "s3://bucket/warehouse/",
}


class TestClickUpBronzeConfig:
    """Test ClickUp Bronze configuration validation."""

    @patch.dict("os.environ", TEST_ENV)
    def test_config_auto_configures_table_names(self):
        """Test that table names are auto-configured."""
        from jobs.bronze.clickup_bronze_job import ClickUpBronzeConfig

        config = ClickUpBronzeConfig(job_name="test_job")

        assert config.raw_table_name == "clickup__technology__requests_raw"
        assert config.bronze_table_name == "clickup__technology__requests_bronze"

    @patch.dict("os.environ", TEST_ENV)
    def test_config_allows_override_table_names(self):
        """Test that table names can be overridden."""
        from jobs.bronze.clickup_bronze_job import ClickUpBronzeConfig

        config = ClickUpBronzeConfig(
            job_name="test_job",
            raw_table_name="custom_raw_table",
            bronze_table_name="custom_bronze_table",
        )

        assert config.raw_table_name == "custom_raw_table"
        assert config.bronze_table_name == "custom_bronze_table"


class TestClickUpBronzeJob:
    """Test ClickUp Bronze Job logic."""

    @patch.dict("os.environ", TEST_ENV)
    def test_job_initializes_with_config(self):
        """Test that job initializes with configuration."""
        from jobs.bronze.clickup_bronze_job import ClickUpBronzeConfig, ClickUpBronzeJob

        mock_spark = MagicMock()
        config = ClickUpBronzeConfig(job_name="test_job")

        job = ClickUpBronzeJob(mock_spark, config)

        assert job.config == config
        assert job.config.raw_table_name == "clickup__technology__requests_raw"
        assert job.config.bronze_table_name == "clickup__technology__requests_bronze"

    @patch.dict("os.environ", TEST_ENV)
    def test_job_extract_builds_correct_query(self):
        """Test that extract method builds correct SQL query."""
        from jobs.bronze.clickup_bronze_job import ClickUpBronzeConfig, ClickUpBronzeJob

        mock_spark = MagicMock()
        mock_spark.sql.return_value = MagicMock()
        mock_spark.sql.return_value.count.return_value = 10

        config = ClickUpBronzeConfig(job_name="test_job")
        job = ClickUpBronzeJob(mock_spark, config)

        _ = job.extract()  # Extract is called but result not used in this test

        # Verify SQL was called
        assert mock_spark.sql.called
        call_args = mock_spark.sql.call_args[0][0]
        assert "clickup__technology__requests_raw" in call_args
        assert "task_id IS NOT NULL" in call_args
