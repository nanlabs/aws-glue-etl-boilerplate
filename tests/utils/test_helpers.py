"""
Common test utilities and helpers for the test suite.

This module provides reusable utilities, assertions, and helpers
to make tests more maintainable and consistent.
"""

from typing import Any, Dict, Optional
from unittest.mock import Mock

from pyspark.sql import DataFrame, SparkSession


class TestDataFrameBuilder:
    """Helper class to build mock DataFrames for testing."""

    @staticmethod
    def create_mock_dataframe(
        count: int = 10, columns: Optional[list] = None, data: Optional[list] = None
    ) -> Mock:
        """Create a mock DataFrame with specified properties."""
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = count
        mock_df.columns = columns or ["id", "name", "value"]
        mock_df.show.return_value = None
        mock_df.collect.return_value = data or []
        return mock_df


class TestSparkSessionBuilder:
    """Helper class to build mock Spark sessions for testing."""

    @staticmethod
    def create_mock_spark_session() -> Mock:
        """Create a fully configured mock Spark session."""
        mock_spark = Mock(spec=SparkSession)
        mock_spark.version = "3.5.4"

        # Mock SQL operations
        mock_spark.sql.return_value = TestDataFrameBuilder.create_mock_dataframe()

        # Mock DataFrame creation
        mock_spark.createDataFrame.return_value = (
            TestDataFrameBuilder.create_mock_dataframe()
        )

        # Mock table operations
        mock_spark.table.return_value = TestDataFrameBuilder.create_mock_dataframe()

        # Mock catalog operations
        mock_catalog = Mock()
        mock_catalog.tableExists.return_value = True
        mock_spark.catalog = mock_catalog

        return mock_spark


class TestJobParametersBuilder:
    """Helper class to build test job parameters."""

    @staticmethod
    def create_base_parameters(
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """Create base job parameters for testing."""
        params = {
            "JOB_NAME": "test_job",
            "source_bucket": "test-bucket",
            "target_database": "test_db",
            "target_table": "test_table",
            "batch_size": "1000",
            "log_level": "INFO",
            "environment": "develop",
            "stage": "local",
            "aws_region": "us-east-1",
        }

        if overrides:
            params.update(overrides)

        return params


class TestAssertions:
    """Common assertions for testing patterns."""

    @staticmethod
    def assert_spark_config_contains(config: Dict[str, str], expected_keys: list):
        """Assert that Spark config contains expected keys."""
        for key in expected_keys:
            assert key in config, f"Expected Spark config key '{key}' not found"

    @staticmethod
    def assert_iceberg_catalog_configured(config: Dict[str, str]):
        """Assert that Iceberg catalog is properly configured."""
        expected_keys = [
            "spark.sql.catalog.iceberg_catalog.type",
            "spark.sql.catalog.iceberg_catalog.warehouse",
            "spark.sql.extensions",
        ]
        TestAssertions.assert_spark_config_contains(config, expected_keys)

    @staticmethod
    def assert_job_has_required_attributes(job, required_attrs: list):
        """Assert that job instance has required attributes."""
        for attr in required_attrs:
            assert hasattr(job, attr), f"Job missing required attribute: {attr}"


def mock_aws_client(service_name: str, **_kwargs) -> Mock:
    """Factory function to create AWS service mocks."""
    if service_name == "s3":
        mock_client = Mock()
        mock_client.list_objects_v2.return_value = {"Contents": []}
        mock_client.head_object.return_value = {"ContentLength": 1024}
        return mock_client

    elif service_name == "secretsmanager":
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": '{"key": "test_value"}'
        }
        return mock_client

    elif service_name == "glue":
        mock_client = Mock()
        mock_client.get_databases.return_value = {"DatabaseList": []}
        mock_client.get_tables.return_value = {"TableList": []}
        return mock_client

    else:
        return Mock()


def create_test_environment_variables(
    overrides: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Create a standard set of test environment variables."""
    env_vars = {
        "JOB_NAME": "test_job",
        "TEST_JOB_ENVIRONMENT": "develop",
        "TEST_JOB_STAGE": "local",
        "TEST_JOB_LOG_LEVEL": "INFO",
        "AWS_DEFAULT_REGION": "us-east-1",
        "SPARK_LOCAL_HOSTNAME": "localhost",
    }

    if overrides:
        env_vars.update(overrides)

    return env_vars
