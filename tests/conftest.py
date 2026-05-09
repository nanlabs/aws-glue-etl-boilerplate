"""
Test Configuration and Fixtures for Data Warehouse Testing Framework

This module provides shared fixtures and configuration for testing across:
- Multiple data sources
- Medallion Architecture layers (Raw, Bronze, Silver, Gold)
- Different domains

Usage:
    pytest tests/unit/                    # Run unit tests only
    pytest tests/integration/             # Run integration tests only
    pytest tests/e2e/                     # Run end-to-end tests only
"""

import json
import os
import tempfile
from typing import Dict, Generator
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

# Import logging fix FIRST to prevent py4j logging errors
from tests.spark_logging_fix import setup_spark_logging_fix

setup_spark_logging_fix()

# ============================================================================
# Test Environment Configuration
# ============================================================================


def get_test_env_vars() -> Dict[str, str]:
    """Get test environment variables."""
    return {
        "AWS_ACCESS_KEY_ID": "test-access-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret-key",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ENDPOINT_URL_S3": "http://localhost:4566",  # LocalStack S3 endpoint
        "AWS_ENDPOINT_URL_GLUE": "http://localhost:4566",  # LocalStack Glue endpoint
        "WAREHOUSE_S3_LOCATION": "s3://test-bucket/test-project/",
        "PROJECT_NAME": "test-project",
        "ENVIRONMENT": "test",
    }


# ============================================================================
# Environment Fixtures
# ============================================================================

# ============================================================================
# Environment Fixtures
# ============================================================================


@pytest.fixture(scope="function")
def test_env_vars() -> Generator[Dict[str, str], None, None]:
    """Set up test environment variables."""
    original_env = dict(os.environ)

    # Set test environment variables
    for key, value in get_test_env_vars().items():
        os.environ[key] = value

    yield get_test_env_vars()

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture(scope="function")
def temp_directory() -> Generator[str, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


# ============================================================================
# Mock Fixtures for Unit Tests
# ============================================================================


@pytest.fixture(scope="function")
def mock_spark() -> Mock:
    """Mock Spark session for unit tests."""
    mock_spark = Mock(spec=SparkSession)
    mock_spark.sql.return_value = Mock()
    mock_spark.createDataFrame.return_value = Mock()
    mock_spark.read.return_value = Mock()
    return mock_spark


@pytest.fixture(scope="function")
def mock_glue_context():
    """Mock Glue context for unit testing."""
    mock_context = Mock()
    mock_logger = Mock()
    mock_context.get_logger.return_value = mock_logger
    return mock_context


@pytest.fixture(scope="function")
def mock_aws_services():
    """Mock AWS services for unit testing."""
    with patch("boto3.client") as mock_client:
        # Mock S3 client
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        mock_s3.put_object.return_value = {"ETag": "test-etag"}

        # Mock Secrets Manager client
        mock_secrets = Mock()
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps({"api_key": "test-api-key"})
        }

        # Mock Glue client
        mock_glue = Mock()
        mock_glue.get_databases.return_value = {"DatabaseList": []}
        mock_glue.get_tables.return_value = {"TableList": []}

        def client_side_effect(service_name, **_kwargs):
            if service_name == "s3":
                return mock_s3
            elif service_name == "secretsmanager":
                return mock_secrets
            elif service_name == "glue":
                return mock_glue
            return Mock()

        mock_client.side_effect = client_side_effect

        yield {"s3": mock_s3, "secretsmanager": mock_secrets, "glue": mock_glue}


# ============================================================================
# Spark Session Fixtures for Integration Tests
# ============================================================================


@pytest.fixture(scope="function")
def local_spark_session():
    """Create a local SparkSession for integration tests."""
    warehouse_dir = tempfile.mkdtemp()

    spark = (
        SparkSession.builder.appName("test_session")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup
    try:
        spark.stop()
    except Exception:
        pass


@pytest.fixture(scope="function")
def local_spark_with_iceberg():
    """Create a local SparkSession with Iceberg for integration tests."""
    warehouse_dir = tempfile.mkdtemp()

    spark = (
        SparkSession.builder.appName("iceberg_test_session")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.test_catalog.type", "hadoop")
        .config("spark.sql.catalog.test_catalog.warehouse", f"file://{warehouse_dir}")
        .config("spark.sql.defaultCatalog", "test_catalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup
    try:
        spark.stop()
    except Exception:
        pass


# ============================================================================
# Data Fixtures
# ============================================================================

# ============================================================================
# Data Fixtures
# ============================================================================


# ============================================================================
# Composite Fixtures
# ============================================================================


@pytest.fixture(scope="function")
def unit_test_environment(test_env_vars, temp_directory, mock_aws_services):
    """Complete environment for unit tests."""
    return {
        "env_vars": test_env_vars,
        "temp_dir": temp_directory,
        "aws_services": mock_aws_services,
    }


@pytest.fixture(scope="function")
def integration_test_environment(test_env_vars, temp_directory, local_spark_session):
    """Complete environment for integration tests."""
    return {
        "env_vars": test_env_vars,
        "temp_dir": temp_directory,
        "spark": local_spark_session,
    }
