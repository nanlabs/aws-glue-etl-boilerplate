"""
Unit tests for ClickUp Raw Job.

Tests focus on:
- Configuration validation and list IDs handling
- API endpoint configuration
- Custom fields handling
- Data transformation logic
- S3 path construction

Fast execution without external dependencies (API, S3, Secrets Manager).
"""

from unittest.mock import patch

import pytest
from pydantic import ValidationError

# Test environment with required variables
CLICKUP_TEST_ENV = {
    "SOURCE_NAME": "clickup",
    "PROJECT_NAME": "test_project",
    "WAREHOUSE_PATH": "s3://test-bucket/warehouse/",
    "RAW_ZONE_PATH": "s3://test-bucket/raw/",
    "CLICKUP_API_SECRET_NAME": "test/clickup/api-token",
    "CLICKUP_API_BASE_URL": "https://api.clickup.com",
    "ENVIRONMENT": "develop",
}


class TestClickUpRawConfig:
    """Test ClickUp Raw configuration validation."""

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    def test_config_requires_list_ids(self):
        """Test that configuration requires list_ids parameter."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig

        # Should fail without list_ids
        with pytest.raises((ValidationError, TypeError)):
            ClickUpRawConfig(job_name="test_job")

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    def test_config_with_valid_list_ids(self):
        """Test configuration with valid list IDs."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig

        config = ClickUpRawConfig(
            job_name="test_clickup_requests", list_ids="12345678,87654321"
        )

        assert config.list_ids == "12345678,87654321"
        assert config.source_name == "clickup"
        assert config.api_base_url == "https://api.clickup.com"
        assert config.rate_limit_per_minute == 100

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    def test_config_auto_generates_raw_write_path(self):
        """Test that raw_write_path is automatically generated."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig

        config = ClickUpRawConfig(job_name="test_clickup_requests", list_ids="12345678")

        # Should auto-generate: raw_zone_path + clickup/tasks/
        expected_path = "s3://test-bucket/raw/clickup/tasks/"
        assert config.raw_write_path == expected_path

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    def test_config_rate_limit_defaults(self):
        """Test that rate limit has sensible defaults."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig

        config = ClickUpRawConfig(job_name="test_job", list_ids="12345678")

        assert config.rate_limit_per_minute == 100  # Business plan
        assert config.max_retries == 3
        assert config.batch_size == 1000
        assert config.include_closed is True
        assert config.include_custom_fields is True

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    def test_config_parses_list_ids(self):
        """Test that list IDs are parsed correctly."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig, ClickUpRawJob

        config = ClickUpRawConfig(
            job_name="test_job", list_ids="12345678,87654321,11223344"
        )

        # Mock secrets client and API client
        with (
            patch(
                "jobs.raw.clickup_raw_job.ClickUpRawJob._load_clickup_credentials_from_secret"
            ),
            patch("jobs.raw.clickup_raw_job.ClickUpAPIClient"),
        ):
            job = ClickUpRawJob(config)
            assert len(job.list_ids) == 3
            assert "12345678" in job.list_ids
            assert "87654321" in job.list_ids
            assert "11223344" in job.list_ids


class TestClickUpRawJob:
    """Test ClickUp Raw Job logic."""

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    @patch(
        "jobs.raw.clickup_raw_job.ClickUpRawJob._load_clickup_credentials_from_secret"
    )
    @patch("jobs.raw.clickup_raw_job.ClickUpAPIClient")
    def test_job_initializes_with_list_ids(self, mock_client, mock_secrets):
        """Test that job initializes with parsed list IDs."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig, ClickUpRawJob

        config = ClickUpRawConfig(job_name="test_job", list_ids="12345678,87654321")

        job = ClickUpRawJob(config)

        assert len(job.list_ids) == 2
        assert "12345678" in job.list_ids
        assert "87654321" in job.list_ids

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    @patch(
        "jobs.raw.clickup_raw_job.ClickUpRawJob._load_clickup_credentials_from_secret"
    )
    @patch("jobs.raw.clickup_raw_job.ClickUpAPIClient")
    def test_job_fetches_custom_fields(self, mock_client_class, mock_secrets):
        """Test that job fetches custom fields for lists."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig, ClickUpRawJob

        # Mock API client
        mock_client = mock_client_class.return_value
        mock_client.get_list_custom_fields.return_value = [
            {"id": "abc123", "name": "external value (customer satisfaction) (emoji)"},
            {"id": "def456", "name": "🏠 Stakeholder Area (labels)"},
        ]

        config = ClickUpRawConfig(job_name="test_job", list_ids="12345678")
        job = ClickUpRawJob(config)

        # Test custom field fetching
        field_ids = job._get_custom_field_ids("12345678")

        assert len(field_ids) == 2
        assert "abc123" in field_ids
        assert "def456" in field_ids
        mock_client.get_list_custom_fields.assert_called_once_with("12345678")

    @patch.dict("os.environ", CLICKUP_TEST_ENV)
    @patch(
        "jobs.raw.clickup_raw_job.ClickUpRawJob._load_clickup_credentials_from_secret"
    )
    @patch("jobs.raw.clickup_raw_job.ClickUpAPIClient")
    def test_job_handles_empty_list_ids(self, mock_client, mock_secrets):
        """Test that job raises error for empty list IDs."""
        from jobs.raw.clickup_raw_job import ClickUpRawConfig, ClickUpRawJob

        config = ClickUpRawConfig(job_name="test_job", list_ids="")

        with pytest.raises(ValueError, match="No valid list IDs"):
            ClickUpRawJob(config)
