"""
Unit tests for Team Tailor Raw Job.

Tests focus on:
- Configuration validation and entity type handling
- API endpoint auto-configuration
- Date filtering logic for supported entities
- Pagination parameter building
- Data transformation logic
- S3 path construction

Fast execution without external dependencies (API, S3, Secrets Manager).
"""

from unittest.mock import patch

import pytest
from pydantic import ValidationError

# Test environment with required variables
TEAMTAILOR_TEST_ENV = {
    "SOURCE_NAME": "teamtailor",
    "PROJECT_NAME": "test_project",
    "WAREHOUSE_PATH": "s3://test-bucket/warehouse/",
    "RAW_ZONE_PATH": "s3://test-bucket/raw/",
    "TEAMTAILOR_API_SECRET_NAME": "test/teamtailor/api-token",
    "TEAMTAILOR_API_BASE_URL": "https://api.teamtailor.com",
    "ENVIRONMENT": "develop",
}


class TestTeamTailorRawConfig:
    """Test Team Tailor Raw configuration validation."""

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    def test_config_requires_entity_type(self):
        """Test that configuration requires entity_type parameter."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        # Should fail without entity_type
        with pytest.raises((ValidationError, TypeError)):
            TeamTailorRawConfig(job_name="test_job")

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    def test_config_with_valid_entity_type(self):
        """Test configuration with valid entity type."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        config = TeamTailorRawConfig(
            job_name="test_teamtailor_candidates", entity_type="candidates"
        )

        assert config.entity_type == "candidates"
        assert config.source_name == "teamtailor"
        assert config.api_base_url == "https://api.teamtailor.com"

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    def test_config_auto_generates_raw_write_path(self):
        """Test that raw_write_path is automatically generated."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        config = TeamTailorRawConfig(
            job_name="test_teamtailor_candidates", entity_type="candidates"
        )

        # Should auto-generate: raw_zone_path + teamtailor/{entity_type}/
        expected_path = "s3://test-bucket/raw/teamtailor/candidates/"
        assert config.raw_write_path == expected_path

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    def test_config_with_date_range(self):
        """Test configuration with start and end dates."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        config = TeamTailorRawConfig(
            job_name="test_teamtailor_candidates",
            entity_type="candidates",
            start_date="2025-01-01",
            end_date="2025-01-31",
        )

        assert config.start_date == "2025-01-01"
        assert config.end_date == "2025-01-31"

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    def test_config_rate_limit_defaults(self):
        """Test that rate limit has sensible defaults."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        config = TeamTailorRawConfig(job_name="test_job", entity_type="candidates")

        assert config.rate_limit_per_second == 2.0
        assert config.max_pages_per_batch == 20
        assert config.max_retries == 3
        assert config.batch_size == 1000


class TestTeamTailorEntityConfiguration:
    """Test Team Tailor entity configuration and validation."""

    def test_all_supported_entities_exist(self):
        """Test that all expected entities are configured."""
        from jobs.raw.teamtailor_raw_job import TEAMTAILOR_ENTITIES

        expected_entities = [
            "candidates",
            "jobs",
            "applications",
            "interviews",
            "users",
            "departments",
            "stages",
            "application_stage_transitions",
            "nps_responses",
        ]

        for entity in expected_entities:
            assert (
                entity in TEAMTAILOR_ENTITIES
            ), f"Entity '{entity}' not found in config"

    def test_entity_config_has_required_fields(self):
        """Test that each entity has required configuration fields."""
        from jobs.raw.teamtailor_raw_job import TEAMTAILOR_ENTITIES

        required_fields = [
            "s3_prefix",
            "supports_date_filter",
            "description",
        ]

        for entity_name, entity_config in TEAMTAILOR_ENTITIES.items():
            for field in required_fields:
                assert (
                    field in entity_config
                ), f"Entity '{entity_name}' missing required field '{field}'"

    def test_candidates_entity_supports_date_filtering(self):
        """Test that candidates entity supports date filtering."""
        from jobs.raw.teamtailor_raw_job import TEAMTAILOR_ENTITIES

        candidates_config = TEAMTAILOR_ENTITIES["candidates"]
        assert candidates_config["supports_date_filter"] is True
        assert candidates_config["endpoint"] == "/v1/candidates"

    def test_departments_entity_no_date_filtering(self):
        """Test that departments entity does not support date filtering."""
        from jobs.raw.teamtailor_raw_job import TEAMTAILOR_ENTITIES

        departments_config = TEAMTAILOR_ENTITIES["departments"]
        assert departments_config["supports_date_filter"] is False
        assert departments_config["endpoint"] == "/v1/departments"


class TestTeamTailorRawJobInitialization:
    """Test Team Tailor Raw Job initialization logic."""

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    def test_job_validates_entity_type(self, mock_load_creds):
        """Test that job validates entity type during initialization."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        config = TeamTailorRawConfig(job_name="test_job", entity_type="invalid_entity")

        # Should raise ValueError for invalid entity type
        with pytest.raises(ValueError) as exc_info:
            TeamTailorRawJob(config)

        assert "Invalid entity_type" in str(exc_info.value)
        assert "invalid_entity" in str(exc_info.value)

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    def test_job_auto_configures_endpoint(self, mock_load_creds):
        """Test that job auto-configures API endpoint based on entity type."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        config = TeamTailorRawConfig(job_name="test_job", entity_type="candidates")

        job = TeamTailorRawJob(config)

        # Should auto-configure endpoint from TEAMTAILOR_ENTITIES
        assert job.config.api_endpoint == "/v1/candidates"
        assert job.entity_config["endpoint"] == "/v1/candidates"

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    def test_job_respects_explicit_endpoint(self, mock_load_creds):
        """Test that explicit endpoint is not overridden."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        config = TeamTailorRawConfig(
            job_name="test_job",
            entity_type="candidates",
            api_endpoint="/v1/custom/endpoint",
        )

        job = TeamTailorRawJob(config)

        # Should keep explicit endpoint
        assert job.config.api_endpoint == "/v1/custom/endpoint"

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    def test_job_sets_date_filter_support_flag(self, mock_load_creds):
        """Test that job sets supports_date_filter flag correctly."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        # Candidates should support date filtering
        candidates_config = TeamTailorRawConfig(
            job_name="test_candidates", entity_type="candidates"
        )
        candidates_job = TeamTailorRawJob(candidates_config)
        assert candidates_job.supports_date_filter is True

        # Departments should NOT support date filtering
        departments_config = TeamTailorRawConfig(
            job_name="test_departments", entity_type="departments"
        )
        departments_job = TeamTailorRawJob(departments_config)
        assert departments_job.supports_date_filter is False


class TestTeamTailorDataExtraction:
    """Test data extraction and pagination logic."""

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    @patch("jobs.raw.teamtailor_raw_job.TeamTailorAPIClient")
    def test_fetch_data_uses_api_client(self, mock_client_class, mock_load_creds):
        """Test that API client is used for data fetching."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        # Mock API client
        mock_client = mock_client_class.return_value
        mock_client.get_candidates.return_value = iter(
            [{"id": "1", "type": "candidates"}]
        )

        config = TeamTailorRawConfig(
            job_name="test_job", entity_type="candidates", api_token="test-token"
        )
        job = TeamTailorRawJob(config)

        # Fetch data
        data_records, _ = job.fetch_data_with_pagination()

        # Verify API client was called
        assert mock_client.get_candidates.called
        assert len(data_records) == 1

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    @patch("jobs.raw.teamtailor_raw_job.TeamTailorAPIClient")
    def test_fetch_data_handles_pagination(self, mock_client_class, mock_load_creds):
        """Test that pagination is handled correctly."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        # Mock paginated responses
        mock_client = mock_client_class.return_value
        mock_client.get_candidates.return_value = iter(
            [
                {"id": "1", "type": "candidates"},
                {"id": "2", "type": "candidates"},
                {"id": "3", "type": "candidates"},
            ]
        )

        config = TeamTailorRawConfig(
            job_name="test_job", entity_type="candidates", api_token="test-token"
        )
        job = TeamTailorRawJob(config)

        # Fetch data
        data_records, _ = job.fetch_data_with_pagination()

        # Should have all records
        assert len(data_records) == 3

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    @patch("jobs.raw.teamtailor_raw_job.TeamTailorAPIClient")
    def test_fetch_data_respects_max_records(self, mock_client_class, mock_load_creds):
        """Test that max_pages_per_batch limit is respected."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        # Mock infinite data stream
        def infinite_candidates():
            i = 0
            while True:
                yield {"id": f"candidate_{i}", "type": "candidates"}
                i += 1

        mock_client = mock_client_class.return_value
        mock_client.get_candidates.return_value = infinite_candidates()

        config = TeamTailorRawConfig(
            job_name="test_job",
            entity_type="candidates",
            api_token="test-token",
            max_pages_per_batch=5,  # Limit to 5 pages
        )
        job = TeamTailorRawJob(config)

        # Fetch data
        data_records, _ = job.fetch_data_with_pagination()

        # Should stop at max_pages_per_batch * page_size
        max_records = config.max_pages_per_batch * job.entity_config["page_size"]
        assert len(data_records) <= max_records


class TestTeamTailorDataTransformation:
    """Test data transformation logic."""

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    @patch(
        "jobs.raw.teamtailor_raw_job.TeamTailorRawJob._load_teamtailor_credentials_from_secret"
    )
    def test_transform_preserves_json_api_structure(self, mock_load_creds):
        """Test that transform preserves JSON API structure."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig, TeamTailorRawJob

        config = TeamTailorRawConfig(job_name="test_job", entity_type="candidates")
        job = TeamTailorRawJob(config)

        # Input data in JSON API format
        input_records = [
            {
                "id": "1",
                "type": "candidates",
                "attributes": {"first_name": "John", "email": "john@example.com"},
            },
            {
                "id": "2",
                "type": "candidates",
                "attributes": {"first_name": "Jane", "email": "jane@example.com"},
            },
        ]

        # Transform
        transformed, _ = job.transform((input_records, []))

        # Should preserve JSON API structure
        assert len(transformed) == 2
        assert transformed[0]["id"] == "1"
        assert transformed[0]["type"] == "candidates"
        assert "attributes" in transformed[0]


class TestTeamTailorS3PathGeneration:
    """Test S3 path construction logic."""

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    def test_raw_write_path_includes_entity_type(self):
        """Test that S3 path includes entity type."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        config = TeamTailorRawConfig(job_name="test_job", entity_type="candidates")

        # Path should be: raw_zone_path + teamtailor/candidates/
        assert config.raw_write_path is not None
        assert "teamtailor/candidates/" in config.raw_write_path
        assert config.raw_write_path.startswith("s3://test-bucket/raw/")

    @patch.dict("os.environ", TEAMTAILOR_TEST_ENV)
    def test_raw_write_path_ends_with_slash(self):
        """Test that S3 path always ends with slash."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        config = TeamTailorRawConfig(job_name="test_job", entity_type="jobs")

        assert config.raw_write_path is not None
        assert config.raw_write_path.endswith("/")

    @patch.dict(
        "os.environ", {**TEAMTAILOR_TEST_ENV, "RAW_ZONE_PATH": "s3://bucket/raw"}
    )
    def test_raw_write_path_handles_missing_trailing_slash(self):
        """Test that path construction works even if raw_zone_path lacks trailing slash."""
        from jobs.raw.teamtailor_raw_job import TeamTailorRawConfig

        config = TeamTailorRawConfig(job_name="test_job", entity_type="applications")

        # Should still generate valid path
        assert config.raw_write_path is not None
        assert "teamtailor/applications/" in config.raw_write_path
