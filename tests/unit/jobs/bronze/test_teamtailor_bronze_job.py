"""
Unit tests for Team Tailor Bronze Job.

Tests cover:
- Entity configuration with multiple entity types
- Partition configuration per entity
- Merge strategies (append vs merge_by_id)
- Table name auto-configuration
- Entity validation

Focus: Multi-entity business logic, not Spark infrastructure.
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


# ==========================================
# TEST: ENTITY CONFIGURATION
# ==========================================
class TestTeamTailorEntityConfiguration:
    """Test entity-specific configuration lookup."""

    def test_all_entities_have_required_fields(self):
        """Each entity config should have all required fields."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        required_fields = {
            "partitioned",
            "partition_cols",
            "merge_strategy",
            "raw_table_name",
            "bronze_table_name",
            "merge_key",
            "key_fields",
            "description",
        }

        for entity_name, entity_config in TEAMTAILOR_BRONZE_ENTITIES.items():
            assert (
                set(entity_config.keys()) == required_fields
            ), f"Entity '{entity_name}' missing required fields"

    def test_applications_entity_is_partitioned(self):
        """Applications entity should be partitioned by year/month."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        applications_config = TEAMTAILOR_BRONZE_ENTITIES["applications"]

        assert applications_config["partitioned"] is True
        assert applications_config["partition_cols"] == ["year", "month"]

    def test_candidates_entity_not_partitioned(self):
        """Candidates entity should not be partitioned."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        candidates_config = TEAMTAILOR_BRONZE_ENTITIES["candidates"]

        assert candidates_config["partitioned"] is False
        assert candidates_config["partition_cols"] == []

    def test_all_entities_have_key_fields(self):
        """Each entity should have key fields defined."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        for entity_name, entity_config in TEAMTAILOR_BRONZE_ENTITIES.items():
            assert (
                len(entity_config["key_fields"]) > 0
            ), f"Entity '{entity_name}' has no key fields"


# ==========================================
# TEST: MERGE STRATEGIES
# ==========================================
class TestTeamTailorMergeStrategies:
    """Test merge strategies by entity type."""

    def test_applications_uses_append_strategy(self):
        """Applications should use append (no merge)."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        assert TEAMTAILOR_BRONZE_ENTITIES["applications"]["merge_strategy"] == "append"
        assert TEAMTAILOR_BRONZE_ENTITIES["applications"]["merge_key"] is None

    def test_candidates_uses_merge_by_id(self):
        """Candidates should use merge_by_id strategy."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        assert (
            TEAMTAILOR_BRONZE_ENTITIES["candidates"]["merge_strategy"] == "merge_by_id"
        )
        assert TEAMTAILOR_BRONZE_ENTITIES["candidates"]["merge_key"] == "id"

    def test_jobs_uses_merge_by_id(self):
        """Jobs should use merge_by_id strategy."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        assert TEAMTAILOR_BRONZE_ENTITIES["jobs"]["merge_strategy"] == "merge_by_id"
        assert TEAMTAILOR_BRONZE_ENTITIES["jobs"]["merge_key"] == "id"

    def test_merge_entities_have_merge_key(self):
        """All merge_by_id entities must have merge_key."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        for entity_name, entity_config in TEAMTAILOR_BRONZE_ENTITIES.items():
            if entity_config["merge_strategy"] == "merge_by_id":
                assert (
                    entity_config["merge_key"] is not None
                ), f"Entity '{entity_name}' has merge_by_id but no merge_key"


# ==========================================
# TEST: TABLE NAME GENERATION
# ==========================================
class TestTeamTailorTableNames:
    """Test table name patterns."""

    def test_candidates_table_names_follow_pattern(self):
        """Candidates table names should follow naming convention."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        candidates = TEAMTAILOR_BRONZE_ENTITIES["candidates"]

        assert candidates["raw_table_name"] == "teamtailor__talent__candidates_raw"
        assert (
            candidates["bronze_table_name"] == "teamtailor__talent__candidates_bronze"
        )

    def test_jobs_table_names_follow_pattern(self):
        """Jobs table names should follow naming convention."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        jobs = TEAMTAILOR_BRONZE_ENTITIES["jobs"]

        assert jobs["raw_table_name"] == "teamtailor__talent__jobs_raw"
        assert jobs["bronze_table_name"] == "teamtailor__talent__jobs_bronze"

    def test_all_table_names_have_raw_bronze_suffix(self):
        """All table names should end with _raw or _bronze."""
        from jobs.bronze.teamtailor_bronze_job import TEAMTAILOR_BRONZE_ENTITIES

        for entity_name, entity_config in TEAMTAILOR_BRONZE_ENTITIES.items():
            assert entity_config["raw_table_name"].endswith(
                "_raw"
            ), f"Entity '{entity_name}' raw table doesn't end with _raw"
            assert entity_config["bronze_table_name"].endswith(
                "_bronze"
            ), f"Entity '{entity_name}' bronze table doesn't end with _bronze"


# ==========================================
# TEST: JOB INITIALIZATION
# ==========================================
class TestTeamTailorBronzeJobInitialization:
    """Test job initialization with entity validation."""

    @patch.dict("os.environ", TEST_ENV)
    def test_initializes_with_valid_entity(self):
        """Job should initialize with valid entity type."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job", source_name="teamtailor", entity_type="candidates"
        )

        mock_context = MagicMock()
        job = TeamTailorBronzeJob(mock_context, config)

        assert job.entity_type == "candidates"
        assert job.entity_config["partitioned"] is False

    @patch.dict("os.environ", TEST_ENV)
    def test_rejects_invalid_entity_type(self):
        """Job should reject invalid entity types."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job",
            source_name="teamtailor",
            entity_type="invalid_entity",
        )

        mock_context = MagicMock()

        try:
            TeamTailorBronzeJob(mock_context, config)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Invalid entity_type 'invalid_entity'" in str(e)

    @patch.dict("os.environ", TEST_ENV)
    def test_loads_entity_configuration(self):
        """Job should load correct entity configuration."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job", source_name="teamtailor", entity_type="applications"
        )

        mock_context = MagicMock()
        job = TeamTailorBronzeJob(mock_context, config)

        assert job.entity_config["merge_strategy"] == "append"
        assert job.entity_config["partitioned"] is True


# ==========================================
# TEST: PARTITION PROPERTY
# ==========================================
class TestTeamTailorPartitionProperty:
    """Test is_partitioned property."""

    @patch.dict("os.environ", TEST_ENV)
    def test_applications_is_partitioned(self):
        """Applications entity should be partitioned."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job", source_name="teamtailor", entity_type="applications"
        )

        mock_context = MagicMock()
        job = TeamTailorBronzeJob(mock_context, config)

        assert job.is_partitioned is True

    @patch.dict("os.environ", TEST_ENV)
    def test_candidates_not_partitioned(self):
        """Candidates entity should not be partitioned."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job", source_name="teamtailor", entity_type="candidates"
        )

        mock_context = MagicMock()
        job = TeamTailorBronzeJob(mock_context, config)

        assert job.is_partitioned is False


# ==========================================
# TEST: DATA EXTRACTION
# ==========================================
class TestTeamTailorDataExtraction:
    """Test data extraction logic."""

    @patch.dict("os.environ", TEST_ENV)
    def test_extract_calls_read_external_table(self):
        """Extract should call read_external_table with correct params."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job", source_name="teamtailor", entity_type="applications"
        )

        mock_context = MagicMock()
        job = TeamTailorBronzeJob(mock_context, config)
        job.read_external_table = MagicMock(return_value=MagicMock())

        job.extract()

        job.read_external_table.assert_called_once()
        call_args = job.read_external_table.call_args
        assert call_args[1]["is_partitioned"] is True

    @patch.dict("os.environ", TEST_ENV)
    def test_extract_respects_partition_flag(self):
        """Extract should use partition flag from entity config."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job",
            source_name="teamtailor",
            entity_type="candidates",  # not partitioned
        )

        mock_context = MagicMock()
        job = TeamTailorBronzeJob(mock_context, config)
        job.read_external_table = MagicMock(return_value=MagicMock())

        job.extract()

        call_args = job.read_external_table.call_args
        assert call_args[1]["is_partitioned"] is False


# ==========================================
# TEST: TRANSFORMATION
# ==========================================
class TestTeamTailorTransformation:
    """Test transformation logic."""

    @patch.dict("os.environ", TEST_ENV)
    def test_handles_empty_dataframe(self):
        """Transform should handle empty DataFrames."""
        from jobs.bronze.teamtailor_bronze_job import (
            TeamTailorBronzeConfig,
            TeamTailorBronzeJob,
        )

        config = TeamTailorBronzeConfig(
            job_name="test_job", source_name="teamtailor", entity_type="candidates"
        )

        mock_context = MagicMock()
        job = TeamTailorBronzeJob(mock_context, config)

        mock_df = MagicMock()
        mock_df.count.return_value = 0

        result = job.transform(mock_df)

        # Should return original DataFrame
        assert result == mock_df
