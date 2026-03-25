"""
Integration tests for Team Tailor Bronze layer processing.

These tests verify that the Bronze layer components work together correctly,
including Spark operations, Iceberg table creation, and data transformations.
"""

import pytest


class TestTeamTailorBronzeLayerIntegration:
    """Integration tests for Team Tailor Bronze layer."""

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.bronze
    def test_bronze_table_creation(self, local_spark_with_iceberg, test_env_vars):
        """Test Bronze table creation with Iceberg."""
        spark = local_spark_with_iceberg

        # Create test database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        # Test table creation
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor__talent__candidates_bronze (
            candidate_id STRING NOT NULL,
            email STRING,
            first_name STRING,
            last_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """

        spark.sql(create_table_sql)

        # Verify table exists
        tables = spark.sql("SHOW TABLES IN test_bronze_zone").collect()
        table_names = [row.tableName for row in tables]
        assert "teamtailor__talent__candidates_bronze" in table_names

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.bronze
    def test_bronze_data_transformation(
        self, local_spark_with_iceberg, teamtailor_raw_candidates_data
    ):
        """Test Bronze data transformation from Raw format."""
        spark = local_spark_with_iceberg

        # Create test database and table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor__talent__candidates_bronze (
            candidate_id STRING NOT NULL,
            email STRING,
            first_name STRING,
            last_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_table_sql)

        # Create test data DataFrame
        test_data = []
        for candidate in teamtailor_raw_candidates_data:
            test_data.append(
                {
                    "candidate_id": candidate["id"],
                    "email": candidate["attributes"].get("email", ""),
                    "first_name": candidate["attributes"].get("first-name", ""),
                    "last_name": candidate["attributes"].get("last-name", ""),
                    "created_at": candidate["attributes"].get("created-at", ""),
                    "updated_at": candidate["attributes"].get("updated-at", ""),
                    "source": "teamtailor",
                    "ingestion_ts": "2024-01-15T10:30:00Z",
                    "payload": str(candidate),  # Simplified for test
                    "metadata": '{"test": "metadata"}',
                }
            )

        df = spark.createDataFrame(test_data)

        # Insert data into Bronze table
        df.writeTo("test_bronze_zone.teamtailor__talent__candidates_bronze").append()

        # Verify data was inserted
        result = spark.sql(
            "SELECT COUNT(*) as count FROM test_bronze_zone.teamtailor__talent__candidates_bronze"
        )
        count = result.collect()[0]["count"]
        assert count == len(teamtailor_raw_candidates_data)

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.bronze
    def test_bronze_schema_validation(
        self, local_spark_with_iceberg, teamtailor_bronze_candidates_schema
    ):
        """Test Bronze schema validation."""
        spark = local_spark_with_iceberg

        # Create test table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor__talent__candidates_bronze (
            candidate_id STRING NOT NULL,
            email STRING,
            first_name STRING,
            last_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_table_sql)

        # Get actual schema
        actual_schema = spark.table(
            "test_bronze_zone.teamtailor__talent__candidates_bronze"
        ).schema

        # Verify schema fields
        actual_field_names = [field.name for field in actual_schema.fields]
        expected_field_names = list(teamtailor_bronze_candidates_schema.keys())

        for field_name in expected_field_names:
            assert field_name in actual_field_names

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.bronze
    def test_bronze_data_quality_checks(self, local_spark_with_iceberg):
        """Test Bronze data quality validation."""
        spark = local_spark_with_iceberg

        # Create test table with some invalid data
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor__talent__candidates_bronze (
            candidate_id STRING NOT NULL,
            email STRING,
            first_name STRING,
            last_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_table_sql)

        # Insert test data with some quality issues
        test_data = [
            {
                "candidate_id": "valid_candidate_001",
                "email": "valid@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:30:00Z",
                "source": "teamtailor",
                "ingestion_ts": "2024-01-15T10:30:00Z",
                "payload": "valid_payload",
                "metadata": "valid_metadata",
            },
            {
                "candidate_id": "invalid_candidate_002",
                "email": "invalid-email",  # Invalid email format
                "first_name": "Jane",
                "last_name": "Smith",
                "created_at": None,  # Missing timestamp
                "updated_at": "2024-01-15T10:30:00Z",
                "source": "teamtailor",
                "ingestion_ts": "2024-01-15T10:30:00Z",
                "payload": "invalid_payload",
                "metadata": "invalid_metadata",
            },
        ]

        df = spark.createDataFrame(test_data)
        df.writeTo("test_bronze_zone.teamtailor__talent__candidates_bronze").append()

        # Test data quality queries
        # Count valid emails
        valid_email_count = spark.sql(
            """
            SELECT COUNT(*) as count
            FROM test_bronze_zone.teamtailor__talent__candidates_bronze
            WHERE email LIKE '%@%.%'
        """
        ).collect()[0]["count"]

        assert valid_email_count == 1

        # Count records with missing timestamps
        missing_timestamp_count = spark.sql(
            """
            SELECT COUNT(*) as count
            FROM test_bronze_zone.teamtailor__talent__candidates_bronze
            WHERE created_at IS NULL
        """
        ).collect()[0]["count"]

        assert missing_timestamp_count == 1


class TestTeamTailorRawToBronzeIntegration:
    """Integration tests for Raw to Bronze data flow."""

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.raw
    @pytest.mark.bronze
    def test_raw_to_bronze_data_flow(
        self, local_spark_with_iceberg, teamtailor_raw_candidates_data
    ):
        """Test complete Raw to Bronze data flow."""
        spark = local_spark_with_iceberg

        # Create databases
        spark.sql("CREATE DATABASE IF NOT EXISTS test_raw_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        # Create Raw external table
        create_raw_table_sql = """
        CREATE TABLE IF NOT EXISTS test_raw_zone.teamtailor_candidates_raw (
            payload STRING,
            metadata STRING
        ) USING json
        """
        spark.sql(create_raw_table_sql)

        # Create Bronze Iceberg table
        create_bronze_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor__talent__candidates_bronze (
            candidate_id STRING NOT NULL,
            email STRING,
            first_name STRING,
            last_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_bronze_table_sql)

        # Insert test data into Raw table
        raw_data = []
        for candidate in teamtailor_raw_candidates_data:
            raw_data.append(
                {
                    "payload": str(candidate),
                    "metadata": '{"source": "teamtailor", "ingestion_time": "2024-01-15T10:30:00Z"}',
                }
            )

        raw_df = spark.createDataFrame(raw_data)
        raw_df.writeTo("test_raw_zone.teamtailor_candidates_raw").append()

        # Transform Raw to Bronze (simplified transformation)
        bronze_data = []
        for candidate in teamtailor_raw_candidates_data:
            bronze_data.append(
                {
                    "candidate_id": candidate["id"],
                    "email": candidate["attributes"].get("email", ""),
                    "first_name": candidate["attributes"].get("first-name", ""),
                    "last_name": candidate["attributes"].get("last-name", ""),
                    "created_at": candidate["attributes"].get("created-at", ""),
                    "updated_at": candidate["attributes"].get("updated-at", ""),
                    "source": "teamtailor",
                    "ingestion_ts": "2024-01-15T10:30:00Z",
                    "payload": str(candidate),
                    "metadata": '{"source": "teamtailor", "ingestion_time": "2024-01-15T10:30:00Z"}',
                }
            )

        bronze_df = spark.createDataFrame(bronze_data)
        bronze_df.writeTo(
            "test_bronze_zone.teamtailor__talent__candidates_bronze"
        ).append()

        # Verify data flow
        raw_count = spark.sql(
            "SELECT COUNT(*) as count FROM test_raw_zone.teamtailor_candidates_raw"
        ).collect()[0]["count"]
        bronze_count = spark.sql(
            "SELECT COUNT(*) as count FROM test_bronze_zone.teamtailor__talent__candidates_bronze"
        ).collect()[0]["count"]

        assert raw_count == len(teamtailor_raw_candidates_data)
        assert bronze_count == len(teamtailor_raw_candidates_data)
        assert raw_count == bronze_count  # Data preservation

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.raw
    @pytest.mark.bronze
    def test_data_lineage_tracking(self, local_spark_with_iceberg):
        """Test data lineage tracking from Raw to Bronze."""
        spark = local_spark_with_iceberg

        # Create test tables
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor__talent__candidates_bronze (
            candidate_id STRING NOT NULL,
            email STRING,
            first_name STRING,
            last_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_table_sql)

        # Insert test data with lineage information
        test_data = [
            {
                "candidate_id": "test_candidate_001",
                "email": "test@example.com",
                "first_name": "Test",
                "last_name": "Candidate",
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:30:00Z",
                "source": "teamtailor",
                "ingestion_ts": "2024-01-15T10:30:00Z",
                "payload": "test_payload",
                "metadata": '{"raw_table": "teamtailor_candidates_raw", "raw_file": "candidates_001.jsonl", "etl_job": "teamtailor_bronze_job"}',
            }
        ]

        df = spark.createDataFrame(test_data)
        df.writeTo("test_bronze_zone.teamtailor__talent__candidates_bronze").append()

        # Verify lineage information is preserved
        lineage_query = """
        SELECT
            candidate_id,
            source,
            ingestion_ts,
            get_json_object(metadata, '$.raw_table') as raw_table,
            get_json_object(metadata, '$.raw_file') as raw_file,
            get_json_object(metadata, '$.etl_job') as etl_job
        FROM test_bronze_zone.teamtailor__talent__candidates_bronze
        """

        result = spark.sql(lineage_query).collect()[0]

        assert result["source"] == "teamtailor"
        assert result["raw_table"] == "teamtailor_candidates_raw"
        assert result["raw_file"] == "candidates_001.jsonl"
        assert result["etl_job"] == "teamtailor_bronze_job"
