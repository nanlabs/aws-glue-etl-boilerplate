"""
Integration tests for complete Team Tailor pipeline.

These tests verify that the entire Team Tailor pipeline (Raw → Bronze → Silver)
works correctly end-to-end with real Spark operations.
"""

import pytest


class TestTeamTailorPipelineIntegration:
    """Integration tests for complete Team Tailor pipeline."""

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.e2e
    def test_complete_pipeline_candidates(
        self, local_spark_with_iceberg, teamtailor_raw_candidates_data
    ):
        """Test complete pipeline for candidates: Raw → Bronze → Silver."""
        spark = local_spark_with_iceberg

        # Create all databases
        spark.sql("CREATE DATABASE IF NOT EXISTS test_raw_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_silver_zone")

        # ============================================================================
        # STEP 1: Create Raw table and insert data
        # ============================================================================

        create_raw_table_sql = """
        CREATE TABLE IF NOT EXISTS test_raw_zone.teamtailor_candidates_raw (
            payload STRING,
            metadata STRING
        ) USING json
        """
        spark.sql(create_raw_table_sql)

        # Insert Raw data
        raw_data = []
        for candidate in teamtailor_raw_candidates_data:
            import json

            raw_data.append(
                {
                    "payload": json.dumps(candidate),
                    "metadata": '{"source": "teamtailor", "ingestion_time": "2024-01-15T10:30:00Z"}',
                }
            )

        raw_df = spark.createDataFrame(raw_data)
        raw_df.writeTo("test_raw_zone.teamtailor_candidates_raw").append()

        # ============================================================================
        # STEP 2: Create Bronze table and transform data
        # ============================================================================

        create_bronze_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor_candidates_bronze (
            id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            sourced_by_user_id STRING,
            status STRING,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_bronze_table_sql)

        # Transform Raw to Bronze
        bronze_data = []
        for candidate in teamtailor_raw_candidates_data:
            import json

            attributes = candidate.get("attributes", {})
            relationships = candidate.get("relationships", {})
            sourced_by_user = relationships.get("sourced-by-user", {}).get("data", {})

            bronze_data.append(
                {
                    "id": candidate["id"],
                    "first_name": attributes.get("first_name"),
                    "last_name": attributes.get("last_name"),
                    "email": attributes.get("email"),
                    "phone": attributes.get("phone"),
                    "created_at": attributes.get("created_at"),
                    "updated_at": attributes.get("updated_at"),
                    "sourced_by_user_id": (
                        sourced_by_user.get("id") if sourced_by_user else None
                    ),
                    "status": attributes.get("status"),
                    "source": "teamtailor",
                    "ingestion_ts": "2024-01-15T10:30:00Z",
                    "payload": json.dumps(candidate),
                    "metadata": '{"source": "teamtailor", "ingestion_time": "2024-01-15T10:30:00Z"}',
                }
            )

        bronze_df = spark.createDataFrame(bronze_data)
        bronze_df.writeTo("test_bronze_zone.teamtailor_candidates_bronze").append()

        # ============================================================================
        # STEP 3: Create Silver table and transform data
        # ============================================================================

        create_silver_table_sql = """
        CREATE TABLE IF NOT EXISTS test_silver_zone.talent_dim_candidate_profiles (
            candidate_id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            candidate_status STRING,
            profile_created_at TIMESTAMP,
            profile_updated_at TIMESTAMP,
            sourced_by_user_id STRING,
            full_name STRING,
            is_active_candidate BOOLEAN,
            silver_ingestion_timestamp TIMESTAMP,
            source STRING,
            etl_job_name STRING,
            record_hash STRING
        ) USING iceberg
        """
        spark.sql(create_silver_table_sql)

        # Transform Bronze to Silver
        silver_data = []
        for candidate in teamtailor_raw_candidates_data:
            attributes = candidate.get("attributes", {})
            first_name = attributes.get("first_name", "")
            last_name = attributes.get("last_name", "")
            full_name = f"{first_name} {last_name}".strip()
            status = attributes.get("status", "")

            silver_data.append(
                {
                    "candidate_id": candidate["id"],
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": attributes.get("email"),
                    "phone": attributes.get("phone"),
                    "candidate_status": status,
                    "profile_created_at": attributes.get("created_at"),
                    "profile_updated_at": attributes.get("updated_at"),
                    "sourced_by_user_id": candidate.get("relationships", {})
                    .get("sourced-by-user", {})
                    .get("data", {})
                    .get("id"),
                    "full_name": full_name,
                    "is_active_candidate": status == "active",
                    "silver_ingestion_timestamp": "2024-01-15T10:35:00Z",
                    "source": "teamtailor",
                    "etl_job_name": "teamtailor_silver_candidates",
                    "record_hash": f"hash_{candidate['id']}",
                }
            )

        silver_df = spark.createDataFrame(silver_data)
        silver_df.writeTo("test_silver_zone.talent_dim_candidate_profiles").append()

        # ============================================================================
        # STEP 4: Verify pipeline results
        # ============================================================================

        # Verify record counts
        raw_count = spark.sql(
            "SELECT COUNT(*) as count FROM test_raw_zone.teamtailor_candidates_raw"
        ).collect()[0]["count"]
        bronze_count = spark.sql(
            "SELECT COUNT(*) as count FROM test_bronze_zone.teamtailor_candidates_bronze"
        ).collect()[0]["count"]
        silver_count = spark.sql(
            "SELECT COUNT(*) as count FROM test_silver_zone.talent_dim_candidate_profiles"
        ).collect()[0]["count"]

        assert raw_count == len(teamtailor_raw_candidates_data)
        assert bronze_count == len(teamtailor_raw_candidates_data)
        assert silver_count == len(teamtailor_raw_candidates_data)

        # Verify data quality in Silver layer
        quality_stats = spark.sql(
            """
            SELECT
                COUNT(CASE WHEN is_active_candidate = true THEN 1 END) as active_count,
                COUNT(CASE WHEN full_name IS NOT NULL AND full_name != '' THEN 1 END) as has_name_count,
                COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as has_email_count
            FROM test_silver_zone.talent_dim_candidate_profiles
        """
        ).collect()[0]

        assert quality_stats["has_email_count"] > 0
        assert quality_stats["has_name_count"] > 0

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.e2e
    def test_pipeline_data_lineage(self, local_spark_with_iceberg):
        """Test data lineage tracking through the complete pipeline."""
        spark = local_spark_with_iceberg

        # Create databases
        spark.sql("CREATE DATABASE IF NOT EXISTS test_raw_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_silver_zone")

        # Create tables with lineage tracking
        create_raw_table_sql = """
        CREATE TABLE IF NOT EXISTS test_raw_zone.teamtailor_candidates_raw (
            payload STRING,
            metadata STRING
        ) USING json
        """
        spark.sql(create_raw_table_sql)

        create_bronze_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor_candidates_bronze (
            id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            sourced_by_user_id STRING,
            status STRING,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_bronze_table_sql)

        create_silver_table_sql = """
        CREATE TABLE IF NOT EXISTS test_silver_zone.talent_dim_candidate_profiles (
            candidate_id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            candidate_status STRING,
            profile_created_at TIMESTAMP,
            profile_updated_at TIMESTAMP,
            sourced_by_user_id STRING,
            full_name STRING,
            is_active_candidate BOOLEAN,
            silver_ingestion_timestamp TIMESTAMP,
            source STRING,
            etl_job_name STRING,
            record_hash STRING
        ) USING iceberg
        """
        spark.sql(create_silver_table_sql)

        # Insert test data with lineage
        import json

        test_candidate = {
            "id": "candidate_lineage_test",
            "type": "candidates",
            "attributes": {
                "first_name": "Lineage",
                "last_name": "Test",
                "email": "lineage@example.com",
                "phone": "+1234567890",
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:30:00Z",
                "status": "active",
            },
            "relationships": {
                "sourced-by-user": {"data": {"id": "user_001", "type": "users"}}
            },
        }

        # Raw data
        raw_data = [
            {
                "payload": json.dumps(test_candidate),
                "metadata": '{"raw_file": "candidates_lineage.jsonl", "raw_job": "teamtailor_raw_candidates", "ingestion_time": "2024-01-15T10:30:00Z"}',
            }
        ]

        raw_df = spark.createDataFrame(raw_data)
        raw_df.writeTo("test_raw_zone.teamtailor_candidates_raw").append()

        # Bronze data
        bronze_data = [
            {
                "id": test_candidate["id"],
                "first_name": test_candidate["attributes"]["first_name"],
                "last_name": test_candidate["attributes"]["last_name"],
                "email": test_candidate["attributes"]["email"],
                "phone": test_candidate["attributes"]["phone"],
                "created_at": test_candidate["attributes"]["created_at"],
                "updated_at": test_candidate["attributes"]["updated_at"],
                "sourced_by_user_id": test_candidate["relationships"][
                    "sourced-by-user"
                ]["data"]["id"],
                "status": test_candidate["attributes"]["status"],
                "source": "teamtailor",
                "ingestion_ts": "2024-01-15T10:30:00Z",
                "payload": json.dumps(test_candidate),
                "metadata": '{"raw_file": "candidates_lineage.jsonl", "raw_job": "teamtailor_raw_candidates", "bronze_job": "teamtailor_bronze_candidates", "bronze_time": "2024-01-15T10:32:00Z"}',
            }
        ]

        bronze_df = spark.createDataFrame(bronze_data)
        bronze_df.writeTo("test_bronze_zone.teamtailor_candidates_bronze").append()

        # Silver data
        silver_data = [
            {
                "candidate_id": test_candidate["id"],
                "first_name": test_candidate["attributes"]["first_name"],
                "last_name": test_candidate["attributes"]["last_name"],
                "email": test_candidate["attributes"]["email"],
                "phone": test_candidate["attributes"]["phone"],
                "candidate_status": test_candidate["attributes"]["status"],
                "profile_created_at": test_candidate["attributes"]["created_at"],
                "profile_updated_at": test_candidate["attributes"]["updated_at"],
                "sourced_by_user_id": test_candidate["relationships"][
                    "sourced-by-user"
                ]["data"]["id"],
                "full_name": f"{test_candidate['attributes']['first_name']} {test_candidate['attributes']['last_name']}",
                "is_active_candidate": test_candidate["attributes"]["status"]
                == "active",
                "silver_ingestion_timestamp": "2024-01-15T10:35:00Z",
                "source": "teamtailor",
                "etl_job_name": "teamtailor_silver_candidates",
                "record_hash": f"hash_{test_candidate['id']}",
            }
        ]

        silver_df = spark.createDataFrame(silver_data)
        silver_df.writeTo("test_silver_zone.talent_dim_candidate_profiles").append()

        # Verify lineage tracking
        lineage_query = """
        SELECT
            candidate_id,
            email,
            profile_created_at,
            profile_updated_at,
            silver_ingestion_timestamp,
            source
        FROM test_silver_zone.talent_dim_candidate_profiles
        WHERE candidate_id = 'candidate_lineage_test'
        """

        result = spark.sql(lineage_query).collect()[0]

        assert result["candidate_id"] == "candidate_lineage_test"
        assert result["email"] == "lineage@example.com"
        assert result["source"] == "teamtailor"
        assert result["silver_ingestion_timestamp"] is not None

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.e2e
    def test_pipeline_error_handling(self, local_spark_with_iceberg):
        """Test error handling in the pipeline."""
        spark = local_spark_with_iceberg

        # Create databases
        spark.sql("CREATE DATABASE IF NOT EXISTS test_raw_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        # Create tables
        create_raw_table_sql = """
        CREATE TABLE IF NOT EXISTS test_raw_zone.teamtailor_candidates_raw (
            payload STRING,
            metadata STRING
        ) USING json
        """
        spark.sql(create_raw_table_sql)

        create_bronze_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor_candidates_bronze (
            id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            sourced_by_user_id STRING,
            status STRING,
            source STRING,
            ingestion_ts TIMESTAMP,
            payload STRING,
            metadata STRING
        ) USING iceberg
        """
        spark.sql(create_bronze_table_sql)

        # Test with malformed data
        import json

        malformed_data = [
            {"payload": "invalid_json", "metadata": '{"source": "teamtailor"}'},
            {
                "payload": json.dumps(
                    {
                        "id": "valid_candidate",
                        "type": "candidates",
                        "attributes": {"email": "valid@example.com"},
                    }
                ),
                "metadata": '{"source": "teamtailor"}',
            },
        ]

        raw_df = spark.createDataFrame(malformed_data)
        raw_df.writeTo("test_raw_zone.teamtailor_candidates_raw").append()

        # Verify that valid data is processed correctly
        valid_count = spark.sql(
            """
            SELECT COUNT(*) as count
            FROM test_raw_zone.teamtailor_candidates_raw
            WHERE payload LIKE '%valid_candidate%'
        """
        ).collect()[0]["count"]

        assert valid_count == 1
