"""
Team Tailor-specific test fixtures and data generators.

This module provides fixtures and utilities specific to testing Team Tailor data processing
across all Medallion layers (Raw, Bronze, Silver, Gold).
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

# ============================================================================
# Fixture Data Paths
# ============================================================================


def get_fixture_path(fixture_name: str) -> Path:
    """Get the path to a fixture file."""
    return Path(__file__).parent / fixture_name


# ============================================================================
# Raw Layer Fixtures
# ============================================================================


@pytest.fixture(scope="function")
def teamtailor_raw_candidates_data() -> List[Dict[str, Any]]:
    """Load sample Team Tailor candidates data from fixtures."""
    fixture_path = get_fixture_path("raw/candidates_sample.jsonl")

    candidates = []
    with open(fixture_path, "r") as f:
        for line in f:
            candidates.append(json.loads(line.strip()))

    return candidates


@pytest.fixture(scope="function")
def teamtailor_raw_jobs_data() -> List[Dict[str, Any]]:
    """Load sample Team Tailor jobs data from fixtures."""
    fixture_path = get_fixture_path("raw/jobs_sample.jsonl")

    jobs = []
    with open(fixture_path, "r") as f:
        for line in f:
            jobs.append(json.loads(line.strip()))

    return jobs


@pytest.fixture(scope="function")
def teamtailor_raw_applications_data() -> List[Dict[str, Any]]:
    """Load sample Team Tailor applications data from fixtures."""
    fixture_path = get_fixture_path("raw/applications_sample.jsonl")

    applications = []
    with open(fixture_path, "r") as f:
        for line in f:
            applications.append(json.loads(line.strip()))

    return applications


# ============================================================================
# Bronze Layer Fixtures
# ============================================================================


@pytest.fixture(scope="function")
def teamtailor_bronze_candidates_schema() -> Dict[str, str]:
    """Expected Bronze schema for Team Tailor candidates."""
    return {
        "id": "string",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "phone": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "sourced_by_user_id": "string",
        "status": "string",
        "source": "string",
        "ingestion_ts": "timestamp",
        "payload": "string",
        "metadata": "string",
    }


@pytest.fixture(scope="function")
def teamtailor_bronze_jobs_schema() -> Dict[str, str]:
    """Expected Bronze schema for Team Tailor jobs."""
    return {
        "id": "string",
        "title": "string",
        "status": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "department_id": "string",
        "employment_type": "string",
        "source": "string",
        "ingestion_ts": "timestamp",
        "payload": "string",
        "metadata": "string",
    }


@pytest.fixture(scope="function")
def teamtailor_bronze_applications_schema() -> Dict[str, str]:
    """Expected Bronze schema for Team Tailor applications."""
    return {
        "id": "string",
        "candidate_id": "string",
        "job_id": "string",
        "status": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "stage_id": "string",
        "source": "string",
        "ingestion_ts": "timestamp",
        "payload": "string",
        "metadata": "string",
        "year": "int",
        "month": "int",
    }


# ============================================================================
# Silver Layer Fixtures
# ============================================================================


@pytest.fixture(scope="function")
def teamtailor_silver_candidates_schema() -> Dict[str, str]:
    """Expected Silver schema for Team Tailor candidates."""
    return {
        "candidate_id": "string",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "phone": "string",
        "candidate_status": "string",
        "profile_created_at": "timestamp",
        "profile_updated_at": "timestamp",
        "sourced_by_user_id": "string",
        "full_name": "string",
        "is_active_candidate": "boolean",
        "silver_ingestion_timestamp": "timestamp",
        "source": "string",
        "etl_job_name": "string",
        "record_hash": "string",
    }


@pytest.fixture(scope="function")
def teamtailor_silver_jobs_schema() -> Dict[str, str]:
    """Expected Silver schema for Team Tailor jobs."""
    return {
        "job_id": "string",
        "job_title": "string",
        "job_status": "string",
        "job_created_at": "timestamp",
        "job_updated_at": "timestamp",
        "department_id": "string",
        "employment_type": "string",
        "is_open_job": "boolean",
        "silver_ingestion_timestamp": "timestamp",
        "source": "string",
        "etl_job_name": "string",
        "record_hash": "string",
    }


# ============================================================================
# Test Data Generators
# ============================================================================


@pytest.fixture(scope="function")
def teamtailor_test_data_generator():
    """Generator for creating test data dynamically."""

    class TeamTailorTestDataGenerator:
        """Generate test data for Team Tailor testing."""

        @staticmethod
        def generate_candidate(
            candidate_id: str = None, email: str = None
        ) -> Dict[str, Any]:
            """Generate a test candidate."""
            return {
                "id": candidate_id or "candidate_001",
                "type": "candidates",
                "attributes": {
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": email or "john.doe@example.com",
                    "phone": "+1234567890",
                    "created_at": "2024-01-15T10:30:00Z",
                    "updated_at": "2024-01-15T10:30:00Z",
                    "status": "active",
                },
                "relationships": {
                    "sourced-by-user": {"data": {"id": "user_001", "type": "users"}}
                },
            }

        @staticmethod
        def generate_job(job_id: str = None, title: str = None) -> Dict[str, Any]:
            """Generate a test job."""
            return {
                "id": job_id or "job_001",
                "type": "jobs",
                "attributes": {
                    "title": title or "Software Engineer",
                    "status": "open",
                    "created_at": "2024-01-15T10:30:00Z",
                    "updated_at": "2024-01-15T10:30:00Z",
                    "employment_type": "full_time",
                },
                "relationships": {
                    "department": {"data": {"id": "dept_001", "type": "departments"}}
                },
            }

        @staticmethod
        def generate_application(
            application_id: str = None,
            candidate_id: str = None,
            job_id: str = None,
        ) -> Dict[str, Any]:
            """Generate a test application."""
            return {
                "id": application_id or "application_001",
                "type": "applications",
                "attributes": {
                    "status": "active",
                    "created_at": "2024-01-15T10:30:00Z",
                    "updated_at": "2024-01-15T10:30:00Z",
                },
                "relationships": {
                    "candidate": {
                        "data": {
                            "id": candidate_id or "candidate_001",
                            "type": "candidates",
                        }
                    },
                    "job": {"data": {"id": job_id or "job_001", "type": "jobs"}},
                    "stage": {"data": {"id": "stage_001", "type": "stages"}},
                },
            }

        @staticmethod
        def generate_batch_candidates(count: int = 10) -> List[Dict[str, Any]]:
            """Generate a batch of test candidates."""
            candidates = []
            for i in range(count):
                candidates.append(
                    TeamTailorTestDataGenerator.generate_candidate(
                        f"candidate_{i:03d}", f"candidate{i}@example.com"
                    )
                )
            return candidates

        @staticmethod
        def generate_batch_jobs(count: int = 10) -> List[Dict[str, Any]]:
            """Generate a batch of test jobs."""
            jobs = []
            titles = [
                "Software Engineer",
                "Product Manager",
                "Data Scientist",
                "Designer",
            ]
            for i in range(count):
                jobs.append(
                    TeamTailorTestDataGenerator.generate_job(
                        f"job_{i:03d}", titles[i % len(titles)]
                    )
                )
            return jobs

        @staticmethod
        def generate_batch_applications(
            count: int = 10, candidate_ids: List[str] = None, job_ids: List[str] = None
        ) -> List[Dict[str, Any]]:
            """Generate a batch of test applications."""
            applications = []
            for i in range(count):
                candidate_id = (
                    candidate_ids[i % len(candidate_ids)]
                    if candidate_ids
                    else f"candidate_{i:03d}"
                )
                job_id = job_ids[i % len(job_ids)] if job_ids else f"job_{i:03d}"
                applications.append(
                    TeamTailorTestDataGenerator.generate_application(
                        f"application_{i:03d}", candidate_id, job_id
                    )
                )
            return applications

    return TeamTailorTestDataGenerator()


# ============================================================================
# Validation Fixtures
# ============================================================================


@pytest.fixture(scope="function")
def teamtailor_data_quality_rules():
    """Data quality rules for Team Tailor data validation."""
    return {
        "candidates": {
            "required_fields": ["id", "email"],
            "email_format": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone_format": r"^\+[1-9]\d{1,14}$",
            "valid_statuses": ["active", "inactive", "archived"],
        },
        "jobs": {
            "required_fields": ["id", "title"],
            "valid_statuses": ["open", "closed", "draft"],
            "valid_employment_types": ["full_time", "part_time", "contract"],
        },
        "applications": {
            "required_fields": ["id", "candidate_id", "job_id"],
            "valid_statuses": ["active", "rejected", "hired", "withdrawn"],
        },
    }
