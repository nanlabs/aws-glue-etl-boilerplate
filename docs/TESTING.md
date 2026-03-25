# Testing Guidelines

## Overview

This document outlines the testing requirements and best practices for the NaNLABS Data Warehouse platform.

## Testing Strategy

### Test Types

#### Unit Tests

- **Purpose**: Test individual functions and methods
- **Scope**: Business logic, data transformations, validation
- **Dependencies**: Mocked external services
- **Execution**: Fast, isolated, repeatable

#### Integration Tests

- **Purpose**: Test complete workflows and service interactions
- **Scope**: End-to-end data pipelines, service connectivity
- **Dependencies**: LocalStack services, test data
- **Execution**: Slower, requires test environment

### Test Structure

#### Modern Scalable Structure (Recommended)

The platform uses a scalable testing structure organized by source, domain, and Medallion layer:

```bash
tests/
├── pytest.ini                           # Pytest configuration with markers
├── conftest.py                           # Global fixtures and configuration
├──
├── fixtures/                             # Test data organized by source
│   ├── teamtailor/
│   │   ├── raw/
│   │   │   ├── candidates_sample.jsonl    # Sample Team Tailor candidates
│   │   │   ├── jobs_sample.jsonl          # Sample Team Tailor jobs
│   │   │   └── applications_sample.jsonl  # Sample Team Tailor applications
│   │   └── conftest.py                   # Team Tailor-specific fixtures
│   └── shared/                           # Shared test data
│
├── unit/                                 # Unit tests (fast, isolated)
│   ├── jobs/
│   │   ├── raw/
│   │   │   └── test_teamtailor_raw_job.py # Team Tailor Raw job logic tests
│   │   └── bronze/
│   │       └── test_teamtailor_bronze_job.py # Team Tailor Bronze job tests
│   ├── libs/
│   │   └── common/
│   │       └── teamtailor/
│   │           └── test_client.py         # Team Tailor API client tests
│   └── libs/                             # Library unit tests
│
├── integration/                          # Integration tests (components working together)
│   ├── medallion_layers/
│   │   └── test_bronze_layer_integration.py  # Bronze layer integration tests
│   └── source_pipelines/
│       └── test_teamtailor_pipeline_integration.py  # Complete pipeline tests
│
├── e2e/                                  # End-to-end tests (full workflows)
├── utils/                                # Testing utilities
└── performance/                          # Performance tests
```

## Unit Testing

### Test Requirements

- Test all business logic functions
- Mock external dependencies
- Validate error handling
- Test edge cases and boundary conditions

### Modern Unit Test Examples

#### Source-Specific Tests (`tests/unit/jobs/raw/`)

```python
# tests/unit/jobs/raw/test_teamtailor_raw_job.py
import pytest
from unittest.mock import Mock, patch

class TestTeamTailorRawJobUnit:
    """Unit tests for Team Tailor Raw job."""

    @pytest.mark.unit
    @pytest.mark.teamtailor
    @pytest.mark.raw
    def test_config_initialization(self, test_env_vars):
        """Test that configuration initializes correctly."""
        # Test configuration logic
        pass

    @pytest.mark.unit
    @pytest.mark.teamtailor
    @pytest.mark.raw
    def test_data_transformation(self, teamtailor_raw_candidates_data):
        """Test data transformation logic."""
        sample_data = teamtailor_raw_candidates_data[0]

        # Test transformation logic
        # transformed = TeamTailorRawJob._transform_candidate_data(sample_data)
        # assert "payload" in transformed
        # assert "metadata" in transformed
        pass

    @pytest.mark.unit
    @pytest.mark.teamtailor
    def test_candidate_data_validation(self, teamtailor_raw_candidates_data, teamtailor_data_quality_rules):
        """Test candidate data validation."""
        sample_candidate = teamtailor_raw_candidates_data[0]
        rules = teamtailor_data_quality_rules["candidates"]

        # Test required fields
        assert "id" in sample_candidate
        assert "attributes" in sample_candidate
        assert "email" in sample_candidate["attributes"]

        # Test email format
        email = sample_candidate["attributes"]["email"]
        import re
        assert re.match(rules["email_format"], email)
```

#### Domain-Specific Tests (`tests/unit/domains/engagement/`)

```python
# tests/unit/domains/engagement/test_profiles_unit.py
import pytest

class TestEngagementProfilesUnit:
    """Unit tests for engagement profiles domain logic."""

    @pytest.mark.unit
    @pytest.mark.engagement
    def test_candidate_id_extraction(self, teamtailor_raw_candidates_data):
        """Test candidate ID extraction from Team Tailor."""
        sample_candidate = teamtailor_raw_candidates_data[0]

        # Test candidate ID extraction
        candidate_id = sample_candidate["id"]
        assert candidate_id is not None
        assert isinstance(candidate_id, str)

    @pytest.mark.unit
    @pytest.mark.talent
    def test_email_normalization(self, teamtailor_test_data_generator):
        """Test email normalization logic."""
        # Test email normalization
        test_emails = [
            "  JOHN@EXAMPLE.COM  ",
            "jane@EXAMPLE.com",
            "bob.wilson@test.org"
        ]

        expected_emails = [
            "john@example.com",
            "jane@example.com",
            "bob.wilson@test.org"
        ]

        for test_email, expected in zip(test_emails, expected_emails):
            # This would test actual normalization logic
            # normalized = EngagementProfiles.normalize_email(test_email)
            # assert normalized == expected
            pass
```

## Integration Testing

### Integration Test Requirements

- Test complete data pipelines
- Use LocalStack for AWS services
- Mock external APIs
- Validate data flow end-to-end

### Modern Integration Test Examples

#### Medallion Layer Tests (`tests/integration/medallion_layers/`)

```python
# tests/integration/medallion_layers/test_bronze_layer_integration.py
import pytest
from pyspark.sql import DataFrame

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
            id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            email STRING,
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
    def test_bronze_schema_validation(self, local_spark_with_iceberg, teamtailor_bronze_candidates_schema):
        """Test Bronze schema validation."""
        spark = local_spark_with_iceberg

        # Create test table
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_bronze_zone.teamtailor__talent__candidates_bronze (
            id STRING NOT NULL,
            first_name STRING,
            last_name STRING,
            email STRING,
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
        actual_schema = spark.table("test_bronze_zone.teamtailor__talent__candidates_bronze").schema

        # Verify schema fields
        actual_field_names = [field.name for field in actual_schema.fields]
        expected_field_names = list(teamtailor_bronze_candidates_schema.keys())

        for field_name in expected_field_names:
            assert field_name in actual_field_names
```

#### Source Pipeline Tests (`tests/integration/source_pipelines/`)

```python
# tests/integration/source_pipelines/test_teamtailor_pipeline_integration.py
import pytest

class TestTeamTailorPipelineIntegration:
    """Integration tests for complete Team Tailor pipeline."""

    @pytest.mark.integration
    @pytest.mark.teamtailor
    @pytest.mark.e2e
    def test_complete_pipeline_candidates(self, local_spark_with_iceberg, teamtailor_raw_candidates_data):
        """Test complete pipeline for candidates: Raw → Bronze → Silver."""
        spark = local_spark_with_iceberg

        # Create all databases
        spark.sql("CREATE DATABASE IF NOT EXISTS test_raw_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_bronze_zone")
        spark.sql("CREATE DATABASE IF NOT EXISTS test_silver_zone")

        # Test complete pipeline flow
        # ... implementation details ...

        # Verify record counts
        raw_count = spark.sql("SELECT COUNT(*) as count FROM test_raw_zone.teamtailor_candidates_raw").collect()[0]["count"]
        bronze_count = spark.sql("SELECT COUNT(*) as count FROM test_bronze_zone.teamtailor__talent__candidates_bronze").collect()[0]["count"]
        silver_count = spark.sql("SELECT COUNT(*) as count FROM test_silver_zone.talent_dim_candidate_profiles").collect()[0]["count"]

        assert raw_count == len(teamtailor_raw_candidates_data)
        assert bronze_count == len(teamtailor_raw_candidates_data)
        assert silver_count == len(teamtailor_raw_candidates_data)
```

## Test Execution

### Running Tests

#### Modern Test Execution (Recommended)

```bash
# Run all tests
pytest tests/

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/ --with-integration

# Run only end-to-end tests
pytest tests/e2e/

# Run tests by source
pytest -m teamtailor                # All Team Tailor tests

# Run tests by Medallion layer
pytest -m raw                       # All Raw layer tests
pytest -m bronze                    # All Bronze layer tests
pytest -m silver                    # All Silver layer tests
pytest -m gold                      # All Gold layer tests

# Run tests by domain
pytest -m talent                    # All talent domain tests

# Run tests by type and source
pytest -m "unit and teamtailor"     # Team Tailor unit tests only
pytest -m "integration and bronze" # Bronze integration tests only
pytest -m "e2e and teamtailor"     # Team Tailor end-to-end tests only

# Run specific test files
pytest tests/unit/jobs/raw/test_teamtailor_raw_job.py
pytest tests/unit/jobs/bronze/test_teamtailor_bronze_job.py
pytest tests/integration/medallion_layers/test_bronze_layer_integration.py

# Run with coverage
pytest --cov=libs --cov=jobs tests/

# Run with verbose output
pytest -v tests/

# Run with debugging
pytest --pdb tests/
```

### Test Configuration

#### Modern Pytest Configuration

```ini
# tests/pytest.ini
[tool:pytest]
# Test Discovery
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Plugin configuration
plugins = integration-mark

# Markers for different test types
markers =
    unit: Unit tests - fast, isolated, no external dependencies
    integration: Integration tests - test component interactions
    e2e: End-to-end tests - full pipeline testing
    performance: Performance tests - load and performance testing
    slow: Slow tests - tests that take more than 5 seconds
    teamtailor: Tests specific to Team Tailor data source
    raw: Tests for Raw layer
    bronze: Tests for Bronze layer
    silver: Tests for Silver layer
    gold: Tests for Gold layer
    talent: Tests for talent domain

# Test Execution
addopts =
    --strict-markers
    --strict-config
    --verbose
    --tb=short
    --disable-warnings
    --color=yes
    --durations=10

# Filtering
filterwarnings =
    ignore::DeprecationWarning
    ignore::PendingDeprecationWarning
    ignore::UserWarning:pyspark.*
    ignore::UserWarning:py4j.*

# Logging
log_cli = true
log_cli_level = INFO
log_cli_format = %(asctime)s [%(levelname)8s] %(name)s: %(message)s
log_cli_date_format = %Y-%m-%d %H:%M:%S

# Coverage (if pytest-cov is installed)
# addopts = --cov=libs --cov=jobs --cov-report=html --cov-report=term-missing

# Test Timeout (if pytest-timeout is installed)
timeout = 300
timeout_method = thread
```

## Test Fixtures and Data Management

### Modern Fixture Structure

The platform uses a hierarchical fixture system organized by source and layer:

#### Global Fixtures (`tests/conftest.py`)

- Environment configuration
- Mock AWS services
- Spark session management
- Common test utilities

#### Source-Specific Fixtures (`tests/fixtures/{source}/conftest.py`)

- Source-specific test data
- Data generators
- Validation rules
- Schema definitions

#### Example Fixture Usage

```python
# tests/fixtures/teamtailor/conftest.py
@pytest.fixture(scope="function")
def teamtailor_raw_candidates_data() -> List[Dict[str, Any]]:
    """Load sample Team Tailor candidates data from fixtures."""
    fixture_path = get_fixture_path("raw/candidates_sample.jsonl")

    candidates = []
    with open(fixture_path, 'r') as f:
        for line in f:
            candidates.append(json.loads(line.strip()))

    return candidates

@pytest.fixture(scope="function")
def teamtailor_test_data_generator():
    """Generator for creating test data dynamically."""

    class TeamTailorTestDataGenerator:
        @staticmethod
        def generate_candidate(candidate_id: str = None, email: str = None) -> Dict[str, Any]:
            """Generate a test candidate."""
            return {
                "id": candidate_id or "candidate_001",
                "type": "candidates",
                "attributes": {
                    "email": email or "test@example.com",
                    "first_name": "Test",
                    "last_name": "User",
                    "phone": "+1234567890",
                    "created_at": "2024-01-15T10:30:00Z",
                    "updated_at": "2024-01-15T10:30:00Z",
                    "status": "active"
                }
            }

    return TeamTailorTestDataGenerator()
```

### Test Data Sources

- **Mock Data**: Generated test data for unit tests
- **Sample Data**: Real data samples for integration tests (JSONL format)
- **Synthetic Data**: Generated data that mimics production patterns
- **Data Generators**: Dynamic test data creation

### Data Privacy

- Never use real customer data in tests
- Use data masking for sensitive information
- Implement data anonymization for test datasets
- Store test data in `tests/fixtures/` directory only

## Continuous Integration

### Pre-commit Hooks

- Code formatting (black, isort)
- Linting (flake8, pylint)
- Type checking (mypy)
- Security scanning (bandit)

### CI Pipeline

1. **Code Quality Checks**
   - Linting and formatting
   - Type checking
   - Security scanning

2. **Test Execution**
   - Unit tests
   - Integration tests
   - Coverage reporting

3. **Deployment Validation**
   - Build verification
   - Smoke tests
   - Performance benchmarks

## Best Practices

### Modern Testing Practices (Recommended)

#### 1. Test Organization

- **Source-First**: Organize tests by data source (Team Tailor, etc.)
- **Domain-Driven**: Group tests by business domain (talent)
- **Layer-Specific**: Separate tests by Medallion layer (Raw, Bronze, Silver, Gold)
- **Scalable Structure**: Use hierarchical directory structure for easy navigation

#### 2. Test Markers and Filtering

- **Use Markers**: Apply appropriate markers (`@pytest.mark.teamtailor`, `@pytest.mark.bronze`)
- **Granular Execution**: Run tests by source, domain, or layer
- **CI/CD Integration**: Use markers for different CI/CD stages
- **Performance Optimization**: Skip slow tests in development

#### 3. Fixture Management

- **Hierarchical Fixtures**: Global → Source → Test-specific fixtures
- **Data Generators**: Use dynamic data generation for complex scenarios
- **Reusable Components**: Create shared fixtures for common patterns
- **Clean Separation**: Keep test data separate from test logic

#### 4. Test Data Strategy

- **Realistic Data**: Use sample data that mimics production patterns
- **Privacy-First**: Never use real customer data
- **Version Control**: Store test data in `tests/fixtures/` directory
- **JSONL Format**: Use JSONL for structured test data

#### 5. Integration Testing

- **LocalStack**: Use LocalStack for AWS service simulation
- **Iceberg Support**: Test with real Iceberg tables and operations
- **End-to-End**: Test complete data pipelines
- **Error Scenarios**: Test failure modes and recovery

## Troubleshooting

### Common Issues

1. **Tests failing**: Check dependencies and test data
2. **Slow tests**: Optimize test setup and teardown
3. **Flaky tests**: Improve test isolation and stability
4. **Coverage issues**: Add missing test cases

### Debugging

- Use `pytest -v` for verbose output
- Use `pytest --pdb` for debugging
- Check test logs and error messages
- Use IDE debugging tools

## References

- [Development Guide](./DEVELOPMENT.md)
- [Architecture](./ARCHITECTURE.md)
- [Best Practices](./BEST_PRACTICES.md)
