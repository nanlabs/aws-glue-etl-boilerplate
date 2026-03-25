# Unit Tests Structure

## Overview

This directory contains **fast, lightweight unit tests** that validate core logic without heavy dependencies like Spark. Unit tests should complete in milliseconds, not seconds.

## Best Practices for Data Project Unit Tests

### 1. **No Spark Dependencies**
- Unit tests should NOT create Spark sessions or DataFrames
- Use pure Python logic and Pydantic validation
- Mock any external dependencies (S3, APIs, databases)

### 2. **Test Configuration, Not Execution**
- Validate configuration parsing and parameter resolution
- Test business logic functions independently
- Verify validation rules and error handling

### 3. **Fast Execution**
- All unit tests should complete in < 1 second
- Use `@patch.dict('os.environ', {...})` to mock environment variables
- Avoid file I/O, network calls, or heavy computation

### 4. **Simple and Maintainable**
- Each test should be self-contained and clear
- Use descriptive test names that explain what is being tested
- Group related tests in classes
- Keep test files under 500 lines

## Test Structure

```
tests/unit/
├── README.md                    # This file
├── jobs/
│   ├── test_job_config.py      # Configuration tests (✅ 15 tests, < 0.1s)
│   ├── test_jobs_unit.py       # Legacy tests (⚠️ needs refactoring)
│   ├── raw/
│   │   └── test_teamtailor_raw_job.py  # Team Tailor Raw Job (✅ 21 tests, 0.18s)
│   ├── bronze/
│   │   └── test_teamtailor_bronze_job.py  # Team Tailor Bronze Job (✅ tests)
│   └── silver/
│       └── test_teamtailor_silver_job.py  # (pending)
└── conftest.py                  # Shared fixtures
```

## Current Test Coverage

### General Configuration Tests ✅
**File**: `test_job_config.py`  
**Tests**: 15 passing in 0.09 seconds  
**Coverage**:
- Configuration defaults and overrides
- Parameter resolution hierarchy
- Path validation
- Database naming conventions

### Team Tailor Raw Job Tests ✅
**File**: `raw/test_teamtailor_raw_job.py`  
**Tests**: 21 passing in 0.18 seconds  
**Coverage**:
- ✅ Configuration validation (5 tests)
- ✅ Entity configuration (candidates, jobs, applications)
- ✅ API endpoint auto-configuration
- ✅ Date filtering logic
- ✅ Pagination handling
- ✅ Data transformation
- ✅ S3 path construction

**Key Test Classes**:
- `TestTeamTailorRawConfig` - Configuration validation
- `TestTeamTailorEntityConfiguration` - Entity setup
- `TestTeamTailorRawJobInitialization` - Job init logic
- `TestTeamTailorDataExtraction` - API calls and pagination
- `TestTeamTailorDataTransformation` - Data processing
- `TestTeamTailorS3PathGeneration` - S3 paths

## Test Execution Summary

```bash
# Run all unit tests
pytest tests/unit/ -v

# Current status: 36+ tests passing
# ✅ test_job_config.py: 15/15 passing
# ✅ raw/test_teamtailor_raw_job.py: 21/21 passing  
# ✅ bronze/test_teamtailor_bronze_job.py: tests passing
# ⚠️ test_jobs_unit.py: 2/8 passing (legacy, needs refactoring)
```

## What to Test in Unit Tests

### ✅ DO Test
- Configuration parsing and validation
- Parameter resolution (CLI args > Environment vars > Defaults)
- Field validators and error handling
- Business logic functions (transformations, calculations)
- Data quality rules (without Spark)
- Utility functions
- Path construction and formatting
- API request building (mocked)
- Pagination logic
- Data validation rules

### ❌ DON'T Test
- Spark session creation
- Iceberg table operations
- S3 read/write operations
- Database connections
- Real API calls (mock them instead)
- End-to-end job execution
- Boto3 client initialization

## How to Run Tests

```bash
# Run all unit tests
pytest tests/unit/ -v

# Run specific layer
pytest tests/unit/jobs/raw/ -v
pytest tests/unit/jobs/bronze/ -v
pytest tests/unit/jobs/silver/ -v

# Run specific job test
pytest tests/unit/jobs/raw/test_teamtailor_raw_job.py -v
pytest tests/unit/jobs/bronze/test_teamtailor_bronze_job.py -v

# Run with coverage
pytest tests/unit/ --cov=libs --cov=jobs --cov-report=html

# Run specific test class
pytest tests/unit/jobs/raw/test_teamtailor_raw_job.py::TestTeamTailorRawConfig -v

# Run specific test
pytest tests/unit/jobs/raw/test_teamtailor_raw_job.py::TestTeamTailorRawConfig::test_config_requires_entity_type -v
```

## Adding New Job Tests

### Template Structure

Each job should have its own test file with tests organized by concern:

```python
"""
Unit tests for [Source] [Layer] Job.

Tests focus on:
- Configuration validation
- Business logic
- Data transformations
- Path construction
- [Job-specific concerns]

Fast execution without external dependencies.
"""

import pytest
from unittest.mock import patch, mock_open

# Test environment
TEST_ENV = {
    'SOURCE_NAME': 'source',
    'PROJECT_NAME': 'test_project',
    'WAREHOUSE_PATH': 's3://test-bucket/warehouse/',
    'RAW_ZONE_PATH': 's3://test-bucket/raw/',
    # Add job-specific env vars
}

class TestJobConfig:
    """Test job configuration validation."""

    @patch.dict('os.environ', TEST_ENV)
    def test_config_validation(self):
        from jobs.layer.source_job import JobConfig

        config = JobConfig(job_name="test_job")
        assert config.source_name == "source"

class TestJobLogic:
    """Test job-specific business logic."""

    def test_transformation_logic(self):
        # Test without external dependencies
        result = transform_function(input_data)
        assert result == expected_output
```

### Guidelines for New Tests

1. **Keep it under 500 lines** - If test file grows > 500 lines, split into multiple files
2. **Group by concern** - Use test classes to organize related tests
3. **Test the interesting stuff** - Skip trivial setters/getters
4. **Mock external dependencies** - Use `@patch` for S3, APIs, databases
5. **Use descriptive names** - Test names should explain what is being tested
6. **Fast execution** - All tests in a file should complete in < 1 second

### What to Test Per Layer

#### Raw Layer Tests
- ✅ API/SFTP configuration
- ✅ Data extraction logic
- ✅ File validation
- ✅ S3 path construction
- ✅ Entity type handling
- ✅ Rate limiting logic

#### Bronze Layer Tests
- ✅ Schema validation
- ✅ Raw-to-bronze transformation
- ✅ Data type conversions
- ✅ Partitioning logic
- ✅ Deduplication rules

#### Silver Layer Tests
- ✅ Data cleaning logic
- ✅ Business rules
- ✅ Enrichment logic
- ✅ Data quality checks
- ✅ Schema evolution handling

#### Gold Layer Tests
- ✅ Aggregation logic
- ✅ Metric calculations
- ✅ Business KPIs
- ✅ Data mart structure

## Integration vs Unit Tests

| Aspect | Unit Tests | Integration Tests |
|--------|-----------|-------------------|
| **Speed** | < 1 second | Seconds to minutes |
| **Scope** | Single function/class | Multiple components |
| **Dependencies** | Mocked | Real (LocalStack, Spark) |
| **Purpose** | Validate logic | Validate integration |
| **Location** | `tests/unit/` | `tests/integration/` |
| **When to Run** | Every commit | Before merge/deploy |

## Next Steps

1. **Complete Raw Layer Tests** ✅
   - Team Tailor Raw Job: **DONE** (21 tests)

2. **Add Bronze Layer Tests** ✅
   - Team Tailor Bronze Job: **DONE** (tests)
   - Target: ~20 tests per job, < 500 lines each

3. **Add Silver Layer Tests** 📝
   - Create `silver/test_teamtailor_silver_job.py`
   - Focus on transformation and enrichment logic

4. **Refactor Legacy Tests** ⚠️
   - Fix `test_jobs_unit.py` broken imports
   - Migrate useful tests to new structure
   - Remove obsolete tests

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [unittest.mock Guide](https://docs.python.org/3/library/unittest.mock.html)
- [Pydantic Validation](https://docs.pydantic.dev/latest/)
- [Project Testing Guidelines](../../docs/TESTING.md)

---

**Last Updated**: 2025-10-29  
**Status**: ✅ 36+ tests passing  
**Execution Time**: < 0.3 seconds  
**Coverage**: Configuration (15) + Team Tailor Raw Job (21) + Team Tailor Bronze Job (tests) + Legacy (2)
