"""
Integration test suite for the AWS Glue job framework.

This package contains integration tests that verify components work together
correctly. These tests focus on:

- Configuration and validation integration
- Job component interactions
- Utility component integration
- Source-specific component integration

## Running Integration Tests

Integration tests are controlled by the pytest-integration-mark plugin and
will be SKIPPED by default. To run integration tests, use:

    # Run only integration tests
    pytest tests/integration/ --with-integration -v

    # Run all tests including integration tests
    pytest --with-integration -v

    # Using the run_tests.py script (recommended)
    python run_tests.py --type integration --verbose

## Test Structure

- `test_config_validation_integration.py` - Config and validation working together
- `test_job_integration.py` - Job component integration tests
- `test_utils_integration.py` - Utility component integration tests
- `test_source_integration.py` - Source component integration tests

These tests are designed to be:
- Simple and focused on component interactions
- Fast-running without external dependencies
- Independent and isolated from each other
- Easy to understand and maintain

## Design Principles

1. **Simple**: Tests focus on basic component interactions
2. **Scalable**: Easy to add new integration tests following the same patterns
3. **Best Practices**: Follow pytest and Python testing best practices
4. **No External Dependencies**: Use mocking to avoid external service calls
5. **Clear Assertions**: Each test has clear, specific assertions
6. **Good Logging**: Integration tests log their progress for debugging
"""
