# Unit Tests

Unit tests validate isolated behavior for shared utilities and configuration.

## Current scope

- `tests/unit/jobs/test_job_config.py`
- `tests/unit/jobs/test_jobs.py`
- `tests/unit/libs/common/config/`
- `tests/unit/libs/common/utils/`

## Notes

- Source-specific suites from legacy private integrations were removed during boilerplate sanitization.
- New source-specific tests should target the public API sample (`public_api_*`) or future generic sources.
