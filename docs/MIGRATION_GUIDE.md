# Migration Guide: Private Source to Boilerplate

This guide explains how to migrate an existing private data source into this boilerplate without leaking organization-specific details.

## Goal

Transform source-specific jobs into reusable, source-agnostic boilerplate components while preserving behavior.

## Migration Checklist

1. Create a dedicated branch for the source migration.
2. Scaffold boilerplate files with:
   ```bash
   make scaffold-source SOURCE=<source_name> ENTITY_TYPE=<entity_type>
   ```
3. Move source-specific extraction logic into the generated Raw job.
4. Keep shared logic in `libs/common/` and `libs/pyspark/` generic.
5. Replace hardcoded values with config fields and env vars.
6. Add unit tests for config defaults and transform behavior.
7. Add one integration smoke test for the full raw->gold flow.
8. Run validation:
   ```bash
   make test-unit
   make lint
   make type-check
   ```

## File-by-File Mapping

- Raw extraction job:
  - from: private source raw module
  - to: `jobs/raw/<source>_raw_job.py`

- Bronze normalization job:
  - from: private source bronze module
  - to: `jobs/bronze/<source>_bronze_job.py`

- Silver standardization job:
  - from: private source silver module
  - to: `jobs/silver/<source>_silver_job.py`

- Gold aggregation job:
  - from: private source gold module
  - to: `jobs/gold/<source>_gold_job.py`

- Source tests:
  - from: private tests
  - to: `tests/unit/jobs/test_<source>_jobs.py`

## De-Identification Rules

Before opening a PR, remove or rename:

- Internal service URLs and hostnames
- Company-specific identifiers and business labels
- Account IDs and bucket names
- Secret names tied to internal conventions
- Proprietary metric names in Gold outputs

Use neutral defaults such as `public_api`, `customers`, `raw_zone`, and `gold_zone`.

## Config Migration Pattern

Use this pattern when replacing hardcoded constants:

1. Add fields to the corresponding config class (`RawJobConfig`, `BronzeJobConfig`, etc.).
2. Provide safe defaults for local/dev usage.
3. Resolve runtime values via Glue args and env vars.
4. Derive table names in `model_post_init`.

Example:

```python
class CustomerApiRawConfig(RawJobConfig):
    source_name: str = Field(default="customer_api")
    entity_type: str = Field(default="customers")
    api_base_url: str = Field(default="https://example.com")
```

## Testing Strategy

Minimum expected coverage for migrated sources:

- Unit tests:
  - config defaults and table naming
  - transform behavior on valid and empty input

- Integration tests:
  - single smoke flow raw->bronze->silver->gold

## Common Pitfalls

- Reusing private schema names in default table/database values
- Leaving source-specific exceptions in shared libs
- Hardcoding S3 paths instead of using `RAW_ZONE_PATH` and `WAREHOUSE_PATH`
- Skipping a Gold assertion for aggregation consistency

## Definition of Done

A migration is complete when:

1. Source jobs run with boilerplate defaults in local mode.
2. No private identifiers remain in generated code or docs.
3. Unit tests pass for the new source.
4. Smoke integration test passes.
5. Lint and type-check are green.
