# Environment Variables

## Core

- `STAGE`
- `ENVIRONMENT`
- `AWS_REGION`
- `JOB_NAME`
- `SOURCE_NAME`

## Storage

- `RAW_ZONE_PATH`
- `WAREHOUSE_PATH`
- `RAW_DATABASE_NAME`
- `BRONZE_DATABASE_NAME`
- `SILVER_DATABASE_NAME`
- `GOLD_DATABASE_NAME`

## Public API source

- `ENTITY_TYPE`
- `API_BASE_URL`
- `API_ENDPOINT`
- `API_KEY` (optional)
- `RATE_LIMIT_PER_SECOND`
- `MAX_RETRIES`
- `BATCH_SIZE`

All values can be provided as environment variables or Glue job arguments.
