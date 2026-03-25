# Public API Raw Ingestion

This folder contains Python Shell jobs that extract data from public APIs and store JSONL files in the Raw zone.

## Default sample

- Job: `public_api_raw_job.py`
- Source: JSONPlaceholder (`https://jsonplaceholder.typicode.com`)
- Default endpoint: `/posts`

## Runtime configuration

Use environment variables or Glue job arguments:

- `SOURCE_NAME` (default: `public_api`)
- `ENTITY_TYPE` (default: `posts`)
- `API_BASE_URL`
- `API_ENDPOINT`
- `API_KEY` (optional)
- `RAW_ZONE_PATH`
- `WAREHOUSE_PATH`

## Example local run

```bash
python jobs/raw/public_api_raw_job.py \
  --JOB_NAME public_api_raw_posts \
  --SOURCE_NAME public_api \
  --ENTITY_TYPE posts \
  --API_BASE_URL https://jsonplaceholder.typicode.com \
  --API_ENDPOINT /posts \
  --RAW_ZONE_PATH s3://example-data-lake/raw-zone/ \
  --WAREHOUSE_PATH s3://example-data-lake/warehouse/
```
