# Development Guide

## Recommended local flow

```bash
make bootstrap
make check-env
make lint
make type-check
make test-unit
```

Optional NaNLABS commands (if available):

```bash
make nan-health
make nan-skills
```

## Local run examples

```bash
make run-raw DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-bronze DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-silver DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-gold DATA_SOURCE=public_api ENTITY_TYPE=posts
```

## Direct runs

```bash
python jobs/raw/public_api_raw_job.py --ENTITY_TYPE posts
python jobs/bronze/public_api_bronze_job.py --ENTITY_TYPE posts
python jobs/silver/public_api_silver_job.py --ENTITY_TYPE posts
python jobs/gold/public_api_gold_job.py --ENTITY_TYPE posts
```
