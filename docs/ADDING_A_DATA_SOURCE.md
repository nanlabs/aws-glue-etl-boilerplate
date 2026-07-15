# Adding a New Data Source

This guide walks through adding a new data source to the Medallion pipeline (Raw -> Bronze -> Silver -> Gold) using the scaffolding script, and explains what to customize afterward. See `docs/ARCHITECTURE.md` for the layer overview this guide builds on.

## 1. Generate the scaffold

Run the scaffolding script from the repository root:

```bash
make scaffold-source SOURCE=<source_name> ENTITY_TYPE=<entity_type>
```

`SOURCE` and `ENTITY_TYPE` must both be snake_case, for example `SOURCE=crm_api ENTITY_TYPE=customers`. The script validates this pattern and refuses to run otherwise, and it refuses to overwrite any file that already exists.

This generates five new files. `jobs/raw/<source_name>_raw_job.py` is a Python Shell job with a config class and a job class, both prefixed with a PascalCase version of `<source_name>`, implementing `extract`, `transform`, and `load`. `jobs/bronze/<source_name>_bronze_job.py` is a PySpark job that reads the raw JSON written by the raw job and writes normalized records to an Iceberg table. `jobs/silver/<source_name>_silver_job.py` and `jobs/gold/<source_name>_gold_job.py` follow the same config/job pattern for the Silver (quality/typing) and Gold (aggregation) layers. `tests/unit/jobs/test_<source_name>_jobs.py` is the starting point for that source's unit tests.

## 2. Where each layer's code belongs

Layer-specific job code lives in `jobs/raw/`, `jobs/bronze/`, `jobs/silver/`, and `jobs/gold/`. Anything reused across sources, such as config base classes, env/logging helpers, the Spark session factory, or the Medallion base classes, belongs in `libs/common/`, `libs/pyshell/`, or `libs/pyspark/` instead. Do not add source-specific branching (for example checking the source name) to these shared modules; keep that logic inside the generated per-source job files so the shared libraries stay generic.

## 3. What to customize

The generated `extract()` in the raw job defaults to calling a JSON API at `api_base_url` plus `api_endpoint` (the JSONPlaceholder sample by default) and should be replaced with real extraction logic for your source, whether that is a different REST API, SFTP, or a database export. The generated `transform()` and `load()` methods are usually a reasonable starting point and mainly need field-level adjustments once the real payload shape is known. The Bronze, Silver, and Gold jobs ship with placeholder column selections that should be updated to match your source's real schema.

## 4. Configuration

Configuration is resolved through four tiers, in order of precedence: Glue Workflow Properties, then CLI arguments, then environment variables, then the field defaults declared on the Pydantic config class (see `docs/ENVIRONMENT_VARIABLES.md`). New sources typically only need to add fields to their generated config classes as `pydantic.Field` declarations rather than wiring anything by hand, since the resolution logic is inherited from the shared base classes. Add any new environment variables you introduce to `.env.example` so other contributors know they exist.

## 5. Tests and local validation

Adapt the generated `tests/unit/jobs/test_<source_name>_jobs.py` file to cover your source's extract and transform logic. Per `docs/TESTING.md`, keep tests source-agnostic where possible and only add source-specific tests for actively supported sample sources. Before opening a PR, run:

```bash
make lint
make type-check
make test-unit
```

You can run a single source's job locally with `make run-raw DATA_SOURCE=<source_name> ENTITY_TYPE=<entity_type>` and the equivalent `run-bronze`, `run-silver`, and `run-gold` targets, or invoke the scripts directly as shown in the main README's "Running Jobs Locally" section.

## 6. Before opening a PR

Update `.env.example` and `docs/ENVIRONMENT_VARIABLES.md` if you added new configuration. Confirm `make lint`, `make type-check`, and `make test-unit` all pass, and follow `docs/TESTING.md`'s guidance to list the commands you ran and their results in the PR description.
