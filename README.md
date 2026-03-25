# AWS Glue ETL Boilerplate

A production-ready starting point for building **AWS Glue v5** data pipelines following the [Medallion Architecture](docs/ARCHITECTURE.md) (Raw → Bronze → Silver → Gold).

Ships with:
- Generic `public_api` sample jobs across all four layers (using [JSONPlaceholder](https://jsonplaceholder.typicode.com))
- Pydantic v2 four-tier config resolution (Workflow Properties → CLI args → env vars → defaults)
- Apache Iceberg tables via AWS Glue Data Catalog
- LocalStack-based local development environment
- Full unit test suite with no external dependencies

---

## Architecture

```
External API / SFTP
        │
        ▼
┌──────────────┐    Python Shell (PyShell)
│     Raw      │    Extract → write JSONL to S3
└──────┬───────┘
       │
       ▼
┌──────────────┐    PySpark
│    Bronze    │    Read raw JSONL → normalize → Iceberg
└──────┬───────┘
       │
       ▼
┌──────────────┐    PySpark
│    Silver    │    Quality / typing / hashing → Iceberg
└──────┬───────┘
       │
       ▼
┌──────────────┐    PySpark
│     Gold     │    Business aggregates → Iceberg
└──────────────┘
```

Each layer has a dedicated base class in `libs/pyspark/` and config class in `libs/common/config/`.  
See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for more details.

---

## Project Structure

```
.
├── jobs/
│   ├── raw/          # Python Shell extraction jobs
│   ├── bronze/       # PySpark normalization jobs
│   ├── silver/       # PySpark quality/standardization jobs
│   └── gold/         # PySpark aggregation jobs
├── libs/
│   ├── common/       # Shared config, utils, logging
│   ├── pyshell/      # PyShellJobBase for raw layer
│   └── pyspark/      # SparkSessionFactory + Medallion base classes
├── tests/
│   ├── unit/         # Fast, no-Spark tests (<1 s each)
│   └── integration/  # LocalStack + Spark integration tests
├── scripts/          # build, deploy, sync helper scripts
├── docs/             # Extended documentation
├── .devcontainer/    # VS Code dev container (awsglue + localstack)
├── .env.example      # Reference env file
└── Makefile          # Developer shortcuts
```

---

## Requirements

| Tool | Version |
|---|---|
| Python | 3.11 |
| uv | latest |
| Docker + Docker Compose | 24+ |
| Java (for Spark) | 11 or 17 |

---

## Quick Start

### 1. Clone and create the virtual environment

```bash
git clone https://github.com/your-org/aws-glue-etl-boilerplate.git
cd aws-glue-etl-boilerplate

# install uv if not already available
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

uv venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt
uv pip install pytest pytest-cov pytest-mock pyspark==3.5.1
uv pip install -e . --no-deps
```

### 2. Set up environment variables

```bash
cp .env.example .env
# edit .env with your values
```

Key variables (see [docs/ENVIRONMENT_VARIABLES.md](docs/ENVIRONMENT_VARIABLES.md) for the full list):

| Variable | Default | Description |
|---|---|---|
| `SOURCE_NAME` | `public_api` | Identifier for the data source |
| `ENTITY_TYPE` | `posts` | Entity being processed |
| `RAW_ZONE_PATH` | — | S3 path for raw JSONL output |
| `WAREHOUSE_PATH` | — | S3 path for Iceberg warehouse |
| `RAW_DATABASE_NAME` | `raw_zone` | Glue database for raw layer |
| `BRONZE_DATABASE_NAME` | `bronze_zone` | Glue database for bronze layer |
| `SILVER_DATABASE_NAME` | `silver_zone` | Glue database for silver layer |
| `GOLD_DATABASE_NAME` | `gold_zone` | Glue database for gold layer |
| `API_BASE_URL` | `https://jsonplaceholder.typicode.com` | Base URL for public API source |
| `API_ENDPOINT` | `/posts` | Endpoint path |

### 3. Start the local infrastructure

```bash
# Starts LocalStack (S3, Glue, SecretsManager) + SFTP server
docker compose -f .devcontainer/compose.yml up -d
```

Or open the project in VS Code and use **Reopen in Container** for the full dev container experience.

---

## Running Jobs Locally

Use the Makefile shortcuts:

```bash
make run-raw    DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-bronze DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-silver DATA_SOURCE=public_api ENTITY_TYPE=posts
make run-gold   DATA_SOURCE=public_api ENTITY_TYPE=posts
```

Or invoke directly:

```bash
python jobs/raw/public_api_raw_job.py \
  --JOB_NAME=local_test \
  --ENTITY_TYPE=posts \
  --API_BASE_URL=https://jsonplaceholder.typicode.com

spark-submit jobs/bronze/public_api_bronze_job.py \
  --JOB_NAME=local_test \
  --ENTITY_TYPE=posts
```

---

## Testing

```bash
# Unit tests only (no Spark, no AWS, fast)
make test-unit
# or:
python -m pytest tests/unit/ -q

# Integration tests (requires LocalStack running)
make test-integration
```

See [docs/TESTING.md](docs/TESTING.md) for conventions and marker usage.

---

## Adding a New Data Source

1. **Create config + job** in each layer following the `public_api_*` files as a template:
   - `jobs/raw/{source}_raw_job.py` — extend `RawJobConfig` + `PyShellJobBase`
   - `jobs/bronze/{source}_bronze_job.py` — extend `BronzeJobConfig` + `BronzeJobBase`
   - `jobs/silver/{source}_silver_job.py` — extend `SilverJobConfig` + `SilverJobBase`
   - `jobs/gold/{source}_gold_job.py` — extend `GoldJobConfig` + `GoldJobBase`

2. **Add unit tests** under `tests/unit/jobs/test_{source}_jobs.py`.

3. **Add env vars** for the new source in `.env.example`.

The config system resolves parameters automatically — no wiring needed beyond the field definitions.

---

## Documentation

| Doc | Description |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Medallion layer overview |
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | Local run examples |
| [docs/ENVIRONMENT_VARIABLES.md](docs/ENVIRONMENT_VARIABLES.md) | All supported env vars |
| [docs/LIBS_STRUCTURE.md](docs/LIBS_STRUCTURE.md) | Library layout |
| [docs/TESTING.md](docs/TESTING.md) | Testing conventions |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guidelines |
| [AGENTS.md](AGENTS.md) | AI agent usage guide |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
