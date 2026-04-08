# AWS Glue Data Lake Jobs - Dev Container Makefile
#
# ⚠️  IMPORTANT: This project ONLY supports development with Dev Containers
#
# Requirements:
# - VS Code with Dev Containers extension, OR
# - Dev Container CLI: https://containers.dev/supporting#devcontainer-cli
#
# Setup:
# 1. VS Code: Open project → "Reopen in Container"
# 2. CLI: devcontainer up --workspace-folder .
#
# All commands below are designed to run INSIDE the dev container environment.

.PHONY: help bootstrap install install-dev requirements clean test test-unit test-integration test-coverage lint type-check format autofix validate migrate migrate-upload migrate-dry-run services-status spark-submit pyshell-run notebook prepare-localstack clean-localstack aws-login run-raw run-bronze run-silver run-gold

# Default target
help:

	@echo ""
	@echo "🚀 AWS Glue ETL Boilerplate"
	@echo "=================================================="
	@echo ""
	@echo "⚠️  This project ONLY supports development with Dev Containers"
	@echo "   VS Code: Open project → 'Reopen in Container'"
	@echo "   CLI: devcontainer up --workspace-folder ."
	@echo ""
	@echo "📦 Environment & Dependencies:"
	@echo "  bootstrap       - One-command local setup (uv venv + deps + sanity check)"
	@echo "  install         - Install production dependencies"
	@echo "  install-dev     - Install development dependencies"
	@echo "  requirements    - Generate requirements.txt from Pipfile"
	@echo "  validate        - Validate dev container environment"
	@echo ""
	@echo "🧪 Testing & Quality:"
	@echo "  test            - Run all tests (unit + integration)"
	@echo "  test-unit       - Run unit tests only"
	@echo "  test-integration- Run integration tests only"
	@echo "  test-coverage   - Run tests with coverage report"
	@echo "  lint            - Run code linting with Ruff"
	@echo "  type-check      - Run mypy on core boilerplate modules"
	@echo "  format          - Format code with black"
	@echo "  autofix         - Auto-fix code formatting and imports"
	@echo ""
	@echo "🔧 Development Services:"
	@echo "  services-status - Check status of all services"
	@echo "  notebook        - Open Jupyter Lab (http://localhost:8888)"
	@echo "  prepare-localstack - Prepare LocalStack with secrets and SSM parameters from AWS real"
	@echo "  clean-localstack - Clean all LocalStack resources (secrets, SSM, S3, Glue)"
	@echo ""
	@echo "🗃️  Database & Migrations:"
	@echo "  migrate         - Run database migrations"
	@echo "  migrate-upload  - Upload migration files to S3 (LocalStack)"
	@echo "  migrate-dry-run - Show migration configuration"
	@echo ""
	@echo "🚀 Job Execution:"
	@echo "  spark-submit JOB=<path> [ARGS=\"<args>\"] - Run Spark job locally"
	@echo "  pyshell-run JOB=<path> [ARGS=\"<args>\"] - Run PyShell job locally"
	@echo ""
	@echo "📊 Medallion Jobs by Tier:"
	@echo "  run-raw DATA_SOURCE=<source> ENTITY_TYPE=<type> [ARGS=\"<args>\"] - Run Raw job"
	@echo "  run-bronze DATA_SOURCE=<source> ENTITY_TYPE=<type> [ARGS=\"<args>\"] - Run Bronze job"
	@echo "  run-silver DATA_SOURCE=<source> ENTITY_TYPE=<type> [ARGS=\"<args>\"] - Run Silver job"
	@echo "  run-gold DATA_SOURCE=<source> ENTITY_TYPE=<type> [ARGS=\"<args>\"] - Run Gold job"
	@echo "   Example: make run-raw DATA_SOURCE=public_api ENTITY_TYPE=posts"
	@echo ""
	@echo "🧪 Testing & Quality:"
	@echo "  test            - Run all tests (unit + integration)"
	@echo "  test-unit       - Run unit tests only"
	@echo "  test-integration- Run integration tests only"
	@echo "  test-coverage   - Run tests with coverage report"
	@echo "  lint            - Run code linting with Ruff"
	@echo "  type-check      - Run mypy on core boilerplate modules"
	@echo "  format          - Format code with black"
	@echo "  autofix         - Auto-fix code formatting and imports"
	@echo ""
	@echo "🗃️  Database Migrations:"
	@echo "  migrate         - Run database migrations (local environment)"
	@echo "  migrate-upload  - Upload migration files to S3 (LocalStack)"
	@echo "  migrate-dry-run - Show migration configuration without executing"
	@echo ""
	@echo "📦 Packaging & Deployment:"
	@echo "  package         - Build standard package (wheels + ZIP)"
	@echo "  glue-wheels     - Build AWS Glue optimized uber wheels"
	@echo "  clean-build     - Clean build artifacts"
	@echo "  package-clean   - Clean and rebuild packages"

# One-command local setup: installs uv (if missing), creates .venv, installs all deps, runs sanity check
bootstrap:
	@echo ""
	@echo "🚀 Bootstrapping local development environment..."
	@echo ""
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "📦 uv not found — installing..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
		. "$$HOME/.local/bin/env"; \
		echo "✅ uv installed"; \
	else \
		echo "✅ uv already available: $$(uv --version)"; \
	fi
	@echo ""
	@echo "🐍 Creating virtual environment with Python 3.11..."
	@. "$$HOME/.local/bin/env" 2>/dev/null || true; \
	uv venv --allow-existing --python 3.11 2>&1 || { echo "❌ Could not create venv with Python 3.11. Is it installed?"; exit 1; }
	@echo ""
	@echo "📦 Installing runtime dependencies..."
	@. "$$HOME/.local/bin/env" 2>/dev/null || true; \
	uv pip install -r requirements.txt --python .venv/bin/python
	@echo ""
	@echo "🔬 Installing dev + test dependencies..."
	@. "$$HOME/.local/bin/env" 2>/dev/null || true; \
	uv pip install pytest pytest-cov pytest-mock pyspark==3.5.1 typing-inspection ruff mypy --python .venv/bin/python
	@echo ""
	@echo "📎 Installing package in editable mode..."
	@. "$$HOME/.local/bin/env" 2>/dev/null || true; \
	uv pip install -e . --no-deps --python .venv/bin/python
	@echo ""
	@echo "🧪 Running sanity check (unit tests)..."
	@. "$$HOME/.local/bin/env" 2>/dev/null || true; \
	.venv/bin/python -m pytest tests/unit/ -q --no-header --tb=short 2>&1 || { echo "❌ Sanity check failed"; exit 1; }
	@echo ""
	@echo "✅ Bootstrap complete!"
	@echo "   Activate with: source .venv/bin/activate"
	@echo "   Copy env:      cp .env.example .env  (then edit .env)"
	@echo "   Run tests:     make test-unit"
	@echo ""

# Install production dependencies
install:
	@echo "Creating virtual environment for production..."
	pipenv --python 3.11
	@echo "Installing production dependencies..."
	pipenv install --system --deploy --ignore-pipfile
	@echo "Installation complete."

# Install development dependencies
install-dev:
	@echo "Creating virtual environment for development..."
	pipenv --python 3.11
	@echo "Installing development dependencies..."
	pipenv install --dev
	@echo "Installation complete."

# Generate requirements.txt from Pipfile
requirements:
	python scripts/generate_requirements.py

# Clean up build artifacts and caches
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf build/
	rm -rf dist/

# Validate dev container environment
validate:
	@echo "🔍 Validating dev container environment..."
	./scripts/validate-env.sh

# Check status of all services
services-status:
	@echo "🔧 Checking dev container services status..."
	@echo "LocalStack (AWS services):"
	@curl -fsS http://localstack:4566/_localstack/health >/dev/null 2>&1 && echo "✅ LocalStack: Running" || echo "❌ LocalStack: Not available"
	@echo ""
	@echo "Jupyter Lab:"
	@curl -fsS http://localhost:8888 2>/dev/null && echo "✅ Jupyter Lab: Running" || echo "❌ Jupyter Lab: Not available"
	@echo ""
	@echo "Services URLs:"
	@echo "  - Jupyter Lab: http://localhost:8888"
	@echo "  - LocalStack: http://localhost:4566"
	@echo "  - Spark UI: http://localhost:4040 (when jobs are running)"

# Open Jupyter Lab
notebook:
	@echo "🪐 Opening Jupyter Lab..."
	@echo "Jupyter Lab is available at: http://localhost:8888"
	@which open >/dev/null 2>&1 && open http://localhost:8888 || echo "Open http://localhost:8888 in your browser"

# Sync secrets and SSM parameters from AWS real to LocalStack
# Note: Run 'direnv allow' first to enable direnv in this directory
prepare-localstack:
	@echo "🔄 Preparing LocalStack with secrets and SSM parameters from AWS real..."
	@./scripts/prepare-localstack.sh

# Clean all LocalStack resources
# Note: Assumes environment variables are already loaded via direnv (run 'direnv allow' first)
clean-localstack:
	./scripts/clean-localstack.sh

# Login to AWS SSO for data workload accounts
# This script handles AWS SSO login, temporarily removing LocalStack variables
aws-login:
	@echo "🔐 Logging in to AWS SSO for data workload accounts..."
	@./scripts/aws-login.sh

# Run all tests
test:
	@if [ -f .venv/bin/python ]; then \
		.venv/bin/python run_tests.py --type all; \
	else \
		pipenv run python run_tests.py --type all; \
	fi

# Run unit tests only
test-unit:
	@if [ -f .venv/bin/python ]; then \
		.venv/bin/python -m pytest tests/unit/ -q --no-header --tb=short; \
	else \
		pipenv run python run_tests.py --type unit; \
	fi

# Run integration tests only
test-integration:
	@if [ -f .venv/bin/python ]; then \
		.venv/bin/python run_tests.py --type integration; \
	else \
		pipenv run python run_tests.py --type integration; \
	fi

# Run tests with coverage
test-coverage:
	@if [ -f .venv/bin/python ]; then \
		.venv/bin/python run_tests.py --type all --coverage; \
	else \
		pipenv run python run_tests.py --type all --coverage; \
	fi

# Run code linting
lint:
	@if [ -f .venv/bin/python ]; then \
		.venv/bin/python -m ruff check libs jobs tests; \
	else \
		pipenv run ruff check libs jobs tests; \
	fi

# Run static type checks
type-check:
	@if [ -f .venv/bin/python ]; then \
		.venv/bin/python -m mypy \
			libs/common/config/config_base.py \
			jobs/bronze/public_api_bronze_job.py \
			jobs/silver/public_api_silver_job.py \
			jobs/gold/public_api_gold_job.py; \
	else \
		pipenv run mypy \
			libs/common/config/config_base.py \
			jobs/bronze/public_api_bronze_job.py \
			jobs/silver/public_api_silver_job.py \
			jobs/gold/public_api_gold_job.py; \
	fi

# Format code with black
format:
	pipenv run black .

# Auto-fix code formatting and imports
autofix:
	@echo "🔧 Auto-fixing code formatting and imports..."
	@pipenv run black . || echo "⚠️  Black formatting failed"
	@pipenv run isort . || echo "⚠️  Import sorting failed"
	@pipenv run autoflake --in-place --remove-all-unused-imports --remove-unused-variables --recursive libs jobs || echo "⚠️  Autoflake failed"
	@echo "✅ Auto-fix completed"

# Submit a Spark job locally (usage: make spark-submit JOB=jobs/bronze/my_job.py ARGS="--arg1 value1")
spark-submit:
	@if [ -z "$(JOB)" ]; then \
		echo "❌ Usage: make spark-submit JOB=<job_path> [ARGS=\"<args>\"]"; \
		echo "   Example: make spark-submit JOB=jobs/bronze/public_api_bronze_job.py ARGS=\"--CREATE_TABLES true --ENTITY_TYPE posts\""; \
		exit 1; \
	fi
	@echo "🚀 Running Spark job locally: $(JOB)"
	@echo "   Environment: LocalStack (STAGE=local)"
	@bash -c 'export STAGE=local && \
	unset AWS_PROFILE && \
	python $(JOB) $(ARGS)'

# Run a PyShell job locally (usage: make pyshell-run JOB=jobs/raw/my_job.py ARGS="--arg1 value1")
pyshell-run:
	@if [ -z "$(JOB)" ]; then \
		echo "❌ Usage: make pyshell-run JOB=<job_path> [ARGS=\"<args>\"]"; \
		echo "   Example: make pyshell-run JOB=jobs/raw/public_api_raw_job.py ARGS=\"--ENTITY_TYPE posts\""; \
		exit 1; \
	fi
	@echo "🚀 Running PyShell job locally: $(JOB)"
	@echo "   Environment: LocalStack (STAGE=local)"
	@bash -c 'export STAGE=local && \
	unset AWS_PROFILE && \
	python $(JOB) $(ARGS)'

# ==========================================
# MEDALLION JOB RUNNERS BY TIER
# ==========================================
# These targets configure LocalStack and execute jobs.
# Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)
# Usage: make run-raw DATA_SOURCE=public_api ENTITY_TYPE=posts [ARGS="--MAX_PAGES_PER_BATCH 2"]

# RAW Tier - Generic Raw Jobs
# Usage: make run-raw DATA_SOURCE=public_api ENTITY_TYPE=posts [ARGS="--MAX_PAGES_PER_BATCH 2"]
# Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)
run-raw:
	@if [ -z "$(DATA_SOURCE)" ] || [ -z "$(ENTITY_TYPE)" ]; then \
		echo "❌ Usage: make run-raw DATA_SOURCE=<source> ENTITY_TYPE=<entity_type> [ARGS=\"<additional_args>\"]"; \
		echo "   Data sources: public_api"; \
		echo "   Example: make run-raw DATA_SOURCE=public_api ENTITY_TYPE=posts ARGS=\"--MAX_PAGES_PER_BATCH 2\""; \
		exit 1; \
	fi
	@if [ ! -f "jobs/raw/$(DATA_SOURCE)_raw_job.py" ]; then \
		echo "❌ Error: Raw job not found: jobs/raw/$(DATA_SOURCE)_raw_job.py"; \
		exit 1; \
	fi
	@echo "🚀 Running RAW job: $(DATA_SOURCE) - $(ENTITY_TYPE)"
	@echo "   Environment: LocalStack (STAGE=local)"
	@echo "   Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)"
	@bash -c ' \
	# Ensure STAGE=local and unset AWS_PROFILE (direnv already exports other vars) \
	export STAGE=local && \
	unset AWS_PROFILE && \
	# Execute job with common and specific arguments \
	python jobs/raw/$(DATA_SOURCE)_raw_job.py \
		--ENTITY_TYPE $(ENTITY_TYPE) \
		$${RAW_ZONE_PATH:+--RAW_ZONE_PATH "$$RAW_ZONE_PATH"} \
		$${WAREHOUSE_PATH:+--WAREHOUSE_PATH "$$WAREHOUSE_PATH"} \
		$${API_BASE_URL:+--API_BASE_URL "$$API_BASE_URL"} \
		$${API_ENDPOINT:+--API_ENDPOINT "$$API_ENDPOINT"} \
		$${API_KEY:+--API_KEY "$$API_KEY"} \
		$(ARGS)'

# BRONZE Tier - Generic Bronze Jobs
# Usage: make run-bronze DATA_SOURCE=public_api ENTITY_TYPE=posts [ARGS="--CREATE_TABLES true"]
# Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)
run-bronze:
	@if [ -z "$(DATA_SOURCE)" ] || [ -z "$(ENTITY_TYPE)" ]; then \
		echo "❌ Usage: make run-bronze DATA_SOURCE=<source> ENTITY_TYPE=<entity_type> [ARGS=\"<additional_args>\"]"; \
		echo "   Data sources: public_api"; \
		echo "   Example: make run-bronze DATA_SOURCE=public_api ENTITY_TYPE=posts"; \
		exit 1; \
	fi
	@if [ ! -f "jobs/bronze/$(DATA_SOURCE)_bronze_job.py" ]; then \
		echo "❌ Error: Bronze job not found: jobs/bronze/$(DATA_SOURCE)_bronze_job.py"; \
		exit 1; \
	fi
	@echo "🚀 Running BRONZE job: $(DATA_SOURCE) - $(ENTITY_TYPE)"
	@echo "   Environment: LocalStack (STAGE=local)"
	@echo "   Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)"
	@bash -c ' \
	# Ensure STAGE=local and unset AWS_PROFILE (direnv already exports other vars) \
	export STAGE=local && \
	unset AWS_PROFILE && \
	# Execute job with common and specific arguments \
	python jobs/bronze/$(DATA_SOURCE)_bronze_job.py \
		--ENTITY_TYPE $(ENTITY_TYPE) \
		--RAW_DATABASE_NAME $${RAW_DATABASE_NAME:-raw_zone} \
		--BRONZE_DATABASE_NAME $${BRONZE_DATABASE_NAME:-bronze_zone} \
		$${RAW_ZONE_PATH:+--RAW_ZONE_PATH "$$RAW_ZONE_PATH"} \
		$${WAREHOUSE_PATH:+--WAREHOUSE_PATH "$$WAREHOUSE_PATH"} \
		$(ARGS)'

# SILVER Tier - Generic Silver Jobs
# Usage: make run-silver DATA_SOURCE=public_api ENTITY_TYPE=posts [ARGS="--CREATE_TABLES true"]
# Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)
run-silver:
	@if [ -z "$(DATA_SOURCE)" ] || [ -z "$(ENTITY_TYPE)" ]; then \
		echo "❌ Usage: make run-silver DATA_SOURCE=<source> ENTITY_TYPE=<entity_type> [ARGS=\"<additional_args>\"]"; \
		echo "   Data sources: public_api"; \
		echo "   Example: make run-silver DATA_SOURCE=public_api ENTITY_TYPE=posts"; \
		exit 1; \
	fi
	@if [ ! -f "jobs/silver/$(DATA_SOURCE)_silver_job.py" ]; then \
		echo "❌ Error: Silver job not found: jobs/silver/$(DATA_SOURCE)_silver_job.py"; \
		exit 1; \
	fi
	@echo "🚀 Running SILVER job: $(DATA_SOURCE) - $(ENTITY_TYPE)"
	@echo "   Environment: LocalStack (STAGE=local)"
	@echo "   Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)"
	@bash -c ' \
	# Ensure STAGE=local and unset AWS_PROFILE (direnv already exports other vars) \
	export STAGE=local && \
	unset AWS_PROFILE && \
	# Execute job with common and specific arguments \
	python jobs/silver/$(DATA_SOURCE)_silver_job.py \
		--ENTITY_TYPE $(ENTITY_TYPE) \
		--BRONZE_DATABASE_NAME $${BRONZE_DATABASE_NAME:-bronze_zone} \
		--SILVER_DATABASE_NAME $${SILVER_DATABASE_NAME:-silver_zone} \
		$${WAREHOUSE_PATH:+--WAREHOUSE_PATH "$$WAREHOUSE_PATH"} \
		$(ARGS)'

# GOLD Tier - Generic Gold Jobs
# Usage: make run-gold DATA_SOURCE=public_api ENTITY_TYPE=posts [ARGS="--CREATE_TABLES true"]
# Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)
run-gold:
	@if [ -z "$(DATA_SOURCE)" ] || [ -z "$(ENTITY_TYPE)" ]; then \
		echo "❌ Usage: make run-gold DATA_SOURCE=<source> ENTITY_TYPE=<entity_type> [ARGS=\"<additional_args>\"]"; \
		echo "   Data sources: public_api"; \
		echo "   Example: make run-gold DATA_SOURCE=public_api ENTITY_TYPE=posts"; \
		exit 1; \
	fi
	@if [ ! -f "jobs/gold/$(DATA_SOURCE)_gold_job.py" ]; then \
		echo "❌ Error: Gold job not found: jobs/gold/$(DATA_SOURCE)_gold_job.py"; \
		exit 1; \
	fi
	@echo "🚀 Running GOLD job: $(DATA_SOURCE) - $(ENTITY_TYPE)"
	@echo "   Environment: LocalStack (STAGE=local)"
	@echo "   Note: Assumes environment variables are loaded via direnv (run 'direnv allow' first)"
	@bash -c ' \
	# Ensure STAGE=local and unset AWS_PROFILE (direnv already exports other vars) \
	export STAGE=local && \
	unset AWS_PROFILE && \
	# Execute job with common and specific arguments \
		python jobs/gold/$(DATA_SOURCE)_gold_job.py \
		--ENTITY_TYPE $(ENTITY_TYPE) \
		--SILVER_DATABASE_NAME $${SILVER_DATABASE_NAME:-silver_zone} \
		--GOLD_DATABASE_NAME $${GOLD_DATABASE_NAME:-gold_zone} \
		$${WAREHOUSE_PATH:+--WAREHOUSE_PATH "$$WAREHOUSE_PATH"} \
		$(ARGS)'

# Run database migrations
migrate:
	./scripts/run-migrations.sh

# Upload migration files to S3 (LocalStack for dev container)
migrate-upload:
	@echo "📁 Uploading migration files to LocalStack S3..."
	@if [ ! -d "migrations" ]; then \
		echo "❌ migrations/ directory not found"; \
		exit 1; \
	fi
	@echo "📡 Uploading to LocalStack S3..."
	aws --endpoint-url=http://localstack:4566 s3 cp migrations/ s3://local-bucket/migrations/ --recursive
	@echo "✅ Migration files uploaded successfully"

# Show migration configuration without executing
migrate-dry-run:
	@echo "🔍 Migration configuration (dev container):"
	@echo "Environment: dev-container"
	@echo "Bucket: local-bucket"
	@echo "Database Prefix: boilerplate"
	@echo "Migrations Path: migrations/"
	@echo "Raw Zone Path: s3://local-bucket/bronze/"
	@echo "Iceberg Database: local_iceberg_db"
	@echo "AWS Region: us-east-1"
	@echo "LocalStack Endpoint: http://localhost:4566"
	@echo ""
	@echo "Enhanced Features:"
	@echo "  Rollback: true"
	@echo "  Preconditions: true"
	@echo "  Timeout: 300s"
	@echo "  Retry Attempts: 3"
	@echo ""
	@echo "To run migrations: make migrate"

# =============================================================================
# Code Registry Integration (for production deployment)
# =============================================================================

.PHONY: package glue-wheels clean-build package-clean deploy-latest

package:
	@echo "🏗️ Packaging Glue job artifacts with Python wheels..."
	./scripts/package.sh

# Build AWS Glue optimized uber wheels
glue-wheels:
	@echo "🚀 Building AWS Glue optimized uber wheels..."
	./scripts/build-glue-wheels.sh

# Build uber wheels with custom options
glue-wheels-custom:
	@echo "🚀 Building AWS Glue optimized uber wheels with custom options..."
	@echo "Usage: make glue-wheels-custom ARGS='-v 1.2.3 -g 5.0'"
	./scripts/build-glue-wheels.sh $(ARGS)

clean-build:
	@echo "🧹 Cleaning build artifacts..."
	rm -rf build/

package-clean: clean-build package
	@echo "✅ Clean package completed"

deploy-latest: package
	@echo "🚀 Deploying latest version to Code Registry..."
	@if [ -z "$$CODE_REGISTRY_BUCKET" ]; then \
		echo "❌ CODE_REGISTRY_BUCKET environment variable not set"; \
		exit 1; \
	fi
	aws s3 cp build/wheels.tar.gz s3://$$CODE_REGISTRY_BUCKET/wheels/libs-wheels-latest.tar.gz
	aws s3 sync build/wheels/ s3://$$CODE_REGISTRY_BUCKET/wheels/latest/ --delete
	aws s3 cp build/requirements.txt s3://$$CODE_REGISTRY_BUCKET/dependencies/requirements-latest.txt
	aws s3 sync jobs/ s3://$$CODE_REGISTRY_BUCKET/scripts/latest/jobs/ --delete
	@if [ -d "migrations" ]; then \
		aws s3 sync migrations/ s3://$$CODE_REGISTRY_BUCKET/migrations/latest/; \
	fi
	@echo "✅ Latest deployment completed"
