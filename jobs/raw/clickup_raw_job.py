"""
ClickUp Raw Data Ingestion - Technology Requests PyShell Script

This script fetches data from ClickUp API v2 and writes to S3 using the PyShellJobBase framework.
Extracts tasks from multiple lists (Architecture Requests, Learning R&D Requests, Modernization & Infrastructure Requests)
with custom fields support.

Designed specifically for AWS Glue PyShell environment.

Features:
- Multi-list support with configuration
- Direct API integration with ClickUp API v2
- Custom fields extraction
- Rate limiting and retry logic via ClickUpAPIClient
- Pagination handling
- S3 storage as JSONL files with partitioning by date created
- Pydantic configuration validation

Usage:
    # Extract tasks from all configured lists
    python jobs/raw/clickup_raw_job.py \
        --JOB_NAME clickup_raw_requests \
        --CLICKUP_API_SECRET_NAME nan-data-lake-develop/clickup-api \
        --LIST_IDS "12345678,87654321,11223344"
"""

# ==========================================
# AUTO-FIX: AWS Glue adds the directory containing libs.zip to sys.path,
# but Python needs the ZIP file itself in sys.path to import from it.
# This fix automatically detects and adds the ZIP file.
# ==========================================
import glob
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from botocore.exceptions import ClientError

glue_libs_dirs = glob.glob("/tmp/glue-python-libs-*")
for glue_dir in glue_libs_dirs:
    # Find any .zip file in the directory (AWS Glue may use different names)
    zip_files = glob.glob(os.path.join(glue_dir, "*.zip"))
    for libs_zip_path in zip_files:
        if libs_zip_path not in sys.path:
            sys.path.insert(0, libs_zip_path)
            # Found and added a zip file, exit both loops
            break
    else:
        # Continue to next glue_dir if no zip was added
        continue
    # Break outer loop if we added a zip file
    break

# ==========================================
# IMPORTS
# ==========================================
# Note: Imports are after the Glue auto-fix code above, which is required for AWS Glue environment
from pydantic import Field  # noqa: E402

from libs.common import (  # noqa: E402
    RawJobConfig,
    print_workflow_properties,
    setup_logging,
)
from libs.common.clickup import ClickUpAPIClient, ClickUpNotFoundError  # noqa: E402
from libs.pyshell import PyShellJobBase  # noqa: E402

# ==========================================
# LOGGING
# Logging is configured automatically by PyShellJobBase for CloudWatch visibility.
# Log messages will appear in CloudWatch log group: /aws-glue/python-jobs/output
# ==========================================
logger = logging.getLogger(__name__)


def _get_required_env_var(var_name: str) -> str:
    """Get required environment variable or raise error."""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value


# ==========================================
# CLICKUP RAW CONFIGURATION
# ==========================================
class ClickUpRawConfig(RawJobConfig):
    """
    ClickUp-specific Raw configuration.

    Extends RawJobConfig with ClickUp-specific fields.
    Auto-configures raw_write_path for organized S3 structure.
    """

    # Source name with default value
    source_name: str = Field(
        default="clickup",
        description="Source system name (ClickUp)",
    )

    # Job config parameters (Glue/Env vars)
    # Required fields - ConfigBase resolves: Workflow → CLI → ENV → Error
    clickup_api_secret_name: str = Field(
        ...,
        description="AWS Secrets Manager secret name containing ClickUp API token",
    )
    list_ids: str = Field(
        ...,
        description=(
            "Comma-separated list of ClickUp list IDs to extract tasks from. "
            "Example: '12345678,87654321,11223344'"
        ),
    )

    # Optional fields with defaults - ConfigBase resolves: Workflow → CLI → ENV → Default
    api_base_url: str = Field(
        default="https://api.clickup.com",
        description="ClickUp API base URL",
    )
    rate_limit_per_minute: int = Field(
        default=100,
        description="Rate limit for ClickUp API calls per minute (Business plan: 100)",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for API requests",
    )
    batch_size: int = Field(
        default=1000,
        description="Batch size for S3 writes",
    )
    include_closed: bool = Field(
        default=True,
        description="Whether to include closed tasks",
    )
    include_custom_fields: bool = Field(
        default=True,
        description="Whether to fetch custom fields for tasks",
    )
    api_token: Optional[str] = Field(
        default=None,
        description="ClickUp API token (loaded from Secrets Manager if not provided)",
    )
    raw_write_path: Optional[str] = Field(
        default=None,
        description="Computed S3 path for writing ClickUp raw data (auto-configured in model_post_init)",
    )
    raw_database_name: Optional[str] = Field(
        default=None,
        description="Raw database name for Glue Data Catalog table creation (optional, can be set via RAW_DATABASE_NAME env var)",
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure raw_write_path: raw_zone_path + clickup/tasks/"""
        # Construct write path: raw_zone_path + clickup/tasks/
        # Example: s3://bucket/raw-zone/ + clickup/tasks/ = s3://bucket/raw-zone/clickup/tasks/
        self.raw_write_path = f"{self.raw_zone_path}clickup/tasks/"


# ==========================================
# CLICKUP PYSHELL JOB
# ==========================================
class ClickUpRawJob(PyShellJobBase):
    """
    ClickUp raw data ingestion job using PyShell.

    This job:
    1. Fetches tasks from multiple ClickUp lists with pagination
    2. Handles rate limiting and retries
    3. Writes data to S3 with partitioning by date created
    """

    def __init__(self, config: ClickUpRawConfig):
        """
        Initialize ClickUp PyShell job.

        Automatically configures list-specific settings and loads API credentials.
        """
        super().__init__(config)
        self.config: ClickUpRawConfig = config  # Type hint for IDE support

        # Parse list IDs from comma-separated string
        self.list_ids = [
            list_id.strip()
            for list_id in self.config.list_ids.split(",")
            if list_id.strip()
        ]
        if not self.list_ids:
            raise ValueError(
                f"No valid list IDs found in '{self.config.list_ids}'. "
                f"Please provide comma-separated list IDs."
            )

        self.logger.info(f"📦 List IDs: {len(self.list_ids)} lists")
        for i, list_id in enumerate(self.list_ids, 1):
            self.logger.info(f"   {i}. {list_id}")

        # Use raw_zone_path as the S3 path (from Terraform)
        self.logger.info(f"   Using raw zone path: {self.config.raw_zone_path}")

        # Load API credentials from Secrets Manager (required)
        self.logger.info(
            f"Loading ClickUp API credentials from Secrets Manager: {self.config.clickup_api_secret_name}"
        )

        # Load credentials (this will try Secrets Manager first, then fallback to env var)
        self._load_clickup_credentials_from_secret()

        self.logger.info("✅ ClickUp API credentials loaded successfully")

        # Initialize ClickUp API client
        self.api_client = ClickUpAPIClient(
            api_token=self.config.api_token,
            base_url=self.config.api_base_url,
            max_retries=self.config.max_retries,
            backoff_factor=1.0,
            max_backoff=60.0,
            rate_limit_per_minute=self.config.rate_limit_per_minute,
        )

        self.logger.info("✅ ClickUp API client initialized")

        # Cache for custom field mappings (list_id -> field_id -> field_name)
        self.custom_field_cache: Dict[str, Dict[str, str]] = {}

    def _load_clickup_credentials_from_secret(self) -> None:
        """Load API token from AWS Secrets Manager."""
        try:
            self.logger.info(
                f"Loading credentials from secret: {self.config.clickup_api_secret_name}"
            )
            response = self.secrets_client.get_secret_value(
                SecretId=self.config.clickup_api_secret_name
            )
            secret_data = json.loads(response["SecretString"])
            # Update config with secret values
            if "api_token" in secret_data and not self.config.api_token:
                self.config.api_token = (
                    secret_data["api_token"].strip()
                    if isinstance(secret_data["api_token"], str)
                    else secret_data["api_token"]
                )
            elif "token" in secret_data and not self.config.api_token:
                self.config.api_token = (
                    secret_data["token"].strip()
                    if isinstance(secret_data["token"], str)
                    else secret_data["token"]
                )

            # Validate token was loaded
            if self.config.api_token:
                token_preview = (
                    f"{self.config.api_token[:4]}...{self.config.api_token[-4:]}"
                    if len(self.config.api_token) > 8
                    else "****"
                )
                self.logger.info(
                    f"✅ Credentials loaded successfully "
                    f"(token: {token_preview}, length: {len(self.config.api_token)})"
                )
            else:
                self.logger.warning("⚠️ Token not found in secret data")
        except ClientError as e:
            self.logger.warning(f"Failed to load credentials from Secrets Manager: {e}")
            self.logger.info("Will try to use environment variable or other methods")
            # Try environment variable as fallback
            if not self.config.api_token:
                env_token = os.getenv("CLICKUP_API_TOKEN")
                if env_token:
                    self.config.api_token = env_token.strip()
                    token_preview = (
                        f"{self.config.api_token[:4]}...{self.config.api_token[-4:]}"
                        if len(self.config.api_token) > 8
                        else "****"
                    )
                    self.logger.info(
                        f"Using API token from environment variable "
                        f"(token: {token_preview}, length: {len(self.config.api_token)})"
                    )

        if not self.config.api_token:
            raise ValueError(
                "ClickUp API token not found in Secrets Manager or environment variables"
            )

    def _get_custom_field_ids(self, list_id: str) -> List[str]:
        """
        Get custom field IDs for a list.

        Args:
            list_id: ClickUp list ID

        Returns:
            List of custom field IDs
        """
        if list_id in self.custom_field_cache:
            return list(self.custom_field_cache[list_id].keys())

        try:
            self.logger.debug(f"Fetching custom fields for list {list_id}")
            custom_fields = self.api_client.get_list_custom_fields(list_id)

            # Build cache mapping field_id -> field_name
            field_mapping = {}
            field_ids = []
            for field in custom_fields:
                field_id = field.get("id")
                field_name = field.get("name", "")
                if field_id:
                    field_mapping[field_id] = field_name
                    field_ids.append(field_id)

            self.custom_field_cache[list_id] = field_mapping
            self.logger.info(f"Found {len(field_ids)} custom fields for list {list_id}")
            return field_ids

        except ClickUpNotFoundError:
            self.logger.warning(
                f"Custom fields endpoint not found for list {list_id}. "
                f"Continuing without custom fields."
            )
            self.custom_field_cache[list_id] = {}
            return []
        except Exception as e:
            self.logger.warning(
                f"Error fetching custom fields for list {list_id}: {str(e)}. "
                f"Continuing without custom fields."
            )
            self.custom_field_cache[list_id] = {}
            return []

    def fetch_data_with_pagination(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch tasks from all configured ClickUp lists with pagination support.

        Returns:
            Tuple of (data_records, request_info_list)
        """
        data_records = []
        request_info_list = []
        total_tasks = 0

        self.logger.info(f"Fetching tasks from {len(self.list_ids)} ClickUp lists")

        for list_id in self.list_ids:
            list_start_time = datetime.now()
            list_tasks_count = 0

            self.logger.info(f"📋 Processing list: {list_id}")

            try:
                # Note: We don't pass custom_fields as a parameter because:
                # 1. ClickUp API returns custom_fields in task responses by default
                # 2. The custom_fields[] parameter is for filtering, not inclusion
                # 3. Passing custom_fields causes "Failed to parse and verify custom field filters" errors
                # Custom fields will still be included in the task responses automatically

                # Fetch all tasks from this list (custom_fields are included automatically)
                for task in self.api_client.get_tasks(
                    list_id=list_id,
                    include_closed=self.config.include_closed,
                    custom_fields=None,  # Don't pass custom_fields - they come in response automatically
                ):
                    # Add metadata about which list this task came from
                    task_with_metadata = task.copy()
                    task_with_metadata["_metadata"] = {
                        "list_id": list_id,
                        "extracted_at": datetime.now().isoformat() + "Z",
                    }

                    data_records.append(task_with_metadata)
                    list_tasks_count += 1
                    total_tasks += 1

                    # Log progress periodically
                    if list_tasks_count % 100 == 0:
                        self.logger.info(
                            f"Processed {list_tasks_count} tasks from list {list_id}"
                        )

                # Create request info for this list
                list_end_time = datetime.now()
                request_info = {
                    "request_timestamp": list_start_time.isoformat() + "Z",
                    "request_url": f"{self.config.api_base_url}/api/v2/list/{list_id}/task",
                    "response_status_code": 200,
                    "response_time_ms": int(
                        (list_end_time - list_start_time).total_seconds() * 1000
                    ),
                    "list_id": list_id,
                    "tasks_fetched": list_tasks_count,
                }
                request_info_list.append(request_info)

                self.logger.info(
                    f"✅ Fetched {list_tasks_count} tasks from list {list_id}"
                )

            except ClickUpNotFoundError as e:
                self.logger.error(
                    f"❌ List not found (404) for list ID '{list_id}'. "
                    f"This list may not exist or may not be accessible."
                )
                self.logger.error(f"Error details: {str(e)}")
                self.logger.warning(f"⚠️ Skipping list '{list_id}' - not found")
                # Continue with next list instead of failing the job
                continue
            except Exception as e:
                self.logger.error(f"Error fetching tasks from list {list_id}: {str(e)}")
                # For critical errors, we might want to raise, but for now continue
                self.logger.warning(f"⚠️ Skipping list '{list_id}' due to error")
                continue

        self.logger.info(
            f"✅ Fetched {total_tasks} total tasks from {len(self.list_ids)} lists"
        )

        return data_records, request_info_list

    def extract(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract data from ClickUp API.

        Returns:
            Tuple of (data_records, request_info_list)
        """
        self.logger.info("=" * 80)
        self.logger.info("📥 EXTRACT PHASE: Fetching tasks from ClickUp API")
        self.logger.info("=" * 80)

        # Fetch data with pagination
        data_records, request_info_list = self.fetch_data_with_pagination()

        self.logger.info(
            f"✅ Successfully extracted {len(data_records)} tasks from ClickUp API"
        )
        self.logger.info("=" * 80)

        return data_records, request_info_list

    def transform(
        self, data: Tuple[List[Dict], List[Dict]]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Transform data.

        Default implementation passes data through as-is.
        ClickUp API returns JSON format, so we preserve the full structure.

        Args:
            data: Tuple of (data_records, request_info_list)

        Returns:
            Transformed data tuple
        """
        self.logger.info("=" * 80)
        self.logger.info("🔄 TRANSFORM PHASE: Processing tasks")
        self.logger.info("=" * 80)

        data_records, request_info_list = data

        self.logger.info(f"Transforming {len(data_records)} tasks...")

        # ClickUp API returns JSON format - preserve full structure
        processed_records = []
        for record in data_records:
            # Preserve full JSON structure
            processed_records.append(record)

        self.logger.info(f"✅ Successfully transformed {len(processed_records)} tasks")
        self.logger.info("=" * 80)

        return processed_records, request_info_list

    def load(self, data: Tuple[List[Dict], List[Dict]]) -> None:
        """
        Load data to S3.

        Partitioning strategy:
        - Tasks are partitioned by year/month based on date_created field
        - If date_created is not available, use current date

        Args:
            data: Tuple of (data_records, request_info_list)
        """
        self.logger.info("=" * 80)
        self.logger.info("💾 LOAD PHASE: Writing to S3")
        self.logger.info("=" * 80)

        data_records, request_info_list = data

        if not data_records:
            self.logger.warning("⚠️  No tasks to load. Exiting.")
            self.logger.info("=" * 80)
            return

        self.logger.info(f"Loading {len(data_records)} tasks to S3...")
        self.logger.info(f"S3 Path: {self.config.raw_write_path}")

        # Group tasks by partition (year/month based on date_created)
        # ClickUp task date_created is in milliseconds timestamp
        partitioned_tasks: Dict[Tuple[int, int], List[Dict]] = {}
        current_time = datetime.now()
        default_partition = (current_time.year, current_time.month)

        for task in data_records:
            # Try to extract date_created from task
            partition = default_partition
            date_created = task.get("date_created")
            if date_created:
                try:
                    # ClickUp uses milliseconds timestamp
                    if isinstance(date_created, (int, float)):
                        timestamp_ms = int(date_created)
                        timestamp_s = timestamp_ms / 1000
                        task_date = datetime.fromtimestamp(timestamp_s)
                        partition = (task_date.year, task_date.month)
                except (ValueError, OSError, OverflowError) as e:
                    self.logger.debug(
                        f"Could not parse date_created '{date_created}': {e}. "
                        f"Using default partition."
                    )

            if partition not in partitioned_tasks:
                partitioned_tasks[partition] = []
            partitioned_tasks[partition].append(task)

        self.logger.info(
            f"📅 Partitioning strategy: {len(partitioned_tasks)} partitions by year/month"
        )

        # Write each partition separately
        total_written = 0
        for (year, month), tasks in partitioned_tasks.items():
            partition_cols = {"year": year, "month": month}
            self.logger.info(
                f"Writing {len(tasks)} tasks to partition year={year}, month={month}"
            )

            # Write to S3 using base class method
            self.write_to_s3(
                data=tasks,
                request_info_list=request_info_list,
                partition_cols=partition_cols,
            )
            total_written += len(tasks)

        self.logger.info(f"✅ Successfully loaded {total_written} tasks to S3")
        self.logger.info("=" * 80)
        # Note: Table creation is handled by the Bronze job, not the Raw job
        # The Raw job only writes data to S3


# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    """Main execution function."""
    setup_logging()
    logger.info("=" * 80)
    logger.info("🚀 CLICKUP RAW DATA INGESTION - STARTING")
    logger.info("=" * 80)
    logger.info(f"📅 Timestamp: {datetime.now()}")
    logger.info(f"🐍 Python: {sys.version}")
    logger.info("=" * 80)

    # Print workflow runtime properties if available
    print_workflow_properties()

    try:
        # Load configuration with validation
        config = ClickUpRawConfig.from_args()

        # Reconfigure logging with config value (in case it was set via Workflow Properties)
        # This ensures the final log level matches the resolved config
        logging.getLogger().setLevel(getattr(logging, config.log_level))

        # Log complete configuration with sources
        config.log_configuration()

        # Log configuration
        logger.info("=" * 80)
        logger.info("📋 CONFIGURATION")
        logger.info("=" * 80)
        logger.info(f"  Job Name: {config.job_name}")
        logger.info(f"  List IDs: {config.list_ids}")
        logger.info(f"  Source: {config.source_name}")
        logger.info(f"  AWS Region: {config.aws_region}")
        logger.info(f"  Raw Zone Path: {config.raw_zone_path}")
        logger.info(f"  Warehouse Path: {config.warehouse_path}")
        logger.info(f"  ClickUp API Base URL: {config.api_base_url}")
        logger.info(f"  Secrets Manager: {config.clickup_api_secret_name}")
        # Safely display API token (mask most of it) - loaded from Secrets Manager
        api_token_value = str(config.api_token) if config.api_token else ""
        masked_token = (
            "***" + api_token_value[-4:]
            if len(api_token_value) > 4
            else "LOADED FROM SECRETS"
        )
        logger.info(f"  API Token Status: {masked_token}")
        logger.info(f"  Rate Limit: {config.rate_limit_per_minute}/min")
        logger.info(f"  Include Closed: {config.include_closed}")
        logger.info(f"  Include Custom Fields: {config.include_custom_fields}")
        logger.info("=" * 80)

        # Create and run job
        job = ClickUpRawJob(config)
        job.run()

        logger.info("=" * 80)
        logger.info("✅ JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ JOB FAILED: {type(e).__name__}")
        logger.error(f"   Message: {str(e)}")
        logger.error("=" * 80)

        import traceback

        logger.error("\n📋 Full traceback:")
        logger.error(traceback.format_exc())

        sys.exit(1)


if __name__ == "__main__":
    main()
