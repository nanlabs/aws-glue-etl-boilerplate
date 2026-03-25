"""
Team Tailor Raw Data Ingestion - Multi-Entity PyShell Script

This script fetches data from Team Tailor API and writes to S3 using the PyShellJobBase framework.
Supports 9 Team Tailor entities (candidates, jobs, applications, interviews, users, departments, stages,
application_stage_transitions fetched as relationships, plus nps_responses).
Activities are fetched as relationships from parent resources (recommended approach).
Designed specifically for AWS Glue PyShell environment.

Features:
- Multi-entity support with configuration
- Direct API integration with Team Tailor (JSON API Specification)
- Activities fetched as relationships (works even when /v1/activities endpoint is unavailable)
- Rate limiting and retry logic via PyShellJobBase
- Pagination handling
- S3 storage as JSON files with partitioning
- Pydantic configuration validation

Supported Entities (9 total):
- candidates: Candidate profiles and information
- jobs: Job postings and positions
- applications: Candidate applications to jobs
- interviews: Interview events and outcomes
- users: Recruiters and hiring managers
- departments: Department structure
- stages: Application stages
- application_stage_transitions: Stage transition activities for applications (Time to Hire tracking)
- nps_responses: NPS (Net Promoter Score) survey responses (direct endpoint)

Usage:
    # Extract candidates (default)
    python jobs/raw/teamtailor_raw_job.py \
        --JOB_NAME teamtailor_raw_candidates \
        --ENTITY_TYPE candidates

    # Extract applications
    python jobs/raw/teamtailor_raw_job.py \
        --JOB_NAME teamtailor_raw_applications \
        --ENTITY_TYPE applications

    # Extract interviews
    python jobs/raw/teamtailor_raw_job.py \
        --JOB_NAME teamtailor_raw_interviews \
        --ENTITY_TYPE interviews
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

from pydantic import Field  # noqa: E402

from libs.common import (  # noqa: E402
    RawJobConfig,
    print_workflow_properties,
    setup_logging,
)
from libs.common.teamtailor import (  # noqa: E402
    TeamTailorAPIClient,
    TeamTailorNotFoundError,
)
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
# TEAM TAILOR RAW CONFIGURATION
# ==========================================
class TeamTailorRawConfig(RawJobConfig):
    """
    Team Tailor-specific Raw configuration.

    Extends RawJobConfig with Team Tailor-specific fields.
    Auto-configures raw_write_path for organized S3 structure.
    """

    # Source name with default value
    source_name: str = Field(
        default="teamtailor",
        description="Source system name (Team Tailor)",
    )

    # Job config parameters (Glue/Env vars)
    # Required fields - ConfigBase resolves: Workflow → CLI → ENV → Error
    teamtailor_api_secret_name: str = Field(
        ...,
        description="AWS Secrets Manager secret name containing Team Tailor API token",
    )
    entity_type: str = Field(
        ...,
        description=(
            "Entity type: candidates, jobs, applications, interviews, users, departments, stages, "
            "application_stage_transitions, nps_responses. "
            "Activity entity types fetch data as relationships from parent resources."
        ),
    )

    # Optional fields with defaults - ConfigBase resolves: Workflow → CLI → ENV → Default
    teamtailor_api_base_url_param: Optional[str] = Field(
        default_factory=lambda: os.getenv(
            "TEAMTAILOR_API_BASE_URL_PARAM",
            "/nan-wl-workloads-data-lake-develop/teamtailor-api-base-url",
        ),
        description="SSM Parameter Store parameter name containing Team Tailor API base URL (overrides api_base_url if provided)",
    )
    api_base_url: str = Field(
        default="https://api.teamtailor.com",
        description="Team Tailor API base URL (EU: api.teamtailor.com, US West: api.na.teamtailor.com). Ignored if teamtailor_api_base_url_param is set.",
    )
    rate_limit_per_second: float = Field(
        default=2.0,
        description="Rate limit for Team Tailor API calls per second",
    )
    max_pages_per_batch: int = Field(
        default=50,
        description="Maximum pages to fetch per batch",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries for API requests",
    )
    batch_size: int = Field(
        default=1000,
        description="Batch size for S3 writes",
    )
    api_filter_params: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional API filter parameters to pass to TeamTailor API (e.g., {'filter[status]': 'all'}). If None, no filters are applied (gets all records).",
    )
    api_token: Optional[str] = Field(
        default=None,
        description="Team Tailor API token (loaded from Secrets Manager if not provided)",
    )
    raw_write_path: Optional[str] = Field(
        default=None,
        description="Computed S3 path for writing Team Tailor raw data (auto-configured in model_post_init)",
    )

    def model_post_init(self, __context) -> None:
        """Auto-configure raw_write_path: raw_zone_path + teamtailor/{entity_type}/"""
        # Construct write path: raw_zone_path + teamtailor/{entity_type}/
        # Example: s3://bucket/raw-zone/ + teamtailor/candidates/ = s3://bucket/raw-zone/teamtailor/candidates/
        self.raw_write_path = f"{self.raw_zone_path}teamtailor/{self.entity_type}/"


# ==========================================
# TEAM TAILOR ENTITIES CONFIGURATION
# ==========================================
TEAMTAILOR_ENTITIES = {
    "candidates": {
        "s3_prefix": "teamtailor/talent/candidates",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "Candidate profiles and information",
        "default_api_filters": {},  # No filters needed - get all candidates
    },
    "jobs": {
        "s3_prefix": "teamtailor/talent/jobs",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "Job postings and positions",
        "default_api_filters": {
            "filter[status]": "all"
        },  # Get all jobs including closed/archived
    },
    "applications": {
        "s3_prefix": "teamtailor/talent/applications",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "Candidate applications to jobs",
        "default_api_filters": {
            "include": "job,candidate,stage"  # Include related IDs in response
        },
    },
    "interviews": {
        "s3_prefix": "teamtailor/talent/interviews",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "Interview events and outcomes",
        "default_api_filters": {},  # No filters needed - get all interviews
    },
    "users": {
        "s3_prefix": "teamtailor/talent/users",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "Recruiters and hiring managers",
        "default_api_filters": {},  # No filters needed - get all users
    },
    "departments": {
        "s3_prefix": "teamtailor/talent/departments",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "Department structure",
        "default_api_filters": {},  # No filters needed - get all departments
    },
    "stages": {
        "s3_prefix": "teamtailor/talent/stages",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "Application stages",
        "default_api_filters": {},  # No filters needed - get all stages
    },
    # DISABLED: application_stage_transitions
    # The Activities API endpoint was REMOVED by TeamTailor on 2020-10-01
    # See: https://docs.teamtailor.com/changelog - "since it didn't work anyway"
    # Use changed-stage-at field in job-applications instead for time tracking
    # "application_stage_transitions": {
    #     "s3_prefix": "teamtailor/talent/application_stage_transitions",
    #     "supports_date_filter": False,
    #     "page_size": 30,
    #     "description": "Stage transition activities for applications (Time to Hire tracking)",
    #     "requires_parent_fetch": True,
    #     "parent_resource": "job-applications",
    #     "activity_types": ["stage-transition"],
    #     "default_api_filters": {},
    # },
    "nps_responses": {
        "s3_prefix": "teamtailor/talent/nps_responses",
        "supports_date_filter": False,
        "page_size": 30,
        "description": "NPS (Net Promoter Score) survey responses from candidates",
        "default_api_filters": {},  # No filters needed - get all NPS responses
    },
}


# ==========================================
# TEAM TAILOR PYSHELL JOB
# ==========================================
class TeamTailorRawJob(PyShellJobBase):
    """
    Team Tailor raw data ingestion job using PyShell.

    This job:
    1. Fetches data from Team Tailor API with pagination
    2. Handles rate limiting and retries
    3. Writes data to S3 with partitioning
    """

    def __init__(self, config: TeamTailorRawConfig):
        """
        Initialize Team Tailor PyShell job.

        Automatically configures entity-specific settings and loads API credentials.
        """
        super().__init__(config)
        self.config: TeamTailorRawConfig = config  # Type hint for IDE support

        # Validate and configure entity type
        if self.config.entity_type not in TEAMTAILOR_ENTITIES:
            valid_entities = ", ".join(TEAMTAILOR_ENTITIES.keys())
            raise ValueError(
                f"Invalid entity_type '{self.config.entity_type}'. "
                f"Valid options: {valid_entities}"
            )

        self.entity_config = TEAMTAILOR_ENTITIES[self.config.entity_type]
        self.logger.info(f"📦 Entity Type: {self.config.entity_type}")
        self.logger.info(f"   Description: {self.entity_config['description']}")

        # Use raw_zone_path as the S3 path (from Terraform)
        self.logger.info(f"   Using raw zone path: {self.config.raw_zone_path}")

        # Load API credentials from Secrets Manager (required)
        self.logger.info(
            f"Loading Team Tailor API credentials from Secrets Manager: {self.config.teamtailor_api_secret_name}"
        )

        # Load credentials (this will try Secrets Manager first, then fallback to env var)
        self._load_teamtailor_credentials_from_secret()

        # Load API base URL from SSM Parameter Store if configured
        self._load_teamtailor_base_url_from_ssm()

        self.logger.info("✅ Team Tailor API credentials loaded successfully")

        # Initialize Team Tailor API client
        self.api_client = TeamTailorAPIClient(
            api_token=self.config.api_token,
            base_url=self.config.api_base_url,
            max_retries=self.config.max_retries,
            backoff_factor=1.0 / self.config.rate_limit_per_second,
        )

        self.logger.info("✅ Team Tailor API client initialized")

    def _load_teamtailor_credentials_from_secret(self) -> None:
        """Load API token from AWS Secrets Manager."""
        try:
            self.logger.info(
                f"Loading credentials from secret: {self.config.teamtailor_api_secret_name}"
            )
            response = self.secrets_client.get_secret_value(
                SecretId=self.config.teamtailor_api_secret_name
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
            if "api_base_url" in secret_data and not self.config.api_base_url:
                self.config.api_base_url = secret_data["api_base_url"]

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
                env_token = os.getenv("TEAMTAILOR_API_TOKEN")
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
                "Team Tailor API token not found in Secrets Manager or environment variables"
            )

    def _load_teamtailor_base_url_from_ssm(self) -> None:
        """Load API base URL from SSM Parameter Store if configured.

        If teamtailor_api_base_url_param is a URL (starts with http:// or https://),
        use it directly. Otherwise, treat it as an SSM parameter name and read the value.
        """
        if not self.config.teamtailor_api_base_url_param:
            self.logger.debug(
                "No teamtailor_api_base_url_param configured, using default or config value"
            )
            return

        # Check if it's already a URL (from .envrc which exports the value directly)
        param_value = self.config.teamtailor_api_base_url_param.strip()
        if param_value.startswith("http://") or param_value.startswith("https://"):
            self.config.api_base_url = param_value
            self.logger.info(
                f"✅ API base URL set directly from parameter: {self.config.api_base_url}"
            )
            return

        # Otherwise, treat it as an SSM parameter name and read the value
        try:
            from libs.common.utils.aws import create_boto3_client

            self.logger.info(
                f"Loading API base URL from SSM Parameter Store: {self.config.teamtailor_api_base_url_param}"
            )

            ssm_client = create_boto3_client(
                service_name="ssm",
                region_name=self.config.aws_region,
                endpoint_url=self.config.aws_endpoint_url,
            )

            response = ssm_client.get_parameter(
                Name=self.config.teamtailor_api_base_url_param, WithDecryption=True
            )

            base_url = response["Parameter"]["Value"].strip()
            if base_url:
                self.config.api_base_url = base_url
                self.logger.info(
                    f"✅ API base URL loaded from SSM: {self.config.api_base_url}"
                )
            else:
                self.logger.warning(
                    "⚠️ SSM parameter returned empty value, using default"
                )

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "ParameterNotFound":
                self.logger.warning(
                    f"SSM parameter not found: {self.config.teamtailor_api_base_url_param}. "
                    f"Using default API base URL: {self.config.api_base_url}"
                )
            else:
                self.logger.warning(
                    f"Failed to load API base URL from SSM: {e}. "
                    f"Using default API base URL: {self.config.api_base_url}"
                )
        except Exception as e:
            self.logger.warning(
                f"Unexpected error loading API base URL from SSM: {e}. "
                f"Using default API base URL: {self.config.api_base_url}"
            )

    def fetch_data_with_pagination(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch data from Team Tailor API with pagination support.

        Works for any entity type (candidates, jobs, applications, etc.).

        Returns:
            Tuple of (data_records, request_info_list)
        """
        data_records = []
        request_info_list = []
        record_count = 0

        self.logger.info(f"Fetching all {self.config.entity_type} from Team Tailor API")

        # Check if this entity type requires fetching activities as relationships
        if self.entity_config.get("requires_parent_fetch"):
            return self._fetch_activities_as_relationships()

        # Map entity_type to actual API endpoint name
        # Note: TeamTailor API uses 'job-applications' instead of 'applications'
        entity_endpoint_map = {
            "candidates": "candidates",
            "jobs": "jobs",
            "applications": "job-applications",  # TeamTailor API endpoint name
            "interviews": "interviews",
            "users": "users",
            "departments": "departments",
            "stages": "stages",
            "nps_responses": "nps-responses",
        }

        # Get the appropriate method from API client based on entity type
        entity_method_map = {
            "candidates": self.api_client.get_candidates,
            "jobs": self.api_client.get_jobs,
            "applications": self.api_client.get_applications,
            "interviews": self.api_client.get_interviews,
            "users": self.api_client.get_users,
            "departments": self.api_client.get_departments,
            "stages": self.api_client.get_stages,
            "nps_responses": self.api_client.get_nps_responses,
        }

        get_entity_method = entity_method_map.get(self.config.entity_type)
        if not get_entity_method:
            raise ValueError(f"Unknown entity type: {self.config.entity_type}")

        try:
            # Track request info for the batch
            batch_start_time = datetime.now()

            # Build API query parameters with default filters for the entity
            # Default filters ensure we get ALL records (including closed/inactive/historical)
            page_size_param = self.entity_config["page_size"]
            api_params = {}

            # Apply default API filters for this entity type (e.g., filter[status]=all for jobs)
            default_filters = self.entity_config.get("default_api_filters", {})
            if default_filters:
                api_params.update(default_filters)
                self.logger.info(
                    f"Applied default API filters for {self.config.entity_type}: {default_filters}"
                )

            # Override with explicit filter parameters from config if provided
            # This allows users to override defaults if needed
            if self.config.api_filter_params:
                api_params.update(self.config.api_filter_params)
                self.logger.info(
                    f"Overriding with custom API filter parameters: {self.config.api_filter_params}"
                )

            # Log final filter configuration
            if api_params.get("filter[status]") == "all" or any(
                k.startswith("filter[") for k in api_params.keys()
            ):
                self.logger.info(
                    f"Fetching ALL {self.config.entity_type} with filters: {list(api_params.keys())}"
                )
            else:
                self.logger.info(
                    f"Fetching ALL {self.config.entity_type} (no status filters applied)"
                )

            # Use the API client's iterator method with parameters
            # Pass page_size as named parameter and filters as **kwargs
            for entity in get_entity_method(page_size=page_size_param, **api_params):
                entity_id = entity.get("id")

                # Fetch custom-field-values ONLY for jobs (candidates has ~3500 records
                # and only ~10% have custom fields - causes 1+ hour fetch time)
                # Jobs are fewer (~11) and ~100% have custom fields
                if self.config.entity_type == "jobs":
                    try:
                        custom_field_values = list(
                            self.api_client.get_custom_field_values(
                                entity_type=self.config.entity_type,
                                entity_id=entity_id,
                                page_size=30,
                            )
                        )

                        if custom_field_values:
                            # Add custom-field-values to entity record
                            # Store in a way that preserves the relationship structure
                            entity["included_custom_field_values"] = custom_field_values
                            self.logger.debug(
                                f"Fetched {len(custom_field_values)} custom field values for {self.config.entity_type} {entity_id}"
                            )
                    except Exception as e:
                        # Log warning but continue - custom fields are optional
                        self.logger.debug(
                            f"Could not fetch custom-field-values for {self.config.entity_type} {entity_id}: {e}"
                        )
                        # Set empty list to maintain consistent structure
                        entity["included_custom_field_values"] = []

                data_records.append(entity)
                record_count += 1

                # Log progress periodically
                if record_count % 100 == 0:
                    self.logger.info(
                        f"Processed {record_count} {self.config.entity_type} records"
                    )

                # Check max records limit
                max_records = (
                    self.config.max_pages_per_batch * self.entity_config["page_size"]
                )
                if record_count >= max_records:
                    self.logger.warning(
                        f"Max records ({max_records}) reached. Stopping pagination."
                    )
                    break

            # Create request info for the batch
            batch_end_time = datetime.now()
            # Use the actual API endpoint name (e.g., 'job-applications' instead of 'applications')
            api_endpoint = entity_endpoint_map.get(
                self.config.entity_type, self.config.entity_type
            )
            request_info = {
                "request_timestamp": batch_start_time.isoformat() + "Z",
                "request_url": f"{self.config.api_base_url}/v1/{api_endpoint}",
                "response_status_code": 200,
                "response_time_ms": int(
                    (batch_end_time - batch_start_time).total_seconds() * 1000
                ),
                "entity_type": self.config.entity_type,
                "records_fetched": record_count,
            }
            request_info_list.append(request_info)

        except TeamTailorNotFoundError as e:
            self.logger.error(
                f"❌ Endpoint not found (404) for entity type '{self.config.entity_type}'. "
                f"This endpoint may not be available in your TeamTailor account or may require different permissions."
            )
            self.logger.error(f"Error details: {str(e)}")
            self.logger.warning(
                f"⚠️ Skipping entity type '{self.config.entity_type}' - endpoint not available"
            )
            # Return empty results instead of failing the job
            return [], []
        except Exception as e:
            self.logger.error(f"Error during API pagination: {str(e)}")
            raise

        self.logger.info(
            f"✅ Fetched {len(data_records)} {self.config.entity_type} from Team Tailor API"
        )

        return data_records, request_info_list

    def _fetch_activities_as_relationships(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch activities as relationships from parent resources (candidates, applications, etc.).

        This method:
        1. Fetches all parent resources (e.g., all candidates)
        2. For each parent, fetches activities using relationship links
        3. Filters by activity types if specified in entity config
        4. Enriches activities with parent resource metadata

        Returns:
            Tuple of (data_records, request_info_list)
        """
        from libs.common.teamtailor.client import TeamTailorNotFoundError

        data_records = []
        request_info_list = []
        total_activities = 0
        parent_resources_processed = 0

        parent_resource = self.entity_config.get("parent_resource", "candidates")
        activity_types = self.entity_config.get("activity_types")

        self.logger.info(
            f"Fetching {self.config.entity_type} as relationships from {parent_resource}"
        )
        if activity_types:
            self.logger.info(f"Filtering by activity types: {activity_types}")

        batch_start_time = datetime.now()

        try:
            # Step 1: Fetch all parent resources
            parent_method_map = {
                "candidates": self.api_client.get_candidates,
                "job-applications": self.api_client.get_applications,
                "applications": self.api_client.get_applications,
                "jobs": self.api_client.get_jobs,
                "users": self.api_client.get_users,
            }

            get_parent_method = parent_method_map.get(parent_resource)
            if not get_parent_method:
                raise ValueError(
                    f"Unsupported parent resource for activities: {parent_resource}"
                )

            # Step 2: For each parent, fetch its activities
            for parent_entity in get_parent_method(
                page_size=self.entity_config["page_size"]
            ):
                parent_id = parent_entity.get("id")
                if not parent_id:
                    continue

                try:
                    # Fetch activities for this parent
                    activities = self.api_client.get_activities_as_relationships(
                        parent_resource=parent_resource,
                        parent_id=parent_id,
                        activity_types=activity_types,
                        page_size=self.entity_config["page_size"],
                    )

                    # Enrich each activity with parent metadata
                    for activity in activities:
                        # Add parent relationship info to activity
                        enriched_activity = activity.copy()

                        # Add parent reference in relationships if not present
                        if "relationships" not in enriched_activity:
                            enriched_activity["relationships"] = {}

                        # Add parent relationship
                        parent_type_map = {
                            "candidates": "candidate",
                            "job-applications": "application",
                            "applications": "application",
                            "jobs": "job",
                            "users": "user",
                        }
                        parent_type = parent_type_map.get(
                            parent_resource, parent_resource
                        )
                        enriched_activity["relationships"][parent_type] = {
                            "data": {
                                "id": parent_id,
                                "type": parent_resource.replace("-", "_"),
                            }
                        }

                        # Add metadata about parent resource
                        enriched_activity["meta"] = enriched_activity.get("meta", {})
                        enriched_activity["meta"]["parent_resource"] = parent_resource
                        enriched_activity["meta"]["parent_id"] = parent_id
                        enriched_activity["meta"]["fetched_as_relationship"] = True

                        data_records.append(enriched_activity)
                        total_activities += 1

                        # Log progress periodically
                        if total_activities % 100 == 0:
                            self.logger.info(
                                f"Processed {total_activities} activities from "
                                f"{parent_resources_processed} {parent_resource}"
                            )

                except TeamTailorNotFoundError:
                    # Parent resource or activities not found - skip silently
                    self.logger.debug(
                        f"No activities found for {parent_resource}/{parent_id}"
                    )
                    continue
                except Exception as e:
                    self.logger.warning(
                        f"Error fetching activities for {parent_resource}/{parent_id}: {str(e)}"
                    )
                    continue

                parent_resources_processed += 1

                # Check max records limit
                max_records = (
                    self.config.max_pages_per_batch * self.entity_config["page_size"]
                )
                if total_activities >= max_records:
                    self.logger.warning(
                        f"Max activities ({max_records}) reached. Stopping."
                    )
                    break

            # Create request info for the batch
            batch_end_time = datetime.now()
            request_info = {
                "request_timestamp": batch_start_time.isoformat() + "Z",
                "request_url": f"{self.config.api_base_url}/v1/{parent_resource}?include=activities",
                "response_status_code": 200,
                "response_time_ms": int(
                    (batch_end_time - batch_start_time).total_seconds() * 1000
                ),
                "entity_type": self.config.entity_type,
                "records_fetched": total_activities,
                "parent_resources_processed": parent_resources_processed,
                "activity_types": activity_types,
            }
            request_info_list.append(request_info)

        except Exception as e:
            self.logger.error(f"Error fetching activities as relationships: {str(e)}")
            raise

        self.logger.info(
            f"✅ Fetched {total_activities} {self.config.entity_type} from "
            f"{parent_resources_processed} {parent_resource} resources"
        )

        return data_records, request_info_list

    def extract(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract data from Team Tailor API.

        Works for any entity type configured in the job.

        Returns:
            Tuple of (data_records, request_info_list)
        """
        self.logger.info("=" * 80)
        self.logger.info(
            f"📥 EXTRACT PHASE: Fetching {self.config.entity_type} from Team Tailor API"
        )
        self.logger.info("=" * 80)

        # Fetch data with pagination
        data_records, request_info_list = self.fetch_data_with_pagination()

        self.logger.info(
            f"✅ Successfully extracted {len(data_records)} {self.config.entity_type} from Team Tailor API"
        )
        self.logger.info("=" * 80)

        return data_records, request_info_list

    def transform(
        self, data: Tuple[List[Dict], List[Dict]]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Transform data.

        Default implementation passes data through as-is.
        Team Tailor uses JSON API format, so we preserve the full structure.

        Args:
            data: Tuple of (data_records, request_info_list)

        Returns:
            Transformed data tuple
        """
        self.logger.info("=" * 80)
        self.logger.info(f"🔄 TRANSFORM PHASE: Processing {self.config.entity_type}")
        self.logger.info("=" * 80)

        data_records, request_info_list = data

        self.logger.info(
            f"Transforming {len(data_records)} {self.config.entity_type}..."
        )

        # Team Tailor uses JSON API format - preserve full structure
        # Each record is already in JSON API format with id, type, attributes, relationships
        processed_records = []
        for record in data_records:
            # Preserve full JSON API structure
            processed_records.append(record)

        self.logger.info(
            f"✅ Successfully transformed {len(processed_records)} {self.config.entity_type}"
        )
        self.logger.info("=" * 80)

        return processed_records, request_info_list

    def load(self, data: Tuple[List[Dict], List[Dict]]) -> None:
        """
        Load data to S3.

        Partitioning strategy:
        - Time-based entities (applications, interviews, application_stage_transitions, nps_responses): Partitioned by year/month
        - Snapshot entities (candidates, jobs, users, departments, stages): No partitions (flat structure)

        Args:
            data: Tuple of (data_records, request_info_list)
        """
        self.logger.info("=" * 80)
        self.logger.info("💾 LOAD PHASE: Writing to S3")
        self.logger.info("=" * 80)

        data_records, request_info_list = data

        if not data_records:
            self.logger.warning(f"⚠️  No {self.config.entity_type} to load. Exiting.")
            self.logger.info("=" * 80)
            return

        self.logger.info(
            f"Loading {len(data_records)} {self.config.entity_type} to S3..."
        )
        self.logger.info(f"S3 Path: {self.config.raw_write_path}")

        # Determine partitioning strategy based on entity type
        # Time-based entities should be partitioned for efficient querying
        partition_cols = None
        time_based_entities = [
            "applications",
            "interviews",
            "application_stage_transitions",
            "nps_responses",
        ]
        if self.config.entity_type in time_based_entities:
            # Time-based: partition by year/month of ingestion
            current_time = datetime.now()
            partition_cols = {"year": current_time.year, "month": current_time.month}
            self.logger.info(
                f"📅 Partitioning strategy: year={current_time.year}, month={current_time.month}"
            )
        else:
            # Snapshots: no partitions (flat structure)
            self.logger.info(
                "📦 Partitioning strategy: None (snapshot entity - flat structure)"
            )

        # Write to S3 using base class method
        self.write_to_s3(
            data=data_records,
            request_info_list=request_info_list,
            partition_cols=partition_cols,
        )

        self.logger.info(
            f"✅ Successfully loaded {len(data_records)} {self.config.entity_type} to S3"
        )
        self.logger.info("=" * 80)


# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    """Main execution function."""
    setup_logging()
    logger.info("=" * 80)
    logger.info("🚀 TEAM TAILOR RAW DATA INGESTION - STARTING")
    logger.info("=" * 80)
    logger.info(f"📅 Timestamp: {datetime.now()}")
    logger.info(f"🐍 Python: {sys.version}")
    logger.info("=" * 80)

    # Print workflow runtime properties if available
    print_workflow_properties()

    try:
        # Load configuration with validation
        config = TeamTailorRawConfig.from_args()

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
        logger.info(f"  Entity Type: {config.entity_type}")
        logger.info(f"  Source: {config.source_name}")
        logger.info(f"  AWS Region: {config.aws_region}")
        logger.info(f"  Raw Zone Path: {config.raw_zone_path}")
        logger.info(f"  Warehouse Path: {config.warehouse_path}")
        logger.info(f"  Team Tailor API Base URL: {config.api_base_url}")
        if config.teamtailor_api_base_url_param:
            logger.info(
                f"  SSM Parameter for Base URL: {config.teamtailor_api_base_url_param}"
            )
        logger.info(f"  Secrets Manager: {config.teamtailor_api_secret_name}")
        # Safely display API token (mask most of it) - loaded from Secrets Manager
        api_token_value = str(config.api_token) if config.api_token else ""
        masked_token = (
            "***" + api_token_value[-4:]
            if len(api_token_value) > 4
            else "LOADED FROM SECRETS"
        )
        logger.info(f"  API Token Status: {masked_token}")
        logger.info(f"  Rate Limit: {config.rate_limit_per_second}/s")
        logger.info(f"  Max Pages: {config.max_pages_per_batch}")
        logger.info("=" * 80)

        # Create and run job
        job = TeamTailorRawJob(config)
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
