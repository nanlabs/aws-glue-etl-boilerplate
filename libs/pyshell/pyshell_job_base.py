"""
PyShell Job Base Class - AWS Glue PyShell

Base class for PyShell jobs that extract data from external APIs and store in S3.
Similar to MedallionJobBase but designed for lightweight Python Shell execution.

Features:
- API client management with rate limiting and retry logic
- S3 storage with partitioning
- Pydantic-based configuration management
- Centralized logging and error handling
- Extract-Transform-Load pattern
- Environment/Glue-parameter based authentication values

Usage:
    from libs.base import PyShellJobBase, RawJobConfig

    class MyConfig(RawJobConfig):
        # Add job-specific config fields
        api_endpoint: str = "/events"

    class MyPyShellJob(PyShellJobBase):
        def __init__(self, config: MyConfig):
            super().__init__(config)

        def extract(self):
            # Fetch data from API
            return self.make_api_request(url, params)

        def transform(self, data):
            # Transform data
            return processed_data

        def load(self, data):
            # Write to S3
            self.write_to_s3(data)
"""

import json
import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from botocore.exceptions import ClientError

from libs.common.config.job_config import RawJobConfig

# ==========================================
# LOGGING CONFIGURATION FOR AWS GLUE PYTHON SHELL
# ==========================================
# Configure logging to stdout for CloudWatch visibility.
# AWS Glue Python Shell captures stdout/stderr and sends them to CloudWatch Logs.
# Log group: /aws-glue/python-jobs/output
#
# Reference: https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging.html
#
# Note: This configuration must be done at module import time to ensure
# all loggers use the correct handler. The 'force=True' parameter ensures
# this configuration overrides any previous logging setup.
# ==========================================
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)


class PyShellJobBase:
    """
    Base class for PyShell jobs that extract data from APIs and store in S3.

    This class provides common functionality for PyShell jobs including:
    - API request handling with rate limiting and retries
    - S3 data storage with partitioning
    - Configuration management via Pydantic
    - Extract-Transform-Load pattern implementation

    Subclasses should implement:
    - extract(): Fetch data from source
    - transform(): Process and clean data
    - load(): Store data to destination
    """

    def __init__(self, config: RawJobConfig):
        """
        Initialize the PyShell job base class.

        Args:
            config: RawJobConfig instance with validated configuration
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

        # API request tracking
        self.last_request_time = 0
        self.request_count = 0

        # Initialize AWS clients
        self.s3_client = self._initialize_s3_client()

        # Fallback: Use environment variable if available
        if (
            hasattr(self.config, "api_key")
            and self.config.api_key
            and not hasattr(self, "api_key")
        ):
            self.api_key = self.config.api_key
            self.logger.info("Using API key from environment variable")

        self.logger.info("PyShellJobBase initialized")
        self.logger.info(f"Job: {self.config.job_name}")
        self.logger.info(f"Source: {self.config.source_name}")
        self.logger.info(f"S3 Target: {self.config.raw_zone_path}")

    def _initialize_s3_client(self):
        """Initialize S3 client with configuration."""
        from libs.common.utils.aws import create_boto3_client

        return create_boto3_client(
            service_name="s3",
            region_name=self.config.aws_region,
            endpoint_url=self.config.aws_endpoint_url,
        )

    def make_api_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        method: str = "GET",
        attempt: int = 1,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Make an API request with rate limiting and retry logic.

        Args:
            url: API endpoint URL
            params: Query parameters
            headers: Request headers
            method: HTTP method (GET, POST, etc.)
            attempt: Current retry attempt number

        Returns:
            Tuple of (response_data, request_info)

        Raises:
            requests.exceptions.RequestException: If all retries fail
        """
        # Rate limiting
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.config.rate_limit_per_second

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)

        # Build headers
        request_headers = headers or {}
        if self.config.api_key and "Authorization" not in request_headers:
            request_headers["Authorization"] = f"Bearer {self.config.api_key}"

        try:
            self.last_request_time = time.time()
            self.request_count += 1

            self.logger.debug(f"API request #{self.request_count}: {method} {url}")
            start_time = time.time()

            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=request_headers,
                timeout=30,
            )

            response_time = int((time.time() - start_time) * 1000)
            response.raise_for_status()

            # Capture request metadata
            request_info = {
                "request_timestamp": datetime.now().isoformat() + "Z",
                "request_url": url,
                "request_method": method,
                "response_status_code": response.status_code,
                "response_time_ms": response_time,
                "attempt": attempt,
            }

            return response.json(), request_info

        except requests.exceptions.RequestException as e:
            self.logger.error(
                f"API request failed (Attempt {attempt}/{self.config.max_retries}): {e}"
            )

            if attempt < self.config.max_retries:
                sleep_time = 1.0 * (2 ** (attempt - 1))  # Fixed retry delay
                self.logger.info(f"Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
                return self.make_api_request(url, params, headers, method, attempt + 1)
            else:
                self.logger.error(f"Max retries reached for URL: {url}")
                raise

    def write_to_s3(
        self,
        data: List[Dict[str, Any]],
        request_info_list: Optional[List[Dict[str, Any]]] = None,
        partition_cols: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write data to S3 as JSONL files with optional partitioning.

        Partitioning strategies:
        - partition_cols = {...}: Use provided partition columns (e.g., {"year": 2025, "month": 1})
        - partition_cols = None: No partitioning (flat structure in S3 prefix)

        Args:
            data: List of records to write
            request_info_list: List of request metadata for each batch
            partition_cols: Optional partition columns. If None, no partitioning is applied.

        Raises:
            ClientError: If S3 write fails
        """
        if not data:
            self.logger.warning("No data to write to S3")
            return

        current_time = datetime.now()

        # Use raw_write_path if available (source-specific path), otherwise use raw_zone_path
        # This allows organization like <source_name>/<entity_type>/
        write_path = (
            getattr(self.config, "raw_write_path", None) or self.config.raw_zone_path
        )

        # Parse bucket from write path
        s3_path_without_scheme = write_path.replace("s3://", "")
        bucket = s3_path_without_scheme.split("/")[0]

        # Build S3 key prefix (with or without partitions)
        if partition_cols:
            # With partitions: add partition path
            partition_path = "/".join([f"{k}={v}" for k, v in partition_cols.items()])
            # Extract prefix from write_path and add partition path
            prefix = (
                s3_path_without_scheme.split("/", 1)[1]
                if "/" in s3_path_without_scheme[1:]
                else ""
            )
            prefix = prefix.rstrip("/") if prefix else ""
            key_prefix = (
                f"{prefix}/{partition_path}/" if prefix else f"{partition_path}/"
            )
        else:
            # No partitions: extract prefix from write_path
            prefix = (
                s3_path_without_scheme.split("/", 1)[1]
                if "/" in s3_path_without_scheme[1:]
                else ""
            )
            prefix = prefix.rstrip("/") if prefix else ""
            key_prefix = f"{prefix}/" if prefix else ""
            partition_cols = {}  # Empty dict for metadata inclusion

        self.logger.info(f"Writing {len(data)} records to s3://{bucket}/{key_prefix}")

        # Write in batches
        batch_size = self.config.batch_size
        timestamp = current_time.strftime("%Y%m%d_%H%M%S")

        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            batch_number = (i // batch_size) + 1

            # Create filename
            filename = f"{self.config.source_name}_data_{timestamp}_batch_{batch_number:03d}.jsonl"
            # Build s3_key
            s3_key = f"{key_prefix}{filename}"

            # Build JSONL content with metadata
            json_lines = []
            for j, record in enumerate(batch):
                # Get request info for this record
                request_info = request_info_list[0] if request_info_list else {}

                # Create structured record with payload and metadata
                structured_record = {
                    "payload": record,
                    **partition_cols,  # Include partition columns
                    "metadata": {
                        # Request metadata
                        "request_timestamp": request_info.get(
                            "request_timestamp", current_time.isoformat() + "Z"
                        ),
                        "request_url": request_info.get("request_url", ""),
                        "response_status_code": request_info.get(
                            "response_status_code", 0
                        ),
                        "response_time_ms": request_info.get("response_time_ms", 0),
                        # Ingestion metadata
                        "ingestion_timestamp": current_time.isoformat() + "Z",
                        "ingestion_job": self.config.job_name,
                        "ingestion_source": self.config.source_name,
                        "ingestion_method": "api_polling",
                        # Data quality metadata
                        "record_id": f"{timestamp}_{batch_number:03d}_{j:06d}",
                        "batch_id": f"{timestamp}_batch_{batch_number:03d}",
                        "file_name": filename,
                    },
                }

                json_lines.append(json.dumps(structured_record, separators=(",", ":")))

            json_data = "\n".join(json_lines)

            try:
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=json_data.encode("utf-8"),
                    ContentType="application/json",
                )
                self.logger.info(
                    f"✅ Uploaded batch {batch_number} to s3://{bucket}/{s3_key}"
                )
            except ClientError as e:
                self.logger.error(f"Failed to upload batch {batch_number}: {e}")
                raise

        self.logger.info(f"✅ Successfully wrote {len(data)} records to S3")

    def run(self) -> None:
        """
        Execute the complete ETL pipeline: Extract -> Transform -> Load.

        This method orchestrates the job execution and provides error handling.
        """
        self.logger.info("=" * 80)
        self.logger.info(f"🚀 Starting {self.config.job_name}")
        self.logger.info("=" * 80)

        try:
            # Extract
            self.logger.info("📥 Step 1: Extracting data...")
            extracted_data = self.extract()
            self.logger.info(
                f"✅ Extracted {len(extracted_data) if isinstance(extracted_data, (list, tuple)) else 'N/A'} items"
            )

            # Transform
            self.logger.info("🔄 Step 2: Transforming data...")
            transformed_data = self.transform(extracted_data)
            self.logger.info(
                f"✅ Transformed {len(transformed_data) if isinstance(transformed_data, (list, tuple)) else 'N/A'} items"
            )

            # Load
            self.logger.info("💾 Step 3: Loading data...")
            self.load(transformed_data)
            self.logger.info("✅ Data loaded successfully")

            # Summary
            self.logger.info("=" * 80)
            self.logger.info("🎉 Job completed successfully!")
            self.logger.info(f"📊 API Requests: {self.request_count}")
            self.logger.info(f"📍 S3 Location: {self.config.raw_zone_path}")
            self.logger.info("=" * 80)

        except Exception as e:
            self.logger.error("=" * 80)
            self.logger.error(f"❌ Job failed: {e}")
            self.logger.error("=" * 80)
            raise

    def extract(self) -> Any:
        """
        Extract data from source.

        Must be implemented by subclasses.

        Returns:
            Extracted data (typically list of records)
        """
        raise NotImplementedError("Subclasses must implement extract() method")

    def transform(self, data: Any) -> Any:
        """
        Transform data according to business requirements.

        Default implementation returns data as-is.
        Subclasses can override for custom transformations.

        Args:
            data: Data from extract phase

        Returns:
            Transformed data
        """
        return data

    def load(self, data: Any) -> None:
        """
        Load data to destination.

        Must be implemented by subclasses.

        Args:
            data: Transformed data to load
        """
        raise NotImplementedError("Subclasses must implement load() method")
