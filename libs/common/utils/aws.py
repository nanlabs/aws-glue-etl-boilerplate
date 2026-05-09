"""
AWS utilities for boto3 client initialization.

This module provides reusable functions for creating boto3 clients with automatic
LocalStack endpoint detection and consistent configuration patterns.
"""

import os
from typing import Any, Optional

try:
    import boto3

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from .logger import get_logger

logger = get_logger("utils.aws")


def detect_localstack_endpoint(service_name: Optional[str] = None) -> Optional[str]:
    """
    Auto-detect LocalStack endpoint from environment.

    When STAGE=local, all services (including SSM and Secrets Manager) use LocalStack
    by default. Secret synchronization from AWS real to LocalStack is handled automatically.

    Checks multiple environment variables in order of precedence:
    1. AWS_ENDPOINT_URL_{SERVICE} (service-specific endpoint, highest priority)
    2. LOCALSTACK_ENDPOINT (general endpoint)
    3. STAGE=local defaults to http://localstack:4566 (for all services)

    Note: We do NOT use AWS_ENDPOINT_URL general to avoid conflicts.
    Use service-specific endpoints (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL_GLUE, etc.) instead.

    Args:
        service_name: Optional service name to check for service-specific endpoints
                     (e.g., 's3', 'secretsmanager', 'ssm', 'glue')

    Returns:
        Optional[str]: LocalStack endpoint URL if detected, None to use real AWS

    Examples:
        >>> endpoint = detect_localstack_endpoint()
        >>> endpoint = detect_localstack_endpoint(service_name='s3')
    """
    # Check for service-specific endpoint first (highest priority)
    # This allows explicit override for any service
    if service_name:
        service_upper = service_name.upper()
        service_endpoint = os.getenv(f"AWS_ENDPOINT_URL_{service_upper}")
        if service_endpoint:
            return service_endpoint

    # Check for general LocalStack endpoint
    localstack_endpoint = os.getenv("LOCALSTACK_ENDPOINT")
    if localstack_endpoint:
        return localstack_endpoint

    # Check if we're in a local development environment
    # When STAGE=local, all services (including SSM/Secrets Manager) use LocalStack
    if os.getenv("STAGE", "").lower() == "local":
        return "http://localstack:4566"

    return None


def create_boto3_client(
    service_name: str,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> Any:
    """
    Create a boto3 client with automatic region and LocalStack endpoint detection.

    This function provides a consistent way to initialize boto3 clients across
    the codebase with proper endpoint configuration for both AWS and LocalStack.

    Region detection priority (highest to lowest):
    1. Explicit region_name parameter
    2. AWS_REGION or AWS_DEFAULT_REGION environment variables
    3. boto3.Session() automatic detection (from IAM role, metadata service, etc.)
    4. Fallback to us-east-1 (only if absolutely necessary, with warning)

    Args:
        service_name: AWS service name (e.g., 's3', 'secretsmanager', 'ssm', 'glue')
        region_name: AWS region name. If not provided, will be auto-detected using
                    the priority order described above.
        endpoint_url: Optional endpoint URL (for LocalStack). If not provided,
                     will auto-detect from environment.

    Returns:
        boto3 client instance configured for the service

    Raises:
        ImportError: If boto3 is not available

    Examples:
        >>> s3_client = create_boto3_client('s3', region_name='us-east-1')
        >>> ssm_client = create_boto3_client('ssm')  # Auto-detects region
        >>> secrets_client = create_boto3_client('secretsmanager', endpoint_url='http://localhost:4566')
    """
    if not BOTO3_AVAILABLE:
        raise ImportError("boto3 is not available. Install it with: pip install boto3")

    # Priority 1: Use explicit parameter
    region_source = "explicit parameter"
    if not region_name:
        # Priority 2: Check environment variables
        region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if region_name:
            region_source = "environment variable (AWS_REGION/AWS_DEFAULT_REGION)"

        # Priority 3: Use boto3 Session automatic detection
        if not region_name:
            try:
                session = boto3.Session()
                region_name = session.region_name
                if region_name:
                    region_source = "boto3.Session() auto-detection (IAM role metadata)"
            except Exception as e:
                logger.debug("Could not auto-detect region from boto3.Session(): %s", e)

        # Priority 4: Fallback (only if all else fails)
        if not region_name:
            logger.warning(
                "Could not determine AWS region. Using fallback: us-east-1. "
                "This may cause issues. Please set AWS_REGION environment variable."
            )
            region_name = "us-east-1"
            region_source = "fallback (us-east-1)"

    # Auto-detect endpoint if not provided
    if not endpoint_url:
        endpoint_url = detect_localstack_endpoint(service_name=service_name)

    # Build client kwargs
    client_kwargs = {"service_name": service_name}

    if region_name:
        client_kwargs["region_name"] = region_name

    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
        logger.debug("Using LocalStack endpoint for %s: %s", service_name, endpoint_url)

    # Log region information at INFO level
    logger.info(
        "Creating boto3 client for %s with region %s (source: %s)",
        service_name,
        region_name,
        region_source,
    )

    return boto3.client(**client_kwargs)
