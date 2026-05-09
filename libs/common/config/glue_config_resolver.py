"""Helper functions for resolving AWS Glue configuration with workflow properties."""

import logging
import os
import sys
from typing import Any, Dict, Optional

try:
    import boto3  # noqa: F401
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

logger = logging.getLogger(__name__)


def _create_boto3_client(
    service_name: str,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> Any:
    """
    Create a boto3 client with automatic LocalStack endpoint detection.

    This helper function uses the centralized AWS utilities for consistency.

    Args:
        service_name: AWS service name (e.g., 'glue', 's3', 'secretsmanager')
        region_name: AWS region name (defaults to AWS_REGION env var)
        endpoint_url: Optional endpoint URL (for LocalStack). If not provided,
                     will auto-detect from environment.

    Returns:
        boto3 client instance configured for the service

    Raises:
        ImportError: If boto3 is not available
    """
    from libs.common.utils.aws import create_boto3_client

    return create_boto3_client(
        service_name=service_name,
        region_name=region_name,
        endpoint_url=endpoint_url,
    )


def _get_workflow_properties_via_boto3(
    workflow_name: str,
    run_id: str,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> Dict[str, str]:
    """
    Get workflow run properties using boto3 client (for PyShell jobs).

    This method is used when GlueContext is not available (PyShell jobs).
    It uses the AWS Glue API directly via boto3.

    Args:
        workflow_name: Workflow name
        run_id: Workflow run ID
        region_name: AWS region (optional, will use env var if not provided)
        endpoint_url: Optional endpoint URL for LocalStack

    Returns:
        Dictionary of workflow run properties (empty dict if not available or error)
    """
    if not BOTO3_AVAILABLE:
        logger.debug(
            "boto3 not available, cannot retrieve workflow properties via boto3"
        )
        return {}

    try:
        # Create Glue client
        glue_client = _create_boto3_client(
            service_name="glue",
            region_name=region_name,
            endpoint_url=endpoint_url,
        )

        # Get workflow run properties
        response = glue_client.get_workflow_run_properties(
            Name=workflow_name, RunId=run_id
        )

        # Extract properties from response
        props = response.get("RunProperties", {})

        logger.info(
            "✅ Loaded %d properties from workflow '%s' run '%s' (via boto3)",
            len(props),
            workflow_name,
            run_id,
        )

        return props

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "EntityNotFoundException":
            logger.debug("Workflow run not found: %s/%s", workflow_name, run_id)
        elif error_code == "AccessDeniedException":
            logger.warning(
                "⚠️  Access denied retrieving workflow properties. "
                "Ensure IAM role has 'glue:GetWorkflowRunProperties' permission."
            )
        else:
            logger.warning(
                "⚠️  Failed to get workflow properties via boto3: %s (%s)",
                e,
                error_code,
            )
        return {}
    except (AttributeError, TypeError, ValueError) as e:
        logger.warning(
            "⚠️  Unexpected error getting workflow properties via boto3: %s", e
        )
        return {}


def get_workflow_run_properties(
    workflow_name: Optional[str] = None,
    run_id: Optional[str] = None,
    glue_context: Optional[Any] = None,
) -> Dict[str, str]:
    """
    Get workflow run properties from AWS Glue.

    Args:
        workflow_name: Workflow name (from WORKFLOW_NAME arg or env var)
        run_id: Workflow run ID (from WORKFLOW_RUN_ID arg or env var)
        glue_context: GlueContext instance (optional, will try to get if not provided)

    Returns:
        Dictionary of workflow run properties (empty dict if not available)

    Examples:
        # From job parameters
        props = get_workflow_run_properties(
            workflow_name=args.get("WORKFLOW_NAME"),
            run_id=args.get("WORKFLOW_RUN_ID")
        )

        # With GlueContext
        props = get_workflow_run_properties(
            workflow_name="my_workflow",
            run_id="run_123",
            glue_context=glue_context
        )
    """
    # If workflow_name or run_id not provided, try to get from environment
    if not workflow_name:
        workflow_name = os.getenv("WORKFLOW_NAME")

    if not run_id:
        run_id = os.getenv("WORKFLOW_RUN_ID")

    # If still not available, return empty dict
    if not workflow_name or not run_id:
        logger.debug(
            "Workflow properties not available: workflow_name=%s, run_id=%s",
            workflow_name,
            run_id,
        )
        return {}

    # Strategy: Try GlueContext first (PySpark), then boto3 (PyShell)
    # 1. Try GlueContext if not provided
    if glue_context is None:
        glue_context = _try_get_glue_context()

    # 2. If GlueContext is available, use it (PySpark/Spark jobs)
    if glue_context is not None:
        try:
            props = glue_context.get_workflow_run_properties(workflow_name, run_id)
            logger.info(
                "✅ Loaded %d properties from workflow '%s' run '%s' (via GlueContext)",
                len(props),
                workflow_name,
                run_id,
            )
            return props
        except (AttributeError, TypeError, ValueError, RuntimeError) as e:
            logger.warning(
                "⚠️ Failed to get workflow properties via GlueContext: %s. "
                "Falling back to boto3 client.",
                e,
            )
            # Fall through to boto3 fallback

    # 3. Fallback to boto3 client (PyShell jobs or when GlueContext fails)
    if BOTO3_AVAILABLE:
        logger.debug(
            "GlueContext not available, using boto3 client for workflow properties"
        )
        # Get region from environment
        region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")

        from libs.common.utils.aws import detect_localstack_endpoint

        endpoint_url = detect_localstack_endpoint(service_name="glue")

        return _get_workflow_properties_via_boto3(
            workflow_name=workflow_name,
            run_id=run_id,
            region_name=region_name,
            endpoint_url=endpoint_url,
        )
    else:
        logger.debug("boto3 not available, cannot retrieve workflow properties")
        return {}


def print_workflow_properties() -> None:
    """
    Print all available workflow run properties at job start.

    This function attempts to retrieve and display all workflow run properties
    available in the current execution context. Useful for debugging and understanding
    what values are available from AWS Glue Workflows.

    The function is safe to call in any context (local, PyShell, Spark) and will
    gracefully handle cases where workflow properties are not available.
    """
    logger.info("=" * 80)
    logger.info("📋 WORKFLOW RUNTIME PROPERTIES")
    logger.info("=" * 80)

    # Try to get workflow name and run ID
    workflow_name = os.getenv("WORKFLOW_NAME")
    run_id = os.getenv("WORKFLOW_RUN_ID")

    # Also check sys.argv for job parameters
    for i, arg in enumerate(sys.argv):
        if arg in ("--WORKFLOW_NAME", "--workflow_name") and i + 1 < len(sys.argv):
            workflow_name = sys.argv[i + 1]
        elif arg.startswith("--WORKFLOW_NAME=") or arg.startswith("--workflow_name="):
            workflow_name = arg.split("=", 1)[1]
        elif arg in ("--WORKFLOW_RUN_ID", "--workflow_run_id") and i + 1 < len(
            sys.argv
        ):
            run_id = sys.argv[i + 1]
        elif arg.startswith("--WORKFLOW_RUN_ID=") or arg.startswith(
            "--workflow_run_id="
        ):
            run_id = arg.split("=", 1)[1]

    if workflow_name:
        logger.info("  Workflow Name: %s", workflow_name)
    else:
        logger.info("  Workflow Name: Not set")

    if run_id:
        logger.info("  Workflow Run ID: %s", run_id)
    else:
        logger.info("  Workflow Run ID: Not set")

    # Try to get workflow properties
    if workflow_name and run_id:
        try:
            props = get_workflow_run_properties(
                workflow_name=workflow_name, run_id=run_id
            )

            if props:
                logger.info("  Properties Available: %d", len(props))
                logger.info("  Properties:")
                # Sort for consistent output
                for key, value in sorted(props.items()):
                    # Mask sensitive values
                    if any(
                        sensitive in key.lower()
                        for sensitive in [
                            "password",
                            "secret",
                            "token",
                            "key",
                            "credential",
                        ]
                    ):
                        masked_value = "********" if value else "(empty)"
                        logger.info("    %s: %s", key, masked_value)
                    else:
                        # Truncate very long values
                        display_value = (
                            value if len(str(value)) <= 100 else f"{str(value)[:97]}..."
                        )
                        logger.info("    %s: %s", key, display_value)
                logger.info("")
            else:
                logger.info("  Properties Available: 0")
                logger.info("  (No properties found - tried GlueContext and boto3)")
                logger.info("")
        except (AttributeError, TypeError, ValueError, RuntimeError) as e:
            logger.warning("  ⚠️  Error retrieving workflow properties: %s", e)
            logger.info("")
    else:
        logger.info("  Properties Available: 0")
        logger.info("  (Workflow name or run ID not provided)")
        logger.info("")
        logger.info("  Note: Workflow properties are only available when:")
        logger.info("    - Running in AWS Glue Workflow context")
        logger.info("    - WORKFLOW_NAME and WORKFLOW_RUN_ID are provided")
        logger.info("")

    logger.info("=" * 80)


def _try_get_glue_context() -> Optional[Any]:
    """
    Try to get GlueContext from current execution context.

    This function only uses an existing SparkContext if one is already available.
    It does NOT create a new SparkContext to avoid conflicts when running in workflows,
    where the SparkContext should be created by session_factory.

    Returns:
        GlueContext instance if available, None otherwise
    """
    try:
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext

        # Only use existing SparkContext, don't create a new one
        # This prevents conflicts when running in workflows where session_factory
        # will create the SparkContext later
        # We check if a SparkContext exists by trying to access it without creating one
        try:
            # Try to get existing SparkContext without creating a new one
            # If no SparkContext exists, getOrCreate would create one, but we want to avoid that
            # So we check if one exists first by accessing the active context
            sc = SparkContext._active_spark_context  # pylint: disable=protected-access
            if sc is None:
                # No active SparkContext, return None to use boto3 fallback
                logger.debug(
                    "No active SparkContext found, will use boto3 for workflow properties"
                )
                return None
        except (AttributeError, RuntimeError, ValueError):
            # SparkContext not available or not initialized
            logger.debug(
                "SparkContext not available, will use boto3 for workflow properties"
            )
            return None

        # Create GlueContext from existing SparkContext
        return GlueContext(sc)
    except (ImportError, AttributeError) as e:
        logger.debug("GlueContext not available: %s", e)
        return None
    except (TypeError, ValueError, RuntimeError) as e:
        logger.debug("Unexpected error getting GlueContext: %s", e)
        return None
