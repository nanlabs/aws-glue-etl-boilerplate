"""
Standardized logging utilities for Glue jobs.

This module provides consistent logging configuration across all jobs
with structured logging, performance metrics, and CloudWatch integration.
"""

import json
import logging
import os
import sys
from typing import Any, Dict, Optional

# Standard log format used across all jobs
DEFAULT_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"


def _get_log_level_early() -> str:
    """
    Get LOG_LEVEL early from command line arguments or environment variable.

    Returns:
        Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL) - defaults to INFO
    """
    # Try command line argument first (--LOG_LEVEL DEBUG or --LOG_LEVEL=DEBUG)
    for i, arg in enumerate(sys.argv):
        if arg in ("--LOG_LEVEL", "--log_level") and i + 1 < len(sys.argv):
            return sys.argv[i + 1].upper()
        if arg.startswith(("--LOG_LEVEL=", "--log_level=")):
            return arg.split("=", 1)[1].upper()

    # Fallback to environment variable
    return os.getenv("LOG_LEVEL", "DEBUG").upper()


def setup_logging() -> None:
    """
    Configure logging early based on LOG_LEVEL parameter.

    This must be called before any logger calls to ensure the correct log level
    is applied from the start. Reads from --LOG_LEVEL argument or LOG_LEVEL env var.

    Uses DEFAULT_LOG_FORMAT for consistent formatting across all jobs.
    """
    log_level = _get_log_level_early()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=DEFAULT_LOG_FORMAT,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # Override any previous logging configuration
    )


def get_logger(
    name: str,
    level: int = logging.DEBUG,
    set_root: bool = False,
    log_format: Optional[str] = None,
) -> logging.Logger:
    """
    Creates and configures a logger with the given name and level.

    Args:
        name: Name of the logger
        level: Logging level (default: INFO)
        set_root: If True, configures the root logger (default: False)
        log_format: Optional custom format string (default: DEFAULT_LOG_FORMAT)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger() if set_root else logging.getLogger(name)
    if set_root:
        logger.name = name  # Set a friendly name for the root logger context

    logger.setLevel(level)

    # Clear existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Configure a new handler with standard format
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(log_format or DEFAULT_LOG_FORMAT))
    logger.addHandler(handler)

    return logger


def mask_sensitive_value(key: str, value: Any) -> Any:
    """
    Mask sensitive values in logs.

    Args:
        key: The configuration key
        value: The configuration value

    Returns:
        Masked value if sensitive, original value otherwise
    """
    sensitive_keys = ["password", "secret", "token", "key", "credential"]
    if (
        any(sensitive_word in key.lower() for sensitive_word in sensitive_keys)
        and value
    ):
        return "********"
    return value


def log_configuration(logger: logging.Logger, config: Any) -> None:
    """
    Logs configuration details with sensitive information masked.

    Args:
        logger: Logger instance
        config: Configuration object containing settings
    """
    # Log AWS client configuration
    if hasattr(config, "aws_client_vars"):
        aws_config = {
            k: mask_sensitive_value(k, v) for k, v in config.aws_client_vars.items()
        }
        logger.info(f"AWS Configuration: {json.dumps(aws_config, indent=2)}")


def log_job_metrics(logger: logging.Logger, metrics: Dict[str, Any]) -> None:
    """
    Log job performance metrics in structured format.

    Args:
        logger: Logger instance
        metrics: Dictionary of metrics to log
    """
    logger.info(f"Job Metrics: {json.dumps(metrics, indent=2)}")
