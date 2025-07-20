import json
import logging
import os
import sys
from typing import Any, Dict, Optional

from awsglue.utils import getResolvedOptions

from ..common import cached_property
from .env import get_envs
from .secrets import get_secrets_resolver

# Set up logger
logger = logging.getLogger("config")
logger.setLevel(logging.INFO)

# Configure a handler if none exists
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Parameter groups - organize parameters by logical groups
parameter_groups = {
    "aws": [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_region",  # Changed from aws_region_name to match actual param
        "aws_endpoint_url",
    ],
    "s3": ["s3_bucket_name"],
    "documentdb": [
        "document_db_engine",
        "document_db_host",
        "document_db_port",
        "document_db_dbname",
        "document_db_username",
        "document_db_password",
        "document_db_ssl",
    ],
    "postgres": [
        "postgres_db_engine",
        "postgres_db_host",
        "postgres_db_port",
        "postgres_db_dbname",
        "postgres_db_username",
        "postgres_db_password",
    ],
}


# Get a specific job parameter safely
def get_job_param(param_name, default=None):
    """
    Get a job parameter safely without exceptions

    :param param_name: The name of the parameter to retrieve
    :param default: Default value if parameter is not found
    :return: The parameter value or default if not found
    """
    # Try different parameter name formats to accommodate Glue's format
    param_formats = [
        param_name,  # Standard format
        f"--{param_name}",  # With prefix
        param_name.replace("_", "-"),  # With hyphens instead of underscores
    ]

    for format in param_formats:
        try:
            params = getResolvedOptions(sys.argv, [format])
            param_value = params.get(format)
            if param_value is not None and param_value != "":
                return param_value
        except Exception:
            continue

    # If we couldn't find it in any format, return the default
    return default


# Helper function to mask sensitive parameter values in logs
def mask_sensitive_param(param_name, param_value):
    """
    Mask sensitive parameter values for logging

    :param param_name: The name of the parameter
    :param param_value: The value of the parameter
    :return: Masked value if sensitive, original value otherwise
    """
    sensitive_terms = ["password", "secret", "key", "token", "credential"]
    if param_value and any(term in param_name.lower() for term in sensitive_terms):
        return "********"
    return param_value


class Config:
    """
    Get the credentials from the AWS Secret Manager or from the environment variables
    """

    def __init__(self, args: Optional[Dict[str, Any]] = None) -> None:
        if args is None:
            args = dict()
        self.env = get_envs(args)
        self.sr = get_secrets_resolver()
        # Get all job parameters at initialization

    @cached_property
    def aws_client_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client.

        :return: A json with the parameters.
        """
        return {
            "aws_access_key_id": self.get_param_with_fallback(
                "AWS_ACCESS_KEY_ID", "aws_access_key_id"
            ),
            "aws_secret_access_key": self.get_param_with_fallback(
                "AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"
            ),
            "aws_region_name": self.get_param_with_fallback("AWS_REGION", "aws_region"),
            "aws_session_token": self.get_param_with_fallback(
                "AWS_SESSION_TOKEN", "aws_session_token"
            ),
            "aws_endpoint_url": self.get_param_with_fallback(
                "AWS_ENDPOINT_URL", "aws_endpoint_url"
            ),
        }

    @cached_property
    def s3_vars(self) -> dict:
        """
        Get connection parameters for the AWS Client and the scan's bucket name.

        :return: A json with the parameters.
        """
        return {
            **self.aws_client_vars,
            "bucket_name": self.get_param_with_fallback(
                "S3_BUCKET_NAME", "s3_bucket_name"
            ),
        }

    @cached_property
    def documentdb_vars(self) -> dict:
        """
        Get connection parameters for the Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.get_param_with_fallback(
                "DOCUMENT_DB_ENGINE", "document_db_engine"
            ),
            "host": self.get_param_with_fallback(
                "DOCUMENT_DB_HOST", "document_db_host"
            ),
            "port": self.get_param_with_fallback(
                "DOCUMENT_DB_PORT", "document_db_port"
            ),
            "database": self.get_param_with_fallback(
                "DOCUMENT_DB_DBNAME", "document_db_dbname"
            ),
            "user": self.get_param_with_fallback(
                "DOCUMENT_DB_USERNAME", "document_db_username"
            ),
            "password": self.get_param_with_fallback(
                "DOCUMENT_DB_PASSWORD", "document_db_password"
            ),
            "ssl": self.get_param_with_fallback("DOCUMENT_DB_SSL", "document_db_ssl")
            == "true",
        }

    @cached_property
    def postgresdb_vars(self) -> dict:
        """
        Get connection parameters for the Database.

        :return: A json with the connection parameters.
        """
        return {
            "engine": self.get_param_with_fallback(
                "POSTGRES_DB_ENGINE", "postgres_db_engine"
            ),
            "host": self.get_param_with_fallback(
                "POSTGRES_DB_HOST", "postgres_db_host"
            ),
            "port": self.get_param_with_fallback(
                "POSTGRES_DB_PORT", "postgres_db_port"
            ),
            "database": self.get_param_with_fallback(
                "POSTGRES_DB_DBNAME", "postgres_db_dbname"
            ),
            "user": self.get_param_with_fallback(
                "POSTGRES_DB_USERNAME", "postgres_db_username"
            ),
            "password": self.get_param_with_fallback(
                "POSTGRES_DB_PASSWORD", "postgres_db_password"
            ),
        }

    def get_param_with_fallback(
        self, env_param_name: str, job_param_name: str, default=None
    ):
        """
        Get a parameter with fallback:
        1. First try environment variable
        2. Then try job parameter
        3. Finally use default value

        :param env_param_name: Environment variable name (uppercase)
        :param job_param_name: Job parameter name (lowercase)
        :param default: Default value if parameter is not found in either source
        :return: The parameter value or default if not found
        """
        # Try environment variable first
        env_value = self.env.get_var(env_param_name, None)
        if env_value is not None and env_value != "":
            logger.info(f"Using environment variable {env_param_name}")
            return env_value

        # If not found in all_job_params, try individual lookup
        job_value = get_job_param(job_param_name, default)
        if job_value is not None and job_value != default:
            logger.info(f"Using job parameter {job_param_name} (individual lookup)")
            return job_value

        # Return default if not found in either place
        logger.debug(f"Using default value for {env_param_name}/{job_param_name}")
        return default

    def log_config(self):
        """
        Log the entire configuration for debugging purposes
        """
        # Log AWS configuration
        aws_config = {
            k: mask_sensitive_param(k, v) for k, v in self.aws_client_vars.items()
        }
        logger.info(f"AWS Configuration: {json.dumps(aws_config, indent=2)}")

        # Log S3 configuration
        s3_config = {"bucket_name": self.s3_vars.get("bucket_name", "N/A")}
        logger.info(f"S3 Configuration: {json.dumps(s3_config, indent=2)}")

        # Log DocumentDB configuration
        documentdb_config = {
            k: mask_sensitive_param(k, v) for k, v in self.documentdb_vars.items()
        }
        logger.info(
            f"DocumentDB Configuration: {json.dumps(documentdb_config, indent=2)}"
        )

        # Log PostgreSQL configuration
        postgres_config = {
            k: mask_sensitive_param(k, v) for k, v in self.postgresdb_vars.items()
        }
        logger.info(
            f"PostgreSQL Configuration: {json.dumps(postgres_config, indent=2)}"
        )


config_instance = None


def get_config(args: Optional[Dict[str, Any]] = None) -> Config:
    logger.info("111111111111111111111111111111111111111111111111")
    """
    Get the config instance. If it doesn't exist, create it.

    :param args: The arguments to pass to the config.
    :return: The config class instance.
    """
    global config_instance
    if args is None:
        args = dict()

    if config_instance is None:
        logger.info("Creating new configuration instance")
        config_instance = Config(args)
        # Log the configuration for debugging
        config_instance.log_config()
        logger.info("Configuration initialized successfully")

    return config_instance
