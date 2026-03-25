"""
AWS Secrets Manager integration utilities.

Simplified access to AWS Secrets Manager with automatic resource naming
following the pattern: nan-wl-workloads-data-lake-{environment}/{secret_key}
"""

import base64
import json
import os
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from .logger import get_logger

logger = get_logger("utils.secrets")


class SecretsManager:
    """
    Simplified AWS Secrets Manager client with automatic resource naming.
    """

    def __init__(
        self, region_name: Optional[str] = None, endpoint_url: Optional[str] = None
    ) -> None:
        """
        Initialize the secrets manager.

        Args:
            region_name: AWS region name
            endpoint_url: Optional endpoint URL (for LocalStack)
        """
        self.region_name: Optional[str] = region_name
        self.cache: Dict[str, str] = {}

        # Auto-detect LocalStack endpoint if not provided
        if endpoint_url is None:
            endpoint_url = self._detect_localstack_endpoint()

        self.endpoint_url = endpoint_url

        # Initialize boto3 client
        session: boto3.Session = boto3.Session()

        # Initialize Secrets Manager client
        secrets_client_kwargs = {
            "service_name": "secretsmanager",
            "region_name": region_name,
        }

        # Add endpoint URL for LocalStack
        if endpoint_url:
            secrets_client_kwargs["endpoint_url"] = endpoint_url
            logger.info(
                f"Using LocalStack endpoint for Secrets Manager: {endpoint_url}"
            )

        self.secrets_client: Any = session.client(**secrets_client_kwargs)

        # Initialize SSM client
        ssm_client_kwargs = {
            "service_name": "ssm",
            "region_name": region_name,
        }

        # Add endpoint URL for LocalStack
        if endpoint_url:
            ssm_client_kwargs["endpoint_url"] = endpoint_url
            logger.info(f"Using LocalStack endpoint for SSM: {endpoint_url}")

        self.ssm_client: Any = session.client(**ssm_client_kwargs)

        logger.info("SecretsManager initialized")

    def _detect_localstack_endpoint(self) -> Optional[str]:
        """
        Auto-detect LocalStack endpoint from environment.

        When STAGE=local, Secrets Manager and SSM use LocalStack by default.
        Secret synchronization from AWS real to LocalStack is handled automatically.

        Returns:
            Optional[str]: LocalStack endpoint URL if detected, None to use real AWS
        """
        # Check for service-specific endpoint (explicit configuration, highest priority)
        if os.getenv("AWS_ENDPOINT_URL_SECRETSMANAGER"):
            return os.getenv("AWS_ENDPOINT_URL_SECRETSMANAGER")

        if os.getenv("AWS_ENDPOINT_URL_SSM"):
            return os.getenv("AWS_ENDPOINT_URL_SSM")

        # Check for general AWS_ENDPOINT_URL (used by .envrc)
        if os.getenv("AWS_ENDPOINT_URL"):
            return os.getenv("AWS_ENDPOINT_URL")

        # Check for general LocalStack endpoint
        localstack_endpoint = os.getenv("LOCALSTACK_ENDPOINT")
        if localstack_endpoint:
            return localstack_endpoint

        # When STAGE=local, use LocalStack by default
        # Secret synchronization from AWS real to LocalStack is handled automatically
        if os.getenv("STAGE", "").lower() == "local":
            return "http://localstack:4566"

        return None

    def get_secret_by_key(
        self, secret_key: str, use_cache: bool = True
    ) -> Optional[str]:
        """
        Get a secret using the resource naming convention.

        Args:
            secret_key: The secret key (e.g., 'teamtailor-api', 'database-credentials')
            use_cache: Whether to use cached values

        Returns:
            Secret value or None if not found

        Examples:
            get_secret_by_key('teamtailor-api') -> retrieves 'nan-wl-workloads-data-lake-develop/teamtailor-api'
        """
        # Option 1: Check for explicit override via environment variable
        env_var_name = f"{secret_key.upper().replace('-', '_')}_SECRET_NAME"
        explicit_secret_name = os.getenv(env_var_name)

        if explicit_secret_name:
            logger.info(
                f"Using explicit secret name from {env_var_name}: {explicit_secret_name}"
            )
            return self.get_secret(explicit_secret_name, use_cache)

        # Option 2: Use convention-based naming
        logger.info(f"Using convention-based secret name: {secret_key}")
        return self.get_secret(secret_key, use_cache)

    def get_secret_json_by_key(
        self,
        secret_key: str,
        json_key: str,
        default: Optional[str] = None,
        use_cache: bool = True,
    ) -> Optional[str]:
        """
        Get a specific key from a JSON secret.

        Args:
            secret_key: The secret key (e.g., 'teamtailor-api')
            json_key: JSON key to extract from the secret value
            default: Default value if key not found
            use_cache: Whether to use cached values

        Returns:
            Secret value for the JSON key or default

        Examples:
            get_secret_json_by_key('teamtailor-api', 'api_token') -> extracts api_token from the JSON
        """
        secret_value = self.get_secret_by_key(secret_key, use_cache)

        if not secret_value:
            return default

        try:
            secret_dict: Dict[str, Any] = json.loads(secret_value)
            return secret_dict.get(json_key, default)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Error parsing JSON secret {secret_key}: {str(e)}")
            return default

    def get_api_key_from_secret(
        self, secret_name: str, api_key_field: str = "api_key", use_cache: bool = True
    ) -> str:
        """
        Get an API key from a secret with JSON parsing and fallback logic.

        This method handles the common pattern of retrieving API keys that may be stored
        as either plain text or JSON with a specific field.

        Args:
            secret_name: Full name of the secret (not using convention-based naming)
            api_key_field: Name of the JSON field containing the API key
            use_cache: Whether to use cached values

        Returns:
            API key value

        Raises:
            ValueError: If secret is not found or API key cannot be retrieved

        Examples:
            # For JSON secret: {"api_token": "abc123", "other": "value"}
            get_api_key_from_secret("my-app/teamtailor-api") -> "abc123"

            # For plain text secret: "abc123"
            get_api_key_from_secret("my-app/teamtailor-api") -> "abc123"
        """
        # Get secret data
        secret_data = self.get_secret(secret_name, use_cache)
        if not secret_data:
            raise ValueError(f"Secret '{secret_name}' not found or is empty.")

        # Try to parse as JSON first
        try:
            secret_json = json.loads(secret_data)
            api_key = secret_json.get(api_key_field)
            if not api_key:
                raise ValueError(
                    f"API key field '{api_key_field}' not found in JSON secret '{secret_name}'"
                )
            logger.info(
                f"Retrieved API key from JSON field '{api_key_field}' in secret '{secret_name}'"
            )
            return api_key
        except json.JSONDecodeError:
            # Fallback for non-JSON secrets - treat the entire secret as the API key
            logger.info(
                f"Using entire secret value as API key for secret '{secret_name}'"
            )
            return secret_data

    def get_secret(self, secret_name: str, use_cache: bool = True) -> Optional[str]:
        """
        Get a secret from AWS Secrets Manager.

        Args:
            secret_name: Name of the secret
            use_cache: Whether to use cached values

        Returns:
            Secret value or None if not found
        """
        # Check cache first
        if use_cache and secret_name in self.cache:
            logger.debug(f"Using cached secret: {secret_name}")
            return self.cache[secret_name]

        try:
            logger.info(f"Retrieving secret: {secret_name}")
            response: Dict[str, Any] = self.secrets_client.get_secret_value(
                SecretId=secret_name
            )

            # Handle string or binary secrets
            secret_value: str
            if "SecretString" in response:
                secret_value = response["SecretString"]
            else:
                secret_value = base64.b64decode(response["SecretBinary"]).decode(
                    "utf-8"
                )

            # Cache the result
            if use_cache:
                self.cache[secret_name] = secret_value

            return secret_value

        except ClientError as e:
            error_code: str = e.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                logger.warning(f"Secret not found: {secret_name}")
            else:
                logger.error(f"Error retrieving secret {secret_name}: {error_code}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving secret {secret_name}: {str(e)}")
            return None

    def get_secret_json(
        self, secret_name: str, key: str, default: Optional[str] = None
    ) -> Optional[str]:
        """
        Get a specific key from a JSON secret.

        Args:
            secret_name: Name of the secret
            key: JSON key to extract
            default: Default value if key not found

        Returns:
            Secret value for the key or default
        """
        secret_value: Optional[str] = self.get_secret(secret_name)
        if not secret_value:
            return default

        try:
            secret_dict: Dict[str, Any] = json.loads(secret_value)
            return secret_dict.get(key, default)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Error parsing JSON secret {secret_name}: {str(e)}")
            return default

    def get_ssm_parameter(
        self, parameter_name: str, use_cache: bool = True, default: Optional[str] = None
    ) -> Optional[str]:
        """
        Get a parameter from AWS SSM Parameter Store.

        Args:
            parameter_name: Name of the parameter (will be prefixed with resource prefix)
            use_cache: Whether to use cached values
            default: Default value if parameter not found

        Returns:
            Parameter value or default if not found
        """
        # Check cache first (use a different cache key to avoid conflicts)
        cache_key = f"ssm:{parameter_name}"
        if use_cache and cache_key in self.cache:
            logger.debug(f"Using cached SSM parameter: {parameter_name}")
            return self.cache[cache_key]

        try:
            logger.info(f"Retrieving SSM parameter: {parameter_name}")
            response: Dict[str, Any] = self.ssm_client.get_parameter(
                Name=parameter_name, WithDecryption=True
            )

            parameter_value = response["Parameter"]["Value"]

            # Cache the result
            if use_cache:
                self.cache[cache_key] = parameter_value

            logger.info(f"Successfully retrieved SSM parameter: {parameter_name}")
            return parameter_value

        except ClientError as e:
            error_code: str = e.response["Error"]["Code"]
            if error_code == "ParameterNotFound":
                logger.warning(f"SSM parameter not found: {parameter_name}")
                if default is not None:
                    logger.info(
                        f"Using default value for SSM parameter: {parameter_name}"
                    )
                    return default
            else:
                logger.error(
                    f"Error retrieving SSM parameter {parameter_name}: {error_code}"
                )
            return default
        except Exception as e:
            logger.error(
                f"Unexpected error retrieving SSM parameter {parameter_name}: {str(e)}"
            )
            return default


# Global instance for singleton pattern
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager(
    region_name: Optional[str] = None, endpoint_url: Optional[str] = None
) -> SecretsManager:
    """
    Get the global secrets manager instance.

    Args:
        region_name: AWS region name
        endpoint_url: Optional endpoint URL (for LocalStack)

    Returns:
        SecretsManager instance
    """
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager(
            region_name=region_name, endpoint_url=endpoint_url
        )
    return _secrets_manager


# Convenience functions
def get_secret_by_key(secret_key: str, use_cache: bool = True) -> Optional[str]:
    """
    Convenience function to get a secret using the naming convention.

    Args:
        secret_key: The secret key (e.g., 'teamtailor-api')
        use_cache: Whether to use cached values

    Returns:
        Secret value or None if not found

    Examples:
        get_secret_by_key('teamtailor-api') -> nan-wl-workloads-data-lake-develop/teamtailor-api

        # Override with environment variable:
        # export TEAMTAILOR_API_SECRET_NAME="my-custom-secret-name"
        # get_secret_by_key('teamtailor-api') -> my-custom-secret-name
    """
    manager = get_secrets_manager()
    return manager.get_secret_by_key(secret_key, use_cache)


def get_secret_json_by_key(
    secret_key: str,
    json_key: str,
    default: Optional[str] = None,
    use_cache: bool = True,
) -> Optional[str]:
    """
    Convenience function to get a JSON key from a secret.

    Args:
        secret_key: The secret key (e.g., 'teamtailor-api')
        json_key: JSON key to extract
        default: Default value if key not found
        use_cache: Whether to use cached values

    Returns:
        Secret value for the JSON key or default

    Examples:
        get_secret_json_by_key('teamtailor-api', 'api_token') -> extracts api_token from JSON
    """
    manager = get_secrets_manager()
    return manager.get_secret_json_by_key(secret_key, json_key, default, use_cache)


def get_ssm_parameter(
    parameter_key: str, use_cache: bool = True, default: Optional[str] = None
) -> Optional[str]:
    """
    Convenience function to get an SSM parameter using the naming convention.

    Args:
        parameter_key: The parameter key (e.g., 'teamtailor-api-base-url')
        use_cache: Whether to use cached values
        default: Default value if parameter not found

    Returns:
        Parameter value or default if not found

    Examples:
        get_ssm_parameter('teamtailor-api-base-url') -> nan-wl-workloads-data-lake-develop/teamtailor-api-base-url
    """
    manager = get_secrets_manager()
    return manager.get_ssm_parameter(parameter_key, use_cache, default)


def get_api_key_from_secret(
    secret_name: str, api_key_field: str = "api_key", use_cache: bool = True
) -> str:
    """
    Convenience function to get an API key from a secret with JSON parsing and fallback logic.

    This function handles the common pattern of retrieving API keys that may be stored
    as either plain text or JSON with a specific field.

    Args:
        secret_name: Full name of the secret (not using convention-based naming)
        api_key_field: Name of the JSON field containing the API key
        use_cache: Whether to use cached values

    Returns:
        API key value

    Raises:
        ValueError: If secret is not found or API key cannot be retrieved

    Examples:
        # For JSON secret: {"api_token": "abc123", "other": "value"}
        get_api_key_from_secret("my-app/teamtailor-api") -> "abc123"

        # For plain text secret: "abc123"
        get_api_key_from_secret("my-app/teamtailor-api") -> "abc123"
    """
    manager = get_secrets_manager()
    return manager.get_api_key_from_secret(secret_name, api_key_field, use_cache)
