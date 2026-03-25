"""Utility modules for the data lake framework."""

from .aws import create_boto3_client, detect_localstack_endpoint
from .env import (
    get_optional_env_var,
    get_optional_env_var_bool,
    get_optional_env_var_float,
    get_optional_env_var_int,
    get_required_env_var,
    get_required_env_var_bool,
    get_required_env_var_float,
    get_required_env_var_int,
)
from .hash_utils import (
    calculate_content_sha256,
    calculate_file_sha256,
    calculate_stream_sha256,
)
from .logger import get_logger, setup_logging
from .secrets import (
    SecretsManager,
    get_secret_by_key,
    get_secret_json_by_key,
    get_secrets_manager,
    get_ssm_parameter,
)
from .sftp_utils import (
    SFTPClient,
    create_ssh_key_from_string,
    load_sftp_credentials_from_secret,
    load_sftp_params_from_ssm,
)

__all__ = [
    # AWS utilities
    "create_boto3_client",
    "detect_localstack_endpoint",
    # Environment utilities
    "get_required_env_var",
    "get_optional_env_var",
    "get_required_env_var_int",
    "get_optional_env_var_int",
    "get_required_env_var_float",
    "get_optional_env_var_float",
    "get_required_env_var_bool",
    "get_optional_env_var_bool",
    # Hash utilities
    "calculate_file_sha256",
    "calculate_content_sha256",
    "calculate_stream_sha256",
    # Logger
    "get_logger",
    "setup_logging",
    # Secrets
    "SecretsManager",
    "get_secrets_manager",
    "get_secret_by_key",
    "get_secret_json_by_key",
    "get_ssm_parameter",
    # SFTP utilities
    "SFTPClient",
    "load_sftp_credentials_from_secret",
    "load_sftp_params_from_ssm",
    "create_ssh_key_from_string",
]
