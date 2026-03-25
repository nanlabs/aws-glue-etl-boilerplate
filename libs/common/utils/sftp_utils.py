"""
SFTP utilities for reusable client connections.

This module provides a reusable SFTP client with automatic credential loading
from AWS Secrets Manager and SSM Parameter Store, supporting multiple SSH key
formats and consistent boto3 client usage.
"""

import json
import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple

try:
    import paramiko

    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

from botocore.exceptions import ClientError

from .aws import create_boto3_client
from .logger import get_logger

logger = get_logger("utils.sftp")


def load_sftp_credentials_from_secret(
    secret_name: str,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> Dict[str, str]:
    """
    Load SFTP credentials from AWS Secrets Manager.

    Args:
        secret_name: Name of the secret in Secrets Manager
        region_name: AWS region name (auto-detected if not provided)
        endpoint_url: Optional endpoint URL (for LocalStack)

    Returns:
        Dict with 'username' and 'privateKey' keys

    Raises:
        ValueError: If secret doesn't contain required fields
        ClientError: If secret cannot be retrieved
    """
    if not PARAMIKO_AVAILABLE:
        raise ImportError(
            "paramiko is not available. Install it with: pip install paramiko"
        )

    logger.info(f"Loading SFTP credentials from secret: {secret_name}")
    secrets_client = create_boto3_client(
        service_name="secretsmanager",
        region_name=region_name,
        endpoint_url=endpoint_url,
    )

    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_string = response["SecretString"]
        logger.debug(f"Secret string length: {len(secret_string)} characters")

        secret_data = json.loads(secret_string)
        logger.debug(f"Secret data keys: {list(secret_data.keys())}")

        credentials = {}
        if "username" in secret_data:
            credentials["username"] = secret_data["username"]
            logger.info(f"✅ SFTP username loaded: {credentials['username']}")
        else:
            logger.warning("⚠️  Username not found in secret data")

        if "privateKey" in secret_data:
            credentials["privateKey"] = secret_data["privateKey"]
            key_length = len(credentials["privateKey"])
            logger.info(f"✅ SFTP private key loaded (length: {key_length} chars)")
        else:
            logger.error("❌ 'privateKey' field not found in secret data")
            raise ValueError(
                "SFTP secret must contain 'privateKey' field. "
                "Password authentication is not supported."
            )

        logger.info("✅ SFTP credentials loaded successfully")
        return credentials

    except ClientError as e:
        logger.warning(f"Failed to load SFTP credentials from Secrets Manager: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse secret JSON: {e}")
        raise


def load_sftp_params_from_ssm(
    host_param: Optional[str] = None,
    port_param: Optional[str] = None,
    remote_path_param: Optional[str] = None,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Load SFTP connection parameters from SSM Parameter Store.

    Args:
        host_param: SSM parameter name for SFTP host
        port_param: SSM parameter name for SFTP port
        remote_path_param: SSM parameter name for SFTP remote path
        region_name: AWS region name (auto-detected if not provided)
        endpoint_url: Optional endpoint URL (for LocalStack)

    Returns:
        Dict with 'host', 'port', 'remote_path' keys (only for loaded params)
    """
    params = {}
    ssm_client = create_boto3_client(
        service_name="ssm",
        region_name=region_name,
        endpoint_url=endpoint_url,
    )

    try:
        if host_param:
            try:
                logger.info(f"Loading SFTP host from SSM: {host_param}")
                response = ssm_client.get_parameter(Name=host_param)
                params["host"] = response["Parameter"]["Value"]
                logger.info(f"✅ SFTP host loaded: {params['host']}")
            except ClientError as e:
                logger.warning(f"Could not load host from SSM: {e}")

        if port_param:
            try:
                logger.info(f"Loading SFTP port from SSM: {port_param}")
                response = ssm_client.get_parameter(Name=port_param)
                params["port"] = int(response["Parameter"]["Value"])
                logger.info(f"✅ SFTP port loaded: {params['port']}")
            except ClientError as e:
                logger.warning(f"Could not load port from SSM: {e}")

        if remote_path_param:
            try:
                logger.info(f"Loading SFTP remote path from SSM: {remote_path_param}")
                response = ssm_client.get_parameter(Name=remote_path_param)
                params["remote_path"] = response["Parameter"]["Value"]
                logger.info(f"✅ SFTP remote path loaded: {params['remote_path']}")
            except ClientError as e:
                logger.warning(f"Could not load remote path from SSM: {e}")

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "ParameterNotFound":
            logger.warning(f"SSM parameter not found: {e}")
        else:
            logger.warning(f"Failed to load SSM parameters: {e}")

    return params


def create_ssh_key_from_string(
    private_key_string: str,
    key_file_path: Optional[str] = None,
) -> Tuple[Any, str]:
    """
    Create paramiko key object from private key string.

    Automatically detects key type by trying multiple formats:
    1. ed25519 (modern, preferred)
    2. RSA (legacy)
    3. ECDSA (legacy)

    Args:
        private_key_string: Private key as string
        key_file_path: Optional path to save key temporarily (auto-created if None)

    Returns:
        Tuple of (paramiko.PKey object, key_type string)

    Raises:
        ValueError: If key cannot be loaded in any supported format
        ImportError: If paramiko is not available
    """
    if not PARAMIKO_AVAILABLE:
        raise ImportError(
            "paramiko is not available. Install it with: pip install paramiko"
        )

    if not private_key_string:
        raise ValueError("private_key_string is required")

    # Create temporary key file if not provided
    should_cleanup = False
    if not key_file_path:
        fd, key_file_path = tempfile.mkstemp(suffix=".key", text=True)
        should_cleanup = True
        try:
            with os.fdopen(fd, "w") as key_file:
                key_file.write(private_key_string)
        except Exception as e:
            logger.error(f"Failed to write key to temporary file: {e}")
            if should_cleanup and os.path.exists(key_file_path):
                os.unlink(key_file_path)
            raise

    try:
        # Try ed25519 first (most common modern format)
        try:
            logger.debug("Attempting to load key as ed25519...")
            private_key = paramiko.Ed25519Key.from_private_key_file(key_file_path)
            key_type = "ed25519"
            logger.info("✅ Loaded ed25519 key")
            return private_key, key_type
        except (ValueError, paramiko.ssh_exception.SSHException) as e:
            logger.debug(f"Failed to load as ed25519: {type(e).__name__}: {e}")

        # Fallback to RSA
        try:
            logger.debug("Attempting to load key as RSA...")
            private_key = paramiko.RSAKey.from_private_key_file(key_file_path)
            key_type = "RSA"
            logger.info("✅ Loaded RSA key")
            return private_key, key_type
        except (ValueError, paramiko.ssh_exception.SSHException) as e:
            logger.debug(f"Failed to load as RSA: {type(e).__name__}: {e}")

        # Try ECDSA
        try:
            logger.debug("Attempting to load key as ECDSA...")
            private_key = paramiko.ECDSAKey.from_private_key_file(key_file_path)
            key_type = "ECDSA"
            logger.info("✅ Loaded ECDSA key")
            return private_key, key_type
        except (ValueError, paramiko.ssh_exception.SSHException) as e:
            logger.debug(f"Failed to load as ECDSA: {type(e).__name__}: {e}")

        # All attempts failed
        logger.error("❌ Failed to load private key: all key type attempts failed")
        # Note: e is from the last except block (line 236), but may not be in scope here
        # Use a generic error message instead
        raise ValueError(
            "Could not load private key. Supported formats: ed25519, RSA, ECDSA. "
            "All key type attempts failed."
        )

    finally:
        # Clean up temporary key file if we created it
        if should_cleanup and key_file_path and os.path.exists(key_file_path):
            os.unlink(key_file_path)


class SFTPClient:
    """
    Reusable SFTP client with automatic credential loading from AWS.

    Features:
    - Loads credentials from Secrets Manager (username, privateKey)
    - Loads connection parameters from SSM Parameter Store (host, port, remote_path)
    - Supports multiple SSH key formats (ed25519, RSA, ECDSA)
    - Automatic connection management with context manager
    - Reuses boto3 clients via create_boto3_client()
    """

    def __init__(
        self,
        secret_name: Optional[str] = None,
        host_param: Optional[str] = None,
        port_param: Optional[str] = None,
        remote_path_param: Optional[str] = None,
        # Direct config (alternative to AWS)
        host: Optional[str] = None,
        port: int = 22,
        username: Optional[str] = None,
        private_key: Optional[str] = None,
        remote_path: Optional[str] = None,
        # AWS config
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        """
        Initialize SFTP client with credentials from AWS or direct config.

        Priority:
        1. Direct parameters (host, username, private_key, etc.)
        2. AWS Secrets Manager + SSM Parameter Store (if secret_name provided)

        Args:
            secret_name: Name of secret in Secrets Manager
            host_param: SSM parameter name for SFTP host
            port_param: SSM parameter name for SFTP port
            remote_path_param: SSM parameter name for SFTP remote path
            host: Direct SFTP host (alternative to SSM)
            port: Direct SFTP port (default: 22)
            username: Direct SFTP username (alternative to Secrets Manager)
            private_key: Direct SFTP private key (alternative to Secrets Manager)
            remote_path: Direct SFTP remote path (alternative to SSM)
            region_name: AWS region name (auto-detected if not provided)
            endpoint_url: Optional endpoint URL (for LocalStack)
        """
        if not PARAMIKO_AVAILABLE:
            raise ImportError(
                "paramiko is not available. Install it with: pip install paramiko"
            )

        self.region_name = region_name
        self.endpoint_url = endpoint_url

        # Store direct config
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self.remote_path = remote_path

        # Load from AWS if secret_name provided
        if secret_name:
            self._load_credentials_from_aws(
                secret_name=secret_name,
                host_param=host_param,
                port_param=port_param,
                remote_path_param=remote_path_param,
            )

        # Validate required fields
        if not all([self.host, self.username, self.private_key]):
            raise ValueError(
                "Missing required SFTP configuration. "
                "Provide either (secret_name + params) or (host, username, private_key) directly."
            )

        # Connection objects (initialized in connect())
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp_client: Optional[paramiko.SFTPClient] = None

    def _load_credentials_from_aws(
        self,
        secret_name: str,
        host_param: Optional[str] = None,
        port_param: Optional[str] = None,
        remote_path_param: Optional[str] = None,
    ) -> None:
        """Load credentials from Secrets Manager and SSM Parameter Store."""
        # Load credentials from Secrets Manager
        try:
            credentials = load_sftp_credentials_from_secret(
                secret_name=secret_name,
                region_name=self.region_name,
                endpoint_url=self.endpoint_url,
            )
            if "username" in credentials and not self.username:
                self.username = credentials["username"]
            if "privateKey" in credentials and not self.private_key:
                self.private_key = credentials["privateKey"]
        except Exception as e:
            logger.warning(f"Failed to load credentials from secret: {e}")
            if not self.username or not self.private_key:
                raise

        # Load parameters from SSM
        ssm_params = load_sftp_params_from_ssm(
            host_param=host_param,
            port_param=port_param,
            remote_path_param=remote_path_param,
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
        )
        if "host" in ssm_params and not self.host:
            self.host = ssm_params["host"]
        if "port" in ssm_params:
            self.port = ssm_params["port"]
        if "remote_path" in ssm_params and not self.remote_path:
            self.remote_path = ssm_params["remote_path"]

    def connect(self) -> paramiko.SFTPClient:
        """
        Connect to SFTP server and return SFTP client.

        Returns:
            paramiko.SFTPClient instance

        Raises:
            ValueError: If connection fails
        """
        if self.sftp_client:
            return self.sftp_client

        logger.info(f"Connecting to SFTP server: {self.host}:{self.port}")

        # Create SSH key from string
        private_key_obj, _ = create_ssh_key_from_string(self.private_key)

        # Create SSH client
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                pkey=private_key_obj,
                look_for_keys=False,
                allow_agent=False,
            )
            logger.info("✅ Connected to SFTP server")

            # Open SFTP channel
            self.sftp_client = self.ssh_client.open_sftp()
            logger.info("✅ SFTP channel opened")

            return self.sftp_client

        except Exception as e:
            logger.error(f"Failed to connect to SFTP server: {e}")
            self.close()
            raise

    def download_file(
        self, remote_path: Optional[str] = None, local_path: str = ""
    ) -> None:
        """
        Download file from SFTP server to local path.

        Supports:
        - Direct file path: Downloads the file
        - Directory path: Lists directory, finds first matching file (e.g., *.csv)

        Args:
            remote_path: Remote file or directory path (uses self.remote_path if None)
            local_path: Local file path to save downloaded file

        Raises:
            ValueError: If file not found or download fails
        """
        if not self.sftp_client:
            self.connect()

        # Use instance remote_path if not provided
        if remote_path is None:
            remote_path = self.remote_path

        if not remote_path:
            raise ValueError(
                "remote_path must be provided either as parameter or in SFTPClient initialization"
            )

        # Check if remote_path is a directory or file
        is_directory = remote_path.endswith("/") or remote_path == ""

        if is_directory:
            # List files in directory
            logger.info(f"Remote path is a directory: {remote_path}")
            try:
                files = self.sftp_client.listdir(remote_path)
                logger.info(f"Found {len(files)} items in directory: {files}")

                # Filter for CSV files
                csv_files = [f for f in files if f.lower().endswith(".csv")]
                if not csv_files:
                    raise ValueError(
                        f"No CSV files found in directory {remote_path}. "
                        f"Available files: {files}"
                    )

                # Use the first CSV file found
                csv_file = csv_files[0]
                if len(csv_files) > 1:
                    logger.warning(
                        f"Multiple CSV files found ({len(csv_files)}). "
                        f"Using first file: {csv_file}"
                    )

                # Construct full path
                if remote_path.endswith("/"):
                    full_remote_path = f"{remote_path}{csv_file}"
                else:
                    full_remote_path = f"{remote_path}/{csv_file}"

                logger.info(f"Selected CSV file: {full_remote_path}")
                remote_path = full_remote_path

            except Exception as e:
                logger.error(f"Failed to list directory {remote_path}: {e}")
                raise

        logger.info(f"Downloading {remote_path} to {local_path}")
        self.sftp_client.get(remote_path, local_path)

        # Get file stats for logging
        stat = self.sftp_client.stat(remote_path)
        logger.info(f"Downloaded {stat.st_size} bytes from SFTP server")

    def list_files(self, remote_path: str, pattern: Optional[str] = None) -> List[str]:
        """
        List files in remote directory, optionally filtered by pattern.

        Args:
            remote_path: Remote directory path
            pattern: Optional glob pattern (e.g., "*.csv")

        Returns:
            List of file names (not full paths)
        """
        if not self.sftp_client:
            self.connect()

        files = self.sftp_client.listdir(remote_path)

        if pattern:
            import fnmatch

            files = [f for f in files if fnmatch.fnmatch(f, pattern)]

        return files

    def get_file_stats(self, remote_path: str) -> Dict[str, Any]:
        """
        Get file statistics (size, mtime, etc.) from remote file.

        Args:
            remote_path: Remote file path

        Returns:
            Dict with file statistics
        """
        if not self.sftp_client:
            self.connect()

        stat = self.sftp_client.stat(remote_path)
        return {
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "atime": stat.st_atime,
        }

    def close(self) -> None:
        """Close SFTP and SSH connections."""
        if self.sftp_client:
            try:
                self.sftp_client.close()
                logger.debug("SFTP channel closed")
            except Exception as e:
                logger.warning(f"Error closing SFTP channel: {e}")
            finally:
                self.sftp_client = None

        if self.ssh_client:
            try:
                self.ssh_client.close()
                logger.debug("SSH connection closed")
            except Exception as e:
                logger.warning(f"Error closing SSH connection: {e}")
            finally:
                self.ssh_client = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - auto-close connections."""
        self.close()
