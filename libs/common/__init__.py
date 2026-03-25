"""
Common utilities and configurations shared between PySpark and PyShell jobs.

This module contains all shared code that doesn't depend on PySpark or PyShell specifics:
- Configuration management (config/)
- Utilities (utils/)
- Exceptions
- Business logic (teamtailor/)
"""

from .config import ConfigBase, DatabaseConfig, print_workflow_properties
from .config.job_config import (
    BronzeJobConfig,
    GoldJobConfig,
    JobConfigBase,
    MedallionLayer,
    RawJobConfig,
    SilverJobConfig,
    validate_config,
)
from .exceptions import (
    ConfigurationError,
    DataValidationError,
    GlueJobError,
    ValidationError,
)
from .utils import (
    get_optional_env_var,
    get_optional_env_var_bool,
    get_optional_env_var_float,
    get_optional_env_var_int,
    get_required_env_var,
    get_required_env_var_bool,
    get_required_env_var_float,
    get_required_env_var_int,
    setup_logging,
)

__all__ = [
    # Config (Base classes only - source-specific configs are in their job files)
    "ConfigBase",
    "DatabaseConfig",
    "JobConfigBase",
    "RawJobConfig",
    "BronzeJobConfig",
    "SilverJobConfig",
    "GoldJobConfig",
    "MedallionLayer",
    "validate_config",
    # Exceptions
    "GlueJobError",
    "ConfigurationError",
    "ValidationError",
    "DataValidationError",
    # Environment utilities
    "get_required_env_var",
    "get_optional_env_var",
    "get_required_env_var_int",
    "get_optional_env_var_int",
    "get_required_env_var_float",
    "get_optional_env_var_float",
    "get_required_env_var_bool",
    "get_optional_env_var_bool",
    # Logging
    "setup_logging",
    # Workflow Properties
    "print_workflow_properties",
]
