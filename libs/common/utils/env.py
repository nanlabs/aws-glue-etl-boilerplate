"""
Environment variable utilities.

Provides type-safe environment variable access with validation.
"""

import os
from typing import Optional


def get_required_env_var(var_name: str) -> str:
    """
    Get required environment variable or raise error.

    Args:
        var_name: Name of the environment variable

    Returns:
        Value of the environment variable

    Raises:
        ValueError: If environment variable is not set
    """
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value


def get_optional_env_var(var_name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get optional environment variable with default value.

    Args:
        var_name: Name of the environment variable
        default: Default value if not set

    Returns:
        Value of the environment variable or default
    """
    return os.getenv(var_name, default)


def get_required_env_var_int(var_name: str) -> int:
    """
    Get required environment variable as integer or raise error.

    Args:
        var_name: Name of the environment variable

    Returns:
        Integer value of the environment variable

    Raises:
        ValueError: If environment variable is not set or not a valid integer
    """
    value = get_required_env_var(var_name)
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {var_name} must be an integer, got: {value}"
        )


def get_optional_env_var_int(var_name: str, default: int) -> int:
    """
    Get optional environment variable as integer with default.

    Args:
        var_name: Name of the environment variable
        default: Default value if not set

    Returns:
        Integer value of the environment variable or default

    Raises:
        ValueError: If environment variable is set but not a valid integer
    """
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {var_name} must be an integer, got: {value}"
        )


def get_required_env_var_float(var_name: str) -> float:
    """
    Get required environment variable as float or raise error.

    Args:
        var_name: Name of the environment variable

    Returns:
        Float value of the environment variable

    Raises:
        ValueError: If environment variable is not set or not a valid float
    """
    value = get_required_env_var(var_name)
    try:
        return float(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {var_name} must be a float, got: {value}"
        )


def get_optional_env_var_float(var_name: str, default: float) -> float:
    """
    Get optional environment variable as float with default.

    Args:
        var_name: Name of the environment variable
        default: Default value if not set

    Returns:
        Float value of the environment variable or default

    Raises:
        ValueError: If environment variable is set but not a valid float
    """
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {var_name} must be a float, got: {value}"
        )


def get_required_env_var_bool(var_name: str) -> bool:
    """
    Get required environment variable as boolean or raise error.

    Accepts: "true", "1", "yes" (case-insensitive) for True
             "false", "0", "no" (case-insensitive) for False

    Args:
        var_name: Name of the environment variable

    Returns:
        Boolean value of the environment variable

    Raises:
        ValueError: If environment variable is not set or not a valid boolean
    """
    value = get_required_env_var(var_name).lower()
    if value in ("true", "1", "yes"):
        return True
    elif value in ("false", "0", "no"):
        return False
    else:
        raise ValueError(
            f"Environment variable {var_name} must be a boolean (true/false/1/0/yes/no), got: {value}"
        )


def get_optional_env_var_bool(var_name: str, default: bool) -> bool:
    """
    Get optional environment variable as boolean with default.

    Accepts: "true", "1", "yes" (case-insensitive) for True
             "false", "0", "no" (case-insensitive) for False

    Args:
        var_name: Name of the environment variable
        default: Default value if not set

    Returns:
        Boolean value of the environment variable or default

    Raises:
        ValueError: If environment variable is set but not a valid boolean
    """
    value = os.getenv(var_name)
    if value is None:
        return default

    value_lower = value.lower()
    if value_lower in ("true", "1", "yes"):
        return True
    elif value_lower in ("false", "0", "no"):
        return False
    else:
        raise ValueError(
            f"Environment variable {var_name} must be a boolean (true/false/1/0/yes/no), got: {value}"
        )


__all__ = [
    "get_required_env_var",
    "get_optional_env_var",
    "get_required_env_var_int",
    "get_optional_env_var_int",
    "get_required_env_var_float",
    "get_optional_env_var_float",
    "get_required_env_var_bool",
    "get_optional_env_var_bool",
]
