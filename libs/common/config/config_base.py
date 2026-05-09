"""
Configuration management for AWS Glue v5 framework.

This module provides the ConfigBase class that implements four-tier parameter
resolution: workflow run properties, job parameters, environment variables, and defaults.
"""

import logging
import os
import sys
from typing import Any, Dict, Optional, Type, TypeVar, get_type_hints

from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="ConfigBase")


class ConfigBase(BaseModel):
    """
    Base configuration class with four-tier parameter resolution.

    Resolution order (highest to lowest priority):
    1. Workflow Run Properties (get_workflow_run_properties)
       - Requires WORKFLOW_NAME and WORKFLOW_RUN_ID
       - Only available when running in Glue Workflow context
    2. Command line parameters (--PARAM_NAME value)
       - From sys.argv or Glue job parameters
    3. Environment variables (PARAM_NAME)
       - From os.environ
    4. Default values (defined in the model)
       - Field defaults in Pydantic model

    Attributes:
        job_name: Name of the job (used for environment variable prefixing)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        environment: Environment name (dev, staging, prod)
        aws_region: AWS region for services
    """

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_strip_whitespace=True,
        use_enum_values=True,
    )

    job_name: str = Field(
        description="Name of the job (used for environment variable prefixing)"
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    environment: str = Field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "develop"),
        description="Environment name (develop, staging, prod)",
    )
    stage: str = Field(
        default_factory=lambda: os.getenv("STAGE", "local"),
        description="Stage name (local, dev, staging, prod)",
    )
    aws_region: str = Field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"),
        description="AWS region for services",
    )

    aws_endpoint_url: Optional[str] = Field(
        default=None, description="AWS endpoint URL for services"
    )

    # Internal tracking of parameter sources
    _parameter_sources: Dict[str, str] = {}

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid logging level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}, got {v}")
        return v_upper

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment name."""
        valid_envs = ["local", "develop", "staging", "prod"]
        v_lower = v.lower()
        if v_lower not in valid_envs:
            raise ValueError(f"environment must be one of {valid_envs}, got {v}")
        return v_lower

    @field_validator("stage")
    @classmethod
    def validate_stage(cls, v: str) -> str:
        """Validate stage name."""
        valid_stages = ["local", "dev", "staging", "prod"]
        v_lower = v.lower()
        if v_lower not in valid_stages:
            raise ValueError(f"stage must be one of {valid_stages}, got {v}")
        return v_lower

    @classmethod
    def from_args(cls: Type[T], job_name: Optional[str] = None) -> T:
        """
        Create configuration instance with three-tier parameter resolution.

        Args:
            job_name: Optional job name override

        Returns:
            Configured instance with resolved parameters
        """
        # Get job name from arguments or environment
        if job_name is None:
            job_name = cls._get_job_name_from_args()

        # Collect parameters from all sources
        resolved_params = cls._resolve_parameters(job_name)

        # Create instance
        instance = cls(**resolved_params)

        # Store parameter sources for debugging
        instance._parameter_sources = cls._get_parameter_sources(
            job_name, resolved_params
        )

        return instance

    @classmethod
    def _get_job_name_from_args(cls) -> str:
        """Extract job name from command line arguments or environment."""
        # Try to get from environment first
        job_name = os.getenv("JOB_NAME")
        if job_name:
            return job_name

        # Try to get from sys.argv (common Glue pattern: --JOB_NAME value)
        args = sys.argv
        for i, arg in enumerate(args):
            if arg == "--JOB_NAME" and i + 1 < len(args):
                return args[i + 1]
            elif arg.startswith("--JOB_NAME="):
                return arg.split("=", 1)[1]

        # Default fallback
        return "glue_job"

    @classmethod
    def _resolve_parameters(cls, job_name: str) -> Dict[str, Any]:
        """
        Resolve parameters using unified four-tier resolution.

        HIERARCHY (highest to lowest priority):
        1. Workflow Run Properties (get_workflow_run_properties)
        2. Command line parameters (--PARAM_NAME value)
        3. Environment variables (PARAM_NAME)
        4. Default values (if available)

        Args:
            job_name: Job name for environment variable prefixing

        Returns:
            Dictionary of resolved parameters
        """
        resolved = {"job_name": job_name}

        # Get workflow properties if available
        workflow_props = cls._get_workflow_properties()

        # Get field information from the model
        type_hints = get_type_hints(cls)

        for field_name, field_info in cls.model_fields.items():
            if field_name == "job_name":
                continue  # Already set

            # Get the field type for conversion
            field_type = type_hints.get(field_name, str)

            # 1. Try workflow run properties (HIGHEST PRIORITY)
            # Normalize workflow property keys: try field_name, --field_name, FIELD_NAME, --FIELD_NAME
            workflow_value = None
            for key_variant in [
                field_name,  # entity_type
                f"--{field_name}",  # --entity_type
                field_name.upper(),  # ENTITY_TYPE
                f"--{field_name.upper()}",  # --ENTITY_TYPE
            ]:
                if key_variant in workflow_props:
                    workflow_value = workflow_props[key_variant]
                    break

            if workflow_value is not None:
                resolved[field_name] = cls._convert_value(workflow_value, field_type)
                continue

            # 2. Try command line parameter (MEDIUM-HIGH PRIORITY)
            job_param_value = cls._get_job_parameter(field_name)
            if job_param_value is not None:
                resolved[field_name] = cls._convert_value(job_param_value, field_type)
                continue

            # 3. Try environment variable (MEDIUM PRIORITY)
            env_value = os.getenv(field_name.upper())
            if env_value is not None:
                resolved[field_name] = cls._convert_value(env_value, field_type)
                continue

            # 4. Use default value (LOWEST PRIORITY)
            if field_info.default is not None and field_info.default != ...:
                # Handle default_factory if present
                if (
                    hasattr(field_info, "default_factory")
                    and field_info.default_factory
                ):
                    try:
                        resolved[field_name] = field_info.default_factory()
                    except Exception:
                        # If default_factory fails, skip
                        pass
                else:
                    resolved[field_name] = field_info.default

        return resolved

    @classmethod
    def _get_workflow_properties(cls) -> Dict[str, str]:
        """
        Get workflow run properties if available.

        Returns:
            Dictionary of workflow properties (empty if not available)
        """
        try:
            from .glue_config_resolver import get_workflow_run_properties

            # Try to get from job parameters first
            workflow_name = cls._get_job_parameter("WORKFLOW_NAME")
            run_id = cls._get_job_parameter("WORKFLOW_RUN_ID")

            # Fallback to environment variables
            if not workflow_name:
                workflow_name = os.getenv("WORKFLOW_NAME")
            if not run_id:
                run_id = os.getenv("WORKFLOW_RUN_ID")

            if workflow_name and run_id:
                return get_workflow_run_properties(
                    workflow_name=workflow_name, run_id=run_id
                )

            return {}
        except Exception as e:
            logger.debug("Could not get workflow properties: %s", e)
            return {}

    @classmethod
    def _get_job_parameter(cls, param_name: str) -> Optional[str]:
        """
        Extract job parameter from command line arguments.

        Args:
            param_name: Parameter name to look for

        Returns:
            Parameter value if found, None otherwise
        """
        args = sys.argv
        param_flag = f"--{param_name}"
        param_flag_upper = f"--{param_name.upper()}"
        param_flag_eq = f"--{param_name}="
        param_flag_eq_upper = f"--{param_name.upper()}="

        for i, arg in enumerate(args):
            if arg in [param_flag, param_flag_upper] and i + 1 < len(args):
                return args[i + 1]
            elif arg.startswith(param_flag_eq) or arg.startswith(param_flag_eq_upper):
                return arg.split("=", 1)[1]

        return None

    @classmethod
    def _convert_value(cls, value: str, target_type: Type) -> Any:
        """
        Convert string value to target type.

        Args:
            value: String value to convert
            target_type: Target type for conversion

        Returns:
            Converted value
        """
        if target_type is str:
            return value
        elif target_type is int:
            return int(value)
        elif target_type is float:
            return float(value)
        elif target_type is bool:
            return value.lower() in ("true", "1", "yes", "on")
        else:
            # For complex types, return as string and let Pydantic handle validation
            return value

    @classmethod
    def _get_parameter_sources(
        cls, job_name: str, resolved_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """
        Determine the source of each parameter for debugging.

        Args:
            job_name: Job name used for resolution
            resolved_params: Dictionary of resolved parameters

        Returns:
            Dictionary mapping parameter names to their sources
        """
        sources = {}

        # Get workflow properties for checking
        workflow_props = cls._get_workflow_properties()

        for field_name in cls.model_fields.keys():
            if field_name == "job_name":
                sources[field_name] = "argument"
                continue

            # Check workflow properties (HIGHEST PRIORITY)
            # Try all variants: field_name, --field_name, FIELD_NAME, --FIELD_NAME
            workflow_value = None
            workflow_key = None
            for key_variant in [
                field_name,  # entity_type
                f"--{field_name}",  # --entity_type
                field_name.upper(),  # ENTITY_TYPE
                f"--{field_name.upper()}",  # --ENTITY_TYPE
            ]:
                if key_variant in workflow_props:
                    workflow_value = workflow_props[key_variant]
                    workflow_key = key_variant
                    break

            if workflow_value is not None:
                sources[field_name] = (
                    f"workflow_properties ({workflow_key}={workflow_value})"
                )
                continue

            # Check job parameter
            if cls._get_job_parameter(field_name) is not None:
                sources[field_name] = f"job_parameter (--{field_name})"
                continue

            # Check environment variable
            env_var_name = f"{job_name.upper()}_{field_name.upper()}"
            if os.getenv(env_var_name) is not None:
                sources[field_name] = f"environment ({env_var_name})"
                continue

            # Also check simple uppercase env var
            if os.getenv(field_name.upper()) is not None:
                sources[field_name] = f"environment ({field_name.upper()})"
                continue

            # Default value
            sources[field_name] = "default"

        return sources

    def get_parameter_source(self, field_name: str) -> str:
        """
        Get the source of a specific parameter.

        Args:
            field_name: Name of the field to check

        Returns:
            Source description for the parameter
        """
        return self._parameter_sources.get(field_name, "unknown")

    def is_local_env(self) -> bool:
        """
        Check if running in local development environment.

        Returns:
            True if running locally, False otherwise
        """
        return self.stage == "local"

    def is_cloud_env(self) -> bool:
        """
        Check if running in cloud environment.

        Returns:
            True if running in cloud, False otherwise
        """
        return not self.is_local_env()

    def log_configuration(
        self, logger_instance: Optional[logging.Logger] = None
    ) -> None:
        """
        Log the complete resolved configuration with parameter sources.

        Args:
            logger_instance: Optional logger instance (uses module logger if not provided)
        """
        if logger_instance is None:
            logger_instance = logger

        logger_instance.info("=" * 80)
        logger_instance.info("📋 RESOLVED CONFIGURATION")
        logger_instance.info("=" * 80)

        # Get parameter sources
        sources = self._get_parameter_sources(self.job_name, self.model_dump())

        # Get model fields (Pydantic v2: model_fields is a class attribute)
        # Try both instance and class access for compatibility
        if hasattr(self, "model_fields"):
            model_fields = self.model_fields
        else:
            model_fields = type(self).model_fields

        # Debug: log field count
        field_count = len(model_fields) if model_fields else 0
        logger_instance.debug(f"Found {field_count} model fields")

        if not model_fields or field_count == 0:
            logger_instance.warning(
                "No model fields found! Using model_dump() instead."
            )
            # Fallback: use model_dump to get all fields
            config_dict = self.model_dump()
            for field_name, value in config_dict.items():
                # Mask sensitive values
                if self._is_sensitive_field(field_name):
                    if value:
                        value_str = str(value)
                        if len(value_str) > 8:
                            display_value = f"{value_str[:4]}...{value_str[-4:]}"
                        else:
                            display_value = "***"
                    else:
                        display_value = "None"
                else:
                    display_value = str(value)

                source = sources.get(field_name, "unknown")
                logger_instance.info(f"  {field_name}: {display_value} [{source}]")
        else:
            # Log each field with its value and source
            for field_name in model_fields.keys():
                value = getattr(self, field_name, None)

                # Mask sensitive values
                if self._is_sensitive_field(field_name):
                    if value:
                        # Show first 4 and last 4 characters, mask the rest
                        value_str = str(value)
                        if len(value_str) > 8:
                            masked = f"{value_str[:4]}...{value_str[-4:]}"
                        else:
                            masked = "***"
                        display_value = masked
                    else:
                        display_value = "None"
                else:
                    display_value = str(value)

                source = sources.get(field_name, "unknown")
                logger_instance.info(f"  {field_name}: {display_value} [{source}]")

        logger_instance.info("=" * 80)

    def _is_sensitive_field(self, field_name: str) -> bool:
        """
        Check if a field contains sensitive information.

        Args:
            field_name: Name of the field to check

        Returns:
            True if field is sensitive, False otherwise
        """
        sensitive_keywords = ["password", "secret", "key", "token", "credential"]
        field_lower = field_name.lower()
        return any(keyword in field_lower for keyword in sensitive_keywords)

    def to_dict(self, include_sources: bool = False) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.

        Args:
            include_sources: Whether to include parameter sources

        Returns:
            Dictionary representation of the configuration
        """
        result = self.model_dump()

        if include_sources:
            result["_parameter_sources"] = self._parameter_sources

        return result

    def validate_required_fields(self) -> None:
        """
        Validate that all required fields are properly set.

        Raises:
            ValueError: If required fields are missing or invalid
        """
        # This method can be overridden by subclasses to add custom validation
