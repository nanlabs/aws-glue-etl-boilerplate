"""
Comprehensive exception hierarchy for AWS Glue 5.0 job framework.

This module provides a complete exception hierarchy with proper exception chaining,
context preservation, meaningful error messages, and actionable recovery suggestions.
All exceptions include structured logging capabilities and error context management.
"""

import logging
from typing import Any, Dict, Optional, Union


class GlueJobError(Exception):
    """
    Base exception for all Glue job errors with comprehensive error handling.

    This is the root exception class that provides:
    - Exception chaining and context preservation
    - Meaningful error messages with actionable recovery suggestions
    - Structured logging integration
    - Error context management
    - Recovery suggestion system
    """

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None,
        recovery_suggestion: Optional[str] = None,
        error_code: Optional[str] = None,
    ) -> None:
        """
        Initialize GlueJobError with comprehensive error information.

        Args:
            message: Primary error message describing what went wrong
            cause: Optional underlying exception that caused this error (for chaining)
            context: Optional dictionary with additional error context
            recovery_suggestion: Optional suggestion for how to recover from this error
            error_code: Optional error code for programmatic error handling
        """
        super().__init__(message)
        self.cause = cause
        self.context = context or {}
        self.recovery_suggestion = recovery_suggestion
        self.error_code = error_code
        self.timestamp = self._get_current_timestamp()

        # Preserve the original traceback if we have a cause
        if cause is not None:
            self.__cause__ = cause

    def _get_current_timestamp(self) -> str:
        """Get current timestamp for error tracking."""
        from datetime import datetime

        return datetime.utcnow().isoformat() + "Z"

    def get_full_message(self) -> str:
        """
        Get the complete error message including context and recovery suggestions.

        Returns:
            Comprehensive error message with all available information
        """
        parts = [str(self)]

        if self.error_code:
            parts.append(f"Error Code: {self.error_code}")

        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"Context: {context_str}")

        if self.recovery_suggestion:
            parts.append(f"Recovery Suggestion: {self.recovery_suggestion}")

        if self.cause:
            parts.append(f"Caused by: {str(self.cause)}")

        return " | ".join(parts)

    def log_error(self, logger: logging.Logger, level: int = logging.ERROR) -> None:
        """
        Log this error with structured format and appropriate log level.

        Args:
            logger: Logger instance to use for logging
            level: Log level to use (default: ERROR)
        """
        # Create structured log entry
        log_data = {
            "error_type": self.__class__.__name__,
            "error_message": str(self),
            "timestamp": self.timestamp,
            "error_code": self.error_code,
            "context": self.context,
            "recovery_suggestion": self.recovery_suggestion,
        }

        # Add cause information if available
        if self.cause:
            log_data["caused_by"] = {
                "type": self.cause.__class__.__name__,
                "message": str(self.cause),
            }

        # Log with structured format
        logger.log(
            level, f"GlueJobError occurred: {self.get_full_message()}", extra=log_data
        )

        # Log the full traceback at debug level
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Full traceback:", exc_info=True)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert error to dictionary for serialization or API responses.

        Returns:
            Dictionary representation of the error
        """
        return {
            "error_type": self.__class__.__name__,
            "message": str(self),
            "error_code": self.error_code,
            "context": self.context,
            "recovery_suggestion": self.recovery_suggestion,
            "timestamp": self.timestamp,
            "caused_by": (
                {
                    "type": self.cause.__class__.__name__,
                    "message": str(self.cause),
                }
                if self.cause
                else None
            ),
        }


class ConfigurationError(GlueJobError):
    """
    Configuration-related errors with specific recovery suggestions.

    This exception is raised when there are issues with:
    - Job parameter validation and parsing
    - Configuration file loading and validation
    - Environment variable configuration
    - Pydantic model validation failures
    - AWS service configuration issues
    """

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        expected_type: Optional[str] = None,
        recovery_suggestion: Optional[str] = None,
    ) -> None:
        """
        Initialize ConfigurationError with configuration-specific context.

        Args:
            message: Error message
            cause: Optional underlying exception
            config_key: Configuration key that caused the error
            config_value: Configuration value that caused the error
            expected_type: Expected type for the configuration value
            recovery_suggestion: Specific recovery suggestion
        """
        # Build context from configuration details
        context = {}
        if config_key:
            context["config_key"] = config_key
        if config_value is not None:
            # Mask sensitive values
            if any(
                sensitive in str(config_key).lower()
                for sensitive in ["password", "secret", "key", "token"]
            ):
                context["config_value"] = "********"
            else:
                context["config_value"] = str(config_value)
        if expected_type:
            context["expected_type"] = expected_type

        # Generate recovery suggestion if not provided
        if not recovery_suggestion:
            recovery_suggestion = self._generate_recovery_suggestion(
                config_key, expected_type
            )

        super().__init__(
            message=message,
            cause=cause,
            context=context,
            recovery_suggestion=recovery_suggestion,
            error_code="CONFIG_ERROR",
        )

    def _generate_recovery_suggestion(
        self, config_key: Optional[str], expected_type: Optional[str]
    ) -> str:
        """Generate context-specific recovery suggestions."""
        suggestions = []

        if config_key:
            suggestions.append(f"Check the '{config_key}' configuration parameter")
            suggestions.append(
                f"Verify environment variable or job parameter for '{config_key}'"
            )

        if expected_type:
            suggestions.append(f"Ensure the value is of type: {expected_type}")

        suggestions.extend(
            [
                "Review job parameters and environment variables",
                "Check configuration file syntax and format",
                "Verify AWS credentials and permissions",
            ]
        )

        return ". ".join(suggestions)


class SecretsManagerError(GlueJobError):
    """Raised for errors related to AWS Secrets Manager."""


class GlueCatalogError(GlueJobError):
    """Raised for errors related to the Glue Catalog."""


class DataValidationError(GlueJobError):
    """Raised for data validation errors."""


class MigrationError(GlueJobError):
    """Raised for migration execution errors."""

    def __init__(self, message, migration_result=None, original_exception=None):
        super().__init__(message, cause=original_exception)
        self.migration_result = migration_result
        self.original_exception = original_exception


class IcebergJobError(GlueJobError):
    """
    Iceberg-specific job errors with table and operation context.

    This exception is raised when there are issues with:
    - Iceberg table operations (create, read, write, merge)
    - Iceberg catalog connectivity and configuration
    - Schema validation and compatibility
    - Partition specification errors
    - Data format and serialization issues
    """

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        table_name: Optional[str] = None,
        operation: Optional[str] = None,
        database: Optional[str] = None,
        catalog: Optional[str] = None,
        recovery_suggestion: Optional[str] = None,
    ) -> None:
        """
        Initialize IcebergJobError with Iceberg-specific context.

        Args:
            message: Error message
            cause: Optional underlying exception
            table_name: Name of the table involved in the error
            operation: Iceberg operation that failed (create, read, write, merge)
            database: Database name
            catalog: Catalog name
            recovery_suggestion: Specific recovery suggestion
        """
        # Build context from Iceberg operation details
        context = {}
        if table_name:
            context["table_name"] = table_name
        if operation:
            context["operation"] = operation
        if database:
            context["database"] = database
        if catalog:
            context["catalog"] = catalog

        # Generate recovery suggestion if not provided
        if not recovery_suggestion:
            recovery_suggestion = self._generate_recovery_suggestion(
                operation, table_name
            )

        super().__init__(
            message=message,
            cause=cause,
            context=context,
            recovery_suggestion=recovery_suggestion,
            error_code="ICEBERG_ERROR",
        )

    def _generate_recovery_suggestion(
        self, operation: Optional[str], table_name: Optional[str]
    ) -> str:
        """Generate operation-specific recovery suggestions."""
        suggestions = []

        if operation == "create":
            suggestions.extend(
                [
                    "Verify table schema and partition specification",
                    "Check database permissions and existence",
                    "Ensure Iceberg catalog is properly configured",
                ]
            )
        elif operation == "read":
            suggestions.extend(
                [
                    "Verify table exists and is accessible",
                    "Check table permissions and catalog connectivity",
                    "Ensure table format is compatible with current Iceberg version",
                ]
            )
        elif operation == "write":
            suggestions.extend(
                [
                    "Verify table schema compatibility with data",
                    "Check write permissions and S3 access",
                    "Ensure sufficient disk space and memory",
                ]
            )
        elif operation == "merge":
            suggestions.extend(
                [
                    "Verify merge condition syntax and column references",
                    "Check that source and target schemas are compatible",
                    "Ensure merge keys exist in both source and target",
                ]
            )
        else:
            suggestions.extend(
                [
                    "Check Iceberg table configuration and permissions",
                    "Verify catalog connectivity and AWS credentials",
                ]
            )

        if table_name:
            suggestions.append(f"Verify table '{table_name}' exists and is accessible")

        suggestions.extend(
            [
                "Check Spark Iceberg extensions configuration",
                "Review AWS Glue Data Catalog permissions",
                "Verify S3 bucket access and permissions",
            ]
        )

        return ". ".join(suggestions)


class ValidationError(GlueJobError):
    """
    Parameter and data validation errors with detailed validation context.

    This exception is raised when there are issues with:
    - Pydantic model validation failures
    - Job parameter type and format validation
    - Data schema validation and compatibility
    - Business rule validation failures
    - Input data format and structure validation
    """

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        validation_rule: Optional[str] = None,
        expected_format: Optional[str] = None,
        recovery_suggestion: Optional[str] = None,
    ) -> None:
        """
        Initialize ValidationError with validation-specific context.

        Args:
            message: Error message
            cause: Optional underlying exception
            field_name: Name of the field that failed validation
            field_value: Value that failed validation
            validation_rule: Validation rule that was violated
            expected_format: Expected format for the field
            recovery_suggestion: Specific recovery suggestion
        """
        # Build context from validation details
        context = {}
        if field_name:
            context["field_name"] = field_name
        if field_value is not None:
            # Mask sensitive values
            if any(
                sensitive in str(field_name).lower()
                for sensitive in ["password", "secret", "key", "token"]
            ):
                context["field_value"] = "********"
            else:
                context["field_value"] = str(field_value)[:100]  # Truncate long values
        if validation_rule:
            context["validation_rule"] = validation_rule
        if expected_format:
            context["expected_format"] = expected_format

        # Generate recovery suggestion if not provided
        if not recovery_suggestion:
            recovery_suggestion = self._generate_recovery_suggestion(
                field_name, expected_format, validation_rule
            )

        super().__init__(
            message=message,
            cause=cause,
            context=context,
            recovery_suggestion=recovery_suggestion,
            error_code="VALIDATION_ERROR",
        )

    def _generate_recovery_suggestion(
        self,
        field_name: Optional[str],
        expected_format: Optional[str],
        validation_rule: Optional[str],
    ) -> str:
        """Generate field-specific recovery suggestions."""
        suggestions = []

        if field_name:
            suggestions.append(f"Check the '{field_name}' parameter value")

        if expected_format:
            suggestions.append(
                f"Ensure the value matches the expected format: {expected_format}"
            )

        if validation_rule:
            suggestions.append(
                f"Verify the value satisfies the rule: {validation_rule}"
            )

        suggestions.extend(
            [
                "Review parameter documentation and examples",
                "Check for typos in parameter names and values",
                "Verify data types match the expected schema",
                "Ensure required parameters are provided",
                "Check parameter value ranges and constraints",
            ]
        )

        return ". ".join(suggestions)


class SparkError(GlueJobError):
    """
    Spark-related errors with Spark context and operation details.

    This exception is raised when there are issues with:
    - SparkSession initialization and configuration
    - Spark SQL execution and query errors
    - DataFrame operations and transformations
    - Spark resource allocation and memory issues
    - Spark driver and executor connectivity
    """

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        spark_operation: Optional[str] = None,
        sql_query: Optional[str] = None,
        app_id: Optional[str] = None,
        recovery_suggestion: Optional[str] = None,
    ) -> None:
        """
        Initialize SparkError with Spark-specific context.

        Args:
            message: Error message
            cause: Optional underlying exception
            spark_operation: Spark operation that failed
            sql_query: SQL query that caused the error (truncated for logging)
            app_id: Spark application ID
            recovery_suggestion: Specific recovery suggestion
        """
        # Build context from Spark operation details
        context = {}
        if spark_operation:
            context["spark_operation"] = spark_operation
        if sql_query:
            # Truncate long queries for logging
            context["sql_query"] = (
                sql_query[:200] + "..." if len(sql_query) > 200 else sql_query
            )
        if app_id:
            context["app_id"] = app_id

        # Generate recovery suggestion if not provided
        if not recovery_suggestion:
            recovery_suggestion = self._generate_recovery_suggestion(spark_operation)

        super().__init__(
            message=message,
            cause=cause,
            context=context,
            recovery_suggestion=recovery_suggestion,
            error_code="SPARK_ERROR",
        )

    def _generate_recovery_suggestion(self, spark_operation: Optional[str]) -> str:
        """Generate Spark operation-specific recovery suggestions."""
        suggestions = []

        if spark_operation == "session_init":
            suggestions.extend(
                [
                    "Check Spark configuration parameters",
                    "Verify AWS Glue version compatibility",
                    "Ensure sufficient memory allocation",
                ]
            )
        elif spark_operation == "sql_execution":
            suggestions.extend(
                [
                    "Verify SQL syntax and table references",
                    "Check column names and data types",
                    "Ensure tables exist and are accessible",
                ]
            )
        elif spark_operation == "dataframe_operation":
            suggestions.extend(
                [
                    "Check DataFrame schema compatibility",
                    "Verify column references and transformations",
                    "Ensure sufficient memory for operations",
                ]
            )
        else:
            suggestions.extend(
                [
                    "Check Spark application logs for detailed errors",
                    "Verify Spark configuration and resources",
                ]
            )

        suggestions.extend(
            [
                "Review Spark UI for job execution details",
                "Check cluster resources and availability",
                "Verify network connectivity between driver and executors",
                "Ensure AWS Glue job has sufficient allocated resources",
            ]
        )

        return ". ".join(suggestions)


class AWSServiceError(GlueJobError):
    """
    AWS service-related errors with service context and operation details.

    This exception is raised when there are issues with:
    - AWS Secrets Manager access and secret retrieval
    - AWS Glue Data Catalog operations
    - S3 bucket access and file operations
    - IAM permissions and credential issues
    - AWS service connectivity and timeouts
    """

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        service_name: Optional[str] = None,
        operation: Optional[str] = None,
        resource_name: Optional[str] = None,
        error_code: Optional[str] = None,
        recovery_suggestion: Optional[str] = None,
    ) -> None:
        """
        Initialize AWSServiceError with AWS service-specific context.

        Args:
            message: Error message
            cause: Optional underlying exception
            service_name: AWS service name (e.g., 'secretsmanager', 's3', 'glue')
            operation: AWS operation that failed
            resource_name: AWS resource name (e.g., secret name, bucket name)
            error_code: AWS error code from the service response
            recovery_suggestion: Specific recovery suggestion
        """
        # Build context from AWS service details
        context = {}
        if service_name:
            context["service_name"] = service_name
        if operation:
            context["operation"] = operation
        if resource_name:
            context["resource_name"] = resource_name

        # Generate recovery suggestion if not provided
        if not recovery_suggestion:
            recovery_suggestion = self._generate_recovery_suggestion(
                service_name, operation, error_code
            )

        super().__init__(
            message=message,
            cause=cause,
            context=context,
            recovery_suggestion=recovery_suggestion,
            error_code=error_code or "AWS_SERVICE_ERROR",
        )

    def _generate_recovery_suggestion(
        self,
        service_name: Optional[str],
        operation: Optional[str],
        aws_error_code: Optional[str],
    ) -> str:
        """Generate AWS service-specific recovery suggestions."""
        suggestions = []

        # Service-specific suggestions
        if service_name == "secretsmanager":
            suggestions.extend(
                [
                    "Verify secret exists in AWS Secrets Manager",
                    "Check IAM permissions for secretsmanager:GetSecretValue",
                    "Ensure secret is in the correct AWS region",
                ]
            )
        elif service_name == "s3":
            suggestions.extend(
                [
                    "Verify S3 bucket exists and is accessible",
                    "Check IAM permissions for S3 operations",
                    "Ensure correct S3 bucket region configuration",
                ]
            )
        elif service_name == "glue":
            suggestions.extend(
                [
                    "Verify AWS Glue Data Catalog permissions",
                    "Check database and table existence in Glue catalog",
                    "Ensure Glue service role has necessary permissions",
                ]
            )

        # Error code-specific suggestions
        if aws_error_code:
            if aws_error_code == "AccessDenied":
                suggestions.extend(
                    [
                        "Check IAM role permissions and policies",
                        "Verify resource-based policies allow access",
                    ]
                )
            elif aws_error_code == "ResourceNotFoundException":
                suggestions.extend(
                    [
                        "Verify the resource name and region",
                        "Check if the resource was deleted or moved",
                    ]
                )
            elif aws_error_code == "ThrottlingException":
                suggestions.extend(
                    [
                        "Implement exponential backoff retry logic",
                        "Reduce request rate to the service",
                    ]
                )

        suggestions.extend(
            [
                "Verify AWS credentials are valid and not expired",
                "Check network connectivity to AWS services",
                "Review AWS service quotas and limits",
                "Ensure correct AWS region configuration",
            ]
        )

        return ". ".join(suggestions)


# Utility functions for error handling and logging


def handle_exception_with_logging(
    logger: logging.Logger,
    exception: Exception,
    operation_context: str,
    additional_context: Optional[Dict[str, Any]] = None,
) -> GlueJobError:
    """
    Handle any exception by converting it to a GlueJobError and logging it.

    Args:
        logger: Logger instance for error logging
        exception: Original exception to handle
        operation_context: Description of what operation was being performed
        additional_context: Additional context information

    Returns:
        GlueJobError instance with proper context and logging
    """
    # Determine the appropriate GlueJobError subclass
    if isinstance(exception, GlueJobError):
        # Already a GlueJobError, just log it
        exception.log_error(logger)
        return exception

    # Convert to appropriate GlueJobError subclass based on exception type
    error_message = f"Error during {operation_context}: {str(exception)}"

    if "configuration" in str(exception).lower() or "config" in str(exception).lower():
        glue_error = ConfigurationError(
            message=error_message,
            cause=exception,
            recovery_suggestion=f"Review configuration for {operation_context}",
        )
    elif "validation" in str(exception).lower() or "invalid" in str(exception).lower():
        glue_error = ValidationError(
            message=error_message,
            cause=exception,
            recovery_suggestion=f"Validate inputs for {operation_context}",
        )
    elif "spark" in str(exception).lower() or "sql" in str(exception).lower():
        glue_error = SparkError(
            message=error_message,
            cause=exception,
            recovery_suggestion=f"Check Spark configuration and query for {operation_context}",
        )
    elif any(
        aws_term in str(exception).lower()
        for aws_term in ["aws", "s3", "glue", "secret"]
    ):
        glue_error = AWSServiceError(
            message=error_message,
            cause=exception,
            recovery_suggestion=f"Check AWS service configuration and permissions for {operation_context}",
        )
    else:
        # Generic GlueJobError
        glue_error = GlueJobError(
            message=error_message,
            cause=exception,
            context=additional_context,
            recovery_suggestion=f"Review logs and configuration for {operation_context}",
        )

    # Log the error
    glue_error.log_error(logger)

    return glue_error


def create_error_context(operation: str, **kwargs: Any) -> Dict[str, Any]:
    """
    Create a standardized error context dictionary.

    Args:
        operation: Operation being performed
        **kwargs: Additional context key-value pairs

    Returns:
        Standardized error context dictionary
    """
    from datetime import datetime

    context = {"operation": operation, "timestamp": datetime.utcnow().isoformat() + "Z"}

    # Add additional context, filtering out None values
    for key, value in kwargs.items():
        if value is not None:
            context[key] = value

    return context


def log_error_with_context(
    logger: logging.Logger,
    error: Union[Exception, GlueJobError],
    context: Dict[str, Any],
    level: int = logging.ERROR,
) -> None:
    """
    Log an error with additional context information.

    Args:
        logger: Logger instance
        error: Error to log
        context: Additional context information
        level: Log level to use
    """
    if isinstance(error, GlueJobError):
        # Add context to existing GlueJobError
        error.context.update(context)
        error.log_error(logger, level)
    else:
        # Create new GlueJobError with context
        glue_error = GlueJobError(
            message=str(error),
            cause=error if isinstance(error, Exception) else None,
            context=context,
        )
        glue_error.log_error(logger, level)
