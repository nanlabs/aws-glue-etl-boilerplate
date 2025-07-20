import json
import logging


def get_logger(name, level=logging.DEBUG):
    """
    Creates and configures a logger with the given name and level.

    Args:
        name (str): Name of the logger
        level (int): Logging level (default: DEBUG)

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Configure a handler if none exists
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def mask_sensitive_value(key, value):
    """
    Mask sensitive values in logs

    Args:
        key (str): The configuration key
        value (any): The configuration value

    Returns:
        any: Masked value if sensitive, original value otherwise
    """
    sensitive_keys = ["password", "secret", "token", "key"]
    if (
        any(sensitive_word in key.lower() for sensitive_word in sensitive_keys)
        and value
    ):
        return "********"
    return value


def log_configuration(logger, config):
    """
    Logs configuration details with sensitive information masked

    Args:
        logger (logging.Logger): Logger instance
        config (object): Configuration object containing AWS and S3 settings
    """
    # Log AWS client configuration
    if hasattr(config, "aws_client_vars"):
        aws_config = {
            k: mask_sensitive_value(k, v) for k, v in config.aws_client_vars.items()
        }
        logger.info(f"AWS Configuration: {json.dumps(aws_config, indent=2)}")

    # Log S3 configuration
    if hasattr(config, "s3_vars"):
        s3_config = {"bucket_name": config.s3_vars.get("bucket_name", "N/A")}
        logger.info(f"S3 Configuration: {json.dumps(s3_config, indent=2)}")
