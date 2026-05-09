"""
Spark logging fix module to prevent py4j logging errors during test cleanup.
"""

import atexit
import logging
import sys


class SafeStreamHandler(logging.StreamHandler):
    """A logging handler that safely handles closed streams."""

    def emit(self, record):
        """Emit a record, safely handling closed streams."""
        try:
            # Check if the stream is closed
            if hasattr(self.stream, "closed") and self.stream.closed:
                return
            super().emit(record)
        except (ValueError, OSError):
            # Stream is closed or unavailable, silently ignore
            pass


def configure_safe_logging():
    """Configure logging to prevent errors when streams are closed during shutdown."""

    # Get the py4j logger specifically
    py4j_logger = logging.getLogger("py4j")

    # Remove existing handlers that might cause issues
    for handler in py4j_logger.handlers[:]:
        py4j_logger.removeHandler(handler)

    # Add a safe handler that won't fail on closed streams
    safe_handler = SafeStreamHandler(sys.stderr)
    safe_handler.setLevel(logging.ERROR)  # Only show errors, not info messages
    safe_handler.setFormatter(
        logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    )

    py4j_logger.addHandler(safe_handler)
    py4j_logger.setLevel(logging.ERROR)  # Set logger level to ERROR
    py4j_logger.propagate = False  # Don't propagate to root logger

    # Also configure the root logger to be more restrictive
    root_logger = logging.getLogger()

    # Replace any existing StreamHandlers with safe ones
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, SafeStreamHandler
        ):
            root_logger.removeHandler(handler)
            safe_replacement = SafeStreamHandler(handler.stream)
            safe_replacement.setLevel(handler.level)
            safe_replacement.setFormatter(handler.formatter)
            root_logger.addHandler(safe_replacement)


def suppress_py4j_logging():
    """Completely suppress py4j logging to prevent any shutdown errors."""

    # Get py4j loggers
    py4j_logger = logging.getLogger("py4j")
    py4j_gateway_logger = logging.getLogger("py4j.java_gateway")
    py4j_clientserver_logger = logging.getLogger("py4j.clientserver")

    # Set them to CRITICAL level so they don't log anything
    for logger in [py4j_logger, py4j_gateway_logger, py4j_clientserver_logger]:
        logger.setLevel(logging.CRITICAL)
        logger.disabled = True

        # Remove all handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Don't propagate to parent loggers
        logger.propagate = False


def setup_spark_logging_fix():
    """Set up comprehensive logging fix for Spark/py4j issues."""

    # Configure safe logging first
    configure_safe_logging()

    # Then suppress py4j logging entirely
    suppress_py4j_logging()

    # Register cleanup function to run at exit
    atexit.register(cleanup_logging)


def cleanup_logging():
    """Clean up logging configuration at exit."""
    try:
        # Disable all py4j loggers
        for logger_name in ["py4j", "py4j.java_gateway", "py4j.clientserver"]:
            logger = logging.getLogger(logger_name)
            logger.disabled = True
            for handler in logger.handlers[:]:
                try:
                    handler.close()
                except Exception:
                    pass
                logger.removeHandler(handler)
    except Exception:
        # Ignore any errors during cleanup
        pass


# Apply the fix immediately when this module is imported
setup_spark_logging_fix()
