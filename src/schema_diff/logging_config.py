#!/usr/bin/env python3
"""
Logging configuration for schema-diff.

This module provides centralized logging configuration with support for
different log levels, formatters, and output destinations.
"""
from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional, Union

from .constants import (
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_LEVEL,
    LOG_FILE_BACKUP_COUNT,
    MAX_LOG_FILE_SIZE,
)


class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to log levels for console output."""

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with colors."""
        # Add color to the level name
        if record.levelname in self.COLORS:
            record.levelname = (
                f"{self.COLORS[record.levelname]}{record.levelname}{self.RESET}"
            )

        return super().format(record)


class SchemaDiffLogger:
    """Centralized logger configuration for schema-diff."""

    _configured = False
    _loggers = {}

    @classmethod
    def setup_logging(
        cls,
        level: Union[str, int] = DEFAULT_LOG_LEVEL,
        log_file: Optional[Union[str, Path]] = None,
        console_output: bool = True,
        colored_output: bool = True,
        format_string: Optional[str] = None,
        max_file_size: int = MAX_LOG_FILE_SIZE * 1024 * 1024,  # Convert MB to bytes
        backup_count: int = LOG_FILE_BACKUP_COUNT,
    ) -> None:
        """
        Configure logging for schema-diff operations.

        Args:
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Path to log file (None to disable file logging)
            console_output: Whether to output logs to console
            colored_output: Whether to use colored output for console
            format_string: Custom format string for log messages
            max_file_size: Maximum size of log file before rotation (bytes)
            backup_count: Number of backup log files to keep
        """
        if cls._configured:
            return

        # Convert string level to logging constant
        if isinstance(level, str):
            level = getattr(logging, level.upper())

        # Use default format if not provided
        if format_string is None:
            format_string = DEFAULT_LOG_FORMAT

        # Get root logger for schema_diff
        root_logger = logging.getLogger("schema_diff")
        root_logger.setLevel(level)

        # Clear any existing handlers
        root_logger.handlers.clear()

        # Console handler
        if console_output:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setLevel(level)

            if colored_output and sys.stderr.isatty():
                console_formatter = ColoredFormatter(format_string)
            else:
                console_formatter = logging.Formatter(format_string)

            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)

        # File handler with rotation
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setLevel(level)

            file_formatter = logging.Formatter(format_string)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)

        # Prevent propagation to avoid duplicate messages
        root_logger.propagate = False

        cls._configured = True

        # Log the configuration
        logger = cls.get_logger("logging_config")
        logger.info(
            "Logging configured - Level: %s, Console: %s, File: %s",
            logging.getLevelName(level),
            console_output,
            log_file,
        )

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get a logger instance for the given module name.

        Args:
            name: Logger name (typically __name__ from the calling module)

        Returns:
            Configured logger instance
        """
        # Ensure logging is configured
        if not cls._configured:
            cls.setup_logging()

        # Create logger with schema_diff prefix
        if not name.startswith("schema_diff"):
            logger_name = f"schema_diff.{name}"
        else:
            logger_name = name

        if logger_name not in cls._loggers:
            logger = logging.getLogger(logger_name)
            cls._loggers[logger_name] = logger

        return cls._loggers[logger_name]

    @classmethod
    def set_level(cls, level: Union[str, int]) -> None:
        """
        Change the logging level for all schema-diff loggers.

        Args:
            level: New logging level
        """
        if isinstance(level, str):
            level = getattr(logging, level.upper())

        root_logger = logging.getLogger("schema_diff")
        root_logger.setLevel(level)

        # Update all handlers
        for handler in root_logger.handlers:
            handler.setLevel(level)

    @classmethod
    def add_file_handler(
        cls,
        log_file: Union[str, Path],
        level: Optional[Union[str, int]] = None,
        format_string: Optional[str] = None,
    ) -> None:
        """
        Add an additional file handler to the logger.

        Args:
            log_file: Path to the log file
            level: Logging level for this handler (uses root level if None)
            format_string: Custom format for this handler
        """
        root_logger = logging.getLogger("schema_diff")

        if level is None:
            level = root_logger.level
        elif isinstance(level, str):
            level = getattr(logging, level.upper())

        if format_string is None:
            format_string = DEFAULT_LOG_FORMAT

        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=MAX_LOG_FILE_SIZE * 1024 * 1024,
            backupCount=LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(format_string))

        root_logger.addHandler(file_handler)

    @classmethod
    def disable_logging(cls) -> None:
        """Disable all logging output."""
        root_logger = logging.getLogger("schema_diff")
        root_logger.disabled = True

    @classmethod
    def enable_logging(cls) -> None:
        """Re-enable logging output."""
        root_logger = logging.getLogger("schema_diff")
        root_logger.disabled = False


# Convenience functions for common logging operations
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the given module.

    This is the primary function that modules should use to get their logger.

    Args:
        name: Module name (typically __name__)

    Returns:
        Configured logger instance

    Example:
        logger = get_logger(__name__)
        logger.info("Processing started")
    """
    return SchemaDiffLogger.get_logger(name)


def setup_logging(
    level: Union[str, int] = DEFAULT_LOG_LEVEL,
    log_file: Optional[Union[str, Path]] = None,
    **kwargs,
) -> None:
    """
    Setup logging configuration.

    Args:
        level: Logging level
        log_file: Optional log file path
        **kwargs: Additional arguments passed to SchemaDiffLogger.setup_logging
    """
    SchemaDiffLogger.setup_logging(level=level, log_file=log_file, **kwargs)


def log_function_call(func):
    """
    Decorator to log function entry and exit.

    This decorator automatically logs when a function is called and when it returns,
    including the function arguments and return value (for debugging).

    Args:
        func: Function to decorate

    Returns:
        Decorated function

    Example:
        @log_function_call
        def process_schema(schema_path: str) -> dict:
            return {"status": "processed"}
    """
    import functools

    logger = get_logger(func.__module__)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Log function entry
        logger.debug("Calling %s with args=%s, kwargs=%s", func.__name__, args, kwargs)

        try:
            result = func(*args, **kwargs)
            logger.debug("Function %s returned: %s", func.__name__, result)
            return result
        except Exception as e:
            logger.error("Function %s raised exception: %s", func.__name__, e)
            raise

    return wrapper


def log_performance(func):
    """
    Decorator to log function performance metrics.

    This decorator logs the execution time of functions, which is useful
    for identifying performance bottlenecks.

    Args:
        func: Function to decorate

    Returns:
        Decorated function
    """
    import functools
    import time

    logger = get_logger(func.__module__)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(
                "Function %s executed in %.3f seconds", func.__name__, execution_time
            )
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                "Function %s failed after %.3f seconds: %s",
                func.__name__,
                execution_time,
                e,
            )
            raise

    return wrapper


# Export public interface
__all__ = [
    "SchemaDiffLogger",
    "ColoredFormatter",
    "get_logger",
    "setup_logging",
    "log_function_call",
    "log_performance",
]
