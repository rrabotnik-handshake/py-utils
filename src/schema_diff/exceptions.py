#!/usr/bin/env python3
"""
Exception hierarchy for schema-diff operations.

This module defines a comprehensive exception hierarchy that provides
structured error handling throughout the schema-diff system.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


class SchemaDiffError(Exception):
    """
    Base exception for all schema-diff operations.

    All schema-diff specific exceptions should inherit from this class
    to provide a consistent error handling interface.
    """

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.cause = cause

    def __str__(self) -> str:
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({detail_str})"
        return self.message


# ──────────────────────────────────────────────────────────────────────────────
# Parsing and Schema Loading Errors
# ──────────────────────────────────────────────────────────────────────────────


class ParseError(SchemaDiffError):
    """Raised when parsing a schema or data file fails."""

    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
        parser_type: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if file_path:
            details["file_path"] = file_path
        if line_number:
            details["line_number"] = str(line_number)
        if parser_type:
            details["parser_type"] = parser_type

        super().__init__(message, details, cause)
        self.file_path = file_path
        self.line_number = line_number
        self.parser_type = parser_type


class SchemaFormatError(ParseError):
    """Raised when a schema file has an invalid or unsupported format."""

    pass


class DataFormatError(ParseError):
    """Raised when a data file has an invalid or unsupported format."""

    pass


class ProtobufError(ParseError):
    """Raised when protobuf parsing fails."""

    def __init__(
        self,
        message: str,
        proto_file: Optional[str] = None,
        message_name: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if proto_file:
            details["proto_file"] = proto_file
        if message_name:
            details["message_name"] = message_name

        super().__init__(message, proto_file, None, "protobuf", cause)
        self.proto_file = proto_file
        self.message_name = message_name


# ──────────────────────────────────────────────────────────────────────────────
# Validation Errors
# ──────────────────────────────────────────────────────────────────────────────


class ValidationError(SchemaDiffError):
    """Raised when validation of schemas or data fails."""

    def __init__(
        self,
        message: str,
        validation_type: Optional[str] = None,
        field_path: Optional[str] = None,
        expected_value: Optional[Any] = None,
        actual_value: Optional[Any] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if validation_type:
            details["validation_type"] = validation_type
        if field_path:
            details["field_path"] = field_path
        if expected_value is not None:
            details["expected"] = expected_value
        if actual_value is not None:
            details["actual"] = actual_value

        super().__init__(message, details, cause)
        self.validation_type = validation_type
        self.field_path = field_path
        self.expected_value = expected_value
        self.actual_value = actual_value


class SchemaValidationError(ValidationError):
    """Raised when schema validation fails."""

    pass


class TypeMismatchError(ValidationError):
    """Raised when there's a type mismatch between schemas."""

    def __init__(
        self,
        message: str,
        field_path: str,
        left_type: str,
        right_type: str,
        cause: Optional[Exception] = None,
    ):
        super().__init__(
            message, "type_mismatch", field_path, left_type, right_type, cause
        )
        self.left_type = left_type
        self.right_type = right_type


# ──────────────────────────────────────────────────────────────────────────────
# Configuration and Setup Errors
# ──────────────────────────────────────────────────────────────────────────────


class ConfigurationError(SchemaDiffError):
    """Raised when there's an issue with configuration."""

    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if config_key:
            details["config_key"] = config_key
        if config_value is not None:
            details["config_value"] = config_value

        super().__init__(message, details, cause)
        self.config_key = config_key
        self.config_value = config_value


class DependencyError(SchemaDiffError):
    """Raised when a required dependency is missing or incompatible."""

    def __init__(
        self,
        message: str,
        dependency_name: Optional[str] = None,
        required_version: Optional[str] = None,
        installed_version: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if dependency_name:
            details["dependency"] = dependency_name
        if required_version:
            details["required_version"] = required_version
        if installed_version:
            details["installed_version"] = installed_version

        super().__init__(message, details, cause)
        self.dependency_name = dependency_name
        self.required_version = required_version
        self.installed_version = installed_version


# ──────────────────────────────────────────────────────────────────────────────
# I/O and File System Errors
# ──────────────────────────────────────────────────────────────────────────────


class FileOperationError(SchemaDiffError):
    """Raised when file operations fail."""

    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        operation: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if file_path:
            details["file_path"] = file_path
        if operation:
            details["operation"] = operation

        super().__init__(message, details, cause)
        self.file_path = file_path
        self.operation = operation


class GCSError(FileOperationError):
    """Raised when Google Cloud Storage operations fail."""

    def __init__(
        self,
        message: str,
        bucket_name: Optional[str] = None,
        object_name: Optional[str] = None,
        operation: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if bucket_name:
            details["bucket"] = bucket_name
        if object_name:
            details["object"] = object_name

        super().__init__(message, None, operation, cause)
        self.details.update(details)
        self.bucket_name = bucket_name
        self.object_name = object_name


# ──────────────────────────────────────────────────────────────────────────────
# BigQuery and Database Errors
# ──────────────────────────────────────────────────────────────────────────────


class BigQueryError(SchemaDiffError):
    """Raised when BigQuery operations fail."""

    def __init__(
        self,
        message: str,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        operation: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if project_id:
            details["project_id"] = project_id
        if dataset_id:
            details["dataset_id"] = dataset_id
        if table_id:
            details["table_id"] = table_id
        if operation:
            details["operation"] = operation

        super().__init__(message, details, cause)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.operation = operation


class SQLError(SchemaDiffError):
    """Raised when SQL operations fail."""

    def __init__(
        self,
        message: str,
        sql_statement: Optional[str] = None,
        error_position: Optional[int] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if sql_statement:
            details["sql"] = (
                sql_statement[:200] + "..."
                if len(sql_statement) > 200
                else sql_statement
            )
        if error_position:
            details["position"] = str(error_position)

        super().__init__(message, details, cause)
        self.sql_statement = sql_statement
        self.error_position = error_position


# ──────────────────────────────────────────────────────────────────────────────
# Schema Generation and Comparison Errors
# ──────────────────────────────────────────────────────────────────────────────


class SchemaGenerationError(SchemaDiffError):
    """Raised when schema generation fails."""

    def __init__(
        self,
        message: str,
        target_format: Optional[str] = None,
        source_data: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if target_format:
            details["target_format"] = target_format
        if source_data:
            details["source_data"] = source_data

        super().__init__(message, details, cause)
        self.target_format = target_format
        self.source_data = source_data


class ComparisonError(SchemaDiffError):
    """Raised when schema comparison fails."""

    def __init__(
        self,
        message: str,
        left_schema: Optional[str] = None,
        right_schema: Optional[str] = None,
        comparison_type: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if left_schema:
            details["left_schema"] = left_schema
        if right_schema:
            details["right_schema"] = right_schema
        if comparison_type:
            details["comparison_type"] = comparison_type

        super().__init__(message, details, cause)
        self.left_schema = left_schema
        self.right_schema = right_schema
        self.comparison_type = comparison_type


# ──────────────────────────────────────────────────────────────────────────────
# CLI and User Interface Errors
# ──────────────────────────────────────────────────────────────────────────────


class CLIError(SchemaDiffError):
    """Raised when CLI operations fail."""

    def __init__(
        self,
        message: str,
        command: Optional[str] = None,
        arguments: Optional[List[str]] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if command:
            details["command"] = command
        if arguments:
            details["arguments"] = (
                " ".join(arguments) if isinstance(arguments, list) else str(arguments)
            )

        super().__init__(message, details, cause)
        self.command = command
        self.arguments = arguments


class ArgumentError(CLIError):
    """Raised when command-line arguments are invalid."""

    def __init__(
        self,
        message: str,
        argument_name: Optional[str] = None,
        argument_value: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        details = {}
        if argument_name:
            details["argument"] = argument_name
        if argument_value:
            details["value"] = argument_value

        super().__init__(message, None, None, cause)
        self.details.update(details)
        self.argument_name = argument_name
        self.argument_value = argument_value


# ──────────────────────────────────────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────────────────────────────────────


def wrap_exception(
    exc: Exception,
    message: Optional[str] = None,
    exception_class: type[SchemaDiffError] = SchemaDiffError,
    **kwargs,
) -> SchemaDiffError:
    """
    Wrap a generic exception in a schema-diff specific exception.

    Args:
        exc: The original exception to wrap
        message: Optional custom message (uses original message if not provided)
        exception_class: The schema-diff exception class to use
        **kwargs: Additional arguments for the exception class

    Returns:
        A schema-diff specific exception wrapping the original
    """
    if isinstance(exc, SchemaDiffError):
        return exc

    error_message = message or str(exc)
    return exception_class(error_message, cause=exc, **kwargs)


def handle_known_exceptions(func):
    """
    Decorator to convert known exceptions to schema-diff exceptions.

    This decorator catches common exceptions and converts them to
    appropriate schema-diff exceptions with better error messages.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            raise FileOperationError(
                f"File not found: {e.filename}",
                file_path=e.filename,
                operation="read",
                cause=e,
            ) from e
        except PermissionError as e:
            raise FileOperationError(
                f"Permission denied: {e.filename}",
                file_path=e.filename,
                operation="access",
                cause=e,
            ) from e
        except json.JSONDecodeError as e:
            raise DataFormatError(
                f"Invalid JSON: {e.msg}", line_number=e.lineno, cause=e
            ) from e
        except ImportError as e:
            raise DependencyError(
                f"Missing dependency: {e.name}", dependency_name=e.name, cause=e
            ) from e
        except Exception as e:
            # Re-raise schema-diff exceptions as-is
            if isinstance(e, SchemaDiffError):
                raise
            # Wrap other exceptions
            raise SchemaDiffError(
                f"Unexpected error in {func.__name__}: {str(e)}", cause=e
            ) from e

    return wrapper


# ──────────────────────────────────────────────────────────────────────────────
# Export all exception classes
# ──────────────────────────────────────────────────────────────────────────────

__all__ = [
    # Base exception
    "SchemaDiffError",
    # Parsing and Schema Loading
    "ParseError",
    "SchemaFormatError",
    "DataFormatError",
    "ProtobufError",
    # Validation
    "ValidationError",
    "SchemaValidationError",
    "TypeMismatchError",
    # Configuration and Setup
    "ConfigurationError",
    "DependencyError",
    # I/O and File System
    "FileOperationError",
    "GCSError",
    # BigQuery and Database
    "BigQueryError",
    "SQLError",
    # Schema Generation and Comparison
    "SchemaGenerationError",
    "ComparisonError",
    # CLI and User Interface
    "CLIError",
    "ArgumentError",
    # Utility functions
    "wrap_exception",
    "handle_known_exceptions",
]
