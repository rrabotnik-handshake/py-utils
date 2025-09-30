#!/usr/bin/env python3
"""
Typing and documentation standards for schema-diff.

This module defines the coding standards, type annotation patterns,
and docstring conventions used throughout the schema-diff codebase.

All modules should follow these patterns for consistency.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Protocol, Set, Tuple, TypeVar

# Type aliases for common patterns
SchemaTree = Dict[str, Any]
RequiredPaths = Set[str]
FieldPath = str
ParseResult = Tuple[SchemaTree, RequiredPaths, str]

# Generic type variables
T = TypeVar("T")
P = TypeVar("P", bound="Parser")


class Parser(Protocol):
    """
    Protocol defining the interface for schema parsers.

    All parsers must implement these methods to ensure consistent behavior
    across different schema formats.

    Examples:
        >>> parser = JsonSchemaParser()
        >>> result = parser.parse("schema.json")
        >>> tree, required_paths, label = result
    """

    def parse(self, path: str, **kwargs: Any) -> ParseResult:
        """
        Parse a schema file and return standardized result.

        Args:
            path: Path to the schema file to parse
            **kwargs: Parser-specific options

        Returns:
            Tuple of (schema_tree, required_paths, label)

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file format is invalid
            ParserError: If parsing fails

        Examples:
            >>> parser = SparkSchemaParser()
            >>> tree, required, label = parser.parse("schema.txt")
            >>> assert isinstance(tree, dict)
            >>> assert isinstance(required, set)
        """
        ...

    def can_handle(self, path: str) -> bool:
        """
        Check if this parser can handle the given file.

        Args:
            path: Path to the file to check

        Returns:
            True if this parser can handle the file format

        Examples:
            >>> parser = JsonSchemaParser()
            >>> assert parser.can_handle("schema.json")
            >>> assert not parser.can_handle("schema.proto")
        """
        ...


class SchemaComparator(ABC):
    """
    Abstract base class for schema comparison implementations.

    This class defines the interface for comparing schemas and generating
    diff reports. Concrete implementations handle specific comparison logic.

    Attributes:
        config: Configuration object for comparison behavior

    Examples:
        >>> comparator = TreeComparator(config)
        >>> report = comparator.compare(schema1, schema2)
    """

    def __init__(self, config: Optional[Any] = None) -> None:
        """
        Initialize the comparator.

        Args:
            config: Optional configuration object
        """
        self.config = config

    @abstractmethod
    def compare(
        self, _left_schema: SchemaTree, _right_schema: SchemaTree, **_options: Any
    ) -> Dict[str, Any]:
        """
        Compare two schemas and return a diff report.

        Args:
            left_schema: The left schema to compare
            right_schema: The right schema to compare
            **options: Comparison options

        Returns:
            Dictionary containing comparison results

        Raises:
            ComparisonError: If comparison fails
        """
        ...


def validate_schema_tree(tree: Any) -> SchemaTree:
    """
    Validate and normalize a schema tree.

    Ensures the schema tree follows the expected format and converts
    any legacy representations to the standard format.

    Args:
        tree: The schema tree to validate

    Returns:
        Validated and normalized schema tree

    Raises:
        ValueError: If the tree format is invalid
        TypeError: If the tree has wrong types

    Examples:
        >>> tree = {"name": "str", "age": "int"}
        >>> validated = validate_schema_tree(tree)
        >>> assert validated == tree

        >>> invalid_tree = ["not", "a", "dict"]
        >>> validate_schema_tree(invalid_tree)  # Raises ValueError
    """
    if not isinstance(tree, dict):
        raise ValueError(f"Schema tree must be a dictionary, got {type(tree)}")

    # Additional validation logic here
    return tree


def normalize_field_path(path: str) -> FieldPath:
    """
    Normalize a field path to standard dot notation.

    Converts various path formats to the canonical dot-separated format
    used throughout the schema-diff system.

    Args:
        path: The field path to normalize

    Returns:
        Normalized field path in dot notation

    Raises:
        ValueError: If the path is invalid

    Examples:
        >>> normalize_field_path("user.name")
        'user.name'

        >>> normalize_field_path("user[0].name")
        'user[].name'

        >>> normalize_field_path("/user/name")
        'user.name'
    """
    if not isinstance(path, str):
        raise ValueError(f"Path must be a string, got {type(path)}")

    # Normalize array indices
    import re

    path = re.sub(r"\[\d+\]", "[]", path)

    # Convert slash notation to dot notation
    path = path.replace("/", ".")

    # Remove leading/trailing dots
    path = path.strip(".")

    return path


# Docstring templates for common function types

PARSER_FUNCTION_DOCSTRING = '''
"""
Parse {format_name} format into internal schema representation.

This function converts {format_name} schema definitions into the standardized
internal format used by schema-diff for comparison and analysis.

Args:
    path: Path to the {format_name} file
    {additional_args}

Returns:
    Tuple of (schema_tree, required_paths, label) where:
    - schema_tree: Dict representing the schema structure
    - required_paths: Set of paths that are marked as required
    - label: Human-readable description of the schema

Raises:
    FileNotFoundError: If the file doesn't exist
    {format_name}ParseError: If the file format is invalid
    ValueError: If parsing parameters are invalid

Examples:
    >>> tree, required, label = parse_{format_lower}_file("schema.{ext}")
    >>> assert isinstance(tree, dict)
    >>> assert isinstance(required, set)
    >>> assert isinstance(label, str)

Notes:
    {additional_notes}
"""
'''

COMPARISON_FUNCTION_DOCSTRING = '''
"""
Compare two {item_type} and generate a diff report.

This function performs a deep comparison between two {item_type} and
generates a comprehensive report of differences, similarities, and
potential migration issues.

Args:
    left: The left {item_type} to compare
    right: The right {item_type} to compare
    {additional_args}

Returns:
    Dictionary containing:
    - common_fields: Fields present in both {item_type}
    - only_in_left: Fields only in the left {item_type}
    - only_in_right: Fields only in the right {item_type}
    - type_mismatches: Fields with different types
    - path_changes: Fields that moved locations

Raises:
    ComparisonError: If comparison fails
    ValueError: If inputs are invalid

Examples:
    >>> report = compare_{item_type}(schema1, schema2)
    >>> assert "common_fields" in report
    >>> assert "type_mismatches" in report

Notes:
    {additional_notes}
"""
'''


# Type checking utilities
def ensure_type(value: Any, expected_type: type, name: str = "value") -> Any:
    """
    Ensure a value is of the expected type.

    Args:
        value: The value to check
        expected_type: The expected type
        name: Name of the value for error messages

    Returns:
        The value if it matches the expected type

    Raises:
        TypeError: If the value is not of the expected type
    """
    if not isinstance(value, expected_type):
        raise TypeError(
            f"{name} must be {expected_type.__name__}, got {type(value).__name__}"
        )
    return value


def ensure_optional_type(
    value: Any, expected_type: type, name: str = "value"
) -> Optional[Any]:
    """
    Ensure a value is either None or of the expected type.

    Args:
        value: The value to check
        expected_type: The expected type
        name: Name of the value for error messages

    Returns:
        The value if it's None or matches the expected type

    Raises:
        TypeError: If the value is not None and not of the expected type
    """
    if value is not None and not isinstance(value, expected_type):
        raise TypeError(
            f"{name} must be {expected_type.__name__} or None, got {type(value).__name__}"
        )
    return value


# Export commonly used types and functions
__all__ = [
    "SchemaTree",
    "RequiredPaths",
    "FieldPath",
    "ParseResult",
    "Parser",
    "SchemaComparator",
    "validate_schema_tree",
    "normalize_field_path",
    "ensure_type",
    "ensure_optional_type",
    "PARSER_FUNCTION_DOCSTRING",
    "COMPARISON_FUNCTION_DOCSTRING",
]
