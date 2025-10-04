#!/usr/bin/env python3
"""
Unified internal schema representation for schema-diff.

This module defines the canonical data structures used throughout the schema-diff
system to represent schemas, fields, types, and constraints in a consistent way.

Using Pydantic for validation, serialization, and type safety.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

from pydantic import BaseModel, Field


class ScalarType(str, Enum):
    """Canonical scalar types used throughout schema-diff."""

    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    STR = "str"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    ANY = "any"
    MISSING = "missing"
    OBJECT = "object"  # For unstructured objects/maps


class FieldConstraint(str, Enum):
    """Field-level constraints."""

    REQUIRED = "required"
    NULLABLE = "nullable"
    UNIQUE = "unique"
    PRIMARY_KEY = "primary_key"


class SchemaType(BaseModel):
    """Base class for all schema types."""

    class Config:
        # Allow arbitrary types for flexibility
        arbitrary_types_allowed = True
        # Use enum values for serialization
        use_enum_values = True


class ScalarSchemaType(SchemaType):
    """Represents a scalar type (int, str, bool, etc.)."""

    type: ScalarType
    constraints: Set[FieldConstraint] = Field(default_factory=set)

    def __str__(self) -> str:
        return self.type if isinstance(self.type, str) else self.type.value


class ArraySchemaType(SchemaType):
    """Represents an array type with element type."""

    element_type: Union["ScalarSchemaType", "ObjectSchemaType", "UnionSchemaType"]
    constraints: Set[FieldConstraint] = Field(default_factory=set)

    def __str__(self) -> str:
        return f"[{self.element_type}]"


class ObjectSchemaType(SchemaType):
    """Represents an object/struct type with named fields."""

    fields: Dict[
        str, Union["ScalarSchemaType", "ArraySchemaType", "UnionSchemaType"]
    ] = Field(default_factory=dict)
    constraints: Set[FieldConstraint] = Field(default_factory=set)

    def __str__(self) -> str:
        if not self.fields:
            return "object"
        field_strs = [f"{k}: {v}" for k, v in self.fields.items()]
        return f"{{{', '.join(field_strs)}}}"


class UnionSchemaType(SchemaType):
    """Represents a union of multiple types."""

    types: List[Union["ScalarSchemaType", "ArraySchemaType", "ObjectSchemaType"]]
    constraints: Set[FieldConstraint] = Field(default_factory=set)

    def model_post_init(self, _context: Any) -> None:
        """Validate that union has at least 2 types."""
        if len(self.types) < 2:
            raise ValueError("Union must have at least 2 types")

    def __str__(self) -> str:
        type_strs = sorted([str(t) for t in self.types])
        return f"union({' | '.join(type_strs)})"


class SchemaField(BaseModel):
    """Represents a field in a schema with metadata."""

    name: str
    type: Union[ScalarSchemaType, ArraySchemaType, ObjectSchemaType, UnionSchemaType]
    path: str  # Dotted path (e.g., "user.profile.name")
    constraints: Set[FieldConstraint] = Field(default_factory=set)
    source_info: Optional[Dict[str, Any]] = None  # Parser-specific metadata

    def is_required(self) -> bool:
        """Check if field is required."""
        return FieldConstraint.REQUIRED in self.constraints

    def is_nullable(self) -> bool:
        """Check if field is nullable."""
        return FieldConstraint.NULLABLE in self.constraints


class Schema(BaseModel):
    """Complete schema representation."""

    name: Optional[str] = None
    fields: List[SchemaField] = Field(default_factory=list)
    source_type: Optional[str] = None  # "spark", "sql", "json_schema", etc.
    source_path: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def get_field_by_path(self, path: str) -> Optional[SchemaField]:
        """Get field by dotted path."""
        for field in self.fields:
            if field.path == path:
                return field
        return None

    def get_required_paths(self) -> Set[str]:
        """Get all required field paths."""
        return {field.path for field in self.fields if field.is_required()}

    def get_field_paths(self) -> Set[str]:
        """Get all field paths."""
        return {field.path for field in self.fields}

    def to_legacy_format(self) -> tuple[Dict[str, Any], Set[str]]:
        """Convert to legacy (tree, required_paths) format for backward compatibility."""
        tree: Dict[str, Any] = {}
        required_paths = set()

        for field in self.fields:
            # Build nested dict structure from dotted path
            path_parts = field.path.split(".")
            current = tree

            for part in path_parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]

            # Set the final value
            final_key = path_parts[-1]
            current[final_key] = self._type_to_legacy(field.type)

            # Track required paths
            if field.is_required():
                required_paths.add(field.path)

        return tree, required_paths

    def _type_to_legacy(
        self,
        schema_type: Union[
            ScalarSchemaType, ArraySchemaType, ObjectSchemaType, UnionSchemaType
        ],
    ) -> Any:
        """Convert SchemaType to legacy format."""
        if isinstance(schema_type, ScalarSchemaType):
            return schema_type.type.value
        elif isinstance(schema_type, ArraySchemaType):
            return [self._type_to_legacy(schema_type.element_type)]
        elif isinstance(schema_type, ObjectSchemaType):
            return {k: self._type_to_legacy(v) for k, v in schema_type.fields.items()}
        elif isinstance(schema_type, UnionSchemaType):
            type_strs = sorted([str(t) for t in schema_type.types])
            return f"union({' | '.join(type_strs)})"
        else:
            return "any"


# Convenience functions for creating schema types
def scalar_type(type_name: str, **kwargs) -> ScalarSchemaType:
    """Create a scalar type."""
    try:
        scalar_enum = ScalarType(type_name)
    except ValueError:
        # Unknown type, default to ANY
        scalar_enum = ScalarType.ANY
    return ScalarSchemaType(type=scalar_enum, **kwargs)


def array_type(element_type, **kwargs) -> ArraySchemaType:
    """Create an array type."""
    return ArraySchemaType(element_type=element_type, **kwargs)


def object_type(fields: Dict[str, Any], **kwargs) -> ObjectSchemaType:
    """Create an object type."""
    return ObjectSchemaType(fields=fields, **kwargs)


def union_type(types: List[Any], **kwargs) -> UnionSchemaType:
    """Create a union type."""
    return UnionSchemaType(types=types, **kwargs)


def from_legacy_tree(
    tree: Optional[Dict[str, Any]] = None,
    required_paths: Optional[Set[str]] = None,
    source_type: Optional[str] = None,
) -> Schema:
    """Convert legacy (tree, required_paths) format to unified Schema."""
    if tree is None:
        tree = {}
    if required_paths is None:
        required_paths = set()

    fields: List[SchemaField] = []
    _extract_fields_from_tree(tree, "", fields, required_paths)

    return Schema(
        fields=fields,
        source_type=source_type,
    )


def _extract_fields_from_tree(
    obj: Any, path_prefix: str, fields: List[SchemaField], required_paths: Set[str]
) -> None:
    """Recursively extract fields from legacy tree structure."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            field_path = f"{path_prefix}.{key}" if path_prefix else key
            if isinstance(value, dict):
                # Nested object
                _extract_fields_from_tree(value, field_path, fields, required_paths)
            else:
                # Leaf field
                field_type = _legacy_to_schema_type(value)
                constraints = set()
                if field_path in required_paths:
                    constraints.add(FieldConstraint.REQUIRED)

                fields.append(
                    SchemaField(
                        name=key,
                        path=field_path,
                        type=field_type,
                        constraints=constraints,
                    )
                )


def _legacy_to_schema_type(
    value: Any,
) -> Union[ScalarSchemaType, ArraySchemaType, ObjectSchemaType, UnionSchemaType]:
    """Convert legacy value to SchemaType."""
    if isinstance(value, str):
        if value.startswith("union("):
            # Parse union type
            union_content = value[6:-1]  # Remove "union(" and ")"
            type_names = [t.strip() for t in union_content.split("|")]
            types = [scalar_type(name) for name in type_names]
            return union_type(types)  # type: ignore
        else:
            # Scalar type
            return scalar_type(value)
    elif isinstance(value, list) and len(value) == 1:
        # Array type
        element_type = _legacy_to_schema_type(value[0])
        return array_type(element_type)  # type: ignore
    elif isinstance(value, dict):
        # Object type
        fields = {k: _legacy_to_schema_type(v) for k, v in value.items()}
        return object_type(fields)
    else:
        # Fallback
        return scalar_type("any")


def to_legacy_tree(schema: Schema) -> tuple[Dict[str, Any], Set[str]]:
    """Convert unified Schema back to legacy (tree, required_paths) format."""
    tree: Dict[str, Any] = {}
    required_paths: Set[str] = set()

    for field in schema.fields:
        _add_field_to_legacy_tree(field, tree, required_paths)

    return tree, required_paths


def _add_field_to_legacy_tree(
    field: SchemaField, tree: Dict[str, Any], required_paths: Set[str]
) -> None:
    """Add a field to the legacy tree structure."""
    # Handle nested paths (e.g., "user.profile.name")
    path_parts = field.path.split(".")
    current = tree

    # Navigate/create nested structure
    for part in path_parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]

    # Set the final field
    final_key = path_parts[-1]
    current[final_key] = _schema_type_to_legacy(field.type)

    # Track required paths
    if FieldConstraint.REQUIRED in field.constraints:
        required_paths.add(field.path)


def _schema_type_to_legacy(schema_type: SchemaType) -> Any:
    """Convert SchemaType back to legacy format."""
    if isinstance(schema_type, ScalarSchemaType):
        return (
            schema_type.type
            if isinstance(schema_type.type, str)
            else schema_type.type.value
        )
    elif isinstance(schema_type, ArraySchemaType):
        return [_schema_type_to_legacy(schema_type.element_type)]
    elif isinstance(schema_type, ObjectSchemaType):
        return {k: _schema_type_to_legacy(v) for k, v in schema_type.fields.items()}
    elif isinstance(schema_type, UnionSchemaType):
        type_names = [
            _schema_type_to_legacy(t) if hasattr(t, "type") else str(t)
            for t in schema_type.types
        ]
        return f"union({'|'.join(sorted(type_names))})"
    else:
        return "any"


# Export the main classes and functions
__all__ = [
    "ScalarType",
    "FieldConstraint",
    "SchemaType",
    "ScalarSchemaType",
    "ArraySchemaType",
    "ObjectSchemaType",
    "UnionSchemaType",
    "SchemaField",
    "Schema",
    "scalar_type",
    "array_type",
    "object_type",
    "union_type",
    "from_legacy_tree",
    "to_legacy_tree",
]
