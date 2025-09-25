#!/usr/bin/env python3
"""
Schema generation module for schema-diff.

Generates schemas in various formats from data files:
- JSON Schema (draft-07)
- SQL DDL (Postgres and BigQuery)
- Spark schema format
- BigQuery schema JSON
- OpenAPI schema
"""
from __future__ import annotations

import json
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from .json_data_file_parser import merged_schema_from_samples

# Optional dependencies
try:
    import sqlparse
except ImportError:
    sqlparse = None

try:
    import jsonschema
except ImportError:
    jsonschema = None


def validate_json_schema(schema_str: str) -> Tuple[bool, Optional[str]]:
    """Validate JSON Schema format."""
    try:
        schema = json.loads(schema_str)

        # Basic JSON Schema structure validation
        required_fields = ["$schema", "type"]
        for field in required_fields:
            if field not in schema:
                return False, f"Missing required field: {field}"

        # Check for valid JSON Schema version
        if not schema["$schema"].startswith("http://json-schema.org/"):
            return False, f"Invalid JSON Schema version: {schema['$schema']}"

        # Try to use jsonschema if available
        if jsonschema is not None:
            try:
                # Validate against the JSON Schema meta-schema
                jsonschema.Draft7Validator.check_schema(schema)
                return True, None
            except Exception as e:
                return False, f"JSON Schema validation error: {str(e)}"
        else:
            # Basic validation without jsonschema library
            if schema["type"] not in [
                "object",
                "array",
                "string",
                "number",
                "integer",
                "boolean",
                "null",
            ]:
                return False, f"Invalid root type: {schema['type']}"
            return True, None

    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {str(e)}"


def validate_sql_ddl(ddl_str: str) -> Tuple[bool, Optional[str]]:
    """Validate SQL DDL syntax."""
    try:
        # Basic SQL DDL validation
        ddl_str = ddl_str.strip()

        if not ddl_str.upper().startswith("CREATE TABLE"):
            return False, "DDL must start with CREATE TABLE"

        # Check for balanced parentheses
        paren_count = ddl_str.count("(") - ddl_str.count(")")
        if paren_count != 0:
            return False, (
                f"Unbalanced parentheses: {paren_count} extra opening"
                if paren_count > 0
                else f"{abs(paren_count)} extra closing"
            )

        # Check for proper semicolon termination
        if not ddl_str.rstrip().endswith(";"):
            return False, "DDL must end with semicolon"

        # Check for common SQL syntax patterns
        if "(" not in ddl_str or ")" not in ddl_str:
            return False, "Missing column definitions"

        # Try to use sqlparse if available
        if sqlparse is not None:
            try:
                parsed = sqlparse.parse(ddl_str)
                if not parsed:
                    return False, "Failed to parse SQL DDL"

                # Check if it's a valid statement
                statement = parsed[0]
                if statement.get_type() != "CREATE":
                    return (
                        False,
                        f"Expected CREATE statement, got: {statement.get_type()}",
                    )

                return True, None
            except Exception as e:
                return False, f"SQL parsing error: {str(e)}"
        else:
            # Basic validation without sqlparse
            return True, None

    except Exception as e:
        return False, f"DDL validation error: {str(e)}"


def validate_bigquery_ddl(ddl_str: str) -> Tuple[bool, Optional[str]]:
    """Validate BigQuery DDL syntax."""
    try:
        ddl_str = ddl_str.strip()

        # Check basic structure
        if not ddl_str.upper().startswith("CREATE TABLE"):
            return False, "DDL must start with CREATE TABLE"

        # Check for balanced angle brackets (for STRUCT/ARRAY types)
        angle_count = ddl_str.count("<") - ddl_str.count(">")
        if angle_count != 0:
            return False, (
                f"Unbalanced angle brackets: {angle_count} extra opening"
                if angle_count > 0
                else f"{abs(angle_count)} extra closing"
            )

        # Check for balanced parentheses
        paren_count = ddl_str.count("(") - ddl_str.count(")")
        if paren_count != 0:
            return False, (
                f"Unbalanced parentheses: {paren_count} extra opening"
                if paren_count > 0
                else f"{abs(paren_count)} extra closing"
            )

        # Check for proper semicolon termination
        if not ddl_str.rstrip().endswith(";"):
            return False, "DDL must end with semicolon"

        # BigQuery-specific validations
        # Check table name is properly quoted
        table_pattern = r"CREATE TABLE `[^`]+`"
        if not re.search(table_pattern, ddl_str):
            return False, "Table name must be quoted with backticks"

        return True, None

    except Exception as e:
        return False, f"BigQuery DDL validation error: {str(e)}"


def validate_spark_schema(schema_str: str) -> Tuple[bool, Optional[str]]:
    """Validate Spark schema format."""
    try:
        lines = schema_str.strip().split("\n")

        if not lines:
            return False, "Empty schema"

        # First line should be "root"
        if lines[0].strip() != "root":
            return False, "Schema must start with 'root'"

        # Check indentation consistency and structure
        for i, line in enumerate(lines[1:], 1):
            line = line.rstrip()
            if not line:
                continue

            # Check for proper field format
            if " |-- " not in line and "|    |-- " not in line:
                return False, f"Line {i+1}: Invalid field format: {line}"

            # Check for valid Spark types
            if ":" in line:
                type_part = line.split(":", 1)[1].strip()
                if not any(
                    spark_type in type_part
                    for spark_type in [
                        "string",
                        "long",
                        "double",
                        "boolean",
                        "array",
                        "struct",
                        "timestamp",
                        "date",
                    ]
                ):
                    return False, f"Line {i+1}: Unknown Spark type in: {type_part}"

        return True, None

    except Exception as e:
        return False, f"Spark schema validation error: {str(e)}"


def validate_bigquery_json(schema_str: str) -> Tuple[bool, Optional[str]]:
    """Validate BigQuery JSON schema format."""
    try:
        schema = json.loads(schema_str)

        # Must be an array of field definitions
        if not isinstance(schema, list):
            return False, "BigQuery schema must be an array of fields"

        for i, field in enumerate(schema):
            if not isinstance(field, dict):
                return False, f"Field {i} must be an object"

            # Required fields
            if "name" not in field:
                return False, f"Field {i} missing 'name'"
            if "type" not in field:
                return False, f"Field {i} missing 'type'"

            # Valid types
            valid_types = [
                "STRING",
                "INT64",
                "FLOAT64",
                "BOOL",
                "DATE",
                "DATETIME",
                "TIME",
                "TIMESTAMP",
                "BYTES",
                "RECORD",
            ]
            if field["type"] not in valid_types:
                return False, f"Field {i} has invalid type: {field['type']}"

        return True, None

    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {str(e)}"
    except Exception as e:
        return False, f"BigQuery JSON validation error: {str(e)}"


def validate_openapi_schema(schema_str: str) -> Tuple[bool, Optional[str]]:
    """Validate OpenAPI schema format."""
    try:
        schema = json.loads(schema_str)

        # Check for required OpenAPI schema fields
        if "type" not in schema:
            return False, "Missing 'type' field"

        # Check for valid OpenAPI types
        valid_types = ["object", "array", "string", "number", "integer", "boolean"]
        if schema["type"] not in valid_types:
            return False, f"Invalid OpenAPI type: {schema['type']}"

        return True, None

    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {str(e)}"
    except Exception as e:
        return False, f"OpenAPI schema validation error: {str(e)}"


def validate_schema(
    output: str, format_type: str, validate: bool = True
) -> Tuple[str, bool, Optional[str]]:
    """
    Validate generated schema based on format type.

    Returns:
        Tuple of (output, is_valid, error_message)
    """
    if not validate:
        return output, True, None

    validators = {
        "json_schema": validate_json_schema,
        "sql_ddl": validate_sql_ddl,
        "bigquery_ddl": validate_bigquery_ddl,
        "spark": validate_spark_schema,
        "bigquery_json": validate_bigquery_json,
        "openapi": validate_openapi_schema,
    }

    validator = validators.get(format_type)
    if not validator:
        # No validator available for this format
        return output, True, None

    is_valid, error_msg = validator(output)
    return output, is_valid, error_msg


def generate_schema_from_data(
    records: List[Dict[str, Any]],
    cfg,
    format: str = "json_schema",
    table_name: str = "generated_table",
    required_fields: Optional[Set[str]] = None,
    validate: bool = True,
) -> str:
    """
    Generate a schema in the specified format from data records.

    Args:
        records: List of data records to analyze
        cfg: Configuration object
        format: Output format ('json_schema', 'sql_ddl', 'bigquery_ddl', 'spark', 'bigquery_json', 'openapi')
        table_name: Name for the table/schema (used in SQL DDL)
        required_fields: Set of field paths that should be marked as required

    Returns:
        Generated schema as a string
    """
    # Generate internal schema from records
    internal_schema = merged_schema_from_samples(records, cfg)

    # Generate the schema based on format
    if format == "json_schema":
        output = _generate_json_schema(internal_schema, required_fields)
    elif format == "sql_ddl":
        output = _generate_sql_ddl(internal_schema, table_name, required_fields)
    elif format == "bigquery_ddl":
        output = _generate_bigquery_ddl(internal_schema, table_name, required_fields)
    elif format == "spark":
        output = _generate_spark_schema(internal_schema)
    elif format == "bigquery_json":
        output = _generate_bigquery_json_schema(internal_schema, required_fields)
    elif format == "openapi":
        output = _generate_openapi_schema(internal_schema, required_fields)
    else:
        raise ValueError(f"Unsupported format: {format}")

    # Validate the generated schema
    validated_output, is_valid, error_msg = validate_schema(output, format, validate)

    if validate and not is_valid:
        error_details = f"Schema validation failed for format '{format}': {error_msg}"
        print(f"⚠️  WARNING: {error_details}", file=sys.stderr)

        # Check if validation failed due to missing dependencies
        if sqlparse is None or jsonschema is None:
            print(
                "💡 Enhanced validation requires optional dependencies. "
                "Install with: pip install -e '.[validation]'",
                file=sys.stderr,
            )
        else:
            print(
                "Generated schema may have syntax errors. Use --no-validate to skip validation.",
                file=sys.stderr,
            )
        # Still return the output but warn the user

    return validated_output


def _generate_json_schema(
    schema: Dict[str, Any], required_fields: Optional[Set[str]] = None
) -> str:
    """Generate JSON Schema (draft-07) from internal schema."""

    def convert_type(field_type: str, field_name: str = "") -> Dict[str, Any]:
        """Convert internal type to JSON Schema type."""
        # Handle union types
        if field_type.startswith("union("):
            # Extract union members
            union_content = field_type[6:-1]  # Remove "union(" and ")"

            # Parse union members (simple parsing)
            parts = union_content.split("|")
            has_null = False
            types = []

            for part in parts:
                part = part.strip()
                if part == "missing":
                    has_null = True
                else:
                    types.append(convert_type(part, field_name))

            if len(types) == 1 and has_null:
                # Simple nullable type
                result = types[0].copy()
                if "type" in result:
                    if isinstance(result["type"], list):
                        result["type"].append("null")
                    else:
                        result["type"] = [result["type"], "null"]
                return result
            elif types:
                # Multiple types
                if has_null:
                    types.append({"type": "null"})
                return {"anyOf": types}

        # Handle array types
        if field_type.startswith("array("):
            item_type = field_type[6:-1]  # Remove "array(" and ")"
            if item_type == "unstructured":
                return {"type": "array"}
            return {"type": "array", "items": convert_type(item_type, field_name)}

        # Handle object types
        if field_type.startswith("object("):
            return {"type": "object"}

        # Basic types
        type_mapping = {
            "str": "string",
            "int": "integer",
            "float": "number",
            "bool": "boolean",
            "unstructured": {},  # No type constraint
        }

        if field_type in type_mapping:
            mapped = type_mapping[field_type]
            if mapped == {}:
                return {}  # No type constraint for unstructured
            return {"type": mapped}

        # Default for unknown types
        return {"type": "string"}

    def build_schema_object(obj: Dict[str, Any], path: str = "") -> Dict[str, Any]:
        """Recursively build JSON Schema object."""
        if not isinstance(obj, dict):
            return convert_type_value(obj, path)

        properties = {}
        required = []

        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key

            if isinstance(value, dict):
                properties[key] = build_schema_object(value, current_path)
            elif isinstance(value, list) and len(value) > 0:
                # Handle arrays
                if isinstance(value[0], dict):
                    # Array of objects
                    properties[key] = {
                        "type": "array",
                        "items": build_schema_object(value[0], current_path),
                    }
                else:
                    # Array of primitives
                    properties[key] = {
                        "type": "array",
                        "items": convert_type_value(value[0], current_path),
                    }
            else:
                properties[key] = convert_type_value(value, current_path)

            # Check if field is required
            if required_fields and current_path in required_fields:
                required.append(key)

        result = {"type": "object", "properties": properties}

        if required:
            result["required"] = sorted(required)

        return result

    def convert_type_value(value: Any, field_name: str = "") -> Dict[str, Any]:
        """Convert a value (which might be string, list, dict, etc.) to JSON Schema type."""
        if isinstance(value, str):
            return convert_type(value, field_name)
        elif isinstance(value, list):
            if len(value) == 0:
                return {"type": "array"}
            elif isinstance(value[0], dict):
                return {"type": "array", "items": build_schema_object(value[0])}
            else:
                return {"type": "array", "items": convert_type_value(value[0])}
        elif isinstance(value, dict):
            return build_schema_object(value)
        else:
            # Handle special values
            if value == "empty_array":
                return {"type": "array"}
            elif value == "missing":
                return {"type": "null"}
            else:
                return convert_type(str(value), field_name)

    # Build the root schema
    root_schema = build_schema_object(schema)

    # Add JSON Schema metadata
    result = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://example.com/schema.json",
        "title": "Generated Schema",
        "description": "Schema generated from data samples",
        **root_schema,
    }

    return json.dumps(result, indent=2, ensure_ascii=False)


def _generate_sql_ddl(
    schema: Dict[str, Any], table_name: str, required_fields: Optional[Set[str]] = None
) -> str:
    """Generate PostgreSQL-style SQL DDL from internal schema."""

    def convert_type(field_type: str) -> str:
        """Convert internal type to SQL type."""
        # Handle union types - pick the first non-null type
        if field_type.startswith("union("):
            union_content = field_type[6:-1]
            parts = [p.strip() for p in union_content.split("|")]
            non_missing = [p for p in parts if p != "missing"]
            if non_missing:
                return convert_type(non_missing[0])

        # Handle array types
        if field_type.startswith("array("):
            item_type = field_type[6:-1]
            if item_type == "unstructured":
                return "JSON[]"
            return f"{convert_type(item_type)}[]"

        # Handle object types
        if field_type.startswith("object("):
            return "JSON"

        # Basic types
        type_mapping = {
            "str": "TEXT",
            "int": "INTEGER",
            "float": "DOUBLE PRECISION",
            "bool": "BOOLEAN",
            "unstructured": "JSON",
        }

        return type_mapping.get(field_type, "TEXT")

    def escape_sql_identifier(name: str) -> str:
        """Escape SQL identifiers that are reserved keywords or contain special characters."""
        # SQL reserved keywords (common across most SQL databases)
        reserved_keywords = {
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TABLE",
            "INDEX",
            "VIEW",
            "DATABASE",
            "SCHEMA",
            "GRANT",
            "REVOKE",
            "COMMIT",
            "ROLLBACK",
            "TRANSACTION",
            "BEGIN",
            "END",
            "IF",
            "ELSE",
            "WHILE",
            "FOR",
            "CASE",
            "WHEN",
            "THEN",
            "AND",
            "OR",
            "NOT",
            "IN",
            "EXISTS",
            "LIKE",
            "BETWEEN",
            "IS",
            "NULL",
            "TRUE",
            "FALSE",
            "UNION",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "OUTER",
            "ON",
            "AS",
            "DISTINCT",
            "ALL",
            "ORDER",
            "GROUP",
            "BY",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "ASC",
            "DESC",
            "PRIMARY",
            "KEY",
            "FOREIGN",
            "REFERENCES",
            "UNIQUE",
            "CHECK",
            "DEFAULT",
            "AUTO_INCREMENT",
            "CONSTRAINT",
            "CHAR",
            "VARCHAR",
            "TEXT",
            "INT",
            "INTEGER",
            "BIGINT",
            "DECIMAL",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "ARRAY",
            "JSON",
            "BLOB",
            "GROUPS",
            "WINDOW",
            "OVER",
            "PARTITION",
            "ROWS",
            "RANGE",
            "PRECEDING",
            "FOLLOWING",
            "UNBOUNDED",
            "CURRENT",
            "ROW",
        }

        # Check if name is a reserved keyword or contains special characters
        if (
            name.upper() in reserved_keywords
            or not name.replace("_", "").isalnum()
            or name[0].isdigit()
        ):
            return f'"{name}"'
        return name

    def build_columns(obj: Dict[str, Any], prefix: str = "") -> List[str]:
        """Build column definitions, flattening nested objects."""
        columns = []

        for key, value in obj.items():
            column_name = f"{prefix}{key}" if prefix else key
            safe_name = escape_sql_identifier(column_name)

            if isinstance(value, dict):
                # Nested object - flatten with underscore separation
                nested_cols = build_columns(value, f"{column_name}_")
                columns.extend(nested_cols)
            elif (
                isinstance(value, list)
                and len(value) > 0
                and isinstance(value[0], dict)
            ):
                # Array of objects - flatten with array notation
                nested_cols = build_columns(value[0], f"{column_name}_")
                columns.extend(nested_cols)
            elif isinstance(value, list):
                # Array of primitives
                if len(value) > 0:
                    item_type = convert_type(str(value[0]))
                else:
                    item_type = "TEXT"
                sql_type = f"{item_type}[]"
                not_null = ""
                if required_fields and column_name in required_fields:
                    not_null = " NOT NULL"

                columns.append(f"  {safe_name} {sql_type}{not_null}")
            else:
                # Handle special values and regular types
                if value == "empty_array":
                    sql_type = "JSON[]"
                elif value == "missing":
                    sql_type = "TEXT"  # Nullable text for missing values
                else:
                    sql_type = convert_type(str(value))

                not_null = ""
                if required_fields and column_name in required_fields:
                    not_null = " NOT NULL"

                columns.append(f"  {safe_name} {sql_type}{not_null}")

        return columns

    columns = build_columns(schema)

    # Add commas to all but the last column
    for i in range(len(columns) - 1):
        columns[i] += ","

    ddl = f"""CREATE TABLE {table_name} (
{chr(10).join(columns)}
);"""

    return ddl


def _generate_bigquery_ddl(
    schema: Dict[str, Any], table_name: str, required_fields: Optional[Set[str]] = None
) -> str:
    """Generate BigQuery DDL from internal schema."""

    def convert_type(field_type: str, is_repeated: bool = False) -> str:
        """Convert internal type to BigQuery type."""
        # Handle union types
        if field_type.startswith("union("):
            union_content = field_type[6:-1]
            parts = [p.strip() for p in union_content.split("|")]
            non_missing = [p for p in parts if p != "missing"]
            if non_missing:
                return convert_type(non_missing[0], is_repeated)

        # Handle array types
        if field_type.startswith("array("):
            item_type = field_type[6:-1]
            if item_type == "unstructured":
                return "JSON"
            return convert_type(item_type, True)

        # Handle object types
        if field_type.startswith("object("):
            return "JSON"

        # Basic types
        type_mapping = {
            "str": "STRING",
            "int": "INT64",
            "float": "FLOAT64",
            "bool": "BOOL",
            "unstructured": "JSON",
        }

        base_type = type_mapping.get(field_type, "STRING")
        return f"ARRAY<{base_type}>" if is_repeated else base_type

    def escape_bq_identifier(name: str) -> str:
        """Escape BigQuery identifiers that are reserved keywords or contain special characters."""
        # BigQuery reserved keywords (partial list of common ones)
        reserved_keywords = {
            "ALL",
            "AND",
            "ANY",
            "ARRAY",
            "AS",
            "ASC",
            "ASSERT_ROWS_MODIFIED",
            "AT",
            "BETWEEN",
            "BY",
            "CASE",
            "CAST",
            "COLLATE",
            "CONTAINS",
            "CREATE",
            "CROSS",
            "CUBE",
            "CURRENT",
            "DEFAULT",
            "DEFINE",
            "DESC",
            "DISTINCT",
            "ELSE",
            "END",
            "ENUM",
            "ESCAPE",
            "EXCEPT",
            "EXCLUDE",
            "EXISTS",
            "EXTRACT",
            "FALSE",
            "FETCH",
            "FOLLOWING",
            "FOR",
            "FROM",
            "FULL",
            "GROUP",
            "GROUPING",
            "GROUPS",
            "HASH",
            "HAVING",
            "IF",
            "IGNORE",
            "IN",
            "INNER",
            "INTERSECT",
            "INTERVAL",
            "INTO",
            "IS",
            "JOIN",
            "LATERAL",
            "LEFT",
            "LIKE",
            "LIMIT",
            "LOOKUP",
            "MERGE",
            "NATURAL",
            "NEW",
            "NO",
            "NOT",
            "NULL",
            "NULLS",
            "OF",
            "ON",
            "OR",
            "ORDER",
            "OUTER",
            "OVER",
            "PARTITION",
            "PRECEDING",
            "PROTO",
            "RANGE",
            "RECURSIVE",
            "RESPECT",
            "RIGHT",
            "ROLLUP",
            "ROWS",
            "SELECT",
            "SET",
            "SOME",
            "STRUCT",
            "TABLESAMPLE",
            "THEN",
            "TO",
            "TREAT",
            "TRUE",
            "UNBOUNDED",
            "UNION",
            "UNNEST",
            "USING",
            "WHEN",
            "WHERE",
            "WINDOW",
            "WITH",
            "WITHIN",
        }

        # Check if name is a reserved keyword or contains special characters
        if (
            name.upper() in reserved_keywords
            or not name.replace("_", "").isalnum()
            or name[0].isdigit()
        ):
            return f"`{name}`"
        return name

    def build_struct_fields(obj: Dict[str, Any], indent_level: int = 1) -> List[str]:
        """Build STRUCT field definitions."""
        fields = []
        indent = "  " * indent_level

        items = list(obj.items())
        for i, (key, value) in enumerate(items):
            is_last = i == len(items) - 1
            comma = "" if is_last else ","
            escaped_key = escape_bq_identifier(key)

            if isinstance(value, dict):
                # Nested struct
                nested_fields = build_struct_fields(value, indent_level + 1)
                mode = ""
                if required_fields and key in required_fields:
                    mode = " NOT NULL"

                fields.append(f"{indent}{escaped_key} STRUCT<")
                fields.extend(nested_fields)
                fields.append(f"{indent}>{mode}{comma}")
            elif isinstance(value, list) and len(value) > 0:
                if isinstance(value[0], dict):
                    # Array of structs
                    nested_fields = build_struct_fields(value[0], indent_level + 1)
                    mode = ""
                    if required_fields and key in required_fields:
                        mode = " NOT NULL"

                    fields.append(f"{indent}{escaped_key} ARRAY<STRUCT<")
                    fields.extend(nested_fields)
                    fields.append(f"{indent}>>{mode}{comma}")
                else:
                    # Array of primitives
                    item_type = convert_type(str(value[0]))
                    base_type = item_type.replace("ARRAY<", "").replace(">", "")
                    mode = ""
                    if required_fields and key in required_fields:
                        mode = " NOT NULL"

                    fields.append(
                        f"{indent}{escaped_key} ARRAY<{base_type}>{mode}{comma}"
                    )
            else:
                # Handle special values and regular types
                if value == "empty_array":
                    bq_type = "ARRAY<STRING>"
                elif value == "missing":
                    bq_type = "STRING"
                else:
                    bq_type = convert_type(str(value))

                mode = ""
                if required_fields and key in required_fields:
                    mode = " NOT NULL"

                fields.append(f"{indent}{escaped_key} {bq_type}{mode}{comma}")

        return fields

    # Check if we need STRUCT for complex schema
    has_complex_fields = any(
        isinstance(v, (dict, list)) or (isinstance(v, str) and v == "empty_array")
        for v in schema.values()
    )

    if has_complex_fields:
        # Complex schema with nested objects or arrays
        fields = build_struct_fields(schema)
        ddl = f"""CREATE TABLE `{table_name}` (
{chr(10).join(fields)}
);"""
    else:
        # Simple flat schema
        columns = []
        items = list(schema.items())
        for i, (key, value) in enumerate(items):
            is_last = i == len(items) - 1
            comma = "" if is_last else ","
            escaped_key = escape_bq_identifier(key)

            if value == "empty_array":
                bq_type = "ARRAY<STRING>"
            elif value == "missing":
                bq_type = "STRING"
            else:
                bq_type = convert_type(str(value))

            mode = ""
            if required_fields and key in required_fields:
                mode = " NOT NULL"

            columns.append(f"  {escaped_key} {bq_type}{mode}{comma}")

        ddl = f"""CREATE TABLE `{table_name}` (
{chr(10).join(columns)}
);"""

    return ddl


def _generate_spark_schema(schema: Dict[str, Any]) -> str:
    """Generate Spark schema format from internal schema."""

    def convert_type(field_type: str) -> str:
        """Convert internal type to Spark type."""
        # Handle union types - pick first non-null type
        if field_type.startswith("union("):
            union_content = field_type[6:-1]
            parts = [p.strip() for p in union_content.split("|")]
            non_missing = [p for p in parts if p != "missing"]
            if non_missing:
                return convert_type(non_missing[0])

        # Handle array types
        if field_type.startswith("array("):
            item_type = field_type[6:-1]
            if item_type == "unstructured":
                return "array<string>"
            return f"array<{convert_type(item_type)}>"

        # Handle object types
        if field_type.startswith("object("):
            return "string"  # JSON string representation

        # Basic types
        type_mapping = {
            "str": "string",
            "int": "long",
            "float": "double",
            "bool": "boolean",
            "unstructured": "string",
        }

        return type_mapping.get(field_type, "string")

    def escape_spark_identifier(name: str) -> str:
        """Escape Spark/SQL identifiers that are reserved keywords or contain special characters."""
        # Spark SQL reserved keywords
        reserved_keywords = {
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TABLE",
            "VIEW",
            "DATABASE",
            "SHOW",
            "DESCRIBE",
            "EXPLAIN",
            "USE",
            "SET",
            "RESET",
            "ADD",
            "REMOVE",
            "REFRESH",
            "CACHE",
            "UNCACHE",
            "CLEAR",
            "AND",
            "OR",
            "NOT",
            "IN",
            "EXISTS",
            "LIKE",
            "RLIKE",
            "REGEXP",
            "BETWEEN",
            "IS",
            "NULL",
            "TRUE",
            "FALSE",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "MINUS",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "OUTER",
            "CROSS",
            "NATURAL",
            "ON",
            "USING",
            "AS",
            "DISTINCT",
            "ALL",
            "ORDER",
            "GROUP",
            "BY",
            "HAVING",
            "SORT",
            "CLUSTER",
            "DISTRIBUTE",
            "LIMIT",
            "OFFSET",
            "ASC",
            "DESC",
            "NULLS",
            "FIRST",
            "LAST",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "IF",
            "CAST",
            "EXTRACT",
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "INTERVAL",
            "WINDOW",
            "OVER",
            "PARTITION",
            "ROWS",
            "RANGE",
            "PRECEDING",
            "FOLLOWING",
            "UNBOUNDED",
            "CURRENT",
            "ROW",
            "GROUPS",
            "CUBE",
            "ROLLUP",
            "GROUPING",
            "SETS",
            "LATERAL",
            "TABLESAMPLE",
            "STRATIFY",
            "ANTI",
            "SEMI",
            "STRUCT",
            "ARRAY",
            "MAP",
            "NAMED_STRUCT",
            "INLINE",
            "INLINE_OUTER",
            "STACK",
            "PIVOT",
            "UNPIVOT",
            "WITH",
            "RECURSIVE",
            "TEMPORARY",
            "GLOBAL",
            "LOCAL",
        }

        # Check if name is a reserved keyword or contains special characters
        if (
            name.upper() in reserved_keywords
            or not name.replace("_", "").isalnum()
            or name[0].isdigit()
            or " " in name
            or "-" in name
        ):
            return f"`{name}`"
        return name

    def build_struct_schema(obj: Dict[str, Any], indent_level: int = 0) -> List[str]:
        """Build Spark struct schema format."""
        lines = []
        indent = " " * (indent_level * 4)

        if indent_level == 0:
            lines.append("root")

        for key, value in obj.items():
            escaped_key = escape_spark_identifier(key)

            if isinstance(value, dict):
                # Nested struct
                lines.append(f"{indent} |-- {escaped_key}: struct (nullable = true)")
                lines.extend(build_struct_schema(value, indent_level + 1))
            elif isinstance(value, list) and len(value) > 0:
                if isinstance(value[0], dict):
                    # Array of structs
                    lines.append(f"{indent} |-- {escaped_key}: array (nullable = true)")
                    lines.append(
                        f"{indent}    |    |-- element: struct (containsNull = true)"
                    )
                    lines.extend(build_struct_schema(value[0], indent_level + 2))
                else:
                    # Array of primitives
                    item_type = convert_type(str(value[0]))
                    lines.append(f"{indent} |-- {escaped_key}: array (nullable = true)")
                    lines.append(
                        f"{indent}    |    |-- element: {item_type} (containsNull = true)"
                    )
            else:
                # Handle special values and regular types
                if value == "empty_array":
                    spark_type = "array"
                    lines.append(
                        f"{indent} |-- {escaped_key}: {spark_type} (nullable = true)"
                    )
                    lines.append(
                        f"{indent}    |    |-- element: string (containsNull = true)"
                    )
                elif value == "missing":
                    spark_type = "string"
                    lines.append(
                        f"{indent} |-- {escaped_key}: {spark_type} (nullable = true)"
                    )
                else:
                    spark_type = convert_type(str(value))
                    nullable = "true"  # Default to nullable for data-inferred schemas
                    lines.append(
                        f"{indent} |-- {escaped_key}: {spark_type} (nullable = {nullable})"
                    )

        return lines

    schema_lines = build_struct_schema(schema)
    return "\n".join(schema_lines)


def _generate_bigquery_json_schema(
    schema: Dict[str, Any], required_fields: Optional[Set[str]] = None
) -> str:
    """Generate BigQuery JSON schema format from internal schema."""

    def convert_field(key: str, field_type: str, path: str = "") -> Dict[str, Any]:
        """Convert field to BigQuery JSON schema format."""
        current_path = f"{path}.{key}" if path else key

        field = {"name": key, "mode": "NULLABLE"}

        # Check if required
        if required_fields and current_path in required_fields:
            field["mode"] = "REQUIRED"

        # Handle union types
        if field_type.startswith("union("):
            union_content = field_type[6:-1]
            parts = [p.strip() for p in union_content.split("|")]
            non_missing = [p for p in parts if p != "missing"]
            if non_missing:
                field_type = non_missing[0]

        # Handle array types
        if field_type.startswith("array("):
            field["mode"] = "REPEATED"
            item_type = field_type[6:-1]
            if item_type == "unstructured":
                field["type"] = "JSON"
            else:
                item_field = convert_field("item", item_type)
                field["type"] = item_field["type"]
                if "fields" in item_field:
                    field["fields"] = item_field["fields"]
        elif field_type.startswith("object("):
            field["type"] = "JSON"
        else:
            # Basic types
            type_mapping = {
                "str": "STRING",
                "int": "INTEGER",
                "float": "FLOAT",
                "bool": "BOOLEAN",
                "unstructured": "JSON",
            }
            field["type"] = type_mapping.get(field_type, "STRING")

        return field

    def build_fields(obj: Dict[str, Any], path: str = "") -> List[Dict[str, Any]]:
        """Build BigQuery fields array."""
        fields = []

        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key

            if isinstance(value, dict) and not str(value).startswith(
                ("union(", "array(", "object(")
            ):
                # Nested record
                field = {
                    "name": key,
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": build_fields(value, current_path),
                }

                if required_fields and current_path in required_fields:
                    field["mode"] = "REQUIRED"

                fields.append(field)
            else:
                fields.append(convert_field(key, str(value), path))

        return fields

    bq_schema = build_fields(schema)
    return json.dumps(bq_schema, indent=2, ensure_ascii=False)


def _generate_openapi_schema(
    schema: Dict[str, Any], required_fields: Optional[Set[str]] = None
) -> str:
    """Generate OpenAPI 3.0 schema from internal schema."""

    def convert_type(field_type: str) -> Dict[str, Any]:
        """Convert internal type to OpenAPI type."""
        # Handle union types
        if field_type.startswith("union("):
            union_content = field_type[6:-1]
            parts = [p.strip() for p in union_content.split("|")]
            non_missing = [p for p in parts if p != "missing"]
            has_null = "missing" in parts

            if len(non_missing) == 1:
                result = convert_type(non_missing[0])
                if has_null:
                    result["nullable"] = True
                return result
            elif non_missing:
                return {"oneOf": [convert_type(t) for t in non_missing]}

        # Handle array types
        if field_type.startswith("array("):
            item_type = field_type[6:-1]
            if item_type == "unstructured":
                return {"type": "array"}
            return {"type": "array", "items": convert_type(item_type)}

        # Handle object types
        if field_type.startswith("object("):
            return {"type": "object"}

        # Basic types
        type_mapping: Dict[str, Dict[str, Any]] = {
            "str": {"type": "string"},
            "int": {"type": "integer"},
            "float": {"type": "number"},
            "bool": {"type": "boolean"},
            "unstructured": {"type": "object"},  # Fix: provide proper dict structure
        }

        return type_mapping.get(field_type, {"type": "string"})

    def build_object_schema(obj: Dict[str, Any], path: str = "") -> Dict[str, Any]:
        """Build OpenAPI object schema."""
        properties = {}
        required = []

        for key, value in obj.items():
            current_path = f"{path}.{key}" if path else key

            if isinstance(value, dict) and not str(value).startswith(
                ("union(", "array(", "object(")
            ):
                properties[key] = build_object_schema(value, current_path)
            else:
                properties[key] = convert_type(str(value))

            if required_fields and current_path in required_fields:
                required.append(key)

        result = {"type": "object", "properties": properties}

        if required:
            result["required"] = sorted(required)

        return result

    openapi_schema = {
        "openapi": "3.0.0",
        "info": {
            "title": "Generated API Schema",
            "version": "1.0.0",
            "description": "Schema generated from data samples",
        },
        "components": {"schemas": {"GeneratedModel": build_object_schema(schema)}},
    }

    return json.dumps(openapi_schema, indent=2, ensure_ascii=False)


def get_supported_formats() -> List[str]:
    """Get list of supported schema output formats."""
    return [
        "json_schema",
        "sql_ddl",
        "bigquery_ddl",
        "spark",
        "bigquery_json",
        "openapi",
    ]


def get_format_description(format: str) -> str:
    """Get human-readable description of a format."""
    descriptions = {
        "json_schema": "JSON Schema (draft-07) - Standard JSON schema definition",
        "sql_ddl": "SQL DDL (PostgreSQL) - CREATE TABLE statement",
        "bigquery_ddl": "BigQuery DDL - CREATE TABLE with STRUCT/ARRAY support",
        "spark": "Spark Schema - Spark DataFrame printSchema() format",
        "bigquery_json": "BigQuery JSON Schema - Google Cloud BigQuery schema format",
        "openapi": "OpenAPI 3.0 Schema - REST API schema definition",
    }
    return descriptions.get(format, f"Unknown format: {format}")
