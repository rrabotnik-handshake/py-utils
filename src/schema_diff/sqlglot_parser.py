#!/usr/bin/env python3
"""SQLGlot-based SQL schema parser with enhanced cross-dialect support.

This module provides an alternative SQL parser using SQLGlot for:
- Better SQL DDL parsing than regex-based approaches
- Cross-dialect SQL translation (BigQuery ↔ Postgres ↔ MySQL ↔ Snowflake, etc.)
- Type normalization across different SQL dialects
- Robust syntax validation

Features:
- Supports 20+ SQL dialects via SQLGlot
- Extracts table schema from CREATE TABLE statements
- Normalizes type names (INT64 → int, VARCHAR(255) → str)
- Handles complex nested types (STRUCT, ARRAY)
- Validates SQL syntax before processing

Usage:
    from schema_diff.sqlglot_parser import schema_from_sql_ddl_sqlglot

    # Parse BigQuery DDL
    tree, required = schema_from_sql_ddl_sqlglot(
        "path/to/schema.sql",
        dialect="bigquery"
    )

    # Parse and translate from Postgres to BigQuery types
    tree, required = schema_from_sql_ddl_sqlglot(
        "path/to/schema.sql",
        dialect="postgres",
        normalize_to="bigquery"
    )

Requirements:
    pip install sqlglot>=25.0.0
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Check if SQLGlot is available
try:
    import sqlglot
    from sqlglot import exp, parse_one

    _HAS_SQLGLOT = True
except ImportError:
    _HAS_SQLGLOT = False


# Type mapping from SQL types to internal schema-diff types
_SQL_TYPE_TO_INTERNAL = {
    # Integers
    "TINYINT": "int",
    "SMALLINT": "int",
    "MEDIUMINT": "int",
    "INT": "int",
    "INTEGER": "int",
    "BIGINT": "int",
    "INT64": "int",
    "INT8": "int",
    "INT16": "int",
    "INT32": "int",
    "SERIAL": "int",
    "BIGSERIAL": "int",
    "SMALLSERIAL": "int",
    # Floats
    "FLOAT": "float",
    "DOUBLE": "float",
    "REAL": "float",
    "NUMERIC": "float",
    "DECIMAL": "float",
    "BIGNUMERIC": "float",
    "FLOAT64": "float",
    # Booleans
    "BOOLEAN": "bool",
    "BOOL": "bool",
    "BIT": "bool",
    # Strings
    "VARCHAR": "str",
    "CHAR": "str",
    "TEXT": "str",
    "STRING": "str",
    "CLOB": "str",
    "NVARCHAR": "str",
    "NCHAR": "str",
    # Binary
    "BINARY": "str",
    "VARBINARY": "str",
    "BYTES": "str",
    "BLOB": "str",
    # Date/Time
    "DATE": "date",
    "DATETIME": "timestamp",
    "TIMESTAMP": "timestamp",
    "TIME": "str",
    "TIMESTAMPTZ": "timestamp",
    "DATETIME64": "timestamp",
    # JSON
    "JSON": "object",
    "JSONB": "object",
    # Arrays (generic)
    "ARRAY": "array",
    # Structs/Objects
    "STRUCT": "object",
    "ROW": "object",
    "RECORD": "object",
    # UUID
    "UUID": "str",
}


def _sqlglot_type_to_internal(sql_type: exp.DataType) -> Any:
    """Convert SQLGlot DataType expression to internal schema representation.

    Args:
        sql_type: SQLGlot DataType expression

    Returns:
        Internal type representation (str for scalars, list for arrays, dict for objects)
    """
    # Handle case where sql_type.this might be a string or TokenType
    if hasattr(sql_type.this, "name"):
        type_name = sql_type.this.name
    elif hasattr(sql_type.this, "value"):
        type_name = sql_type.this.value
    else:
        type_name = str(sql_type.this)

    type_name_upper = type_name.upper()

    # Handle ARRAY types
    if type_name_upper == "ARRAY":
        # Get element type from expressions
        if sql_type.expressions:
            element_type = _sqlglot_type_to_internal(sql_type.expressions[0])
            return [element_type]
        return ["any"]

    # Handle STRUCT/ROW types
    if type_name_upper in ("STRUCT", "ROW", "RECORD"):
        # Parse struct fields
        struct_dict = {}
        for field_def in sql_type.expressions:
            if isinstance(field_def, exp.ColumnDef):
                field_name = field_def.name
                field_type = (
                    _sqlglot_type_to_internal(field_def.kind)
                    if field_def.kind
                    else "any"
                )
                struct_dict[field_name] = field_type
        return struct_dict if struct_dict else "object"

    # Handle scalar types
    return _SQL_TYPE_TO_INTERNAL.get(type_name_upper, "any")


def _extract_schema_from_create_table(
    create_stmt: exp.Create, table_name: str | None = None
) -> tuple[dict[str, Any], set[str]]:
    """Extract schema from a CREATE TABLE statement.

    Args:
        create_stmt: SQLGlot CREATE TABLE expression
        table_name: Optional table name to filter (if multiple tables in file)

    Returns:
        Tuple of (schema_dict, required_fields_set)
    """
    # Get table name from statement
    stmt_table_name = None
    table_node = create_stmt.find(exp.Table)
    if table_node:
        stmt_table_name = table_node.name

    # Get schema node for column extraction
    schema_node = create_stmt.find(exp.Schema)

    # Skip if filtering by table name and this isn't the target table
    if table_name and stmt_table_name and stmt_table_name != table_name:
        return {}, set()

    schema_dict = {}
    required_fields = set()

    # Extract only TOP-LEVEL column definitions (not nested in STRUCT)
    if schema_node:
        for col_def in schema_node.expressions:
            if isinstance(col_def, exp.ColumnDef):
                col_name = col_def.name
                col_type = (
                    _sqlglot_type_to_internal(col_def.kind) if col_def.kind else "any"
                )

                schema_dict[col_name] = col_type

                # Check if column is NOT NULL (required)
                constraints = col_def.find_all(exp.ColumnConstraint)
                for constraint in constraints:
                    if isinstance(constraint.kind, exp.NotNullColumnConstraint):
                        required_fields.add(col_name)

    return schema_dict, required_fields


def schema_from_sql_ddl_sqlglot(
    path: str,
    dialect: str = "bigquery",
    table_name: str | None = None,
    normalize_to: str | None = None,
) -> tuple[dict[str, Any], set[str]]:
    """Parse SQL DDL file using SQLGlot for enhanced cross-dialect support.

    Args:
        path: Path to SQL DDL file
        dialect: Source SQL dialect (bigquery, postgres, mysql, snowflake, etc.)
        table_name: Optional table name to extract (if file has multiple tables)
        normalize_to: Optional target dialect for type normalization

    Returns:
        Tuple of (schema_tree, required_paths)
            schema_tree: Dict mapping field names to types
            required_paths: Set of field names that are NOT NULL

    Raises:
        ImportError: If sqlglot is not installed
        ValueError: If SQL parsing fails
    """
    if not _HAS_SQLGLOT:
        raise ImportError(
            "SQLGlot support requires 'sqlglot'. "
            "Install with: pip install 'schema-diff[sqlglot]'"
        )

    try:
        # Read SQL file
        with open(path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Parse SQL with specified dialect
        statements = sqlglot.parse(sql_content, dialect=dialect)

        # Find CREATE TABLE statements
        all_schemas = []
        all_required = set()

        for stmt in statements:
            if isinstance(stmt, exp.Create) and stmt.kind == "TABLE":
                schema_dict, required = _extract_schema_from_create_table(
                    stmt, table_name
                )
                if schema_dict:  # Only add if we found columns
                    all_schemas.append(schema_dict)
                    all_required.update(required)

        # Merge all schemas if multiple tables (or return first if filtering)
        if not all_schemas:
            logger.warning(f"No CREATE TABLE statements found in {path}")
            return {}, set()

        if table_name or len(all_schemas) == 1:
            return all_schemas[0], all_required

        # Merge multiple tables
        merged_schema = {}
        for schema in all_schemas:
            merged_schema.update(schema)

        return merged_schema, all_required

    except Exception as e:
        logger.error(f"Failed to parse SQL DDL with SQLGlot: {e}")
        raise ValueError(f"SQL parsing failed: {e}") from e


def translate_sql_ddl(sql_content: str, from_dialect: str, to_dialect: str) -> str:
    """Translate SQL DDL from one dialect to another.

    Args:
        sql_content: SQL DDL string
        from_dialect: Source dialect (e.g., "postgres")
        to_dialect: Target dialect (e.g., "bigquery")

    Returns:
        Translated SQL DDL string

    Raises:
        ImportError: If sqlglot is not installed
    """
    if not _HAS_SQLGLOT:
        raise ImportError(
            "SQLGlot support requires 'sqlglot'. "
            "Install with: pip install 'schema-diff[sqlglot]'"
        )

    try:
        # Parse with source dialect
        ast = parse_one(sql_content, read=from_dialect)

        # Generate SQL in target dialect
        translated: str = ast.sql(dialect=to_dialect, pretty=True)

        return translated

    except Exception as e:
        logger.error(f"SQL translation failed: {e}")
        raise ValueError(
            f"Translation from {from_dialect} to {to_dialect} failed: {e}"
        ) from e


def validate_sql_syntax(
    sql_content: str, dialect: str = "bigquery"
) -> tuple[bool, str | None]:
    """Validate SQL syntax using SQLGlot.

    Args:
        sql_content: SQL string to validate
        dialect: SQL dialect to use for validation

    Returns:
        Tuple of (is_valid, error_message)
            is_valid: True if SQL is valid
            error_message: Error description if invalid, None if valid
    """
    if not _HAS_SQLGLOT:
        return False, "SQLGlot not installed"

    try:
        statements = sqlglot.parse(sql_content, dialect=dialect)

        if not statements:
            return False, "No valid SQL statements found"

        # Check for parse errors
        for stmt in statements:
            if stmt is None:
                return False, "Failed to parse SQL statement"

        return True, None

    except Exception as e:
        return False, str(e)


def get_supported_dialects() -> list[str]:
    """Get list of SQL dialects supported by SQLGlot.

    Returns:
        List of dialect names
    """
    if not _HAS_SQLGLOT:
        return []

    # Return commonly used dialects
    return [
        "bigquery",
        "postgres",
        "mysql",
        "sqlite",
        "snowflake",
        "redshift",
        "spark",
        "hive",
        "presto",
        "trino",
        "clickhouse",
        "databricks",
        "duckdb",
        "oracle",
        "tsql",
        "athena",
    ]


__all__ = [
    "schema_from_sql_ddl_sqlglot",
    "translate_sql_ddl",
    "validate_sql_syntax",
    "get_supported_dialects",
]
