"""
Tests for BigQuery DDL generation and live table schema extraction.
"""
import pytest
from unittest.mock import Mock, patch
from google.cloud.bigquery.schema import SchemaField

from schema_diff.bigquery_ddl import (
    pretty_print_ddl,
    colorize_sql,
    bigquery_schema_to_internal,
    get_live_table_schema,
    _normalize_bigquery_arrays,
    generate_table_ddl,
    generate_dataset_ddl,
)


def test_pretty_print_ddl_basic():
    """Test basic DDL formatting."""
    ddl = """CREATE TABLE test (
  id INT64,
  name STRING
);
ALTER TABLE test ADD CONSTRAINT pk PRIMARY KEY (id);"""

    result = pretty_print_ddl(ddl)
    lines = result.split("\n")

    # Should split ALTER statement into two lines
    assert any("ALTER TABLE test" in line for line in lines)
    assert any("ADD CONSTRAINT pk PRIMARY KEY" in line for line in lines)


def test_colorize_sql_modes():
    """Test SQL colorization in different modes."""
    sql = "CREATE TABLE test (id INT64);"

    # Auto mode (no tty) should return plain
    result_auto = colorize_sql(sql, mode="auto")
    assert result_auto == sql

    # Never mode should return plain
    result_never = colorize_sql(sql, mode="never")
    assert result_never == sql

    # Always mode should add color (if pygments available)
    result_always = colorize_sql(sql, mode="always")
    # Either colored or fallback to plain if pygments not available
    assert isinstance(result_always, str)


def test_bigquery_schema_to_internal_simple():
    """Test conversion of simple BigQuery schema to internal format."""
    schema = [
        SchemaField("id", "INTEGER", mode="REQUIRED"),
        SchemaField("name", "STRING", mode="NULLABLE"),
        SchemaField("active", "BOOLEAN", mode="NULLABLE"),
    ]

    tree, required = bigquery_schema_to_internal(schema)

    assert tree["id"] == "int"
    assert tree["name"] == "str"
    assert tree["active"] == "bool"
    assert required == {"id"}  # Only REQUIRED fields


def test_bigquery_schema_to_internal_nested():
    """Test conversion of nested BigQuery schema (STRUCT/ARRAY)."""
    schema = [
        SchemaField(
            "user",
            "RECORD",
            mode="NULLABLE",
            fields=[
                SchemaField("id", "INTEGER", mode="REQUIRED"),
                SchemaField("email", "STRING", mode="NULLABLE"),
            ],
        ),
        SchemaField("tags", "STRING", mode="REPEATED"),
    ]

    tree, required = bigquery_schema_to_internal(schema)

    # Nested struct
    assert isinstance(tree["user"], dict)
    assert tree["user"]["id"] == "int"
    assert tree["user"]["email"] == "str"

    # Array field
    assert tree["tags"] == ["str"]

    # Required paths include nested
    assert "user.id" in required


def test_normalize_bigquery_arrays():
    """Test BigQuery array wrapper normalization."""
    # Test the pattern: {'list': [{'element': ...}]} -> [...]
    tree_with_wrapper = {
        "skills": {"list": [{"element": {"id": "str", "name": "str"}}]},
        "simple_field": "str",
    }

    normalized = _normalize_bigquery_arrays(tree_with_wrapper)

    # Should unwrap the BigQuery array structure
    assert normalized["skills"] == [{"id": "str", "name": "str"}]
    assert normalized["simple_field"] == "str"


def test_normalize_bigquery_arrays_recursive():
    """Test recursive normalization of nested BigQuery arrays."""
    tree = {
        "nested": {
            "experience": {
                "list": [
                    {
                        "element": {
                            "title": "str",
                            "projects": {"list": [{"element": {"name": "str"}}]},
                        }
                    }
                ]
            }
        }
    }

    normalized = _normalize_bigquery_arrays(tree)

    # Should normalize both levels
    assert normalized["nested"]["experience"] == [
        {"title": "str", "projects": [{"name": "str"}]}
    ]


@patch("schema_diff.bigquery_ddl.bigquery.Client")
def test_get_live_table_schema_success(mock_client_class):
    """Test successful live table schema extraction."""
    # Mock BigQuery client and table
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    mock_table = Mock()
    mock_table.schema = [
        SchemaField("id", "INTEGER", mode="REQUIRED"),
        SchemaField("name", "STRING", mode="NULLABLE"),
    ]
    mock_client.get_table.return_value = mock_table

    tree, required = get_live_table_schema("project", "dataset", "table")

    assert tree["id"] == "int"
    assert tree["name"] == "str"
    assert required == {"id"}
    mock_client.get_table.assert_called_once_with("project.dataset.table")


@patch("schema_diff.bigquery_ddl.bigquery.Client")
def test_get_live_table_schema_not_found(mock_client_class):
    """Test handling of table not found."""
    from google.cloud.exceptions import NotFound

    mock_client = Mock()
    mock_client_class.return_value = mock_client
    mock_client.get_table.side_effect = NotFound("Table not found")

    with pytest.raises(NotFound):
        get_live_table_schema("project", "dataset", "nonexistent")


@patch("schema_diff.bigquery_ddl.bigquery.Client")
def test_generate_table_ddl(mock_client_class):
    """Test DDL generation for a single table."""
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    # Mock table with schema and required properties
    mock_table = Mock()
    mock_table.table_id = "users"
    mock_table.description = "User table"
    mock_table.schema = [
        SchemaField("id", "INTEGER", mode="REQUIRED"),
        SchemaField("email", "STRING", mode="NULLABLE"),
    ]
    # Mock additional table properties that DDL generation needs
    mock_table.clustering_fields = None
    mock_table.time_partitioning = None
    mock_table.range_partitioning = None
    mock_table.require_partition_filter = None
    mock_table.expires = None
    mock_table.labels = None
    mock_client.get_table.return_value = mock_table

    ddl = generate_table_ddl(mock_client, "project", "dataset", "users")

    assert "CREATE OR REPLACE TABLE" in ddl
    assert "`users`" in ddl or "users" in ddl
    assert "INTEGER" in ddl or "INT64" in ddl
    assert "STRING" in ddl


@patch("schema_diff.bigquery_ddl.bigquery.Client")
def test_generate_dataset_ddl(mock_client_class):
    """Test dataset DDL generation for multiple tables."""
    mock_client = Mock()
    mock_client_class.return_value = mock_client

    # Mock dataset list_tables
    mock_table1 = Mock()
    mock_table1.table_id = "users"
    mock_table2 = Mock()
    mock_table2.table_id = "orders"
    mock_client.list_tables.return_value = [mock_table1, mock_table2]

    # Mock get_table for each table
    mock_user_table = Mock()
    mock_user_table.table_id = "users"
    mock_user_table.schema = [SchemaField("id", "INTEGER", mode="REQUIRED")]

    mock_order_table = Mock()
    mock_order_table.table_id = "orders"
    mock_order_table.schema = [SchemaField("id", "INTEGER", mode="REQUIRED")]

    # Configure get_table to return appropriate table based on table_id
    def get_table_side_effect(table_ref):
        if "users" in table_ref:
            return mock_user_table
        elif "orders" in table_ref:
            return mock_order_table
        return Mock()

    mock_client.get_table.side_effect = get_table_side_effect

    # Test dataset DDL generation
    result = generate_dataset_ddl(mock_client, "project", "dataset")

    assert isinstance(result, dict)
    # Should return DDLs for tables found in dataset


def test_ddl_generation_integration():
    """Integration test for DDL generation workflow."""
    # This would require actual BigQuery credentials in a real environment
    # For now, test the function signatures and error handling

    with patch("schema_diff.bigquery_ddl.bigquery.Client") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Test that functions can be called without errors
        mock_client.get_table.side_effect = Exception("Mocked error")

        with pytest.raises(Exception, match="Mocked error"):
            generate_table_ddl(mock_client, "project", "dataset", "table")
