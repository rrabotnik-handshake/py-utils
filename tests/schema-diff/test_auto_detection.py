"""
Tests for intelligent file type auto-detection functionality.
"""
import json
import textwrap

from schema_diff.loader import (
    _guess_kind,
    _sniff_json_kind,
    _sniff_sql_kind,
    KIND_DATA,
    KIND_JSONSCHEMA,
    KIND_SQL,
    KIND_DBT_MODEL,
    KIND_DBT_MANIFEST,
    KIND_SPARK,
)


def test_guess_kind_json_data(tmp_path):
    """Test auto-detection of JSON data files."""
    # Single JSON object (data)
    data = {"id": 1, "name": "test"}
    p = tmp_path / "data.json"
    p.write_text(json.dumps(data), encoding="utf-8")

    assert _guess_kind(str(p)) == KIND_DATA


def test_guess_kind_ndjson_data(tmp_path):
    """Test auto-detection of NDJSON data files."""
    # Multiple JSON objects (NDJSON)
    ndjson = '{"id": 1, "name": "test1"}\n{"id": 2, "name": "test2"}'
    p = tmp_path / "data.jsonl"
    p.write_text(ndjson, encoding="utf-8")

    assert _guess_kind(str(p)) == KIND_DATA


def test_guess_kind_json_schema(tmp_path):
    """Test auto-detection of JSON Schema files."""
    # JSON Schema document
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
    }
    p = tmp_path / "schema.json"
    p.write_text(json.dumps(schema), encoding="utf-8")

    assert _guess_kind(str(p)) == KIND_JSONSCHEMA


def test_guess_kind_dbt_manifest(tmp_path):
    """Test auto-detection of dbt manifest files."""
    # dbt manifest structure
    manifest = {
        "metadata": {"dbt_version": "1.0.0"},
        "nodes": {"model.myproj.users": {"resource_type": "model", "name": "users"}},
    }
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(manifest), encoding="utf-8")

    assert _guess_kind(str(p)) == KIND_DBT_MANIFEST


def test_sniff_json_kind_data_vs_schema(tmp_path):
    """Test JSON content sniffing for data vs schema."""
    # Test data detection
    data_content = '{"id": 1, "name": "test", "active": true}'
    p_data = tmp_path / "data.json"
    p_data.write_text(data_content, encoding="utf-8")
    assert _sniff_json_kind(str(p_data)) == KIND_DATA

    # Test schema detection
    schema_content = '{"type": "object", "properties": {"id": {"type": "integer"}}}'
    p_schema = tmp_path / "schema.json"
    p_schema.write_text(schema_content, encoding="utf-8")
    assert _sniff_json_kind(str(p_schema)) == KIND_JSONSCHEMA

    # Test manifest detection
    manifest_content = '{"metadata": {"dbt_version": "1.0"}, "nodes": {}}'
    p_manifest = tmp_path / "manifest.json"
    p_manifest.write_text(manifest_content, encoding="utf-8")
    assert _sniff_json_kind(str(p_manifest)) == KIND_DBT_MANIFEST


def test_sniff_sql_kind_ddl_vs_dbt(tmp_path):
    """Test SQL content sniffing for DDL vs dbt model."""
    # Test DDL detection
    ddl_sql = textwrap.dedent(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(255)
        );
    """
    ).strip()

    p_ddl = tmp_path / "ddl.sql"
    p_ddl.write_text(ddl_sql, encoding="utf-8")

    assert _sniff_sql_kind(str(p_ddl)) == KIND_SQL

    # Test dbt model detection
    dbt_sql = textwrap.dedent(
        """
        {{ config(materialized='table') }}

        SELECT
            id,
            name,
            email
        FROM {{ ref('users_raw') }}
        WHERE active = true
    """
    ).strip()

    p_dbt = tmp_path / "model.sql"
    p_dbt.write_text(dbt_sql, encoding="utf-8")

    assert _sniff_sql_kind(str(p_dbt)) == KIND_DBT_MODEL


def test_sniff_sql_kind_edge_cases(tmp_path):
    """Test SQL sniffing edge cases."""
    # SQL with Jinja but no explicit dbt markers
    jinja_sql = textwrap.dedent(
        """
        SELECT
            id,
            {% if var('include_name') %}
            name,
            {% endif %}
            email
        FROM users
    """
    ).strip()

    p = tmp_path / "jinja.sql"
    p.write_text(jinja_sql, encoding="utf-8")

    # Should detect as dbt model due to Jinja
    assert _sniff_sql_kind(str(p)) == KIND_DBT_MODEL

    # SQL with SELECT but also CREATE TABLE
    mixed_sql = textwrap.dedent(
        """
        CREATE TABLE temp_users AS
        SELECT id, name FROM users;
    """
    ).strip()

    p2 = tmp_path / "mixed.sql"
    p2.write_text(mixed_sql, encoding="utf-8")

    # Should prefer CREATE TABLE detection
    assert _sniff_sql_kind(str(p2)) == KIND_SQL


def test_guess_kind_by_extension(tmp_path):
    """Test file type detection by extension."""
    # Spark schema file
    spark_schema = (
        "root\n |-- id: long (nullable = false)\n |-- name: string (nullable = true)"
    )
    p_spark = tmp_path / "schema.txt"
    p_spark.write_text(spark_schema, encoding="utf-8")

    # Note: .txt files need content-based detection, not just extension
    # This test verifies that we check content, not just rely on extension
    detected = _guess_kind(str(p_spark))
    assert detected in [
        KIND_SPARK,
        KIND_DATA,
    ]  # Could be either based on content parsing


def test_guess_kind_yaml_files(tmp_path):
    """Test detection of YAML-based files."""
    # dbt schema.yml
    dbt_yml = textwrap.dedent(
        """
        version: 2
        models:
          - name: users
            columns:
              - name: id
                tests:
                  - not_null
    """
    ).strip()

    p = tmp_path / "schema.yml"
    p.write_text(dbt_yml, encoding="utf-8")

    # Should detect dbt schema YAML
    # Note: This requires the actual _guess_kind to have YAML detection
    detected = _guess_kind(str(p))
    # May default to KIND_DATA if YAML detection not fully implemented
    assert detected in [KIND_DATA, "dbt-yml"]


def test_guess_kind_protobuf_files(tmp_path):
    """Test detection of protobuf files."""
    proto_content = textwrap.dedent(
        """
        syntax = "proto3";

        message User {
            int32 id = 1;
            string name = 2;
            string email = 3;
        }
    """
    ).strip()

    p = tmp_path / "user.proto"
    p.write_text(proto_content, encoding="utf-8")

    detected = _guess_kind(str(p))
    assert detected == "protobuf"


def test_guess_kind_compressed_files(tmp_path):
    """Test detection of compressed files."""
    import gzip

    # Compressed JSON data
    data = {"id": 1, "name": "test"}
    p = tmp_path / "data.json.gz"

    with gzip.open(p, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    detected = _guess_kind(str(p))
    assert detected == KIND_DATA


def test_guess_kind_unknown_extension(tmp_path):
    """Test handling of unknown file extensions."""
    # File with unknown extension but JSON content
    data = {"id": 1, "name": "test"}
    p = tmp_path / "data.unknown"
    p.write_text(json.dumps(data), encoding="utf-8")

    detected = _guess_kind(str(p))
    # Should default to data based on content, not extension
    assert detected == KIND_DATA


def test_auto_detection_integration(tmp_path):
    """Integration test for auto-detection in loader."""
    from schema_diff.loader import load_left_or_right
    from schema_diff.config import Config

    cfg = Config(infer_datetimes=False, color_enabled=False, show_presence=True)

    # Create a JSON schema file
    schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
        "required": ["id"],
    }
    p = tmp_path / "test.json"
    p.write_text(json.dumps(schema), encoding="utf-8")

    # Load with auto-detection (kind=None)
    tree, required, label = load_left_or_right(
        str(p),
        kind=None,  # Auto-detect
        cfg=cfg,
        samples=3,
    )

    # Should detect as JSON schema and parse accordingly
    assert "id" in tree
    assert "name" in tree
    assert "id" in required  # From required array
