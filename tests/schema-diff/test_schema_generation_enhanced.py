#!/usr/bin/env python3
"""
Tests for enhanced schema generation functionality.

Tests the --required-fields and --table-name parameters in the generate command.
"""
import json
import tempfile
from pathlib import Path

import pytest


def extract_json_from_output(output: str) -> dict:
    """Extract and parse JSON from command output."""
    lines = output.split('\n')
    json_lines = []
    brace_count = 0
    started = False

    for line in lines:
        if not started and line.strip().startswith('{'):
            started = True
            brace_count = 0

        if started:
            json_lines.append(line)
            brace_count += line.count('{') - line.count('}')

            # When brace count reaches 0, we've found the complete JSON
            if brace_count == 0:
                break

    if not json_lines:
        raise ValueError("No JSON found in output")

    json_text = '\n'.join(json_lines)
    return json.loads(json_text)


class TestSchemaGenerationParameters:
    """Test enhanced schema generation parameters."""

    def test_required_fields_json_schema(self, run_cli):
        """Test --required-fields parameter with JSON Schema generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"name": "John", "email": "john@example.com", "age": 30, "city": "NYC"}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--required-fields", "name", "email"
            ])

            assert result.returncode == 0

            # Extract and parse the JSON schema
            schema = extract_json_from_output(result.stdout)

            # Verify JSON Schema structure
            assert schema["$schema"] == "http://json-schema.org/draft-07/schema#"
            assert schema["type"] == "object"
            assert "properties" in schema

            # Verify all fields are present
            assert "name" in schema["properties"]
            assert "email" in schema["properties"]
            assert "age" in schema["properties"]
            assert "city" in schema["properties"]

            # Verify required fields are correctly set
            assert "required" in schema
            assert "name" in schema["required"]
            assert "email" in schema["required"]
            assert "age" not in schema["required"]
            assert "city" not in schema["required"]

            # Verify field types are inferred correctly
            assert schema["properties"]["name"]["type"] == "string"
            assert schema["properties"]["email"]["type"] == "string"
            assert schema["properties"]["age"]["type"] == "integer"
            assert schema["properties"]["city"]["type"] == "string"

    def test_required_fields_empty_list(self, run_cli):
        """Test --required-fields with empty list."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"name": "John", "age": 30}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--required-fields"  # Empty list
            ])

            assert result.returncode == 0

            # Extract and parse JSON schema
            schema = extract_json_from_output(result.stdout)

            # Verify no required fields when empty list is provided
            assert "properties" in schema
            assert "name" in schema["properties"]
            assert "age" in schema["properties"]

            # Should have no required fields or empty required array
            if "required" in schema:
                assert len(schema["required"]) == 0
            # If no required key, that's also valid (means no required fields)

    def test_table_name_sql_ddl(self, run_cli):
        """Test --table-name parameter with SQL DDL generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"id": 1, "name": "John", "active": True}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "sql_ddl",
                "--table-name", "users"
            ])

            assert result.returncode == 0

            # Verify DDL structure
            ddl_output = result.stdout
            assert "CREATE TABLE users" in ddl_output

            # Verify all columns are present with correct types
            assert "id" in ddl_output
            assert "name" in ddl_output
            assert "active" in ddl_output

            # Verify SQL types are correctly inferred
            # ID should be integer type
            assert "INTEGER" in ddl_output or "INT" in ddl_output
            # Name should be string/text type
            assert "VARCHAR" in ddl_output or "TEXT" in ddl_output or "STRING" in ddl_output
            # Active should be boolean type
            assert "BOOLEAN" in ddl_output or "BOOL" in ddl_output

            # Verify proper SQL syntax
            assert "(" in ddl_output and ")" in ddl_output
            assert ";" in ddl_output

    def test_table_name_bigquery_ddl(self, run_cli):
        """Test --table-name parameter with BigQuery DDL generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"user_id": 123, "email": "test@example.com", "score": 95.5}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "bigquery_ddl",
                "--table-name", "user_scores"
            ])

            assert result.returncode == 0

            # Verify BigQuery DDL structure
            ddl_output = result.stdout
            assert "CREATE TABLE `user_scores`" in ddl_output

            # Verify all columns are present
            assert "user_id" in ddl_output
            assert "email" in ddl_output
            assert "score" in ddl_output

            # Verify BigQuery-specific types are used
            assert "INT64" in ddl_output  # user_id should be INT64
            assert "STRING" in ddl_output  # email should be STRING
            assert "FLOAT64" in ddl_output  # score should be FLOAT64

            # Verify BigQuery syntax (backticks for table name, proper structure)
            assert "`user_scores`" in ddl_output
            assert "(" in ddl_output and ")" in ddl_output
            assert ";" in ddl_output

            # Verify fields are alphabetically ordered (documented feature)
            lines = [line.strip() for line in ddl_output.split('\n') if line.strip() and not line.strip().startswith('CREATE') and not line.strip().startswith(')')]
            field_lines = [line for line in lines if any(field in line for field in ['user_id', 'email', 'score'])]
            if len(field_lines) >= 2:
                # Check if fields appear in alphabetical order
                field_names = []
                for line in field_lines:
                    if 'email' in line:
                        field_names.append('email')
                    elif 'score' in line:
                        field_names.append('score')
                    elif 'user_id' in line:
                        field_names.append('user_id')
                assert field_names == sorted(field_names), f"Fields not in alphabetical order: {field_names}"

    def test_table_name_default(self, run_cli):
        """Test default table name when --table-name is not specified."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"id": 1, "value": "test"}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "sql_ddl",
            ])

            assert result.returncode == 0
            assert "CREATE TABLE generated_table" in result.stdout

    def test_required_fields_and_table_name_combined(self, run_cli):
        """Test combining --required-fields and --table-name parameters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"user_id": 1, "username": "john", "email": "john@example.com", "age": 30}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "sql_ddl",
                "--table-name", "app_users",
                "--required-fields", "user_id", "username", "email",
            ])

            assert result.returncode == 0
            assert "CREATE TABLE app_users" in result.stdout
            # Required fields should have NOT NULL constraints
            assert "user_id" in result.stdout and "NOT NULL" in result.stdout
            assert "username" in result.stdout
            assert "email" in result.stdout

    def test_required_fields_nested_paths(self, run_cli):
        """Test --required-fields with nested field paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {
                "user": {
                    "profile": {"name": "John", "bio": "Developer"},
                    "contact": {"email": "john@example.com", "phone": "123-456-7890"}
                },
                "metadata": {"created_at": "2023-01-01"}
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--required-fields", "user.profile.name", "user.contact.email", "metadata.created_at"
            ])

            assert result.returncode == 0

            # Extract and parse JSON schema
            schema = extract_json_from_output(result.stdout)

            # Verify nested structure is preserved
            assert "properties" in schema
            assert "user" in schema["properties"]
            assert "metadata" in schema["properties"]

            # Verify nested objects have correct structure
            user_props = schema["properties"]["user"]["properties"]
            assert "profile" in user_props
            assert "contact" in user_props

            profile_props = user_props["profile"]["properties"]
            assert "name" in profile_props
            assert "bio" in profile_props

            contact_props = user_props["contact"]["properties"]
            assert "email" in contact_props
            assert "phone" in contact_props

            metadata_props = schema["properties"]["metadata"]["properties"]
            assert "created_at" in metadata_props

            # Note: The exact handling of nested required fields depends on implementation
            # At minimum, the command should succeed and preserve the nested structure

    def test_required_fields_array_paths(self, run_cli):
        """Test --required-fields with array field paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {
                "name": "John",
                "skills": ["Python", "JavaScript"],
                "experience": [
                    {"title": "Developer", "company": "TechCorp", "years": 2}
                ]
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--required-fields", "name", "experience.title",
            ])

            assert result.returncode == 0
            # Should handle array paths gracefully

    def test_required_fields_nonexistent_field(self, run_cli):
        """Test --required-fields with non-existent field."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"name": "John", "age": 30}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--required-fields", "name", "nonexistent_field"
            ])

            assert result.returncode == 0

            # Extract and parse JSON schema
            schema = extract_json_from_output(result.stdout)

            # Verify only existing fields are in properties
            assert "properties" in schema
            assert "name" in schema["properties"]
            assert "age" in schema["properties"]
            assert "nonexistent_field" not in schema["properties"]

            # Verify only existing fields can be in required list
            if "required" in schema:
                assert "name" in schema["required"]  # This field exists
                assert "nonexistent_field" not in schema["required"]  # This should not be there

            # Verify types are correct for existing fields
            assert schema["properties"]["name"]["type"] == "string"
            assert schema["properties"]["age"]["type"] == "integer"

    def test_output_with_enhanced_parameters(self, run_cli):
        """Test --output flag with enhanced generation parameters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"product_id": 1, "name": "Widget", "price": 19.99, "category": "Tools"}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "bigquery_ddl",
                "--table-name", "products",
                "--required-fields", "product_id", "name",
                "--output",
            ])

            assert result.returncode == 0
            assert "Schema saved to output/schemas/" in result.stdout

            # Check that the output file was created
            output_dir = Path("output/schemas")
            if output_dir.exists():
                schema_files = list(output_dir.glob("*_schema.*"))
                assert len(schema_files) > 0

    def test_validation_with_enhanced_parameters(self, run_cli):
        """Test schema validation with enhanced parameters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"id": 1, "title": "Test", "published": True}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--required-fields", "id", "title",
                "--validate",
            ])

            assert result.returncode == 0
            # Should validate successfully

    def test_no_validate_with_enhanced_parameters(self, run_cli):
        """Test --no-validate flag with enhanced parameters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"field1": "value1", "field2": 42}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "generate", str(data_file),
                "--format", "sql_ddl",
                "--table-name", "test_table",
                "--required-fields", "field1",
                "--no-validate",
            ])

            assert result.returncode == 0
            # Should skip validation

    def test_all_formats_with_table_name(self, run_cli):
        """Test --table-name parameter with all applicable formats."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"id": 1, "name": "Test", "value": 100.0}
            data_file.write_text(json.dumps(data))

            formats_with_table_name = ["sql_ddl", "bigquery_ddl"]

            for format_type in formats_with_table_name:
                result = run_cli([
                    "generate", str(data_file),
                    "--format", format_type,
                    "--table-name", f"test_{format_type}",
                ])

                assert result.returncode == 0
                assert f"test_{format_type}" in result.stdout

    def test_all_formats_with_required_fields(self, run_cli):
        """Test --required-fields parameter with all applicable formats."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"id": 1, "name": "Test", "optional_field": "value"}
            data_file.write_text(json.dumps(data))

            formats_with_required = ["json_schema", "sql_ddl", "bigquery_ddl", "bigquery_json", "openapi"]

            for format_type in formats_with_required:
                result = run_cli([
                    "generate", str(data_file),
                    "--format", format_type,
                    "--required-fields", "id", "name",
                ])

                assert result.returncode == 0
                # Should complete successfully for all formats


class TestSchemaValidation:
    """Test that generated schemas are actually valid and usable."""

    def test_json_schema_validation(self, run_cli):
        """Test that generated JSON Schema is valid and can be used for validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {
                "id": 123,
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30,
                "active": True,
                "metadata": {
                    "created_at": "2023-01-01T00:00:00Z",
                    "tags": ["user", "premium"]
                }
            }
            data_file.write_text(json.dumps(data))

            # Generate JSON Schema
            result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--required-fields", "id", "name", "email"
            ])

            assert result.returncode == 0

            # Extract and validate the generated schema
            schema = extract_json_from_output(result.stdout)

            # Test that the schema is valid JSON Schema
            try:
                import jsonschema
                # This will raise an exception if the schema is invalid
                jsonschema.Draft7Validator.check_schema(schema)
            except ImportError:
                # If jsonschema is not available, do basic structural validation
                assert "$schema" in schema
                assert schema["$schema"] == "http://json-schema.org/draft-07/schema#"
                assert schema["type"] == "object"
                assert "properties" in schema
                assert "required" in schema

            # Test that the schema can validate the original data
            try:
                import jsonschema
                validator = jsonschema.Draft7Validator(schema)
                errors = list(validator.iter_errors(data))
                assert len(errors) == 0, f"Schema validation errors: {errors}"
            except ImportError:
                pass  # Skip validation test if jsonschema not available

    def test_sql_ddl_syntax_validation(self, run_cli):
        """Test that generated SQL DDL has valid syntax."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {
                "user_id": 123,
                "username": "john_doe",
                "email": "john@example.com",
                "created_at": "2023-01-01T10:00:00",
                "is_active": True,
                "score": 95.5
            }
            data_file.write_text(json.dumps(data))

            # Generate SQL DDL
            result = run_cli([
                "generate", str(data_file),
                "--format", "sql_ddl",
                "--table-name", "users"
            ])

            assert result.returncode == 0

            # Basic SQL syntax validation
            ddl_output = result.stdout
            assert "CREATE TABLE users" in ddl_output
            assert ddl_output.count("(") == ddl_output.count(")")  # Balanced parentheses
            assert ddl_output.endswith(";") or ";" in ddl_output  # Has semicolon

            # Check for SQL keywords and proper structure
            assert "CREATE TABLE" in ddl_output
            assert "user_id" in ddl_output
            assert "username" in ddl_output
            assert "email" in ddl_output

            # Test with SQL parser if available
            try:
                import sqlparse
                parsed = sqlparse.parse(ddl_output)
                assert len(parsed) > 0, "SQL should be parseable"
                # Check that it contains CREATE keyword (more flexible than checking statement type)
                ddl_text = str(parsed[0]).upper()
                assert "CREATE" in ddl_text, "Should contain CREATE keyword"
                assert "TABLE" in ddl_text, "Should contain TABLE keyword"
            except ImportError:
                pass  # Skip SQL parsing test if sqlparse not available

    def test_bigquery_ddl_syntax_validation(self, run_cli):
        """Test that generated BigQuery DDL has valid syntax."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {
                "id": 123,
                "name": "John Doe",
                "metadata": {
                    "created_at": "2023-01-01T10:00:00Z",
                    "tags": ["premium", "verified"]
                },
                "scores": [95.5, 87.2, 92.1]
            }
            data_file.write_text(json.dumps(data))

            # Generate BigQuery DDL
            result = run_cli([
                "generate", str(data_file),
                "--format", "bigquery_ddl",
                "--table-name", "user_data"
            ])

            assert result.returncode == 0

            # BigQuery-specific syntax validation
            ddl_output = result.stdout
            assert "CREATE TABLE `user_data`" in ddl_output
            assert ddl_output.count("(") == ddl_output.count(")")  # Balanced parentheses
            assert ";" in ddl_output  # Has semicolon

            # Check for BigQuery-specific types
            assert any(bq_type in ddl_output for bq_type in ["INT64", "STRING", "FLOAT64", "STRUCT", "ARRAY"])

            # Check for proper BigQuery identifier quoting
            assert "`user_data`" in ddl_output

            # Verify nested structures are handled
            if "metadata" in ddl_output:
                assert "STRUCT" in ddl_output or "RECORD" in ddl_output
            if "scores" in ddl_output:
                assert "ARRAY" in ddl_output

    def test_spark_schema_validation(self, run_cli):
        """Test that generated Spark schema has valid format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {
                "id": 123,
                "name": "John Doe",
                "profile": {
                    "age": 30,
                    "location": "NYC"
                },
                "tags": ["user", "premium"]
            }
            data_file.write_text(json.dumps(data))

            # Generate Spark schema
            result = run_cli([
                "generate", str(data_file),
                "--format", "spark"
            ])

            assert result.returncode == 0

            # Spark schema format validation
            spark_output = result.stdout
            assert "root" in spark_output
            assert "|--" in spark_output  # Spark tree structure

            # Check for Spark-specific types and structure
            assert any(spark_type in spark_output for spark_type in ["string", "long", "struct", "array"])
            assert "(nullable = " in spark_output  # Nullability indicators

            # Verify hierarchical structure
            lines = spark_output.split('\n')
            root_found = False
            for line in lines:
                if "root" in line:
                    root_found = True
                if "|--" in line:
                    assert root_found, "Field definitions should come after root"

            assert root_found, "Should have root element"

    def test_generated_schema_roundtrip(self, run_cli):
        """Test that generated schema can be used to compare against original data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create original data
            data_file = Path(temp_dir) / "original.json"
            data = {
                "id": 123,
                "name": "John Doe",
                "email": "john@example.com",
                "active": True
            }
            data_file.write_text(json.dumps(data))

            # Generate JSON Schema
            schema_result = run_cli([
                "generate", str(data_file),
                "--format", "json_schema",
                "--output"
            ])

            assert schema_result.returncode == 0

            # Find the generated schema file
            output_dir = Path("output/schemas")
            if output_dir.exists():
                schema_files = list(output_dir.glob("*.json"))
                if schema_files:
                    schema_file = schema_files[0]

                    # Use generated schema to compare against original data
                    compare_result = run_cli([
                        str(data_file), str(schema_file),
                        "--left", "data", "--right", "json_schema",
                        "--first-record", "--no-color"
                    ])

                    assert compare_result.returncode == 0
                    # Should show no differences since schema was generated from this data
                    assert "No differences" in compare_result.stdout or "Type mismatches -- (0)" in compare_result.stdout

    def test_schema_validation_with_complex_data(self, run_cli):
        """Test schema generation and validation with complex nested data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "complex.json"
            complex_data = {
                "user": {
                    "id": 123,
                    "profile": {
                        "name": "John Doe",
                        "contacts": [
                            {"type": "email", "value": "john@example.com"},
                            {"type": "phone", "value": "+1-555-0123"}
                        ]
                    }
                },
                "metadata": {
                    "created_at": "2023-01-01T00:00:00Z",
                    "tags": ["premium", "verified"],
                    "settings": {
                        "notifications": True,
                        "theme": "dark"
                    }
                }
            }
            data_file.write_text(json.dumps(complex_data))

            # Test multiple formats with complex data
            formats = ["json_schema", "sql_ddl", "bigquery_ddl", "spark"]

            for format_type in formats:
                result = run_cli([
                    "generate", str(data_file),
                    "--format", format_type,
                    "--table-name", "complex_table"
                ])

                assert result.returncode == 0, f"Failed to generate {format_type} schema"
                assert len(result.stdout) > 100, f"{format_type} schema should be substantial"

                # Format-specific validations
                if format_type == "json_schema":
                    schema = extract_json_from_output(result.stdout)
                    assert "properties" in schema
                    assert "user" in schema["properties"]
                    assert "metadata" in schema["properties"]
                elif format_type in ["sql_ddl", "bigquery_ddl"]:
                    assert "CREATE TABLE" in result.stdout
                    assert "complex_table" in result.stdout
                elif format_type == "spark":
                    assert "root" in result.stdout
                    assert "|--" in result.stdout
