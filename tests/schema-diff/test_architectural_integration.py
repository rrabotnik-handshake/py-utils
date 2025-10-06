#!/usr/bin/env python3
"""
Architectural integration tests for schema-diff.

These tests verify that all major comparison paths work correctly and should
catch architectural issues that unit tests might miss.
"""
import json
import tempfile
from pathlib import Path

import pytest


class TestComparisonPaths:
    """Test all major comparison paths work correctly."""

    def test_data_to_data_comparison(self, tmp_path, run_cli):
        """Test data-to-data comparisons work without explicit --left/--right flags."""
        file1 = tmp_path / "data1.json"
        file2 = tmp_path / "data2.json"

        # Create test data with differences
        data1 = {"id": 1, "name": "Alice", "type": "user"}
        data2 = {"id": 2, "name": "Bob", "type": "admin"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # Should work without --left/--right (auto-detection)
        result = run_cli([str(file1), str(file2), "--no-color"])
        assert result.returncode == 0
        assert "Schema diff" in result.stdout
        assert "No differences" in result.stdout  # Same schema structure

    def test_data_to_data_with_explicit_flags(self, tmp_path, run_cli):
        """Test data-to-data comparisons with explicit --left data --right data."""
        file1 = tmp_path / "data1.json"
        file2 = tmp_path / "data2.json"

        data1 = {"id": 1, "value": "test"}
        data2 = {"id": "1", "value": "test"}  # Different type for id

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        result = run_cli([
            str(file1), str(file2),
            "--left", "data", "--right", "data", "--no-color"
        ])
        assert result.returncode == 0
        assert "Type mismatches" in result.stdout
        assert "int → str" in result.stdout

    def test_data_to_schema_comparison(self, tmp_path, run_cli):
        """Test data-to-schema comparisons work correctly."""
        data_file = tmp_path / "data.json"
        schema_file = tmp_path / "schema.json"

        data = {"id": 1, "name": "Alice"}
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string"}  # Extra field in schema
            },
            "required": ["id", "name"]
        }

        data_file.write_text(json.dumps(data))
        schema_file.write_text(json.dumps(schema))

        result = run_cli([
            str(data_file), str(schema_file),
            "--left", "data", "--right", "json_schema", "--no-color"
        ])
        assert result.returncode == 0
        assert "Only in" in result.stdout
        assert "email" in result.stdout
        assert "Skipped for data-to-schema comparison" in result.stdout

    def test_schema_to_schema_comparison(self, tmp_path, run_cli):
        """Test schema-to-schema comparisons work correctly."""
        schema1_file = tmp_path / "schema1.json"
        schema2_file = tmp_path / "schema2.json"

        schema1 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            },
            "required": ["id"]
        }

        schema2 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            },
            "required": ["id", "name"]  # name becomes required
        }

        schema1_file.write_text(json.dumps(schema1))
        schema2_file.write_text(json.dumps(schema2))

        result = run_cli([
            str(schema1_file), str(schema2_file),
            "--left", "json_schema", "--right", "json_schema", "--no-color"
        ])
        assert result.returncode == 0
        assert "Missing Data / NULL-ABILITY" in result.stdout
        assert "nullable str → str" in result.stdout

    def test_all_comparison_combinations_matrix(self, tmp_path, run_cli):
        """Test matrix of all supported comparison combinations."""
        # Create test files
        data_file = tmp_path / "test.json"
        schema_file = tmp_path / "schema.json"
        spark_file = tmp_path / "schema.txt"

        data = {"id": 1, "name": "test"}
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            }
        }
        spark_schema = "root\n |-- id: integer (nullable = true)\n |-- name: string (nullable = true)"

        data_file.write_text(json.dumps(data))
        schema_file.write_text(json.dumps(schema))
        spark_file.write_text(spark_schema)

        # Test combinations
        combinations = [
            ("data", "data", data_file, data_file),
            ("data", "json_schema", data_file, schema_file),
            ("json_schema", "json_schema", schema_file, schema_file),
            ("json_schema", "spark", schema_file, spark_file),
        ]

        for left_type, right_type, left_file, right_file in combinations:
            result = run_cli([
                str(left_file), str(right_file),
                "--left", left_type, "--right", right_type, "--no-color"
            ])
            assert result.returncode == 0, f"Failed for {left_type} → {right_type}"
            assert "Schema diff" in result.stdout

    def test_cross_format_with_sql_tables(self, tmp_path, run_cli):
        """Test cross-format comparisons involving SQL with table selection."""
        # Create JSON Schema
        schema_file = tmp_path / "schema.json"
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
            "required": ["id", "name"]
        }
        schema_file.write_text(json.dumps(schema))

        # Create SQL with single table
        sql_file = tmp_path / "schema.sql"
        sql_file.write_text("CREATE TABLE users (id BIGINT NOT NULL, name TEXT NOT NULL);")

        # Test jsonschema vs sql with explicit table
        result = run_cli([
            str(schema_file), str(sql_file),
            "--left", "json_schema", "--right", "sql",
            "--right-table", "users", "--no-color"
        ])
        assert result.returncode == 0
        assert "Schema diff" in result.stdout
        assert "No differences" in result.stdout or "Type mismatches -- (0)" in result.stdout

    def test_multi_table_sql_selection(self, tmp_path, run_cli):
        """Test SQL comparison with multiple tables requiring explicit selection."""
        # Create JSON Schema for integer id
        schema_file = tmp_path / "schema.json"
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"]
        }
        schema_file.write_text(json.dumps(schema))

        # Create SQL with multiple tables
        sql_file = tmp_path / "multi.sql"
        sql_content = """
CREATE TABLE table_a (
  id BIGINT NOT NULL
);

CREATE TABLE table_b (
  id TEXT NOT NULL
);
"""
        sql_file.write_text(sql_content)

        # Compare against table_a (should match - integer types)
        result_match = run_cli([
            str(schema_file), str(sql_file),
            "--left", "json_schema", "--right", "sql",
            "--right-table", "table_a", "--no-color"
        ])
        assert result_match.returncode == 0
        assert "Schema diff" in result_match.stdout
        assert "No differences" in result_match.stdout or "Type mismatches -- (0)" in result_match.stdout

        # Compare against table_b (should mismatch - int vs string)
        result_mismatch = run_cli([
            str(schema_file), str(sql_file),
            "--left", "json_schema", "--right", "sql",
            "--right-table", "table_b", "--no-color"
        ])
        assert result_mismatch.returncode == 0
        assert "Schema diff" in result_mismatch.stdout
        assert "Type mismatches" in result_mismatch.stdout
        assert "id" in result_mismatch.stdout

    def test_type_mismatch_detection(self, tmp_path, run_cli):
        """Test detection of type mismatches between data and schema."""
        # Create data with string id
        data_file = tmp_path / "data.json"
        data_file.write_text('{"id": "1"}')  # String id

        # Create schema expecting integer id
        schema_file = tmp_path / "schema.json"
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"]
        }
        schema_file.write_text(json.dumps(schema))

        # Should detect type mismatch
        result = run_cli([
            str(data_file), str(schema_file),
            "--left", "data", "--right", "json_schema",
            "--first-record", "--no-color"
        ])
        assert result.returncode == 0
        assert "Type mismatches" in result.stdout

    def test_data_vs_sql_comparison(self, tmp_path, run_cli):
        """Test data to SQL DDL comparison."""
        # Create data file
        data_file = tmp_path / "data.json"
        data_file.write_text('{"id": 1, "full_name": "Alice"}')

        # Create matching SQL schema
        sql_file = tmp_path / "schema.sql"
        sql_file.write_text("""
CREATE TABLE users (
  id BIGINT NOT NULL,
  full_name TEXT NOT NULL
);
""")

        # Should match
        result = run_cli([
            str(data_file), str(sql_file),
            "--left", "data", "--right", "sql",
            "--right-table", "users", "--first-record", "--no-color"
        ])
        assert result.returncode == 0
        assert "Schema diff" in result.stdout
        assert "No differences" in result.stdout or "Type mismatches -- (0)" in result.stdout


class TestAutoDetection:
    """Test auto-detection functionality."""

    def test_auto_detection_json_data(self, tmp_path, run_cli):
        """Test auto-detection correctly identifies JSON data files."""
        file1 = tmp_path / "data1.json"
        file2 = tmp_path / "data2.json"

        data1 = {"test": "value1"}
        data2 = {"test": "value2"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # No --left/--right flags - should auto-detect as data
        result = run_cli([str(file1), str(file2), "--no-color"])
        assert result.returncode == 0
        assert "Schema diff" in result.stdout

    def test_auto_detection_json_schema(self, tmp_path, run_cli):
        """Test auto-detection correctly identifies JSON Schema files."""
        file1 = tmp_path / "schema1.json"
        file2 = tmp_path / "schema2.json"

        schema1 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"id": {"type": "integer"}}
        }
        schema2 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"id": {"type": "string"}}
        }

        file1.write_text(json.dumps(schema1))
        file2.write_text(json.dumps(schema2))

        # Should auto-detect as JSON Schema
        result = run_cli([str(file1), str(file2), "--no-color"])
        assert result.returncode == 0
        assert "Schema diff" in result.stdout

    def test_auto_detection_mixed_types(self, tmp_path, run_cli):
        """Test auto-detection works with mixed file types."""
        data_file = tmp_path / "data.json"
        schema_file = tmp_path / "schema.json"

        data = {"id": 1}
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {"id": {"type": "integer"}}
        }

        data_file.write_text(json.dumps(data))
        schema_file.write_text(json.dumps(schema))

        # Should auto-detect data vs schema
        result = run_cli([str(data_file), str(schema_file), "--no-color"])
        assert result.returncode == 0
        assert "Schema diff" in result.stdout


class TestMigrationAnalysis:
    """Test migration analysis functionality."""

    def test_migration_analysis_common_fields_calculation(self, tmp_path, run_cli):
        """Test that migration analysis correctly calculates common fields."""
        file1 = tmp_path / "source.json"
        file2 = tmp_path / "target.json"

        # Create data with known common fields
        data1 = {"id": 1, "name": "Alice", "removed": "gone"}
        data2 = {"id": 2, "name": "Bob", "added": "new"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            result = run_cli([
                str(file1), str(file2), "--output", "--no-color"
            ])

            assert result.returncode == 0

            # Check migration analysis file
            analysis_file = tmp_path / "output" / "reports" / "migration_analysis.md"
            assert analysis_file.exists()

            content = analysis_file.read_text()
            # Should show 2 common fields (id, name)
            assert "2 common fields" in content
            assert "1 fields removed" in content
            assert "1 new fields" in content

        finally:
            os.chdir(original_cwd)

    def test_migration_analysis_different_comparison_types(self, tmp_path, run_cli):
        """Test migration analysis works with different comparison types."""
        data_file = tmp_path / "data.json"
        schema_file = tmp_path / "schema.json"

        data = {"id": 1, "name": "test"}
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string"}
            }
        }

        data_file.write_text(json.dumps(data))
        schema_file.write_text(json.dumps(schema))

        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            result = run_cli([
                str(data_file), str(schema_file),
                "--left", "data", "--right", "json_schema",
                "--output", "--no-color"
            ])

            assert result.returncode == 0

            # Should create migration analysis
            analysis_file = tmp_path / "output" / "reports" / "migration_analysis.md"
            assert analysis_file.exists()

        finally:
            os.chdir(original_cwd)


class TestParameterHandling:
    """Test that CLI parameters are properly handled."""

    def test_sampling_parameters(self, tmp_path, run_cli):
        """Test that sampling parameters work correctly."""
        file1 = tmp_path / "large1.json"
        file2 = tmp_path / "large2.json"

        # Create large datasets
        data1 = [{"id": i, "value": f"item{i}"} for i in range(50)]
        data2 = [{"id": i, "value": f"modified{i}"} for i in range(50)]

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # Test different sampling options
        sampling_tests = [
            (["-k", "5"], "should sample 5 records"),
            (["--sample-size", "10"], "should sample 10 records"),
            (["--first-record"], "should use first record only"),
            (["--all-records"], "should use all records"),
        ]

        for args, description in sampling_tests:
            result = run_cli([
                str(file1), str(file2), *args, "--no-color"
            ])
            assert result.returncode == 0, f"Failed: {description}"
            assert "Schema diff" in result.stdout

    def test_output_parameters(self, tmp_path, run_cli):
        """Test that output parameters work correctly."""
        file1 = tmp_path / "test1.json"
        file2 = tmp_path / "test2.json"

        data1 = {"id": 1, "removed": "field"}
        data2 = {"id": 1, "added": "field"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            # Test --output flag
            result = run_cli([
                str(file1), str(file2), "--output", "--no-color"
            ])

            assert result.returncode == 0

            # Should create output directory and files
            output_dir = tmp_path / "output"
            assert output_dir.exists()
            assert (output_dir / "reports").exists()

        finally:
            os.chdir(original_cwd)

    def test_json_output_parameter(self, tmp_path, run_cli):
        """Test that --json-out parameter works correctly."""
        file1 = tmp_path / "test1.json"
        file2 = tmp_path / "test2.json"
        json_out = tmp_path / "diff.json"

        data1 = {"id": 1, "type": "user"}
        data2 = {"id": "1", "type": "user"}  # Type difference

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        result = run_cli([
            str(file1), str(file2),
            "--json-out", str(json_out), "--no-color"
        ])

        assert result.returncode == 0
        assert json_out.exists()

        # Verify JSON content
        json_content = json.loads(json_out.read_text())
        assert "schema_mismatches" in json_content
        assert len(json_content["schema_mismatches"]) > 0


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_nonexistent_files(self, tmp_path, run_cli):
        """Test handling of nonexistent files."""
        from schema_diff.io_utils import CommandError

        nonexistent = tmp_path / "does_not_exist.json"
        valid_file = tmp_path / "valid.json"
        valid_file.write_text(json.dumps({"test": "data"}))

        with pytest.raises(CommandError):
            run_cli([str(nonexistent), str(valid_file)])

    def test_malformed_files(self, tmp_path, run_cli):
        """Test handling of malformed files."""
        from schema_diff.io_utils import CommandError

        bad_file = tmp_path / "malformed.json"
        good_file = tmp_path / "good.json"

        bad_file.write_text('{"invalid": json}')  # Invalid JSON
        good_file.write_text(json.dumps({"valid": "json"}))

        with pytest.raises(CommandError):
            run_cli([str(bad_file), str(good_file)])

    def test_unsupported_combinations(self, tmp_path, run_cli):
        """Test handling of unsupported file type combinations."""
        from schema_diff.io_utils import CommandError

        file1 = tmp_path / "test.json"
        file1.write_text(json.dumps({"test": "data"}))

        # Test with invalid file type - should be rejected by argument parser
        with pytest.raises(CommandError) as exc_info:
            run_cli([
                str(file1), str(file1),
                "--left", "invalid_type", "--right", "data"
            ])

        # Should contain argument validation error
        assert "invalid choice" in str(exc_info.value)


class TestRegressionPrevention:
    """Tests specifically designed to prevent regression of fixed issues."""

    def test_unified_format_data_to_data_regression(self, tmp_path, run_cli):
        """Regression test: ensure data-to-data comparisons work with unified format."""
        file1 = tmp_path / "data1.json"
        file2 = tmp_path / "data2.json"

        data1 = {"field": "value1"}
        data2 = {"field": "value2"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        # This should NOT fail with "Unsupported schema type for unified loading: data"
        result = run_cli([str(file1), str(file2), "--no-color"])
        assert result.returncode == 0
        assert "Unsupported schema type" not in result.stderr

    def test_auto_detection_none_type_regression(self, tmp_path, run_cli):
        """Regression test: ensure auto-detection doesn't pass None to unified loader."""
        file1 = tmp_path / "auto1.json"
        file2 = tmp_path / "auto2.json"

        file1.write_text(json.dumps({"auto": "detect1"}))
        file2.write_text(json.dumps({"auto": "detect2"}))

        # This should NOT fail with "Unsupported schema type for unified loading: None"
        result = run_cli([str(file1), str(file2), "--no-color"])
        assert result.returncode == 0
        assert "Unsupported schema type" not in result.stderr

    def test_migration_analysis_common_fields_regression(self, tmp_path, run_cli):
        """Regression test: ensure migration analysis calculates common fields correctly."""
        file1 = tmp_path / "regression1.json"
        file2 = tmp_path / "regression2.json"

        # Known data with 2 common fields
        data1 = {"common1": "value", "common2": "value", "unique1": "value"}
        data2 = {"common1": "value", "common2": "value", "unique2": "value"}

        file1.write_text(json.dumps(data1))
        file2.write_text(json.dumps(data2))

        original_cwd = Path.cwd()
        try:
            import os
            os.chdir(tmp_path)

            result = run_cli([
                str(file1), str(file2), "--output", "--no-color"
            ])

            assert result.returncode == 0

            analysis_file = tmp_path / "output" / "reports" / "migration_analysis.md"
            assert analysis_file.exists()

            content = analysis_file.read_text()
            # Should NOT show "0 common fields"
            assert "0 common fields" not in content
            assert "2 common fields" in content

        finally:
            os.chdir(original_cwd)
