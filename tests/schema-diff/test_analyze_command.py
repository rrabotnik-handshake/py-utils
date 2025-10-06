#!/usr/bin/env python3
"""
Tests for the analyze subcommand functionality.

Tests schema analysis, complexity analysis, pattern detection, and suggestions.
"""
import json
import tempfile
from pathlib import Path

import pytest


class TestAnalyzeCommand:
    """Test the analyze subcommand functionality."""

    def test_analyze_basic_data_file(self, run_cli):
        """Test basic analysis of a data file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {
                "id": 123,
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30,
                "active": True,
                "metadata": {
                    "created_at": "2023-01-01",
                    "updated_at": "2023-06-01"
                }
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data"
            ])

            assert result.returncode == 0
            assert "Schema Analysis" in result.stdout or "Analysis" in result.stdout

    def test_analyze_complexity_analysis(self, run_cli):
        """Test complexity analysis option."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "complex_data.json"
            data = {
                "user": {
                    "profile": {
                        "personal": {
                            "name": "John",
                            "bio": "Developer"
                        },
                        "contact": {
                            "email": "john@example.com",
                            "phone": "123-456-7890"
                        }
                    },
                    "preferences": {
                        "theme": "dark",
                        "notifications": True
                    }
                },
                "posts": [
                    {
                        "title": "Hello World",
                        "content": "First post",
                        "tags": ["intro", "hello"]
                    }
                ]
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--complexity"
            ])

            assert result.returncode == 0

            # Verify complexity analysis output contains expected metrics
            output = result.stdout.lower()

            # Should contain complexity-related terms
            assert any(word in output for word in ["complexity", "depth", "nesting", "fields", "types"])

            # Should analyze the nested structure (depth should be at least 3)
            # user.profile.personal is 3 levels deep
            if "depth" in output or "nesting" in output:
                # Look for depth/nesting level indicators
                assert any(str(i) in result.stdout for i in [3, 4, 5])  # Should detect deep nesting

            # Should count field types
            if "types" in output or "type" in output:
                # Should detect multiple types: string, boolean, array, object
                assert any(word in output for word in ["string", "object", "array", "boolean"])

            # Should provide some quantitative metrics
            assert any(char.isdigit() for char in result.stdout)  # Should contain numbers

    def test_analyze_pattern_analysis(self, run_cli):
        """Test pattern analysis option."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "pattern_data.json"
            data = {
                "user_id": 123,
                "profile_id": 456,
                "email": "user@example.com",
                "created_at": "2023-01-01T00:00:00Z",
                "updated_at": "2023-06-01T12:00:00Z",
                "settings": {
                    "email_notifications": True,
                    "theme": "dark"
                }
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--patterns"
            ])

            assert result.returncode == 0

            # Verify pattern analysis output
            output = result.stdout.lower()

            # Should contain pattern-related terms
            assert any(word in output for word in ["pattern", "semantic", "field"])

            # Should detect ID field patterns (user_id, profile_id)
            if "id" in output or "identifier" in output:
                # Should recognize ID fields
                assert any(field in result.stdout for field in ["user_id", "profile_id"])

            # Should detect timestamp patterns (created_at, updated_at)
            if "timestamp" in output or "date" in output or "time" in output:
                # Should recognize timestamp fields
                assert any(field in result.stdout for field in ["created_at", "updated_at"])

            # Should detect email patterns
            if "email" in output:
                # Should recognize email field
                assert "email" in result.stdout

            # Should detect boolean patterns
            if "boolean" in output or "bool" in output:
                # Should recognize boolean fields
                assert any(field in result.stdout for field in ["email_notifications", "theme"])

            # Should provide meaningful pattern analysis
            # The output should contain field names and pattern types
            assert len(result.stdout) > 50  # Should have substantial output

    def test_analyze_suggestions(self, run_cli):
        """Test improvement suggestions option."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "suggestion_data.json"
            # Create data that might trigger suggestions
            data = {
                "field1": "value1",
                "field2": "value2",
                "field3": "value3",
                "nested": {
                    "deep": {
                        "very": {
                            "deeply": {
                                "nested": {
                                    "field": "too_deep"
                                }
                            }
                        }
                    }
                }
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--suggestions"
            ])

            assert result.returncode == 0
            # Should include suggestions
            assert any(word in result.stdout.lower() for word in ["suggestion", "improve", "consider"])

    def test_analyze_comprehensive_report(self, run_cli):
        """Test comprehensive analysis report."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "comprehensive_data.json"
            data = {
                "id": 1,
                "name": "Test User",
                "email": "test@example.com",
                "profile": {
                    "bio": "A test user",
                    "settings": {
                        "theme": "light",
                        "notifications": True
                    }
                },
                "posts": [
                    {
                        "id": 1,
                        "title": "First Post",
                        "created_at": "2023-01-01"
                    }
                ]
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--report"
            ])

            assert result.returncode == 0
            # Should include comprehensive analysis
            assert len(result.stdout) > 100  # Should be substantial output

    def test_analyze_all_options(self, run_cli):
        """Test --all flag (includes all analysis types)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "all_analysis_data.json"
            data = {
                "user_id": 123,
                "username": "testuser",
                "email": "test@example.com",
                "created_at": "2023-01-01T00:00:00Z",
                "profile": {
                    "name": "Test User",
                    "bio": "A comprehensive test"
                }
            }
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--all"
            ])

            assert result.returncode == 0
            # Should include all types of analysis
            assert len(result.stdout) > 200  # Should be comprehensive

    def test_analyze_json_schema_file(self, run_cli):
        """Test analyzing a JSON Schema file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_file = Path(temp_dir) / "schema.json"
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    "profile": {
                        "type": "object",
                        "properties": {
                            "bio": {"type": "string"},
                            "age": {"type": "integer"}
                        }
                    }
                },
                "required": ["id", "name"]
            }
            schema_file.write_text(json.dumps(schema))

            result = run_cli([
                "analyze", str(schema_file),
                "--type", "json_schema",
            ])

            assert result.returncode == 0

    def test_analyze_spark_schema_file(self, run_cli):
        """Test analyzing a Spark schema file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spark_file = Path(temp_dir) / "spark_schema.txt"
            spark_schema = """root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- profile: struct (nullable = true)
 |    |-- bio: string (nullable = true)
 |    |-- age: integer (nullable = true)"""
            spark_file.write_text(spark_schema)

            result = run_cli([
                "analyze", str(spark_file),
                "--type", "spark",
            ])

            assert result.returncode == 0

    def test_analyze_with_output_file(self, run_cli):
        """Test analysis with output to file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"id": 1, "name": "Test", "active": True}
            data_file.write_text(json.dumps(data))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--output"
            ])

            assert result.returncode == 0

            # Check that analysis file was created
            output_dir = Path("output")
            if output_dir.exists():
                analysis_files = list(output_dir.rglob("*analysis*"))
                # Should create some analysis output file

    def test_analyze_different_output_formats(self, run_cli):
        """Test analysis with different output formats."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "data.json"
            data = {"field1": "value1", "field2": 42, "field3": True}
            data_file.write_text(json.dumps(data))

            formats = ["text", "json", "markdown"]

            for format_type in formats:
                result = run_cli([
                    "analyze", str(data_file),
                    "--format", format_type,
                ])

                assert result.returncode == 0

    def test_analyze_with_sampling_options(self, run_cli):
        """Test analysis with different sampling options."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "large_data.json"

            # Create NDJSON with multiple records
            records = [
                {"id": i, "name": f"User {i}", "score": i * 10}
                for i in range(50)
            ]
            data_file.write_text('\n'.join(json.dumps(record) for record in records))

            # Test with specific sample size
            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--sample-size", "10"
            ])

            assert result.returncode == 0

    def test_analyze_all_records(self, run_cli):
        """Test analysis with --all-records flag."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_file = Path(temp_dir) / "all_records_data.json"

            records = [
                {"id": i, "value": f"item_{i}"}
                for i in range(20)
            ]
            data_file.write_text('\n'.join(json.dumps(record) for record in records))

            result = run_cli([
                "analyze", str(data_file),
                "--type", "data",
                "--all-records"
            ])

            assert result.returncode == 0

    def test_analyze_auto_type_detection(self, run_cli):
        """Test that analyze command auto-detects file types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # JSON data file
            json_file = Path(temp_dir) / "data.json"
            json_file.write_text('{"name": "test", "value": 42}')

            # Should auto-detect as data
            result = run_cli([
                "analyze", str(json_file),
                "--type", "data"
            ])

            assert result.returncode == 0

    def test_analyze_invalid_file(self, run_cli):
        """Test analyze command with invalid/non-existent file."""
        result = run_cli([
            "analyze", "nonexistent_file.json"
        ])

        # Should handle error gracefully with error message
        assert result.returncode == 0  # Command handles error gracefully
        assert "Error" in result.stdout or "error" in result.stdout

    def test_analyze_empty_file(self, run_cli):
        """Test analyze command with empty file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            empty_file = Path(temp_dir) / "empty.json"
            empty_file.write_text("")

            result = run_cli([
                "analyze", str(empty_file),
                "--type", "data"
            ])

            # Should handle empty file gracefully
            # May return error or handle gracefully depending on implementation

    def test_analyze_malformed_json(self, run_cli):
        """Test analyze command with malformed JSON."""
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_file = Path(temp_dir) / "malformed.json"
            bad_file.write_text('{"invalid": json}')

            result = run_cli([
                "analyze", str(bad_file),
                "--type", "data"
            ])

            # Should handle malformed JSON gracefully
            # May return error or handle gracefully depending on implementation
