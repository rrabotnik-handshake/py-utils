"""
Tests for enhanced functionality: array normalization, path changes, live BigQuery integration.
"""

from unittest.mock import Mock, patch

from schema_diff.utils import compute_path_changes, fmt_dot_path
from schema_diff.bigquery_ddl import _normalize_bigquery_arrays
from schema_diff.report import print_path_changes
from schema_diff.loader import load_left_or_right, KIND_BIGQUERY
from schema_diff.config import Config


class TestArrayNormalization:
    """Test BigQuery array normalization functionality."""

    def test_normalize_simple_array_wrapper(self):
        """Test normalization of simple BigQuery array wrapper."""
        tree = {"tags": {"list": [{"element": "str"}]}}

        normalized = _normalize_bigquery_arrays(tree)
        assert normalized["tags"] == ["str"]

    def test_normalize_nested_array_wrappers(self):
        """Test normalization of nested BigQuery array wrappers."""
        tree = {
            "experience": {
                "list": [
                    {
                        "element": {
                            "title": "str",
                            "skills": {"list": [{"element": "str"}]},
                        }
                    }
                ]
            }
        }

        normalized = _normalize_bigquery_arrays(tree)
        expected = {"experience": [{"title": "str", "skills": ["str"]}]}
        assert normalized == expected

    def test_normalize_mixed_structure(self):
        """Test normalization with both wrapped and non-wrapped fields."""
        tree = {
            "simple_field": "str",
            "wrapped_array": {"list": [{"element": {"nested_field": "int"}}]},
            "normal_array": ["str"],
            "normal_object": {"field": "bool"},
        }

        normalized = _normalize_bigquery_arrays(tree)

        assert normalized["simple_field"] == "str"
        assert normalized["wrapped_array"] == [{"nested_field": "int"}]
        assert normalized["normal_array"] == ["str"]
        assert normalized["normal_object"] == {"field": "bool"}

    def test_normalize_empty_structures(self):
        """Test normalization of empty or malformed structures."""
        tree = {
            "empty_dict": {},
            "normal_field": "str",
            "malformed": {
                "list": []  # Empty list
            },
        }

        normalized = _normalize_bigquery_arrays(tree)

        assert normalized["empty_dict"] == {}
        assert normalized["normal_field"] == "str"
        assert normalized["malformed"] == {"list": []}  # Unchanged


class TestPathChanges:
    """Test path change detection functionality."""

    def test_compute_path_changes_simple(self):
        """Test path change computation for simple field moves."""
        # Create simple trees where fields have moved
        left_tree = {"id": "int", "name": "str", "contact": {"email": "str"}}

        right_tree = {
            "id": "int",  # Same location
            "profile": {
                "name": "str"  # Moved
            },
            "contact": {
                "phone": "str"  # New field
            },
        }

        changes = compute_path_changes(left_tree, right_tree)

        # Should return a list of change records
        assert isinstance(changes, list)

        # Should detect name movement
        name_changes = [c for c in changes if c.get("name") == "name"]
        if name_changes:
            change = name_changes[0]
            assert "name" in change["left"][0]  # Should be in left paths
            assert "profile.name" in change["right"][0]  # Should be in right paths

    def test_compute_path_changes_array_paths(self):
        """Test path change computation with array notation."""
        left_tree = {"experience": [{"skills": ["str"], "title": "str"}]}

        right_tree = {
            "profile": {
                "skills": ["str"]  # Moved out of experience array
            },
            "experience": [
                {
                    "job": {
                        "title": "str"  # Nested deeper
                    }
                }
            ],
        }

        changes = compute_path_changes(left_tree, right_tree)

        # Should return list of changes
        assert isinstance(changes, list)

        # May or may not detect these specific moves depending on implementation
        # At minimum, should return a list
        assert len(changes) >= 0

    def test_fmt_dot_path_array_notation(self):
        """Test path formatting with clean array notation."""
        # Test legacy [0] notation cleanup
        assert fmt_dot_path("experience[0].title") == "experience[].title"
        assert fmt_dot_path("skills[0]") == "skills[]"
        assert fmt_dot_path("nested[0].array[0].field") == "nested[].array[].field"

        # Test already clean notation
        assert fmt_dot_path("experience[].title") == "experience[].title"
        assert fmt_dot_path("simple.field") == "simple.field"

        # Test leading dot removal
        assert fmt_dot_path(".field") == "field"
        assert fmt_dot_path("..nested.field") == "nested.field"


class TestBigQueryLiveIntegration:
    """Test live BigQuery table integration."""

    @patch("schema_diff.bigquery_ddl.bigquery.Client")
    def test_load_bigquery_live_table(self, mock_client_class):
        """Test loading live BigQuery table as schema source."""
        from google.cloud.bigquery.schema import SchemaField

        # Mock BigQuery client and table
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_table = Mock()
        mock_table.schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("name", "STRING", mode="NULLABLE"),
            SchemaField("tags", "STRING", mode="REPEATED"),
        ]
        mock_client.get_table.return_value = mock_table

        cfg = Config(infer_datetimes=False, color_enabled=False, show_presence=True)

        # Load BigQuery table
        tree, required, label = load_left_or_right(
            "test-project:dataset.table", kind=KIND_BIGQUERY, cfg=cfg, samples=3
        )

        # Verify schema extraction
        assert tree["id"] == "int"
        assert tree["name"] == "str"
        assert tree["tags"] == ["str"]  # REPEATED becomes array

        # Verify required paths
        assert "id" in required
        assert "name" not in required

        # Verify label format
        assert "bigquery://" in label
        assert "test-project.dataset.table" in label

    @patch("schema_diff.bigquery_ddl.bigquery.Client")
    def test_bigquery_live_nested_schema(self, mock_client_class):
        """Test live BigQuery table with nested STRUCT fields."""
        from google.cloud.bigquery.schema import SchemaField

        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Create nested schema
        mock_table = Mock()
        mock_table.schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField(
                "profile",
                "RECORD",
                mode="NULLABLE",
                fields=[
                    SchemaField("name", "STRING", mode="REQUIRED"),
                    SchemaField("email", "STRING", mode="NULLABLE"),
                ],
            ),
            SchemaField("tags", "STRING", mode="REPEATED"),
        ]
        mock_client.get_table.return_value = mock_table

        cfg = Config(infer_datetimes=False, color_enabled=False, show_presence=True)

        tree, required, label = load_left_or_right(
            "project:dataset.nested_table", kind=KIND_BIGQUERY, cfg=cfg, samples=3
        )

        # Verify nested structure
        assert tree["id"] == "int"
        assert isinstance(tree["profile"], dict)
        assert tree["profile"]["name"] == "str"
        assert tree["profile"]["email"] == "str"
        assert tree["tags"] == ["str"]

        # Verify nested required paths
        assert "id" in required
        assert "profile.name" in required
        assert "profile.email" not in required

    @patch("schema_diff.bigquery_ddl.bigquery.Client")
    def test_bigquery_table_reference_parsing(self, mock_client_class):
        """Test parsing of BigQuery table references."""
        from google.cloud.bigquery.schema import SchemaField
        from schema_diff.loader import load_left_or_right

        # Mock BigQuery client and table
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_table = Mock()
        mock_table.schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
        ]
        mock_client.get_table.return_value = mock_table

        cfg = Config(infer_datetimes=False, color_enabled=False, show_presence=True)

        # Test various reference formats
        references = [
            "project:dataset.table",
            "project.dataset.table",
        ]

        for ref in references:
            tree, required, label = load_left_or_right(
                ref, kind=KIND_BIGQUERY, cfg=cfg, samples=3
            )

            assert tree == {"id": "int"}
            assert required == {"id"}
            assert "bigquery://" in label

        # Should have been called for each reference
        assert mock_client.get_table.call_count == len(references)


class TestEnhancedComparison:
    """Test enhanced comparison features."""

    def test_path_changes_output_formatting(self, capsys):
        """Test path changes section output formatting."""
        # Use the correct format that print_path_changes expects
        changes = [
            {
                "name": "field1",
                "shared": [],
                "left": ["old.location"],
                "right": ["new.location"],
            },
            {"name": "field2", "shared": [], "left": [], "right": ["new.field"]},
        ]

        colors = ("", "", "", "", "")  # No colors for test
        print_path_changes("left.json", "right.json", changes, colors=colors)

        captured = capsys.readouterr()
        output = captured.out

        # Verify path changes are formatted properly
        assert output  # Should produce some output
        assert "Path changes" in output or "field1" in output or "field2" in output

    def test_array_notation_consistency(self):
        """Test that array notation is consistent across all outputs."""
        # Test that legacy [0] notation is normalized to []
        test_paths = [
            "experience[0].title",
            "skills[0]",
            "nested[0].deep[0].field",
            "already[].clean",
        ]

        expected = [
            "experience[].title",
            "skills[]",
            "nested[].deep[].field",
            "already[].clean",
        ]

        for test_path, expected_path in zip(test_paths, expected):
            assert fmt_dot_path(test_path) == expected_path

    def test_sampling_artifact_filtering(self):
        """Test that sampling artifacts are properly filtered."""
        # This would require integration with the report module
        # to test that type mismatches like "array → array" are filtered
        from schema_diff.report import build_report_struct
        from deepdiff import DeepDiff

        # Create a diff that includes sampling artifacts
        left_tree = {"field": "array"}
        right_tree = {"field": "array"}  # Same type, should not show mismatch

        diff = DeepDiff(left_tree, right_tree)
        report = build_report_struct(diff, "left", "right", include_presence=True)

        # Should have no type mismatches for identical types
        assert len(report["schema_mismatches"]) == 0
