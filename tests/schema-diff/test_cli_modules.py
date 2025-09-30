"""
Tests for consolidated CLI functionality.
"""
import pytest
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock

from schema_diff.cli import main as cli_main, cmd_config, _parse_table_ref, _parse_dataset_ref


class TestCliMain:
    """Test CLI main entry point and command routing."""

    def test_main_routes_to_comparison_by_default(self):
        """Test that main routes to comparison when using compare subcommand."""
        with patch("sys.argv", ["schema-diff", "compare", "file1.json", "file2.json", "--no-color"]):
            with patch("schema_diff.cli.cmd_compare") as mock_compare:
                mock_compare.side_effect = SystemExit(0)  # Mock successful execution
                with pytest.raises(SystemExit):
                    cli_main()
                mock_compare.assert_called_once()

    def test_main_routes_ddl_commands(self):
        """Test that main routes DDL commands correctly."""
        with patch("sys.argv", ["schema-diff", "ddl", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                cli_main()
            # Help should exit with code 0
            assert exc_info.value.code == 0

    def test_main_routes_config_commands(self):
        """Test that main routes config commands correctly."""
        with patch("sys.argv", ["schema-diff", "config", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                cli_main()
            # Help should exit with code 0
            assert exc_info.value.code == 0

    def test_main_routes_generate_commands(self):
        """Test that main routes generate commands correctly."""
        with patch("sys.argv", ["schema-diff", "generate", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                cli_main()
            # Help should exit with code 0
            assert exc_info.value.code == 0


class TestTableRefParsing:
    """Test table reference parsing functions."""

    def test_parse_table_ref_valid(self):
        """Test parsing valid table references."""
        project, dataset, table = _parse_table_ref("my-project:my_dataset.my_table")
        assert project == "my-project"
        assert dataset == "my_dataset"
        assert table == "my_table"

    def test_parse_table_ref_invalid_format(self):
        """Test parsing invalid table reference formats."""
        with pytest.raises(ValueError, match="Invalid table reference format"):
            _parse_table_ref("invalid-format")
        
        with pytest.raises(ValueError, match="Invalid dataset.table format"):
            _parse_table_ref("project:invalid")

    def test_parse_dataset_ref_valid(self):
        """Test parsing valid dataset references."""
        project, dataset = _parse_dataset_ref("my-project:my_dataset")
        assert project == "my-project"
        assert dataset == "my_dataset"

    def test_parse_dataset_ref_invalid(self):
        """Test parsing invalid dataset references."""
        with pytest.raises(ValueError, match="Invalid dataset reference format"):
            _parse_dataset_ref("invalid-format")


class TestConfigCommands:
    """Test config command functionality."""

    def test_cmd_config_show(self):
        """Test config show command."""
        mock_args = Mock()
        mock_args.config_command = "show"
        
        with patch("builtins.print") as mock_print:
            cmd_config(mock_args)
            mock_print.assert_called()

    def test_cmd_config_init(self):
        """Test config init command."""
        mock_args = Mock()
        mock_args.config_command = "init"
        mock_args.config_file = "test_config.yaml"
        mock_args.force = False
        
        with patch("pathlib.Path.exists", return_value=False):
            with patch("builtins.print") as mock_print:
                cmd_config(mock_args)
                mock_print.assert_called()


class TestDDLCommands:
    """Test DDL command functionality."""

    @patch("schema_diff.bigquery_ddl.generate_table_ddl")
    @patch("google.cloud.bigquery.Client")
    def test_cmd_ddl_table_parsing(self, mock_client, mock_generate):
        """Test DDL table command parsing."""
        mock_generate.return_value = "CREATE TABLE test..."
        
        # Test that table reference parsing works
        project, dataset, table = _parse_table_ref("test-project:test_dataset.test_table")
        assert project == "test-project"
        assert dataset == "test_dataset"
        assert table == "test_table"

    def test_parse_table_ref_edge_cases(self):
        """Test edge cases in table reference parsing."""
        # Test with hyphens and underscores
        project, dataset, table = _parse_table_ref("my-project-123:my_dataset_v2.my_table_final")
        assert project == "my-project-123"
        assert dataset == "my_dataset_v2"
        assert table == "my_table_final"