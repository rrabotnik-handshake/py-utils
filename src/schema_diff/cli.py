#!/usr/bin/env python3
"""
Truly consolidated CLI for schema-diff with all functionality in one place.

This module contains ALL CLI functionality without importing from separate cli_*.py files:
- compare: Schema comparison (main functionality)
- generate: Schema generation from data
- ddl: BigQuery DDL generation
- config: Configuration management
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from .helpfmt import ColorDefaultsFormatter


def main() -> int:
    """Main entry point for consolidated CLI."""
    parser = argparse.ArgumentParser(
        prog="schema-diff",
        description="Compare schemas across multiple formats and generate schema documentation",
        formatter_class=ColorDefaultsFormatter,
    )

    subparsers = parser.add_subparsers(
        dest="command",
        help="Available commands",
        metavar="{compare,generate,ddl,config}",
    )

    # Add subcommands
    _add_compare_subcommand(subparsers)
    _add_generate_subcommand(subparsers)
    _add_ddl_subcommand(subparsers)
    _add_config_subcommand(subparsers)

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Dispatch to appropriate handler
    if args.command == "compare":
        cmd_compare(args)
    elif args.command == "generate":
        cmd_generate(args)
    elif args.command == "ddl":
        cmd_ddl(args)
    elif args.command == "config":
        cmd_config(args)
    else:
        parser.print_help()
        return 1

    return 0


def _add_compare_subcommand(subparsers) -> None:
    """Add compare subcommand."""
    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare two schemas or data files",
        description="Compare schemas across JSON/NDJSON data, JSON Schema, Spark, SQL DDL, dbt, and Protobuf formats",
        formatter_class=ColorDefaultsFormatter,
    )

    # Positional arguments
    compare_parser.add_argument("file1", help="Left input (data or schema)")
    compare_parser.add_argument("file2", help="Right input (data or schema)")

    # Record selection group
    record_group = compare_parser.add_argument_group("Record selection")
    record_group.add_argument(
        "--first-record",
        action="store_true",
        help="Compare only the first record (default: %(default)s)",
    )
    record_group.add_argument(
        "--record",
        type=int,
        metavar="N",
        help="Compare the N-th record (1-based) (default: %(default)s)",
    )
    record_group.add_argument(
        "--record1",
        type=int,
        metavar="N",
        help="N-th record for file1 (default: %(default)s)",
    )
    record_group.add_argument(
        "--record2",
        type=int,
        metavar="N",
        help="N-th record for file2 (default: %(default)s)",
    )
    record_group.add_argument(
        "--both-modes",
        action="store_true",
        help="Run both single-record and sampling modes (default: %(default)s)",
    )

    # Sampling group
    sampling_group = compare_parser.add_argument_group("Sampling")
    sampling_group.add_argument(
        "-k",
        "--samples",
        type=int,
        default=1000,
        metavar="N",
        help="Number of samples (default: %(default)s)",
    )
    sampling_group.add_argument(
        "--all-records",
        action="store_true",
        help="Process all records (default: %(default)s)",
    )
    sampling_group.add_argument(
        "--seed",
        type=int,
        help="Random seed for sampling (default: %(default)s)",
    )
    sampling_group.add_argument(
        "--show-samples",
        action="store_true",
        help="Show sample records (default: %(default)s)",
    )

    # Output group
    output_group = compare_parser.add_argument_group("Output")
    output_group.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colored output (default: %(default)s)",
    )
    output_group.add_argument(
        "--force-color",
        action="store_true",
        help="Force colored output (default: %(default)s)",
    )
    output_group.add_argument(
        "--no-presence",
        action="store_true",
        help="Skip presence analysis (default: %(default)s)",
    )
    output_group.add_argument(
        "--show-common",
        action="store_true",
        help="Show common fields (default: %(default)s)",
    )
    output_group.add_argument(
        "--fields",
        nargs="+",
        metavar="FIELD",
        help="Filter to specific fields (default: %(default)s)",
    )
    output_group.add_argument(
        "--json-out",
        metavar="PATH",
        help="Output JSON report (default: %(default)s)",
    )
    output_group.add_argument(
        "--output",
        action="store_true",
        help="Generate analysis report (default: %(default)s)",
    )
    output_group.add_argument(
        "--output-format",
        choices=["markdown", "text", "json"],
        default="markdown",
    )

    # Schema types group
    schema_group = compare_parser.add_argument_group("Schema types")
    schema_group.add_argument(
        "--left",
        choices=[
            "auto",
            "data",
            "jsonschema",
            "spark",
            "sql",
            "dbt-manifest",
            "dbt-yml",
            "dbt-model",
            "bigquery",
            "protobuf",
        ],
        default="auto",
    )
    schema_group.add_argument(
        "--right",
        choices=[
            "auto",
            "data",
            "jsonschema",
            "spark",
            "sql",
            "dbt-manifest",
            "dbt-yml",
            "dbt-model",
            "bigquery",
            "protobuf",
        ],
        default="auto",
    )
    schema_group.add_argument(
        "--left-table",
        help="Table name for left SQL schema (default: %(default)s)",
    )
    schema_group.add_argument(
        "--right-table",
        help="Table name for right SQL schema (default: %(default)s)",
    )
    schema_group.add_argument(
        "--left-model",
        help="Model name for left dbt schema (default: %(default)s)",
    )
    schema_group.add_argument(
        "--right-model",
        help="Model name for right dbt schema (default: %(default)s)",
    )
    schema_group.add_argument(
        "--left-message",
        help="Message name for left Protobuf (default: %(default)s)",
    )
    schema_group.add_argument(
        "--right-message",
        help="Message name for right Protobuf (default: %(default)s)",
    )

    # GCS group
    gcs_group = compare_parser.add_argument_group("Google Cloud Storage")
    gcs_group.add_argument(
        "--gcs-info",
        metavar="GCS_PATH",
        help="Show GCS file information (default: %(default)s)",
    )
    gcs_group.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download GCS files (default: %(default)s)",
    )


def cmd_compare(args) -> None:
    """Handle 'schema-diff compare' command."""
    try:
        # Handle --gcs-info flag (early exit)
        if args.gcs_info:
            from .gcs_utils import get_gcs_info, is_gcs_path

            if not is_gcs_path(args.gcs_info):
                print(f"❌ Not a valid GCS path: {args.gcs_info}")
                print("   GCS paths should start with:")
                print("   - gs://bucket-name/path/to/file")
                print("   - https://storage.cloud.google.com/bucket-name/path/to/file")
                print("   - https://storage.googleapis.com/bucket-name/path/to/file")
                sys.exit(1)

            info = get_gcs_info(args.gcs_info)
            print("📁 GCS File Information:")
            print(f"   Bucket: {info['bucket']}")
            print(f"   Object: {info['object']}")
            print(f"   Size: {info.get('size', 'Unknown')} bytes")
            print(f"   Updated: {info.get('updated', 'Unknown')}")
            return

        # Resolve GCS paths if needed
        from .gcs_utils import get_gcs_status, is_gcs_path, resolve_path

        print(get_gcs_status())

        if is_gcs_path(args.file1):
            args.file1 = resolve_path(args.file1, args.force_download)
        if is_gcs_path(args.file2):
            args.file2 = resolve_path(args.file2, args.force_download)

        # Import comparison functionality
        import json

        from .compare import compare_trees
        from .config import Config
        from .loader import load_left_or_right
        from .migration_analyzer import analyze_migration_impact
        from .output_utils import print_output_success, write_output_file
        from .report import build_report_struct

        # Set up configuration
        config = Config(
            color_enabled=not args.no_color
            and (args.force_color or sys.stdout.isatty())
        )

        # Set random seed if provided
        if args.seed is not None:
            import random

            random.seed(args.seed)

        # Load left and right schemas
        left_tree, left_required, left_label = load_left_or_right(
            args.file1,
            kind=args.left,
            cfg=config,
            sql_table=args.left_table,
            dbt_model=args.left_model,
            proto_message=args.left_message,
            first_record=(
                args.record1 or args.record
                if args.first_record or args.record1 or args.record
                else None
            ),
            samples=args.samples,
            all_records=args.all_records,
        )

        right_tree, right_required, right_label = load_left_or_right(
            args.file2,
            kind=args.right,
            cfg=config,
            sql_table=args.right_table,
            dbt_model=args.right_model,
            proto_message=args.right_message,
            first_record=(
                args.record2 or args.record
                if args.first_record or args.record2 or args.record
                else None
            ),
            samples=args.samples,
            all_records=args.all_records,
        )

        # Perform comparison
        diff = compare_trees(
            left_label,
            right_label,
            left_tree,
            left_required,
            right_tree,
            right_required,
            cfg=config,
            show_common=args.show_common,
        )

        # Build report
        report_struct = build_report_struct(
            diff,
            left_label,
            right_label,
            include_presence=not args.no_presence,
        )

        # Add common fields to report for migration analysis
        if args.output:
            from .normalize import walk_normalize
            from .utils import coerce_root_to_field_dict, flatten_paths

            # Normalize and coerce trees to calculate common fields
            sch1n = walk_normalize(left_tree)
            sch2n = walk_normalize(right_tree)
            sch1n = coerce_root_to_field_dict(sch1n)
            sch2n = coerce_root_to_field_dict(sch2n)

            # Calculate common fields
            def _as_field_dict(x):
                return x if isinstance(x, dict) else {}

            d1 = _as_field_dict(sch1n)
            d2 = _as_field_dict(sch2n)
            paths1 = flatten_paths(d1)
            paths2 = flatten_paths(d2)
            common_paths = sorted(set(paths1) & set(paths2))

            # Add common fields to report
            report_struct["common_fields"] = common_paths

        # Output JSON report if requested
        if args.json_out:
            with open(args.json_out, "w") as f:
                json.dump(report_struct, f, indent=2, ensure_ascii=False)
            print(f"✅ JSON report written to: {args.json_out}")

        # Generate migration analysis if requested
        if args.output:
            analysis = analyze_migration_impact(report_struct, left_label, right_label)

            if args.output_format == "json":
                output_path = write_output_file(
                    json.dumps(analysis.__dict__, indent=2, ensure_ascii=False),
                    "migration_analysis.json",
                    "reports",
                )
            else:
                # Generate markdown report
                from .migration_analyzer import generate_migration_report

                markdown_report = generate_migration_report(analysis)
                output_path = write_output_file(
                    markdown_report, "migration_analysis.md", "reports"
                )

            print_output_success(output_path, "Migration analysis")

        # Console output is already printed by compare_trees function

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _add_generate_subcommand(subparsers) -> None:
    """Add generate subcommand."""
    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate schema from data file",
        description="Generate schema definitions from JSON/NDJSON data files",
        formatter_class=ColorDefaultsFormatter,
    )

    generate_parser.add_argument("data_file", help="Input data file")
    generate_parser.add_argument(
        "--format",
        choices=[
            "json_schema",
            "sql_ddl",
            "bigquery_ddl",
            "spark",
            "bigquery_json",
            "openapi",
        ],
        default="json_schema",
        help="Output schema format (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--table-name",
        default="generated_table",
        help="Table name for DDL formats (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--first-record",
        action="store_true",
        help="Use only the first record (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--record",
        type=int,
        metavar="N",
        help="Use the N-th record (1-based) (default: %(default)s)",
    )
    generate_parser.add_argument(
        "-k",
        "--samples",
        type=int,
        default=1000,
        metavar="N",
        help="Number of samples (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--all-records",
        action="store_true",
        help="Process all records (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for sampling (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--validate",
        action="store_true",
        default=True,
        help="Validate generated schema (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--no-validate",
        action="store_false",
        dest="validate",
        help="Skip schema validation",
    )
    generate_parser.add_argument(
        "--output",
        action="store_true",
        help="Save to output directory (default: %(default)s)",
    )
    generate_parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download GCS files (default: %(default)s)",
    )


def get_file_extension(format_type: str) -> str:
    """Get appropriate file extension for schema format."""
    extensions = {
        "json_schema": ".json",
        "sql_ddl": ".sql",
        "bigquery_ddl": ".sql",
        "spark": ".txt",
        "bigquery_json": ".json",
        "openapi": ".json",
    }
    return extensions.get(format_type, ".txt")


def generate_filename(
    data_file: str, format_type: str, table_name: Optional[str] = None
) -> str:
    """Generate an appropriate filename based on data file and format."""
    # Extract base name from data file
    data_path = Path(data_file)
    base_name = data_path.stem

    # Remove common data file suffixes
    if base_name.endswith(".json"):
        base_name = base_name[:-5]
    elif base_name.endswith(".jsonl"):
        base_name = base_name[:-6]
    elif base_name.endswith(".ndjson"):
        base_name = base_name[:-7]

    # Create descriptive filename
    if format_type == "bigquery_ddl" and table_name and table_name != "generated_table":
        filename = f"{base_name}_{table_name}_bigquery_schema"
    elif format_type == "sql_ddl" and table_name and table_name != "generated_table":
        filename = f"{base_name}_{table_name}_sql_schema"
    elif format_type == "json_schema":
        filename = f"{base_name}_json_schema"
    elif format_type == "spark":
        filename = f"{base_name}_spark_schema"
    elif format_type == "bigquery_json":
        filename = f"{base_name}_bigquery_json_schema"
    elif format_type == "openapi":
        filename = f"{base_name}_openapi_schema"
    else:
        filename = f"{base_name}_{format_type}_schema"

    return filename + get_file_extension(format_type)


def cmd_generate(args) -> None:
    """Handle 'schema-diff generate' command."""
    try:
        # Resolve GCS path if needed
        from .gcs_utils import get_gcs_status, is_gcs_path, resolve_path

        print(get_gcs_status())

        if is_gcs_path(args.data_file):
            args.data_file = resolve_path(args.data_file, args.force_download)

        # Import schema generation
        from .config import Config
        from .io_utils import all_records, nth_record, sample_records
        from .schema_generator import generate_schema_from_data

        # Set up configuration
        config = Config()

        # Set random seed if provided
        if args.seed is not None:
            import random

            random.seed(args.seed)

        # Determine record selection strategy
        if args.first_record:

            def records_fn(path):
                return [nth_record(path, 1)]

        elif args.record:

            def records_fn(path):
                return [nth_record(path, args.record)]

        elif args.all_records:
            records_fn = all_records
        else:

            def records_fn(path):
                return sample_records(path, args.samples)

        # Get records
        records = records_fn(args.data_file)

        # Generate schema
        schema_str = generate_schema_from_data(
            records,
            config,
            format=args.format,
            table_name=args.table_name,
            validate=args.validate,
        )

        # Output handling
        if args.output:
            from .output_utils import print_output_success, write_output_file

            filename = generate_filename(args.data_file, args.format, args.table_name)
            output_path = write_output_file(schema_str, "schemas", filename)
            print_output_success(output_path, "Schema")
        else:
            print(schema_str)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _add_ddl_subcommand(subparsers) -> None:
    """Add DDL subcommand."""
    ddl_parser = subparsers.add_parser(
        "ddl",
        help="Generate DDL from BigQuery tables",
        description="Generate DDL statements from BigQuery tables",
        formatter_class=ColorDefaultsFormatter,
    )

    ddl_subparsers = ddl_parser.add_subparsers(
        dest="ddl_command",
        help="DDL operations",
        metavar="{table,batch,dataset}",
    )

    # Single table DDL
    table_parser = ddl_subparsers.add_parser(
        "table",
        help="Generate DDL for a single table",
        formatter_class=ColorDefaultsFormatter,
    )
    table_parser.add_argument(
        "table_ref", help="Table reference (project:dataset.table)"
    )
    table_parser.add_argument(
        "--output",
        action="store_true",
        help="Save to output directory (default: %(default)s)",
    )

    # Batch DDL
    batch_parser = ddl_subparsers.add_parser(
        "batch",
        help="Generate DDL for multiple tables",
        formatter_class=ColorDefaultsFormatter,
    )
    batch_parser.add_argument("table_refs", nargs="+", help="Table references")
    batch_parser.add_argument(
        "--output",
        action="store_true",
        help="Save to output directory (default: %(default)s)",
    )

    # Dataset DDL
    dataset_parser = ddl_subparsers.add_parser(
        "dataset",
        help="Generate DDL for all tables in a dataset",
        formatter_class=ColorDefaultsFormatter,
    )
    dataset_parser.add_argument(
        "dataset_ref", help="Dataset reference (project:dataset)"
    )
    dataset_parser.add_argument(
        "--output",
        action="store_true",
        help="Save to output directory (default: %(default)s)",
    )


def _parse_table_ref(table_ref: str) -> tuple[str, str, str]:
    """Parse table reference into project, dataset, table components."""
    parts = table_ref.split(":")
    if len(parts) != 2:
        raise ValueError(
            f"Invalid table reference format: {table_ref}. Expected: project:dataset.table"
        )

    project_id = parts[0]
    dataset_table = parts[1]

    dataset_parts = dataset_table.split(".")
    if len(dataset_parts) != 2:
        raise ValueError(
            f"Invalid dataset.table format: {dataset_table}. Expected: dataset.table"
        )

    dataset_id, table_id = dataset_parts
    return project_id, dataset_id, table_id


def _parse_dataset_ref(dataset_ref: str) -> tuple[str, str]:
    """Parse dataset reference into project, dataset components."""
    parts = dataset_ref.split(":")
    if len(parts) != 2:
        raise ValueError(
            f"Invalid dataset reference format: {dataset_ref}. Expected: project:dataset"
        )

    return parts[0], parts[1]


def cmd_ddl(args) -> None:
    """Handle 'schema-diff ddl' command."""
    if not args.ddl_command:
        print("Error: DDL subcommand required", file=sys.stderr)
        sys.exit(1)

    try:
        from .bigquery_ddl import generate_dataset_ddl, generate_table_ddl
        from .output_utils import print_output_success, write_output_file

        # Initialize BigQuery client
        try:
            from google.cloud import bigquery

            client = bigquery.Client()
        except Exception as e:
            print(f"Error: Failed to initialize BigQuery client: {e}", file=sys.stderr)
            print("Make sure you have:")
            print("1. Google Cloud SDK installed and authenticated")
            print("2. A default project set: gcloud config set project YOUR_PROJECT")
            print("3. BigQuery API enabled")
            sys.exit(1)

        if args.ddl_command == "table":
            project_id, dataset_id, table_id = _parse_table_ref(args.table_ref)
            ddl = generate_table_ddl(client, project_id, dataset_id, table_id)

            if args.output:
                filename = (
                    f"{args.table_ref.replace(':', '_').replace('.', '_')}_ddl.sql"
                )
                output_path = write_output_file(ddl, "ddl", filename)
                print_output_success(output_path, "DDL")
            else:
                print(ddl)

        elif args.ddl_command == "batch":
            ddls = {}
            for table_ref in args.table_refs:
                project_id, dataset_id, table_id = _parse_table_ref(table_ref)
                ddl = generate_table_ddl(client, project_id, dataset_id, table_id)
                ddls[table_ref] = ddl

            if args.output:
                for table_ref, ddl in ddls.items():
                    filename = (
                        f"{table_ref.replace(':', '_').replace('.', '_')}_ddl.sql"
                    )
                    output_path = write_output_file(ddl, "ddl", filename)
                    print_output_success(output_path, "DDL")
            else:
                for table_ref, ddl in ddls.items():
                    print(f"-- DDL for {table_ref}")
                    print(ddl)
                    print()

        elif args.ddl_command == "dataset":
            project_id, dataset_id = _parse_dataset_ref(args.dataset_ref)
            ddls = generate_dataset_ddl(client, project_id, dataset_id)

            if args.output:
                for table_ref, ddl in ddls.items():
                    filename = (
                        f"{table_ref.replace(':', '_').replace('.', '_')}_ddl.sql"
                    )
                    output_path = write_output_file(ddl, "ddl", filename)
                    print_output_success(output_path, "DDL")
            else:
                for table_ref, ddl in ddls.items():
                    print(f"-- DDL for {table_ref}")
                    print(ddl)
                    print()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _add_config_subcommand(subparsers) -> None:
    """Add config subcommand."""
    config_parser = subparsers.add_parser(
        "config",
        help="Manage configuration",
        description="Manage schema-diff configuration",
        formatter_class=ColorDefaultsFormatter,
    )

    config_subparsers = config_parser.add_subparsers(
        dest="config_command",
        help="Configuration operations",
        metavar="{show,init}",
    )

    # Show config
    config_subparsers.add_parser(
        "show",
        help="Show current configuration",
        formatter_class=ColorDefaultsFormatter,
    )

    # Initialize config
    init_parser = config_subparsers.add_parser(
        "init",
        help="Initialize configuration file",
        formatter_class=ColorDefaultsFormatter,
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing configuration (default: %(default)s)",
    )


def cmd_config(args) -> None:
    """Handle 'schema-diff config' command."""
    if not args.config_command:
        print("Error: Config subcommand required", file=sys.stderr)
        sys.exit(1)

    try:
        from .config import Config

        if args.config_command == "show":
            config = Config()
            print("Current configuration:")
            print(f"  Infer datetimes: {config.infer_datetimes}")
            print(f"  Color enabled: {config.color_enabled}")
            print(f"  Show presence: {config.show_presence}")

        elif args.config_command == "init":
            config_path = Path.home() / ".schema-diff" / "config.json"

            if config_path.exists() and not args.force:
                print(f"Configuration file already exists: {config_path}")
                print("Use --force to overwrite")
                return

            # Create directory if needed
            config_path.parent.mkdir(parents=True, exist_ok=True)

            # Create default config
            default_config = {
                "infer_datetimes": False,
                "color_enabled": True,
                "show_presence": True,
            }

            with open(config_path, "w") as f:
                json.dump(default_config, f, indent=2)

            print(f"Configuration file created: {config_path}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
