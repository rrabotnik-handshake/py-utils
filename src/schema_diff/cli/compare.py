#!/usr/bin/env python3
"""Compare command implementation for schema-diff CLI.

Handles schema comparison functionality with proper argument parsing and validation.
"""
from __future__ import annotations

from pathlib import Path

from ..compare import compare_trees  # Needed for data-to-data comparisons
from ..constants import DEFAULT_SAMPLE_SIZE
from ..exceptions import ArgumentError
from ..gcs_utils import get_gcs_status, is_gcs_path

# load_left_or_right also unused in unified approach
from ..migration_analyzer import analyze_migration_impact
from ..output_utils import write_output_file

# flatten_paths unused in unified approach


def add_compare_subcommand(subparsers) -> None:
    """Add compare subcommand to the parser."""
    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare two schemas or data files",
        description="Compare schemas from different sources (data files, JSON Schema, Spark, SQL DDL, etc.)",
    )

    # Positional arguments
    compare_parser.add_argument("file1", help="First schema/data file to compare")
    compare_parser.add_argument("file2", help="Second schema/data file to compare")

    # Schema type arguments
    compare_parser.add_argument(
        "--left",
        choices=[
            "data",
            "json_schema",
            "jsonschema",
            "spark",
            "sql",
            "protobuf",
            "dbt-manifest",
            "dbt-yml",
            "dbt-model",
            "bigquery",
        ],
        help="Left file type (auto-detected if not specified)",
    )
    compare_parser.add_argument(
        "--right",
        choices=[
            "data",
            "json_schema",
            "jsonschema",
            "spark",
            "sql",
            "protobuf",
            "dbt-manifest",
            "dbt-yml",
            "dbt-model",
            "bigquery",
        ],
        help="Right file type (auto-detected if not specified)",
    )

    # Sampling arguments
    compare_parser.add_argument(
        "--all-records",
        action="store_true",
        help="Process all records (no sampling limit)",
    )
    compare_parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"Number of records to sample (default: {DEFAULT_SAMPLE_SIZE})",
    )
    compare_parser.add_argument(
        "--first-record",
        action="store_true",
        help="Process only the first record (equivalent to --sample-size 1)",
    )

    # Display options
    compare_parser.add_argument(
        "--show-common",
        action="store_true",
        help="Show common fields in addition to differences",
    )
    compare_parser.add_argument(
        "--only-common",
        action="store_true",
        help="Show only common fields (hide differences)",
    )
    compare_parser.add_argument(
        "--fields",
        nargs="*",
        help="Compare only specific fields (space-separated list)",
    )
    compare_parser.add_argument(
        "--no-color", action="store_true", help="Disable colored output"
    )

    # Output options
    compare_parser.add_argument(
        "--output",
        action="store_true",
        help="Save results to output directory with migration analysis",
    )

    # GCS options
    compare_parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force re-download of GCS files even if they exist locally",
    )
    compare_parser.add_argument(
        "--gcs-info", action="store_true", help="Show GCS file information and exit"
    )

    # BigQuery options
    compare_parser.add_argument(
        "--table", help="BigQuery table name (for BigQuery live table comparisons)"
    )
    compare_parser.add_argument(
        "--right-table", help="Table name for right-side SQL schema (alias for --table)"
    )
    compare_parser.add_argument(
        "--model", help="dbt model name (for dbt manifest/yml comparisons)"
    )

    # Legacy/backward compatibility arguments
    compare_parser.add_argument(
        "-k",
        "--samples",
        type=int,
        help="Number of records to sample (alias for --sample-size)",
    )
    compare_parser.add_argument("--seed", type=int, help="Random seed for sampling")
    compare_parser.add_argument("--json-out", help="Output JSON report to file")
    compare_parser.add_argument(
        "--record", type=int, help="Process specific record number"
    )
    compare_parser.add_argument(
        "--both-modes",
        action="store_true",
        help="Show both ordinal and sampled sections",
    )
    compare_parser.add_argument(
        "--show-samples", action="store_true", help="Show sample data in output"
    )


def cmd_compare(args) -> None:
    """Execute the compare command."""
    # Auto-detect file types if not specified
    from ..loader import _guess_kind

    left_kind = args.left
    right_kind = args.right

    # Auto-detect if not specified
    if left_kind is None:
        left_kind = _guess_kind(args.file1)
    if right_kind is None:
        right_kind = _guess_kind(args.file2)

    # Normalize schema type aliases for internal use
    left_kind = "jsonschema" if left_kind == "json_schema" else left_kind
    right_kind = "jsonschema" if right_kind == "json_schema" else right_kind

    # Handle argument aliases
    if hasattr(args, "right_table") and args.right_table:
        args.table = args.right_table

    # Handle legacy arguments for backward compatibility
    if hasattr(args, "samples") and args.samples:
        args.sample_size = args.samples

    if hasattr(args, "record") and args.record:
        # --record N means process only record N (equivalent to --first-record with offset)
        args.first_record = True

    # Handle GCS info request
    if args.gcs_info:
        if is_gcs_path(args.file1) or is_gcs_path(args.file2):
            get_gcs_status()
        else:
            print("No GCS paths provided.")
        return

    # Show GCS status if GCS paths are involved
    if is_gcs_path(args.file1) or is_gcs_path(args.file2):
        get_gcs_status()

    # Determine record limit
    if args.first_record:
        record_n = 1
    elif args.all_records:
        record_n = None
    else:
        record_n = args.sample_size

    try:
        # Set GCS download context
        from ..io_utils import set_force_download_context

        set_force_download_context(args.force_download)

        # Load configuration
        from ..config import Config

        cfg = Config(color_enabled=not args.no_color)

        # Determine comparison type and perform comparison using unified format
        from ..compare import compare_schemas_unified
        from ..unified_loader import load_schema_unified

        # Check if this is a data-to-schema comparison (left=data, right=schema)
        schema_kinds = {
            "jsonschema",
            "json_schema",
            "spark",
            "sql",
            "protobuf",
            "dbt-manifest",
            "dbt-yml",
            "dbt-model",
            "bigquery",
        }

        if left_kind == "data" and right_kind == "data":
            # Data-to-data comparison - use legacy comparison logic
            from ..io_utils import all_records, nth_record, sample_records
            from ..json_data_file_parser import merged_schema_from_samples

            # Load left side data
            if args.first_record:
                s1_records = nth_record(args.file1, 1)
            elif args.all_records:
                s1_records = all_records(args.file1)
            else:
                s1_records = sample_records(args.file1, record_n or 1000)

            # Load right side data
            if args.first_record:
                s2_records = nth_record(args.file2, 1)
            elif args.all_records:
                s2_records = all_records(args.file2)
            else:
                s2_records = sample_records(args.file2, record_n or 1000)

            # Create schemas from both sides
            left_tree = merged_schema_from_samples(s1_records, cfg)
            right_tree = merged_schema_from_samples(s2_records, cfg)

            # Apply field filtering if specified
            if args.fields:
                from ..utils import filter_schema_by_fields

                left_tree = filter_schema_by_fields(left_tree, args.fields)
                right_tree = filter_schema_by_fields(right_tree, args.fields)

            # Use legacy comparison for data-to-data
            # Add sampling info to title if sampling was used
            sampling_info = ""
            if args.first_record:
                sampling_info = "; first record"
            elif args.all_records:
                sampling_info = "; all records"
            elif (
                record_n and record_n != 1000
            ):  # 1000 is default, so only show if different
                sampling_info = f"; {record_n} samples"

            report_struct = compare_trees(
                left_label=args.file1,
                right_label=args.file2,
                left_tree=left_tree,
                left_required=set(),
                right_tree=right_tree,
                right_required=set(),
                cfg=cfg,
                show_common=args.show_common,
                only_common=args.only_common,
                left_source_type="data",
                right_source_type="data",
                json_out=args.json_out,
                title_suffix=sampling_info,
            )

        elif left_kind == "data" and right_kind in schema_kinds:
            # Data-to-schema comparison
            from ..io_utils import all_records, nth_record, sample_records

            if args.first_record:
                s1_records = nth_record(args.file1, 1)
            elif args.all_records:
                s1_records = all_records(args.file1)
            else:
                s1_records = sample_records(args.file1, record_n or 1000)

            # Convert data to unified schema
            from ..json_data_file_parser import merged_schema_from_samples
            from ..models import from_legacy_tree

            data_tree = merged_schema_from_samples(s1_records, cfg)

            # Apply field filtering if specified
            if args.fields:
                from ..utils import filter_schema_by_fields

                data_tree = filter_schema_by_fields(data_tree, args.fields)

            left_schema = from_legacy_tree(data_tree, set(), source_type="data")

            # Load right schema in unified format
            right_schema = load_schema_unified(
                args.file2,
                right_kind,
                table=args.table,
                model=args.model,
            )

            # Add sampling info to title if sampling was used
            sampling_info = ""
            if args.first_record:
                sampling_info = "; first record"
            elif args.all_records:
                sampling_info = "; all records"
            elif (
                record_n and record_n != 1000
            ):  # 1000 is default, so only show if different
                sampling_info = f"; {record_n} samples"

            # Perform unified comparison
            report_struct = compare_schemas_unified(
                left_schema,
                right_schema,
                cfg=cfg,
                show_common=args.show_common,
                only_common=args.only_common,
                left_label=args.file1,
                right_label=args.file2,
                title_suffix=sampling_info,
            )
        else:
            # Schema-to-schema comparison using unified format
            left_schema = load_schema_unified(
                args.file1,
                left_kind,
                table=args.table,
                model=args.model,
            )
            right_schema = load_schema_unified(
                args.file2,
                right_kind,
                table=args.table,
                model=args.model,
            )

            # Perform unified comparison
            report_struct = compare_schemas_unified(
                left_schema,
                right_schema,
                cfg=cfg,
                show_common=args.show_common,
                only_common=args.only_common,
                left_label=args.file1,
                right_label=args.file2,
            )

        # Handle output
        if args.output and report_struct:
            # Generate migration analysis
            # Calculate common fields for migration analysis if not already present
            if "common_fields" not in report_struct:
                # We need to calculate common fields for migration analysis
                # This is a simplified approach - we'll extract from the trees used in comparison
                if left_kind == "data" and right_kind == "data":
                    # For data-to-data, calculate from the trees we created
                    from ..utils import flatten_paths

                    left_paths = set(flatten_paths(left_tree))
                    right_paths = set(flatten_paths(right_tree))
                    common_paths = left_paths & right_paths
                    report_struct["common_fields"] = list(common_paths)
                else:
                    # For other comparisons, we'll need to calculate differently
                    # For now, use empty list - the migration analyzer will handle this
                    report_struct["common_fields"] = []

            # Generate migration analysis
            migration_analysis = analyze_migration_impact(
                report_struct,
                source_label=Path(args.file1).name,
                target_label=Path(args.file2).name,
            )

            # Write migration analysis
            from ..migration_analyzer import generate_migration_report

            write_output_file(
                generate_migration_report(migration_analysis, format="markdown"),
                "migration_analysis.md",
                "reports",
            )

            print("✅ Migration analysis saved to output/reports/")
        elif args.output:
            print("ℹ️ Migration analysis not available for data-to-schema comparisons")

        # Handle legacy JSON output
        if hasattr(args, "json_out") and args.json_out and report_struct:
            import json

            with open(args.json_out, "w") as f:
                json.dump(report_struct, f, indent=2)
            print(f"✅ JSON report written to: {args.json_out}")

    except ArgumentError as e:
        raise ArgumentError(f"Comparison failed: {e}") from e
    except Exception as e:
        raise Exception(f"Unexpected error during comparison: {e}") from e
