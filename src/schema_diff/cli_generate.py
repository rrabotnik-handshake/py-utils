#!/usr/bin/env python3
"""
CLI commands for schema generation.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from .config import Config
from .io_utils import all_records, nth_record, sample_records
from .output_utils import print_output_success, write_output_file
from .schema_generator import (
    generate_schema_from_data,
    get_format_description,
    get_supported_formats,
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


def add_generate_subcommands(subparsers) -> None:
    """Add schema generation subcommands to the argument parser."""

    # schema-diff generate - Generate schema from data
    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate schema from data file",
        description="Generate schema in various formats from a data file",
    )
    generate_parser.add_argument(
        "data_file",
        help="Data file to analyze (JSON, NDJSON, JSONL, or .gz compressed)",
    )
    generate_parser.add_argument(
        "--format",
        "-f",
        choices=get_supported_formats(),
        default="json_schema",
        help="Output schema format (default: json_schema)",
    )
    generate_parser.add_argument(
        "--output",
        "-o",
        action="store_true",
        help="Save schema to ./output directory with auto-generated filename (default: stdout)",
    )
    generate_parser.add_argument(
        "--table-name",
        "-t",
        default="generated_table",
        help="Table name for SQL DDL formats (default: generated_table)",
    )
    generate_parser.add_argument(
        "--samples",
        "-k",
        type=int,
        default=None,
        help="Number of records to sample (default: adaptive sampling)",
    )
    generate_parser.add_argument(
        "--all-records",
        action="store_true",
        help="Process all records for comprehensive schema",
    )
    generate_parser.add_argument(
        "--first-record", action="store_true", help="Use only the first record"
    )
    generate_parser.add_argument(
        "--required-fields",
        nargs="*",
        help="Field paths that should be marked as required/NOT NULL",
    )
    generate_parser.add_argument(
        "--show-samples",
        action="store_true",
        help="Show the data samples being analyzed",
    )

    generate_parser.add_argument(
        "--validate",
        action="store_true",
        default=True,
        help="Validate generated schema syntax (default: enabled)",
    )

    generate_parser.add_argument(
        "--no-validate",
        dest="validate",
        action="store_false",
        help="Skip schema validation",
    )

    generate_parser.set_defaults(func=cmd_generate)

    # schema-diff formats - List supported formats
    formats_parser = subparsers.add_parser(
        "formats",
        help="List supported schema output formats",
        description="Show all supported schema output formats with descriptions",
    )
    formats_parser.set_defaults(func=cmd_formats)


def cmd_generate(args) -> None:
    """Handle 'schema-diff generate' command."""
    try:
        # Resolve GCS path if needed
        from .gcs_utils import get_gcs_status, is_gcs_path, resolve_path

        original_data_file = args.data_file
        if is_gcs_path(args.data_file):
            print(f"🌐 GCS Status: {get_gcs_status()}", file=sys.stderr)
            print(f"📥 Resolving GCS path: {args.data_file}", file=sys.stderr)

            force_download = getattr(args, "force_download", False)
            try:
                args.data_file = resolve_path(args.data_file, force_download)
            except Exception as e:
                print(f"❌ Failed to resolve GCS path: {e}", file=sys.stderr)
                sys.exit(1)

        # Initialize configuration
        cfg = Config()

        # Determine sampling strategy
        if args.all_records:
            use_all_records = True
            samples: Optional[int] = None
        elif args.first_record:
            use_all_records = False
            samples = 1
        elif args.samples:
            use_all_records = False
            samples = args.samples
        else:
            # Default adaptive sampling
            use_all_records = False
            samples = 3

        # Read data records
        print(f"Reading data from: {args.data_file}", file=sys.stderr)

        if use_all_records:
            print("Processing all records for comprehensive schema...", file=sys.stderr)
        else:
            print(f"Sampling {samples} records...", file=sys.stderr)

        if use_all_records:
            records = all_records(args.data_file)
        elif samples == 1:
            records = nth_record(args.data_file, 1)
        else:
            # samples should never be None here, but add safety check
            assert (
                samples is not None
            ), "samples should not be None when not using all records"
            records = sample_records(args.data_file, samples)

        if not records:
            print("Error: No records found in data file", file=sys.stderr)
            sys.exit(1)

        print(f"Analyzed {len(records)} records", file=sys.stderr)

        # Show samples if requested
        if args.show_samples:
            print("\nData samples:", file=sys.stderr)
            for i, record in enumerate(records[:3], 1):
                print(f"Sample {i}: {record}", file=sys.stderr)
            if len(records) > 3:
                print(f"... and {len(records) - 3} more records", file=sys.stderr)
            print("", file=sys.stderr)

        # Prepare required fields set
        required_fields = set(args.required_fields) if args.required_fields else None

        # Generate schema
        print(f"Generating {args.format} schema...", file=sys.stderr)

        schema_output = generate_schema_from_data(
            records=records,
            cfg=cfg,
            format=args.format,
            table_name=args.table_name,
            required_fields=required_fields,
            validate=args.validate,
        )

        # Output schema
        if args.output:
            # Use utility function to write to output directory
            # Use original GCS path for better filename if available
            filename_source = (
                original_data_file
                if "original_data_file" in locals()
                else args.data_file
            )
            filename = generate_filename(filename_source, args.format, args.table_name)
            output_path = write_output_file(schema_output, filename, "schemas")

            print_output_success(output_path, "Schema")

        else:
            # Output to stdout
            print(schema_output)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_formats(args) -> None:
    """Handle 'schema-diff formats' command."""
    print("Supported schema output formats:")
    print()

    for format_name in get_supported_formats():
        description = get_format_description(format_name)
        print(f"  {format_name:<15} - {description}")

    print()
    print("Usage examples:")
    print("  schema-diff generate data.json --format json_schema")
    print(
        "  schema-diff generate data.ndjson --format bigquery_ddl --table-name my_table"
    )
    print("  schema-diff generate data.json.gz --format spark --output schema.txt")
    print(
        "  schema-diff generate data.json --format sql_ddl --required-fields id name email"
    )
