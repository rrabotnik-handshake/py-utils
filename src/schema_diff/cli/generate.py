#!/usr/bin/env python3
"""Generate command implementation for schema-diff CLI.

Handles schema generation from data files with proper argument parsing.
"""
from __future__ import annotations

from pathlib import Path

from ..exceptions import ArgumentError
from ..gcs_utils import get_gcs_status, is_gcs_path
from ..output_utils import write_output_file
from ..schema_generator import generate_schema_from_data


def add_generate_subcommand(subparsers) -> None:
    """Add generate subcommand to the parser."""
    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate schema from data file",
        description="Infer and generate schema from data files in various formats",
    )

    # Positional arguments
    generate_parser.add_argument("data_file", help="Data file to generate schema from")

    # Schema format options
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
        help="Output schema format (default: json_schema)",
    )

    # Sampling options
    generate_parser.add_argument(
        "--all-records",
        action="store_true",
        help="Process all records (no sampling limit)",
    )
    generate_parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of records to sample (default: 1000)",
    )

    # Schema options
    generate_parser.add_argument(
        "--required-fields",
        nargs="*",
        help="Fields to mark as required (space-separated list)",
    )
    generate_parser.add_argument(
        "--table-name",
        help="Table name for SQL DDL generation (default: generated_table)",
    )

    # Validation options
    generate_parser.add_argument(
        "--validate",
        action="store_true",
        default=True,
        help="Validate generated schema (default: enabled)",
    )
    generate_parser.add_argument(
        "--no-validate", action="store_true", help="Skip schema validation"
    )

    # Output options
    generate_parser.add_argument(
        "--output", action="store_true", help="Save schema to output directory"
    )

    # GCS options
    generate_parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force re-download of GCS files even if they exist locally",
    )
    generate_parser.add_argument(
        "--gcs-info", action="store_true", help="Show GCS file information and exit"
    )


def get_file_extension(format_type: str) -> str:
    """Get appropriate file extension for schema format."""
    extensions = {
        "json_schema": "json",
        "sql_ddl": "sql",
        "bigquery_ddl": "sql",
        "spark": "txt",
        "bigquery_json": "json",
        "openapi": "json",
    }
    return extensions.get(format_type, "txt")


def generate_filename(data_file: str, format_type: str) -> str:
    """Generate appropriate filename for output schema."""
    base_name = Path(data_file).stem
    if base_name.endswith(".json"):
        base_name = base_name[:-5]  # Remove .json from compressed files

    extension = get_file_extension(format_type)
    return f"{base_name}_schema.{extension}"


def cmd_generate(args) -> None:
    """Execute the generate command."""
    # Handle GCS info request
    if args.gcs_info:
        if is_gcs_path(args.data_file):
            get_gcs_status()
        else:
            print("No GCS paths provided.")
        return

    # Show GCS status if GCS path is involved
    if is_gcs_path(args.data_file):
        get_gcs_status()

    # Determine validation setting
    validate = args.validate and not args.no_validate

    # Determine record limit
    record_n = None if args.all_records else args.sample_size

    try:
        # Set GCS download context
        from ..io_utils import set_force_download_context

        set_force_download_context(args.force_download)

        print(f"🔍 Generating {args.format} schema from {args.data_file}")

        # Load data records
        from ..config import Config
        from ..io_utils import all_records, sample_records

        if args.all_records:
            records = all_records(args.data_file)
        else:
            records = sample_records(args.data_file, record_n or 1000)

        # Generate schema
        cfg = Config()

        # Prepare parameters
        table_name = args.table_name or "generated_table"
        required_fields = set(args.required_fields) if args.required_fields else None

        schema = generate_schema_from_data(
            records,
            cfg,
            format=args.format,
            table_name=table_name,
            required_fields=required_fields,
            validate=validate,
        )

        # Handle output
        if args.output:
            filename = generate_filename(args.data_file, args.format)
            write_output_file(schema, filename, "schemas")
            print(f"✅ Schema saved to output/schemas/{filename}")
        else:
            print(schema)

    except ArgumentError as e:
        raise ArgumentError(f"Schema generation failed: {e}") from e
    except Exception as e:
        raise Exception(f"Unexpected error during schema generation: {e}") from e
