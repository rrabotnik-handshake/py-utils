#!/usr/bin/env python3
"""DDL generation command implementation for schema-diff CLI.

Handles BigQuery DDL generation from live tables with proper argument parsing.
"""
from __future__ import annotations

from typing import Tuple

from ..exceptions import BigQueryError
from ..output_utils import write_output_file


def add_ddl_subcommand(subparsers) -> None:
    """Add DDL subcommand to the parser."""
    ddl_parser = subparsers.add_parser(
        "ddl",
        help="Generate DDL from BigQuery tables",
        description="Generate DDL (Data Definition Language) from BigQuery live tables",
    )

    ddl_subparsers = ddl_parser.add_subparsers(
        dest="ddl_command",
        help="DDL generation commands",
        metavar="{table,batch,dataset}",
    )

    # Table DDL command
    table_parser = ddl_subparsers.add_parser(
        "table", help="Generate DDL for a single table"
    )
    table_parser.add_argument(
        "table_ref", help="Table reference (project:dataset.table)"
    )
    table_parser.add_argument(
        "--output", action="store_true", help="Save DDL to output directory"
    )

    # Batch DDL command
    batch_parser = ddl_subparsers.add_parser(
        "batch", help="Generate DDL for multiple tables"
    )
    batch_parser.add_argument("table_refs", nargs="+", help="Multiple table references")
    batch_parser.add_argument(
        "--output", action="store_true", help="Save DDL files to output directory"
    )

    # Dataset DDL command
    dataset_parser = ddl_subparsers.add_parser(
        "dataset", help="Generate DDL for all tables in a dataset"
    )
    dataset_parser.add_argument(
        "dataset_ref", help="Dataset reference (project:dataset)"
    )
    dataset_parser.add_argument(
        "--output", action="store_true", help="Save DDL files to output directory"
    )


def _parse_table_ref(table_ref: str) -> Tuple[str, str, str]:
    """Parse table reference into project, dataset, table components."""
    # Handle project:dataset.table format
    if ":" in table_ref:
        parts = table_ref.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid table reference format: {table_ref}")
        project_part, table_part = parts
        if "." not in table_part:
            raise ValueError(f"Invalid dataset.table format: {table_ref}")
        table_parts = table_part.split(".")
        if len(table_parts) != 2:
            raise ValueError(f"Invalid table reference format: {table_ref}")
        dataset, table = table_parts
        return project_part, dataset, table

    # Handle dataset.table format
    if "." in table_ref:
        parts = table_ref.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid table reference format: {table_ref}")
        dataset, table = parts
        # Try to get project from environment or use default
        try:
            from ..bigquery_ddl import get_default_project

            project = get_default_project()
        except Exception as e:
            raise ValueError(
                "No project specified and unable to determine default project"
            ) from e
        return project, dataset, table

    raise ValueError(f"Invalid table reference format: {table_ref}")


def _parse_dataset_ref(dataset_ref: str) -> Tuple[str, str]:
    """Parse dataset reference into project, dataset components."""
    if ":" in dataset_ref:
        parts = dataset_ref.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid dataset reference format: {dataset_ref}")
        project, dataset = parts
        return project, dataset

    # Reject ambiguous formats like "project.dataset" - should be "project:dataset"
    if "." in dataset_ref:
        raise ValueError(f"Invalid dataset reference format: {dataset_ref}")

    # Try to get project from environment
    try:
        from ..bigquery_ddl import get_default_project

        project = get_default_project()
        return project, dataset_ref
    except Exception as e:
        raise ValueError(
            "No project specified and unable to determine default project"
        ) from e


def _get_bigquery_client():
    """Get a BigQuery client instance."""
    try:
        from google.cloud import bigquery

        return bigquery.Client()
    except Exception as e:
        raise Exception(f"Failed to create BigQuery client: {e}") from e


def cmd_ddl(args) -> None:
    """Execute the DDL command."""
    if not args.ddl_command:
        print("❌ DDL command required. Use 'table', 'batch', or 'dataset'.")
        import sys

        sys.exit(1)

    try:
        from ..bigquery_ddl import generate_dataset_ddl, generate_table_ddl

        if args.ddl_command == "table":
            # Generate DDL for single table
            project, dataset, table = _parse_table_ref(args.table_ref)
            client = _get_bigquery_client()

            print(f"🔍 Generating DDL for {project}:{dataset}.{table}")
            ddl = generate_table_ddl(client, project, dataset, table)

            if args.output:
                filename = f"{dataset}_{table}_ddl.sql"
                output_path = write_output_file(ddl, filename, "ddl")
                from ..output_utils import print_output_success

                print_output_success(output_path, "DDL")
            else:
                print(ddl)

        elif args.ddl_command == "batch":
            # Generate DDL for multiple tables
            client = _get_bigquery_client()
            ddls = {}
            for table_ref in args.table_refs:
                project, dataset, table = _parse_table_ref(table_ref)
                print(f"🔍 Generating DDL for {project}:{dataset}.{table}")
                ddl = generate_table_ddl(client, project, dataset, table)
                ddls[f"{dataset}.{table}"] = ddl

            if args.output:
                for table_name, ddl in ddls.items():
                    filename = f"{table_name.replace('.', '_')}_ddl.sql"
                    write_output_file(ddl, filename, "ddl")
                print(f"✅ {len(ddls)} DDL files saved to output/ddl/")
            else:
                for table_name, ddl in ddls.items():
                    print(f"\n-- DDL for {table_name}")
                    print(ddl)

        elif args.ddl_command == "dataset":
            # Generate DDL for entire dataset
            project, dataset = _parse_dataset_ref(args.dataset_ref)
            client = _get_bigquery_client()

            print(f"🔍 Generating DDL for all tables in {project}:{dataset}")
            ddls = generate_dataset_ddl(client, project, dataset)

            if args.output:
                for table_name, ddl in ddls.items():
                    filename = f"{dataset}_{table_name}_ddl.sql"
                    write_output_file(ddl, filename, "ddl")
                print(f"✅ {len(ddls)} DDL files saved to output/ddl/")
            else:
                for table_name, ddl in ddls.items():
                    print(f"\n-- DDL for {dataset}.{table_name}")
                    print(ddl)

    except BigQueryError as e:
        import sys

        print(f"❌ Error: BigQuery DDL generation failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        import sys

        print(f"❌ Error: Unexpected error during DDL generation: {e}", file=sys.stderr)
        sys.exit(1)
