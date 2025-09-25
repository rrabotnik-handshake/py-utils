#!/usr/bin/env python3
"""
BigQuery DDL generation subcommands for schema-diff.

Provides:
- schema-diff ddl: Generate DDL for single table
- schema-diff ddl-batch: Generate DDL for multiple tables
- schema-diff ddl-dataset: Generate DDL for entire dataset
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from .bigquery_ddl import (
    generate_dataset_ddl,
    generate_table_ddl,
    print_pretty_colored_ddl,
)
from .output_utils import print_output_success, write_json_output, write_output_file


def add_ddl_subcommands(subparsers) -> None:
    """Add DDL generation subcommands to the main parser."""

    # schema-diff ddl
    ddl_parser = subparsers.add_parser(
        "ddl",
        help="Generate DDL for a BigQuery table",
        description="Generate pretty, formatted DDL for a single BigQuery table with constraints and options.",
    )
    ddl_parser.add_argument(
        "table_re",
        help="BigQuery table reference (project:dataset.table or dataset.table)",
    )
    ddl_parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="Colorize SQL output (default: auto; respects NO_COLOR)",
    )
    ddl_parser.add_argument(
        "--no-constraints",
        action="store_true",
        help="Skip primary key and foreign key constraints",
    )
    ddl_parser.add_argument(
        "--out", metavar="PATH", help="Write DDL to file (uncolored)"
    )
    ddl_parser.set_defaults(func=cmd_ddl)

    # schema-diff ddl-batch
    batch_parser = subparsers.add_parser(
        "ddl-batch",
        help="Generate DDL for multiple BigQuery tables",
        description="Generate DDL for multiple tables in a dataset with optimized batch queries.",
    )
    batch_parser.add_argument(
        "dataset_re", help="BigQuery dataset reference (project:dataset or dataset)"
    )
    batch_parser.add_argument(
        "tables", nargs="+", help="Table names to generate DDL for"
    )
    batch_parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="Colorize SQL output",
    )
    batch_parser.add_argument(
        "--no-constraints", action="store_true", help="Skip constraints"
    )
    batch_parser.add_argument(
        "--out-dir",
        metavar="DIR",
        help="Write each table's DDL to separate files in directory",
    )
    batch_parser.add_argument(
        "--combined-out", metavar="PATH", help="Write all DDLs to a single file"
    )
    batch_parser.set_defaults(func=cmd_ddl_batch)

    # schema-diff ddl-dataset
    dataset_parser = subparsers.add_parser(
        "ddl-dataset",
        help="Generate DDL for entire BigQuery dataset",
        description="Generate DDL for all tables in a BigQuery dataset.",
    )
    dataset_parser.add_argument(
        "dataset_re", help="BigQuery dataset reference (project:dataset or dataset)"
    )
    dataset_parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="Colorize SQL output",
    )
    dataset_parser.add_argument(
        "--no-constraints", action="store_true", help="Skip constraints"
    )
    dataset_parser.add_argument(
        "--exclude", nargs="*", default=[], help="Table names to exclude"
    )
    dataset_parser.add_argument(
        "--include",
        nargs="*",
        help="Only include these table names (if specified, others are excluded)",
    )
    dataset_parser.add_argument(
        "--out-dir",
        metavar="DIR",
        help="Write each table's DDL to separate files in directory",
    )
    dataset_parser.add_argument(
        "--combined-out", metavar="PATH", help="Write all DDLs to a single file"
    )
    dataset_parser.add_argument(
        "--manifest",
        metavar="PATH",
        help="Write table list and metadata to JSON manifest file",
    )
    dataset_parser.set_defaults(func=cmd_ddl_dataset)


def parse_table_ref(table_ref: str) -> tuple[str | None, str, str]:
    """
    Parse BigQuery table reference.

    Args:
        table_ref: project:dataset.table or dataset.table

    Returns:
        (project_id, dataset_id, table_id)
    """
    if ":" in table_ref:
        project_part, table_part = table_ref.split(":", 1)
    else:
        project_part = None
        table_part = table_ref

    if "." in table_part:
        dataset_id, table_id = table_part.split(".", 1)
    else:
        raise ValueError(
            f"Invalid table reference: {table_ref}. Expected format: [project:]dataset.table"
        )

    return project_part, dataset_id, table_id


def parse_dataset_ref(dataset_ref: str) -> tuple[str | None, str]:
    """
    Parse BigQuery dataset reference.

    Args:
        dataset_ref: project:dataset or dataset

    Returns:
        (project_id, dataset_id)
    """
    if ":" in dataset_ref:
        project_id, dataset_id = dataset_ref.split(":", 1)
        return project_id, dataset_id
    else:
        return None, dataset_ref


def get_default_project() -> str:
    """Get default BigQuery project from client."""
    try:
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError(
                "BigQuery DDL generation requires optional dependencies. "
                "Install with: pip install -e '.[bigquery]'"
            ) from e
    except ImportError as e:
        raise ImportError(
            "BigQuery DDL generation requires optional dependencies. "
            "Install with: pip install -e '.[bigquery]'"
        ) from e

    try:
        client = bigquery.Client()
        return str(client.project)
    except Exception as e:
        raise ValueError(
            f"Unable to determine default project: {e}. Please specify project:dataset.table format."
        ) from None


def cmd_ddl(args) -> None:
    """Handle 'schema-diff ddl' command."""
    try:
        project_id, dataset_id, table_id = parse_table_ref(args.table_ref)
        if not project_id:
            project_id = get_default_project()

        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError(
                "BigQuery DDL generation requires optional dependencies. "
                "Install with: pip install -e '.[bigquery]'"
            ) from e

        client = bigquery.Client(project=project_id)

        ddl = generate_table_ddl(
            client,
            project_id,
            dataset_id,
            table_id,
            include_constraints=not args.no_constraints,
        )

        pretty_sql = print_pretty_colored_ddl(ddl, color_mode=args.color)

        if args.out:
            filename = Path(args.out).name
            output_path = write_output_file(pretty_sql + "\n", filename, "ddl")
            print_output_success(output_path, "DDL")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_ddl_batch(args) -> None:
    """Handle 'schema-diff ddl-batch' command."""
    try:
        project_id, dataset_id = parse_dataset_ref(args.dataset_ref)
        if not project_id:
            project_id = get_default_project()

        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError(
                "BigQuery DDL generation requires optional dependencies. "
                "Install with: pip install -e '.[bigquery]'"
            ) from e

        client = bigquery.Client(project=project_id)

        ddls = generate_dataset_ddl(
            client,
            project_id,
            dataset_id,
            args.tables,
            include_constraints=not args.no_constraints,
        )

        combined_ddl = []

        for table_id in args.tables:
            if table_id not in ddls:
                print(f"Warning: No DDL generated for {table_id}", file=sys.stderr)
                continue

            ddl = ddls[table_id]

            # Print to terminal with color
            print(f"\n{'='*60}")
            print(f"DDL for {project_id}.{dataset_id}.{table_id}")
            print(f"{'='*60}")
            pretty_sql = print_pretty_colored_ddl(ddl, color_mode=args.color)

            combined_ddl.append(f"-- Table: {project_id}.{dataset_id}.{table_id}")
            combined_ddl.append(pretty_sql)
            combined_ddl.append("")

            # Write individual file if requested
            if args.out_dir:
                ddl_subdir = f"ddl/{Path(args.out_dir).name}"
                filename = f"{table_id}.ddl.sql"
                output_path = write_output_file(pretty_sql + "\n", filename, ddl_subdir)
                print(f"✅ {table_id} DDL written to: {output_path}")

        # Write combined file if requested
        if args.combined_out:
            filename = Path(args.combined_out).name
            output_path = write_output_file("\n".join(combined_ddl), filename, "ddl")
            print_output_success(output_path, "Combined DDL")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_ddl_dataset(args) -> None:
    """Handle 'schema-diff ddl-dataset' command."""
    try:
        project_id, dataset_id = parse_dataset_ref(args.dataset_ref)
        if not project_id:
            project_id = get_default_project()

        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError(
                "BigQuery DDL generation requires optional dependencies. "
                "Install with: pip install -e '.[bigquery]'"
            ) from e

        client = bigquery.Client(project=project_id)

        # Get all tables, apply include/exclude filters
        dataset_ref = client.dataset(dataset_id, project=project_id)
        all_tables = [table.table_id for table in client.list_tables(dataset_ref)]

        if args.include:
            table_ids = [t for t in args.include if t in all_tables]
            missing = set(args.include) - set(table_ids)
            if missing:
                print(
                    f"Warning: Tables not found: {', '.join(missing)}", file=sys.stderr
                )
        else:
            table_ids = all_tables

        if args.exclude:
            table_ids = [t for t in table_ids if t not in args.exclude]

        if not table_ids:
            print("No tables to process after filtering.", file=sys.stderr)
            sys.exit(1)

        print(
            f"Generating DDL for {len(table_ids)} tables in {project_id}.{dataset_id}"
        )

        ddls = generate_dataset_ddl(
            client,
            project_id,
            dataset_id,
            table_ids,
            include_constraints=not args.no_constraints,
        )

        combined_ddl = []
        from datetime import datetime

        manifest_data: dict[str, Any] = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "generated_at": str(datetime.now()),
            "tables": {},
        }

        for table_id in sorted(table_ids):
            if table_id not in ddls:
                print(f"Warning: No DDL generated for {table_id}", file=sys.stderr)
                continue

            ddl = ddls[table_id]

            # Print to terminal with color
            print(f"\n{'='*60}")
            print(f"DDL for {project_id}.{dataset_id}.{table_id}")
            print(f"{'='*60}")
            pretty_sql = print_pretty_colored_ddl(ddl, color_mode=args.color)

            combined_ddl.append(f"-- Table: {project_id}.{dataset_id}.{table_id}")
            combined_ddl.append(pretty_sql)
            combined_ddl.append("")

            # Add to manifest
            manifest_data["tables"][table_id] = {
                "full_name": f"{project_id}.{dataset_id}.{table_id}",
                "ddl_length": len(pretty_sql),
                "has_constraints": "ALTER TABLE" in ddl,
            }

            # Write individual file if requested
            if args.out_dir:
                ddl_subdir = f"ddl/{Path(args.out_dir).name}"
                filename = f"{table_id}.ddl.sql"
                output_path = write_output_file(pretty_sql + "\n", filename, ddl_subdir)
                print(f"✅ {table_id} DDL written to: {output_path}")

        # Write combined file if requested
        if args.combined_out:
            filename = Path(args.combined_out).name
            output_path = write_output_file("\n".join(combined_ddl), filename, "ddl")
            print_output_success(output_path, "Combined DDL")

        # Write manifest if requested
        if args.manifest:
            filename = Path(args.manifest).name
            output_path = write_json_output(manifest_data, filename, "ddl")
            print_output_success(output_path, "Manifest")

        print(f"\nProcessed {len(ddls)} tables successfully.")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
