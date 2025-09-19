#!/usr/bin/env python3
"""
BigQuery DDL generation subcommands for schema-diff.

Provides:
- schema-diff ddl: Generate DDL for single table
- schema-diff ddl-batch: Generate DDL for multiple tables
- schema-diff ddl-dataset: Generate DDL for entire dataset
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from .bigquery_ddl import (
    generate_table_ddl,
    generate_dataset_ddl,
    print_pretty_colored_ddl,
)


def add_ddl_subcommands(subparsers) -> None:
    """Add DDL generation subcommands to the main parser."""
    
    # schema-diff ddl
    ddl_parser = subparsers.add_parser(
        "ddl",
        help="Generate DDL for a BigQuery table",
        description="Generate pretty, formatted DDL for a single BigQuery table with constraints and options."
    )
    ddl_parser.add_argument("table_ref", help="BigQuery table reference (project:dataset.table or dataset.table)")
    ddl_parser.add_argument(
        "--color", choices=["auto", "always", "never"], default="auto",
        help="Colorize SQL output (default: auto; respects NO_COLOR)"
    )
    ddl_parser.add_argument(
        "--no-constraints", action="store_true",
        help="Skip primary key and foreign key constraints"
    )
    ddl_parser.add_argument(
        "--out", metavar="PATH",
        help="Write DDL to file (uncolored)"
    )
    ddl_parser.set_defaults(func=cmd_ddl)
    
    # schema-diff ddl-batch
    batch_parser = subparsers.add_parser(
        "ddl-batch",
        help="Generate DDL for multiple BigQuery tables",
        description="Generate DDL for multiple tables in a dataset with optimized batch queries."
    )
    batch_parser.add_argument("dataset_ref", help="BigQuery dataset reference (project:dataset or dataset)")
    batch_parser.add_argument("tables", nargs="+", help="Table names to generate DDL for")
    batch_parser.add_argument(
        "--color", choices=["auto", "always", "never"], default="auto",
        help="Colorize SQL output"
    )
    batch_parser.add_argument(
        "--no-constraints", action="store_true",
        help="Skip constraints"
    )
    batch_parser.add_argument(
        "--out-dir", metavar="DIR",
        help="Write each table's DDL to separate files in directory"
    )
    batch_parser.add_argument(
        "--combined-out", metavar="PATH",
        help="Write all DDLs to a single file"
    )
    batch_parser.set_defaults(func=cmd_ddl_batch)
    
    # schema-diff ddl-dataset
    dataset_parser = subparsers.add_parser(
        "ddl-dataset",
        help="Generate DDL for entire BigQuery dataset",
        description="Generate DDL for all tables in a BigQuery dataset."
    )
    dataset_parser.add_argument("dataset_ref", help="BigQuery dataset reference (project:dataset or dataset)")
    dataset_parser.add_argument(
        "--color", choices=["auto", "always", "never"], default="auto",
        help="Colorize SQL output"
    )
    dataset_parser.add_argument(
        "--no-constraints", action="store_true",
        help="Skip constraints"
    )
    dataset_parser.add_argument(
        "--exclude", nargs="*", default=[],
        help="Table names to exclude"
    )
    dataset_parser.add_argument(
        "--include", nargs="*", 
        help="Only include these table names (if specified, others are excluded)"
    )
    dataset_parser.add_argument(
        "--out-dir", metavar="DIR",
        help="Write each table's DDL to separate files in directory"
    )
    dataset_parser.add_argument(
        "--combined-out", metavar="PATH",
        help="Write all DDLs to a single file"
    )
    dataset_parser.add_argument(
        "--manifest", metavar="PATH",
        help="Write table list and metadata to JSON manifest file"
    )
    dataset_parser.set_defaults(func=cmd_ddl_dataset)


def parse_table_ref(table_ref: str) -> tuple[Optional[str], str, str]:
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
        raise ValueError(f"Invalid table reference: {table_ref}. Expected format: [project:]dataset.table")
    
    return project_part, dataset_id, table_id


def parse_dataset_ref(dataset_ref: str) -> tuple[Optional[str], str]:
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
        from google.cloud import bigquery
        client = bigquery.Client()
        return client.project
    except Exception as e:
        raise ValueError(f"Unable to determine default project: {e}. Please specify project:dataset.table format.")


def cmd_ddl(args) -> None:
    """Handle 'schema-diff ddl' command."""
    try:
        project_id, dataset_id, table_id = parse_table_ref(args.table_ref)
        if not project_id:
            project_id = get_default_project()
        
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        
        ddl = generate_table_ddl(
            client, project_id, dataset_id, table_id,
            include_constraints=not args.no_constraints
        )
        
        pretty_sql = print_pretty_colored_ddl(ddl, color_mode=args.color)
        
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(pretty_sql + "\n")
            print(f"\nWrote DDL to: {args.out}")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_ddl_batch(args) -> None:
    """Handle 'schema-diff ddl-batch' command."""
    try:
        project_id, dataset_id = parse_dataset_ref(args.dataset_ref)
        if not project_id:
            project_id = get_default_project()
        
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        
        ddls = generate_dataset_ddl(
            client, project_id, dataset_id, args.tables,
            include_constraints=not args.no_constraints
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
                out_dir = Path(args.out_dir)
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"{table_id}.ddl.sql"
                with open(out_file, "w", encoding="utf-8") as f:
                    f.write(pretty_sql + "\n")
                print(f"Wrote {table_id} DDL to: {out_file}")
        
        # Write combined file if requested
        if args.combined_out:
            with open(args.combined_out, "w", encoding="utf-8") as f:
                f.write("\n".join(combined_ddl))
            print(f"\nWrote combined DDL to: {args.combined_out}")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_ddl_dataset(args) -> None:
    """Handle 'schema-diff ddl-dataset' command."""
    try:
        project_id, dataset_id = parse_dataset_ref(args.dataset_ref)
        if not project_id:
            project_id = get_default_project()
        
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        
        # Get all tables, apply include/exclude filters
        dataset_ref = client.dataset(dataset_id, project=project_id)
        all_tables = [table.table_id for table in client.list_tables(dataset_ref)]
        
        if args.include:
            table_ids = [t for t in args.include if t in all_tables]
            missing = set(args.include) - set(table_ids)
            if missing:
                print(f"Warning: Tables not found: {', '.join(missing)}", file=sys.stderr)
        else:
            table_ids = all_tables
        
        if args.exclude:
            table_ids = [t for t in table_ids if t not in args.exclude]
        
        if not table_ids:
            print("No tables to process after filtering.", file=sys.stderr)
            sys.exit(1)
        
        print(f"Generating DDL for {len(table_ids)} tables in {project_id}.{dataset_id}")
        
        ddls = generate_dataset_ddl(
            client, project_id, dataset_id, table_ids,
            include_constraints=not args.no_constraints
        )
        
        combined_ddl = []
        from datetime import datetime
        manifest_data = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "generated_at": str(datetime.now()),
            "tables": {}
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
                "has_constraints": "ALTER TABLE" in ddl
            }
            
            # Write individual file if requested
            if args.out_dir:
                out_dir = Path(args.out_dir)
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"{table_id}.ddl.sql"
                with open(out_file, "w", encoding="utf-8") as f:
                    f.write(pretty_sql + "\n")
                print(f"Wrote {table_id} DDL to: {out_file}")
        
        # Write combined file if requested
        if args.combined_out:
            with open(args.combined_out, "w", encoding="utf-8") as f:
                f.write("\n".join(combined_ddl))
            print(f"\nWrote combined DDL to: {args.combined_out}")
        
        # Write manifest if requested
        if args.manifest:
            with open(args.manifest, "w", encoding="utf-8") as f:
                json.dump(manifest_data, f, indent=2)
            print(f"Wrote manifest to: {args.manifest}")
            
        print(f"\nProcessed {len(ddls)} tables successfully.")
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
