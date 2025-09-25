"""
CLI entrypoint for schema-diff.

This module implements the `schema-diff` command-line tool, which compares
schemas across multiple formats and sources with comprehensive analysis capabilities:

- DATA files (NDJSON, JSON, JSONL, GZIP-compressed) — infers schema by sampling or processing all records
- JSON Schema documents (standard format and BigQuery schema conversion)
- Spark schema dumps (text format with deep nested array<struct<...>> parsing)
- SQL CREATE TABLE definitions (Postgres-like and BigQuery DDL with STRUCT/ARRAY support)
- dbt manifest.json, schema.yml, or model.sql files
- Protobuf message definitions (.proto files)

Key Features:
- Comprehensive analysis with --all-records for complete field discovery
- Focused comparison with --fields for specific field targeting
- Nested field support with dot notation (experience.title) and array semantics ([])
- Path change detection for field location differences
- Clear presence terminology (missing vs nullable)
- Sampling artifact filtering for cleaner type mismatch reporting

Comparison modes supported:
  * DATA ↔ DATA  (record-level type comparison with full nested support)
  * DATA ↔ schema (classic mode with enhanced presence injection)
  * any ↔ any    (general mode with --left/--right kinds and source type awareness)

Output sections:
- Only in left/right: fields present on one side only
- Missing/optional (presence): optionality differences with clear terminology
- Common fields: matching fields (--show-common) with [] array notation
- Type mismatches: real type conflicts excluding sampling artifacts
- Path changes: same field names in different locations

Main entrypoint: `main()` — builds argparse parser, interprets args,
and dispatches to the proper comparison function with source type tracking.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from pathlib import Path

from deepdiff import DeepDiff

from .compare import compare_data_to_ref, compare_trees
from .config import Config
from .helpfmt import ColorDefaultsFormatter
from .io_utils import MAX_RECORD_SAFETY_LIMIT, all_records, nth_record, sample_records
from .json_data_file_parser import merged_schema_from_samples
from .json_schema_parser import schema_from_json_schema_file
from .loader import load_left_or_right
from .normalize import walk_normalize
from .output_utils import (
    generate_timestamp_filename,
    print_output_success,
    write_json_output,
    write_output_file,
)
from .report import build_report_struct, print_report_text, print_samples
from .spark_schema_parser import schema_from_spark_schema_file
from .sql_schema_parser import schema_from_sql_schema_file
from .utils import coerce_root_to_field_dict, filter_schema_by_fields


def _auto_colors(force_color: bool, no_color: bool) -> bool:
    """
    Decide whether to enable ANSI colors in output.
    - Explicit --no-color disables
    - Explicit --force-color enables
    - Otherwise: enabled only if stdout is a TTY and NO_COLOR is not set
    """
    if no_color:
        return False
    if force_color:
        return True
    return sys.stdout.isatty() and (os.environ.get("NO_COLOR") is None)


def build_parser(color_enabled: bool) -> argparse.ArgumentParser:
    """
    Build the argparse parser for schema-diff.
    Groups flags by functional area (record selection, sampling, output, etc.).
    Returns the ready-to-use parser.
    """
    desc = (
        "Compare schema (types) of JSON/NDJSON files (gzip, arrays, NDJSON supported), "
        "or validate against a JSON/Spark/SQL/dbt schema. "
        "Also supports general two-source mode: any ↔ any."
    )
    parser = argparse.ArgumentParser(
        prog="schema-diff",
        description=desc,
        formatter_class=ColorDefaultsFormatter,
    )

    # ----------------------------------------------------------------------
    # Positional arguments
    # ----------------------------------------------------------------------
    parser.add_argument("file1", help="left input (data or schema)")
    parser.add_argument(
        "file2", nargs="?", default=None, help="right input (data or schema)"
    )

    # ----------------------------------------------------------------------
    # Record selection (DATA-only sources)
    # ----------------------------------------------------------------------
    sel = parser.add_argument_group("Record selection")
    sel.add_argument(
        "--first-record",
        action="store_true",
        help="compare only the first record from each DATA file (same as --record 1)",
    )
    sel.add_argument(
        "--record",
        type=int,
        metavar="N",
        help="compare the N-th record from each DATA file (1-based)",
    )
    sel.add_argument(
        "--record1",
        type=int,
        metavar="N",
        help="N-th record for file1 (overrides --record)",
    )
    sel.add_argument(
        "--record2",
        type=int,
        metavar="N",
        help="N-th record for file2 (overrides --record)",
    )
    sel.add_argument(
        "--both-modes",
        action="store_true",
        help="(DATA↔DATA only) run two comparisons: chosen record(s) AND random sampled records",
    )

    # ----------------------------------------------------------------------
    # Sampling controls
    # ----------------------------------------------------------------------
    samp = parser.add_argument_group("Sampling")
    samp.add_argument(
        "-k",
        "--samples",
        type=int,
        default=1000,
        metavar="N",
        help="records to sample per DATA file",
    )
    samp.add_argument(
        "--all-records",
        action="store_true",
        help="process ALL records instead of sampling (may be memory intensive)",
    )
    samp.add_argument("--seed", type=int, help="random seed (reproducible sampling)")
    samp.add_argument(
        "--show-samples", action="store_true", help="print the chosen/sampled records"
    )

    # ----------------------------------------------------------------------
    # Output formatting controls
    # ----------------------------------------------------------------------
    out = parser.add_argument_group("Output control")
    out.add_argument("--no-color", action="store_true", help="disable ANSI colors")
    out.add_argument(
        "--force-color",
        action="store_true",
        help="force ANSI colors even if stdout is not a TTY or NO_COLOR is set",
    )
    out.add_argument(
        "--no-presence",
        action="store_true",
        help="suppress 'Missing / optional (presence)' section",
    )
    out.add_argument(
        "--show-common",
        action="store_true",
        help="print the sorted list of fields that appear in both sides",
    )
    out.add_argument(
        "--fields",
        type=str,
        nargs="+",
        metavar="FIELD",
        help="compare only specific fields (comma-separated or space-separated list)",
    )

    # ----------------------------------------------------------------------
    # Export (structured output)
    # ----------------------------------------------------------------------
    exp = parser.add_argument_group("Export")
    exp.add_argument(
        "--json-out", type=str, metavar="PATH", help="write diff JSON to this path"
    )
    exp.add_argument(
        "--dump-schemas",
        type=str,
        metavar="PATH",
        help="write normalized left/right schemas to this path",
    )
    exp.add_argument(
        "--output",
        "-o",
        action="store_true",
        help="save comparison results and migration analysis to ./output directory (default: console only)",
    )
    exp.add_argument(
        "--output-format",
        choices=["markdown", "text", "json"],
        default="markdown",
        help="format for migration analysis report (default: markdown)",
    )

    # ----------------------------------------------------------------------
    # Classic external-schema options (DATA → schema)
    # ----------------------------------------------------------------------
    sch = parser.add_argument_group("External schema (reference)")
    sch.add_argument(
        "--json-schema",
        type=str,
        metavar="JSON_SCHEMA.json",
        help="compare DATA file1 vs a JSON Schema file",
    )
    sch.add_argument(
        "--spark-schema",
        type=str,
        metavar="SPARK_SCHEMA.txt",
        help="compare DATA file1 vs a Spark-style schema text",
    )
    sch.add_argument(
        "--sql-schema",
        type=str,
        metavar="SCHEMA.sql",
        help="compare DATA file1 vs a SQL schema (CREATE TABLE or column list)",
    )
    sch.add_argument(
        "--sql-table",
        type=str,
        metavar="TABLE",
        help="table name to select if --sql-schema has multiple tables",
    )

    # ----------------------------------------------------------------------
    # General two-source mode (any ↔ any)
    # ----------------------------------------------------------------------
    gen = parser.add_argument_group("General two-source mode (any ↔ any)")
    gen.add_argument(
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
        ],
        help="kind of file1 (default: auto-detect)",
    )
    gen.add_argument(
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
        ],
        help="kind of file2 (default: auto-detect)",
    )
    gen.add_argument("--left-table", help="table to select for file1 when --left sql")
    gen.add_argument("--right-table", help="table to select for file2 when --right sql")
    gen.add_argument(
        "--left-model",
        help="model name for file1 when using dbt manifest/schema.yml/model",
    )
    gen.add_argument(
        "--right-model",
        help="model name for file2 when using dbt manifest/schema.yml/model",
    )
    gen.add_argument(
        "--left-message",
        help="When --left is protobuf (or auto-detected .proto), choose the Protobuf message to use.",
    )
    gen.add_argument(
        "--right-message",
        help="When --right is protobuf (or auto-detected .proto), choose the Protobuf message to use.",
    )

    # ----------------------------------------------------------------------
    # Inference options
    # ----------------------------------------------------------------------
    inf = parser.add_argument_group("Inference")
    inf.add_argument(
        "--infer-datetimes",
        action="store_true",
        help="treat ISO-like strings as timestamp/date/time on the DATA side",
    )

    return parser


def main():
    """
    CLI entrypoint.
    1. Parse arguments
    2. Decide which comparison mode to run (general, classic data→schema, or data↔data)
    3. Dispatch to the appropriate compare function
    """
    parser = build_parser(True)
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Input validation and mode selection
    # ------------------------------------------------------------------
    classic_selected = [args.json_schema, args.spark_schema, args.sql_schema]
    if sum(1 for x in classic_selected if x) > 1:
        parser.error("Use only one of --json-schema, --spark-schema, or --sql-schema.")

    # Auto-detect file types if not explicitly specified
    auto_left = args.left
    auto_right = args.right
    if not auto_left and args.file1:
        from .loader import _guess_kind

        auto_left = _guess_kind(args.file1)
    if not auto_right and args.file2:
        from .loader import _guess_kind

        auto_right = _guess_kind(args.file2)

    # Use general mode if:
    # 1. Explicit type/table/model/message arguments are provided, OR
    # 2. Auto-detected types are different, OR
    # 3. Auto-detected types are non-data (schema sources)
    general_mode = any(
        [
            args.left,
            args.right,
            args.left_table,
            args.right_table,
            args.left_model,
            args.right_model,
            args.left_message,
            args.right_message,
            # Auto-detection triggers
            auto_left != auto_right,  # Different types detected
            auto_left != "data",  # Left is not data
            auto_right != "data",  # Right is not data
        ]
    )

    if not args.file2 and not (
        args.json_schema or args.spark_schema or args.sql_schema
    ):
        parser.error(
            "file2 is required unless you pass --json-schema / --spark-schema / --sql-schema."
        )

    if args.seed is not None:
        random.seed(args.seed)

    cfg = Config(
        infer_datetimes=bool(args.infer_datetimes),
        color_enabled=_auto_colors(args.force_color, args.no_color),
        show_presence=not bool(args.no_presence),
    )
    RED, GRN, YEL, CYN, RST = cfg.colors()

    if args.first_record and args.record is None:
        args.record = 1
    r1_idx = args.record1 if args.record1 is not None else args.record
    r2_idx = args.record2 if args.record2 is not None else args.record

    # ------------------------------------------------------------------
    # Mode 1: General any ↔ any comparison
    # ------------------------------------------------------------------
    if general_mode:
        # Load left + right sides via loader
        left_tree, left_required, left_label = load_left_or_right(
            args.file1,
            kind=args.left or auto_left,
            cfg=cfg,
            samples=args.samples,
            first_record=(r1_idx or 1) if r1_idx is not None else None,
            all_records=args.all_records,
            sql_table=args.left_table,
            dbt_model=args.left_model,
            proto_message=args.left_message,
        )
        right_tree, right_required, right_label = load_left_or_right(
            args.file2,
            kind=args.right or auto_right,
            cfg=cfg,
            samples=args.samples,
            first_record=(r2_idx or 1) if r2_idx is not None else None,
            all_records=args.all_records,
            sql_table=args.right_table,
            dbt_model=args.right_model,
            proto_message=args.right_message,
        )

        # Optionally print sampled records
        if args.show_samples:
            if (args.left or auto_left) in (None, "auto", "data"):
                recs = (
                    nth_record(args.file1, r1_idx or 1)
                    if r1_idx
                    else sample_records(args.file1, args.samples)
                )
                print_samples(args.file1, recs, colors=cfg.colors())
            if (args.right or auto_right) in (None, "auto", "data"):
                recs = (
                    nth_record(args.file2, r2_idx or 1)
                    if r2_idx
                    else sample_records(args.file2, args.samples)
                )
                print_samples(args.file2, recs, colors=cfg.colors())

        # Apply field filtering if specified
        if hasattr(args, "fields") and args.fields:
            # Flatten comma-separated values if any
            fields_list = []
            for field in args.fields:
                fields_list.extend([f.strip() for f in field.split(",")])
            left_tree = filter_schema_by_fields(left_tree, fields_list)
            right_tree = filter_schema_by_fields(right_tree, fields_list)

        report = compare_trees(
            left_label,
            right_label,
            left_tree,
            left_required,
            right_tree,
            right_required,
            cfg=cfg,
            dump_schemas=args.dump_schemas,
            json_out=args.json_out,
            title_suffix="",
            show_common=args.show_common,
            left_source_type=args.left or auto_left,
            right_source_type=args.right or auto_right,
        )

        # Migration analysis (when output is requested)
        if args.output and report:
            import io
            import sys

            from .migration_analyzer import (
                analyze_migration_impact,
                generate_migration_report,
            )

            # Capture console output for full diff
            old_stdout = sys.stdout
            sys.stdout = buffer = io.StringIO()

            # Re-run the comparison to capture console output
            compare_trees(
                left_label,
                right_label,
                left_tree,
                left_required,
                right_tree,
                right_required,
                cfg=cfg,
                title_suffix="",
                show_common=args.show_common,
                left_source_type=args.left or auto_left,
                right_source_type=args.right or auto_right,
            )

            sys.stdout = old_stdout
            full_diff_output = buffer.getvalue()

            # Build commands used for analysis
            commands_used = []
            cmd_parts = ["schema-diff", args.file1, args.file2]
            if args.all_records:
                cmd_parts.append("--all-records")
            if args.show_common:
                cmd_parts.append("--show-common")
            if args.left:
                cmd_parts.extend(["--left", args.left])
            if args.right:
                cmd_parts.extend(["--right", args.right])
            commands_used.append(" ".join(cmd_parts))

            # Add common fields to report if not present
            if "common_fields" not in report:
                from .utils import flatten_paths

                left_dict = coerce_root_to_field_dict(left_tree)
                right_dict = coerce_root_to_field_dict(right_tree)
                paths1 = flatten_paths(left_dict)
                paths2 = flatten_paths(right_dict)
                common_fields_list = sorted(set(paths1) & set(paths2))
                report["common_fields"] = common_fields_list

            # Analyze migration impact
            migration_analysis = analyze_migration_impact(
                report, left_label, right_label, commands_used, full_diff_output
            )

            # Generate and save report
            migration_report = generate_migration_report(
                migration_analysis, args.output_format
            )

            # Auto-generate filename based on format
            file_ext = {"markdown": ".md", "text": ".txt", "json": ".json"}[
                args.output_format
            ]
            filename = generate_timestamp_filename("migration_analysis", file_ext)
            output_path = write_output_file(migration_report, filename, "analysis")
            print_output_success(output_path, "Migration analysis")

        return

    # ------------------------------------------------------------------
    # Mode 2: Classic DATA → external schema
    # ------------------------------------------------------------------
    if args.json_schema or args.spark_schema or args.sql_schema:
        if args.all_records:
            s1 = all_records(args.file1, max_records=MAX_RECORD_SAFETY_LIMIT)
            title1 = f"; all {len(s1)} records"
        elif r1_idx is not None:
            s1 = nth_record(args.file1, r1_idx or 1)
            title1 = f"; record #{r1_idx or 1}"
        else:
            s1 = sample_records(args.file1, args.samples)
            title1 = f"; random {args.samples}-record samples"

        required_paths = None
        if args.json_schema:
            ref_schema, required_paths = schema_from_json_schema_file(args.json_schema)
            label = args.json_schema
        elif args.spark_schema:
            ref_schema, required_paths = schema_from_spark_schema_file(
                args.spark_schema
            )
            label = args.spark_schema
        else:
            ref_schema, required_paths = schema_from_sql_schema_file(
                args.sql_schema, table=args.sql_table
            )
            label = (
                args.sql_schema
                if not args.sql_table
                else f"{args.sql_schema}#{args.sql_table}"
            )

        if args.show_samples:
            print_samples(args.file1, s1, colors=cfg.colors())

        compare_data_to_ref(
            args.file1,
            label,
            ref_schema,
            cfg=cfg,
            s1_records=s1,
            dump_schemas=args.dump_schemas,
            json_out=args.json_out,
            title_suffix=title1,
            required_paths=required_paths,
            show_common=args.show_common,
            ref_source_type=args.right,
        )
        return

    # ------------------------------------------------------------------
    # Mode 3: Classic DATA ↔ DATA
    # ------------------------------------------------------------------
    if args.all_records:
        s1 = all_records(args.file1, max_records=MAX_RECORD_SAFETY_LIMIT)
        s2 = all_records(args.file2, max_records=MAX_RECORD_SAFETY_LIMIT)
        title1 = f"; all {len(s1)} vs {len(s2)} records"
    elif r1_idx is not None:
        s1 = nth_record(args.file1, r1_idx or 1)
        title1 = f"; record #{r1_idx or 1}"
        s2 = (
            nth_record(args.file2, r2_idx or 1)
            if r2_idx is not None
            else sample_records(args.file2, args.samples)
        )
    else:
        s1 = sample_records(args.file1, args.samples)
        title1 = f"; random {args.samples}-record samples"
        s2 = sample_records(args.file2, args.samples)

    runs = [(s1, s2, title1)]
    if args.both_modes and (r1_idx is not None or r2_idx is not None):
        runs.append(
            (
                sample_records(args.file1, args.samples),
                sample_records(args.file2, args.samples),
                f"; random {args.samples}-record samples",
            )
        )

    all_reports = []
    for s1_run, s2_run, title_suffix in runs:
        if args.show_samples:
            print_samples(args.file1, s1_run, colors=cfg.colors())
            print_samples(args.file2, s2_run, colors=cfg.colors())

        sch1 = merged_schema_from_samples(s1_run, cfg)
        sch2 = merged_schema_from_samples(s2_run, cfg)

        # Apply field filtering if specified
        if hasattr(args, "fields") and args.fields:
            # Flatten comma-separated values if any
            fields_list = []
            for field in args.fields:
                fields_list.extend([f.strip() for f in field.split(",")])
            sch1 = filter_schema_by_fields(sch1, fields_list)
            sch2 = filter_schema_by_fields(sch2, fields_list)

        sch1n, sch2n = walk_normalize(sch1), walk_normalize(sch2)

        # Show common fields if requested
        if args.show_common:
            from .report import print_common_fields

            sch1n_dict = coerce_root_to_field_dict(sch1n)
            sch2n_dict = coerce_root_to_field_dict(sch2n)
            print_common_fields(
                args.file1, args.file2, sch1n_dict, sch2n_dict, cfg.colors()
            )

        diff = DeepDiff(sch1n, sch2n, ignore_order=True)
        direction = f"{args.file1} -> {args.file2}"
        if not diff:
            print(
                f"\n{CYN}=== Schema diff (types only, {direction}{title_suffix}) ==={RST}\nNo differences."
            )
            if args.dump_schemas:
                # Use standardized output approach
                schema_data = {"file1": sch1n, "file2": sch2n}
                filename = Path(args.dump_schemas).name
                output_path = write_json_output(schema_data, filename, "schemas")
                print_output_success(output_path, "Schema dump")
            all_reports.append(
                {
                    "meta": {
                        "direction": direction,
                        "mode": title_suffix.strip("; ").strip(),
                    },
                    "note": "No differences",
                }
            )
            continue

        report = build_report_struct(
            diff, args.file1, args.file2, include_presence=cfg.show_presence
        )
        report["meta"]["mode"] = title_suffix.strip("; ").strip()
        print_report_text(
            report,
            args.file1,
            args.file2,
            colors=cfg.colors(),
            show_presence=cfg.show_presence,
            title_suffix=title_suffix,
            left_source_type="data",
            right_source_type="data",
        )

        # Path changes (same field name in different locations)
        from .report import print_path_changes
        from .utils import compute_path_changes

        sch1n_dict = coerce_root_to_field_dict(sch1n)
        sch2n_dict = coerce_root_to_field_dict(sch2n)
        path_changes = compute_path_changes(sch1n_dict, sch2n_dict)
        print_path_changes(args.file1, args.file2, path_changes, colors=cfg.colors())

        # Migration analysis generation (when output is requested)
        if args.output:
            import io
            import sys

            from .migration_analyzer import (
                analyze_migration_impact,
                generate_migration_report,
            )

            # Build commands used for analysis
            commands_used = []
            cmd_parts = ["schema-diff", args.file1, args.file2]
            if args.all_records:
                cmd_parts.append("--all-records")
            if args.show_common:
                cmd_parts.append("--show-common")
            if args.left:
                cmd_parts.extend(["--left", args.left])
            if args.right:
                cmd_parts.extend(["--right", args.right])
            commands_used.append(" ".join(cmd_parts))

            # Capture the diff output that was already printed
            # We need to regenerate it to capture the text
            captured_output = io.StringIO()
            original_stdout = sys.stdout
            sys.stdout = captured_output

            try:
                # Include common fields if requested
                if args.show_common:
                    from .report import print_common_fields

                    print_common_fields(
                        args.file1,
                        args.file2,
                        sch1n_dict,
                        sch2n_dict,
                        ("", "", "", "", ""),
                    )

                # Re-generate the output for capture
                print_report_text(
                    report,
                    args.file1,
                    args.file2,
                    colors=("", "", "", "", ""),
                    show_presence=cfg.show_presence,
                    title_suffix=title_suffix,
                    left_source_type="data",
                    right_source_type="data",
                )

                # Print path changes
                from .report import print_path_changes

                print_path_changes(
                    args.file1, args.file2, path_changes, colors=("", "", "", "", "")
                )

                full_diff_output = captured_output.getvalue()
            finally:
                sys.stdout = original_stdout

            # Include path changes in the report
            report["path_changes"] = path_changes

            # Calculate and include common fields for migration analysis
            from .utils import flatten_paths

            paths1 = flatten_paths(sch1n_dict)
            paths2 = flatten_paths(sch2n_dict)
            common_fields_list = sorted(set(paths1) & set(paths2))
            report["common_fields"] = common_fields_list

            # Analyze migration impact
            migration_analysis = analyze_migration_impact(
                report, args.file1, args.file2, commands_used, full_diff_output
            )

            # Generate and save report
            migration_report = generate_migration_report(
                migration_analysis, args.output_format
            )

            # Auto-generate filename based on format
            file_ext = {"markdown": ".md", "text": ".txt", "json": ".json"}[
                args.output_format
            ]
            filename = generate_timestamp_filename("migration_analysis", file_ext)
            output_path = write_output_file(migration_report, filename, "analysis")
            print_output_success(output_path, "Migration analysis")

        all_reports.append(report)

    if args.json_out:
        # Use standardized output approach
        data = all_reports if len(all_reports) > 1 else all_reports[0]
        filename = Path(args.json_out).name
        output_path = write_json_output(data, filename, "reports")
        print_output_success(output_path, "JSON report")


if __name__ == "__main__":
    main()
