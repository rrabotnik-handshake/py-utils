from __future__ import annotations

import argparse
import os
import sys
import random
import json

from deepdiff import DeepDiff

from .config import Config
from .helpfmt import ColorDefaultsFormatter
from .io_utils import sample_records, nth_record
from .json_schema_parser import schema_from_json_schema_file
from .spark_schema_parser import schema_from_spark_schema_file
from .sql_schema_parser import schema_from_sql_schema_file
from .normalize import walk_normalize
from .json_data_file_parser import merged_schema_from_samples
from .report import build_report_struct, print_report_text, print_samples
from .loader import load_left_or_right
from .compare import compare_trees, compare_data_to_ref


def _auto_colors(force_color: bool, no_color: bool) -> bool:
    if no_color:
        return False
    if force_color:
        return True
    return sys.stdout.isatty() and (os.environ.get("NO_COLOR") is None)


def build_parser(color_enabled: bool) -> argparse.ArgumentParser:
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

    # Positional
    parser.add_argument("file1", help="left input (data or schema)")
    parser.add_argument("file2", nargs="?", default=None,
                        help="right input (data or schema)")

    # Record selection (for DATA sources)
    sel = parser.add_argument_group("Record selection")
    sel.add_argument("--first-record", action="store_true",
                     help="compare only the first record from each DATA file (same as --record 1)")
    sel.add_argument("--record", type=int, metavar="N",
                     help="compare the N-th record from each DATA file (1-based)")
    sel.add_argument("--record1", type=int, metavar="N",
                     help="N-th record for file1 (overrides --record)")
    sel.add_argument("--record2", type=int, metavar="N",
                     help="N-th record for file2 (overrides --record)")
    sel.add_argument("--both-modes", action="store_true",
                     help="(DATA↔DATA only) run two comparisons: chosen record(s) AND random sampled records")

    # Sampling
    samp = parser.add_argument_group("Sampling")
    samp.add_argument("-k", "--samples", type=int, default=3, metavar="N",
                      help="records to sample per DATA file")
    samp.add_argument("--seed", type=int,
                      help="random seed (reproducible sampling)")
    samp.add_argument("--show-samples", action="store_true",
                      help="print the chosen/sampled records")

    # Output control
    out = parser.add_argument_group("Output control")
    out.add_argument("--no-color", action="store_true",
                     help="disable ANSI colors")
    out.add_argument("--force-color", action="store_true",
                     help="force ANSI colors even if stdout is not a TTY or NO_COLOR is set")
    out.add_argument("--no-presence", action="store_true",
                     help="suppress 'Missing / optional (presence)' section (show only true schema mismatches)")
    out.add_argument("--show-common", action="store_true",
                     help="print the sorted list of fields that appear in both sides")

    # Export
    exp = parser.add_argument_group("Export")
    exp.add_argument("--json-out", type=str, metavar="PATH",
                     help="write diff JSON to this path")
    exp.add_argument("--dump-schemas", type=str, metavar="PATH",
                     help="write normalized left/right schemas to this path")

    # “Classic” external schema options (back-compat)
    sch = parser.add_argument_group("External schema (reference)")
    sch.add_argument("--json-schema", type=str, metavar="JSON_SCHEMA.json",
                     help="compare DATA file1 vs a JSON Schema file")
    sch.add_argument("--spark-schema", type=str, metavar="SPARK_SCHEMA.txt",
                     help="compare DATA file1 vs a Spark-style schema text")
    sch.add_argument("--sql-schema",  type=str, metavar="SCHEMA.sql",
                     help="compare DATA file1 vs a SQL schema (CREATE TABLE or column list)")
    sch.add_argument("--sql-table",   type=str, metavar="TABLE",
                     help="table name to select if --sql-schema has multiple tables")

    # General two-source mode (any ↔ any)
    gen = parser.add_argument_group("General two-source mode (any ↔ any)")
    gen.add_argument("--left",  choices=["auto", "data", "jsonschema", "spark", "sql", "dbt-manifest", "dbt-yml"],
                     help="kind of file1 (default: auto-detect)")
    gen.add_argument("--right", choices=["auto", "data", "jsonschema", "spark", "sql", "dbt-manifest", "dbt-yml"],
                     help="kind of file2 (default: auto-detect)")
    gen.add_argument("--left-table",
                     help="table to select for file1 when --left sql")
    gen.add_argument("--right-table",
                     help="table to select for file2 when --right sql")
    gen.add_argument("--left-dbt-model",
                     help="model name for file1 when using dbt manifest/schema.yml")
    gen.add_argument("--right-dbt-model",
                     help="model name for file2 when using dbt manifest/schema.yml")
    gen.add_argument("--left-proto-message",
                     help="When --left is protobuf (or auto-detected .proto), choose the Protobuf message to use.")
    gen.add_argument("--right-proto-message",
                     help="When --right is protobuf (or auto-detected .proto), choose the Protobuf message to use.")

    # Inference
    inf = parser.add_argument_group("Inference")
    inf.add_argument("--infer-datetimes", action="store_true",
                     help="treat ISO-like strings as timestamp/date/time on the DATA side")

    return parser


def main():
    parser = build_parser(True)
    args = parser.parse_args()

    # classic external-schema switches are mutually exclusive
    classic_selected = [args.json_schema, args.spark_schema, args.sql_schema]
    if sum(1 for x in classic_selected if x) > 1:
        parser.error(
            "Use only one of --json-schema, --spark-schema, or --sql-schema.")

    # “General mode” is enabled if any per-side kind/selector is given
    general_mode = any([
        args.left, args.right,
        args.left_table, args.right_table,
        args.left_dbt_model, args.right_dbt_model,
    ])

    # file2 is required unless you are in classic data→schema mode
    if not args.file2 and not (args.json_schema or args.spark_schema or args.sql_schema):
        parser.error(
            "file2 is required unless you pass --json-schema / --spark-schema / --sql-schema.")

    # Seed RNG if requested
    if args.seed is not None:
        random.seed(args.seed)

    cfg = Config(
        infer_datetimes=bool(args.infer_datetimes),
        color_enabled=_auto_colors(args.force_color, args.no_color),
        show_presence=not bool(args.no_presence),
    )
    RED, GRN, YEL, CYN, RST = cfg.colors()

    # Record selection indices for DATA sources
    if args.first_record and args.record is None:
        args.record = 1
    r1_idx = args.record1 if args.record1 is not None else args.record
    r2_idx = args.record2 if args.record2 is not None else args.record

    # ── General two-source mode ───────────────────────────────────────────
    if general_mode:
        if not args.file2:
            parser.error("file2 is required in general two-source mode.")

        # Load left (data or schema)
        left_tree, left_required, left_label = load_left_or_right(
            args.file1,
            kind=args.left,
            cfg=cfg,
            samples=args.samples,
            first_record=(r1_idx or 1) if r1_idx is not None else None,
            sql_table=args.left_table,
            dbt_model=args.left_dbt_model,
            proto_message=args.left_proto_message,
        )

        # Load right (data or schema)
        right_tree, right_required, right_label = load_left_or_right(
            args.file2,
            kind=args.right,
            cfg=cfg,
            samples=args.samples,
            first_record=(r2_idx or 1) if r2_idx is not None else None,
            sql_table=args.right_table,
            dbt_model=args.right_dbt_model,
            proto_message=args.right_proto_message,
        )

        # Optionally print selected samples for DATA sources
        if args.show_samples:
            # For left
            if args.left in (None, "auto", "data"):
                if r1_idx is None:
                    print_samples(args.file1, sample_records(
                        args.file1, args.samples), colors=cfg.colors())
                else:
                    print_samples(args.file1, nth_record(
                        args.file1, r1_idx or 1), colors=cfg.colors())
            # For right
            if args.right in (None, "auto", "data"):
                if r2_idx is None:
                    print_samples(args.file2, sample_records(
                        args.file2, args.samples), colors=cfg.colors())
                else:
                    print_samples(args.file2, nth_record(
                        args.file2, r2_idx or 1), colors=cfg.colors())

        # Presence-aware compare of two trees
        compare_trees(
            left_label, right_label,
            left_tree, left_required,
            right_tree, right_required,
            cfg=cfg,
            dump_schemas=args.dump_schemas,
            json_out=args.json_out,
            title_suffix="",  # labels already include sample/record info for DATA
            show_common=args.show_common,
        )
        return

    # ── Classic external schema branch (DATA → schema) ───────────────────
    if args.json_schema or args.spark_schema or args.sql_schema:
        # Choose file1 record(s)
        s1 = nth_record(args.file1, r1_idx or 1) if r1_idx is not None else sample_records(
            args.file1, args.samples)
        title1 = f"; record #{r1_idx or 1}" if r1_idx is not None else f"; random {args.samples}-record samples"

        required_paths = None
        if args.json_schema:
            ref_schema, required_paths = schema_from_json_schema_file(
                args.json_schema)
            label = args.json_schema
        elif args.spark_schema:
            ref_schema, required_paths = schema_from_spark_schema_file(
                args.spark_schema)
            label = args.spark_schema
        else:
            ref_schema, required_paths = schema_from_sql_schema_file(
                args.sql_schema, table=args.sql_table)
            label = args.sql_schema if not args.sql_table else f"{args.sql_schema}#{args.sql_table}"

        # Show the chosen records for file1 if requested
        if args.show_samples:
            print_samples(args.file1, s1, colors=cfg.colors())

        # Delegate to presence-aware compare adapter
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
        )
        return

    # ── Classic DATA ↔ DATA comparison ───────────────────────────────────
    # file1 chosen set
    s1 = nth_record(args.file1, r1_idx or 1) if r1_idx is not None else sample_records(
        args.file1, args.samples)
    title1 = f"; record #{r1_idx or 1}" if r1_idx is not None else f"; random {args.samples}-record samples"
    # file2 chosen set
    s2 = nth_record(args.file2, r2_idx or 1) if r2_idx is not None else sample_records(
        args.file2, args.samples)

    runs = [(s1, s2, title1)]
    if args.both_modes and (r1_idx is not None or r2_idx is not None):
        runs.append((
            sample_records(args.file1, args.samples),
            sample_records(args.file2, args.samples),
            f"; random {args.samples}-record samples"
        ))

    all_reports = []
    for s1_run, s2_run, title_suffix in runs:
        if args.show_samples:
            print_samples(args.file1, s1_run, colors=cfg.colors())
            print_samples(args.file2, s2_run, colors=cfg.colors())

        sch1 = merged_schema_from_samples(s1_run, cfg)
        sch2 = merged_schema_from_samples(s2_run, cfg)
        sch1n, sch2n = walk_normalize(sch1), walk_normalize(sch2)

        diff = DeepDiff(sch1n, sch2n, ignore_order=True)
        direction = f"{args.file1} -> {args.file2}"
        if not diff:
            print(
                f"\n{CYN}=== Schema diff (types only, {direction}{title_suffix}) ==={RST}\nNo differences.")
            if args.dump_schemas:
                with open(args.dump_schemas, "w", encoding="utf-8") as fh:
                    json.dump({"file1": sch1n, "file2": sch2n}, fh, indent=2)
            all_reports.append({"meta": {"direction": direction, "mode": title_suffix.strip('; ').strip()},
                                "note": "No differences"})
            continue

        report = build_report_struct(
            diff, args.file1, args.file2, include_presence=cfg.show_presence)
        report["meta"]["mode"] = title_suffix.strip("; ").strip()
        print_report_text(report, args.file1, args.file2, colors=cfg.colors(),
                          show_presence=cfg.show_presence, title_suffix=title_suffix)
        all_reports.append(report)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(all_reports if len(all_reports) > 1 else all_reports[0],
                      fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()