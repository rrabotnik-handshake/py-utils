#!/usr/bin/env python3
"""
Analytics subcommand for schema-diff CLI.

Provides advanced schema analysis capabilities.
"""
from __future__ import annotations

from typing import Any


def add_analyze_subcommand(subparsers):
    """Add the analyze subcommand to the CLI."""
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Perform advanced schema analysis",
        description="Analyze schema complexity, patterns, and provide improvement suggestions",
    )

    # Positional arguments
    analyze_parser.add_argument("schema_file", help="Schema file to analyze")

    # Schema type options
    analyze_parser.add_argument(
        "--type",
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
        ],
        help="Schema type (auto-detected if not specified)",
    )

    # Analysis options
    analyze_parser.add_argument(
        "--complexity",
        action="store_true",
        help="Show complexity analysis (nesting depth, type distribution, etc.)",
    )
    analyze_parser.add_argument(
        "--patterns",
        action="store_true",
        help="Show pattern analysis (repeated fields, semantic patterns, etc.)",
    )
    analyze_parser.add_argument(
        "--suggestions",
        action="store_true",
        help="Show improvement suggestions",
    )
    analyze_parser.add_argument(
        "--report",
        action="store_true",
        help="Generate comprehensive analysis report",
    )
    analyze_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all analysis types (equivalent to --complexity --patterns --suggestions --report)",
    )

    # Output options
    analyze_parser.add_argument(
        "--output",
        action="store_true",
        help="Save analysis to output directory",
    )
    analyze_parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format (default: text)",
    )

    # Schema-specific options
    analyze_parser.add_argument("--table", help="BigQuery table name (for SQL schemas)")
    analyze_parser.add_argument("--model", help="dbt model name (for dbt schemas)")
    analyze_parser.add_argument(
        "--message", help="Protobuf message name (for protobuf schemas)"
    )

    # Data sampling options (for data files)
    analyze_parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of records to sample for data files (default: 1000)",
    )
    analyze_parser.add_argument(
        "--all-records",
        action="store_true",
        help="Process all records (no sampling limit)",
    )


def cmd_analyze(args) -> int:
    """Execute the analyze command."""
    try:
        import json
        from pathlib import Path

        from ..advanced_analytics import (
            analyze_schema_complexity,
            find_schema_patterns,
            generate_schema_report,
            suggest_schema_improvements,
        )
        from ..config import Config
        from ..output_utils import write_output_file
        from ..unified_loader import load_schema_unified

        cfg = Config()

        # Determine schema type
        schema_type = args.type
        if not schema_type:
            # Auto-detect based on file extension
            file_path = Path(args.schema_file)
            if file_path.suffix == ".json":
                if "manifest" in file_path.name:
                    schema_type = "dbt-manifest"
                else:
                    schema_type = "json_schema"
            elif file_path.suffix in [".sql", ".ddl"]:
                schema_type = "sql"
            elif file_path.suffix == ".proto":
                schema_type = "protobuf"
            elif file_path.suffix in [".txt", ".schema"]:
                schema_type = "spark"
            elif file_path.suffix in [".yml", ".yaml"]:
                schema_type = "dbt-yml"
            else:
                schema_type = "data"  # Default to data

        # Handle data files differently
        if schema_type == "data":
            from ..io_utils import all_records, sample_records
            from ..json_data_file_parser import merged_schema_from_samples
            from ..models import from_legacy_tree

            if args.all_records:
                records = all_records(args.schema_file)
            else:
                records = sample_records(args.schema_file, args.sample_size)

            # Convert data to schema
            data_tree = merged_schema_from_samples(records, cfg)
            schema = from_legacy_tree(data_tree, set(), source_type="data")
        else:
            # Load schema using unified loader
            schema = load_schema_unified(
                args.schema_file,
                schema_type,
                table=args.table,
                model=args.model,
                message=args.message,
            )

        # Determine what analysis to perform
        show_complexity = args.complexity or args.all
        show_patterns = args.patterns or args.all
        show_suggestions = args.suggestions or args.all
        show_report = args.report or args.all

        # If no specific analysis requested, show basic info
        if not any([show_complexity, show_patterns, show_suggestions, show_report]):
            show_complexity = True
            show_patterns = True

        results: dict[str, Any] = {}

        # Perform requested analysis
        if show_complexity:
            print("🔍 Analyzing schema complexity...")
            complexity = analyze_schema_complexity(schema)
            results["complexity"] = complexity

        if show_patterns:
            print("🔍 Finding schema patterns...")
            patterns = find_schema_patterns(schema)
            results["patterns"] = patterns

        if show_suggestions:
            print("🔍 Generating improvement suggestions...")
            suggestions = suggest_schema_improvements(schema)
            results["suggestions"] = suggestions

        if show_report:
            print("🔍 Generating comprehensive report...")
            report = generate_schema_report(schema)
            results["report"] = report

        # Output results
        if args.format == "json":
            # Convert sets to lists for JSON serialization
            json_results = _prepare_for_json(results)
            output = json.dumps(json_results, indent=2, default=str)
        elif args.format == "markdown":
            output = _format_as_markdown(results, schema)
        else:  # text format
            output = _format_as_text(results, schema)

        if args.output:
            # Save to output directory
            filename = f"schema_analysis_{Path(args.schema_file).stem}.{args.format}"
            write_output_file(output, filename, "analysis")
            print(f"✅ Analysis saved to output/analysis/{filename}")
        else:
            print(output)

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


def _prepare_for_json(results):
    """Prepare results for JSON serialization."""
    json_results = {}
    for key, value in results.items():
        if key == "complexity":
            # Convert defaultdict to regular dict
            json_results[key] = {
                k: (dict(v) if hasattr(v, "items") else v) for k, v in value.items()
            }
        elif key == "patterns":
            # Convert sets to lists
            json_results[key] = {
                k: list(v) if isinstance(v, (set, list)) else v
                for k, v in value.items()
            }
        else:
            json_results[key] = value
    return json_results


def _format_as_text(results, schema):
    """Format results as plain text."""
    output = []
    output.append(f"📊 Schema Analysis: {schema.source_type or 'Unknown'}")
    output.append("=" * 50)
    output.append("")

    if "complexity" in results:
        complexity = results["complexity"]
        output.append("🔍 COMPLEXITY ANALYSIS")
        output.append("-" * 25)
        output.append(f"Total Fields: {complexity['total_fields']}")
        output.append(f"Required Fields: {complexity['required_fields']}")
        output.append(f"Optional Fields: {complexity['optional_fields']}")
        output.append(f"Max Nesting Depth: {complexity['max_nesting_depth']}")
        output.append(f"Average Nesting Depth: {complexity['avg_nesting_depth']:.2f}")
        output.append("")

        output.append("Field Categories:")
        output.append(f"  • Scalar: {complexity['scalar_fields']}")
        output.append(f"  • Array: {complexity['array_fields']}")
        output.append(f"  • Object: {complexity['object_fields']}")
        output.append(f"  • Union: {complexity['union_fields']}")
        output.append("")

    if "patterns" in results:
        patterns = results["patterns"]
        output.append("🔍 PATTERN ANALYSIS")
        output.append("-" * 20)

        if patterns["id_fields"]:
            output.append(f"ID Fields: {', '.join(patterns['id_fields'])}")
        if patterns["timestamp_fields"]:
            output.append(
                f"Timestamp Fields: {', '.join(patterns['timestamp_fields'])}"
            )
        if patterns["email_fields"]:
            output.append(f"Email Fields: {', '.join(patterns['email_fields'])}")
        if patterns["repeated_field_names"]:
            output.append(
                f"Repeated Field Names: {', '.join(patterns['repeated_field_names'])}"
            )
        if patterns["array_of_objects"]:
            output.append(
                f"Array of Objects: {', '.join(patterns['array_of_objects'])}"
            )
        output.append("")

    if "suggestions" in results:
        suggestions = results["suggestions"]
        output.append("💡 IMPROVEMENT SUGGESTIONS")
        output.append("-" * 30)

        if not suggestions:
            output.append("No suggestions - schema looks good! ✅")
        else:
            for i, suggestion in enumerate(suggestions, 1):
                severity_icon = {"info": "ℹ️", "warning": "⚠️", "error": "❌"}.get(
                    suggestion["severity"], "📝"
                )
                output.append(f"{i}. {severity_icon} {suggestion['type'].upper()}")
                output.append(f"   {suggestion['description']}")
                if suggestion["affected_fields"]:
                    fields_str = ", ".join(suggestion["affected_fields"][:3])
                    if len(suggestion["affected_fields"]) > 3:
                        fields_str += (
                            f" (and {len(suggestion['affected_fields']) - 3} more)"
                        )
                    output.append(f"   Affected: {fields_str}")
                output.append("")

    if "report" in results:
        output.append("📋 COMPREHENSIVE REPORT")
        output.append("-" * 25)
        output.append(results["report"])

    return "\n".join(output)


def _format_as_markdown(results, schema):
    """Format results as markdown."""
    if "report" in results:
        return results["report"]  # The report is already in markdown format
    else:
        # Convert text format to markdown
        text_output = _format_as_text(results, schema)
        # Simple conversion: add # to headers
        lines = text_output.split("\n")
        markdown_lines = []
        for line in lines:
            if line.endswith("=" * len(line.rstrip("="))):
                # Main header
                prev_line = markdown_lines[-1] if markdown_lines else ""
                markdown_lines[-1] = f"# {prev_line}"
            elif line.endswith("-" * len(line.rstrip("-"))):
                # Sub header
                prev_line = markdown_lines[-1] if markdown_lines else ""
                markdown_lines[-1] = f"## {prev_line}"
            else:
                markdown_lines.append(line)
        return "\n".join(markdown_lines)


__all__ = ["add_analyze_subcommand", "cmd_analyze"]
