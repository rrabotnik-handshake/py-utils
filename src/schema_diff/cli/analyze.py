#!/usr/bin/env python3
"""Analytics subcommand for schema-diff CLI.

Provides advanced schema analysis capabilities.
"""
from __future__ import annotations

from typing import Any


def add_analyze_subcommand(subparsers):
    """Add the analyze subcommand to the CLI."""
    from ..helpfmt import ColorDefaultsFormatter
    from .colors import BLUE, BOLD, CYAN, GREEN, RESET, YELLOW

    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Perform advanced schema analysis",
        formatter_class=ColorDefaultsFormatter,
        description=f"""
Analyze schema complexity, detect patterns, and get optimization suggestions.

{BOLD}{YELLOW}ANALYSIS TYPES:{RESET}
  {BLUE}--complexity{RESET}    Nesting depth, field counts, type distribution
  {BLUE}--patterns{RESET}      Repeated structures, naming conventions, semantic patterns
  {BLUE}--suggestions{RESET}   Denormalization opportunities, index recommendations
  {BLUE}--report{RESET}        Comprehensive report with all sections
  {BLUE}--all{RESET}           All of the above

{BOLD}{YELLOW}OUTPUT FORMATS:{RESET}
  {BLUE}text{RESET}       Human-readable (default)
  {BLUE}json{RESET}       Machine-readable
  {BLUE}markdown{RESET}   Documentation-ready

{BOLD}{CYAN}EXAMPLES:{RESET}
  {GREEN}# Basic analysis{RESET}
  schema-diff analyze schema.json

  {GREEN}# Specific analyses{RESET}
  schema-diff analyze data.json --complexity --patterns

  {GREEN}# SQL schema with table selection{RESET}
  schema-diff analyze schema.sql --type sql --table users --all

  {GREEN}# BigQuery table with output{RESET}
  schema-diff analyze project:dataset.table --type bigquery --output
        """,
    )

    # Positional arguments
    analyze_parser.add_argument(
        "schema_file",
        help="Schema or data file to analyze (supports all schema-diff formats)",
    )

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
            "bigquery",
        ],
        help="Schema type (auto-detected if not specified)",
    )

    # Analysis options
    analyze_parser.add_argument(
        "--complexity",
        action="store_true",
        help="Show complexity metrics: nesting depth, field counts, type distribution, array usage",
    )
    analyze_parser.add_argument(
        "--patterns",
        action="store_true",
        help="Show pattern analysis: repeated structures, semantic patterns, naming conventions",
    )
    analyze_parser.add_argument(
        "--suggestions",
        action="store_true",
        help="Show improvement suggestions: denormalization, indexing, type optimizations",
    )
    analyze_parser.add_argument(
        "--report",
        action="store_true",
        help="Generate comprehensive analysis report with all sections",
    )
    analyze_parser.add_argument(
        "--all",
        action="store_true",
        help="Show all analysis types (complexity + patterns + suggestions + report)",
    )

    # Output options
    analyze_parser.add_argument(
        "--output",
        action="store_true",
        help="Save analysis to ./output/analysis/ directory",
    )
    analyze_parser.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format: text (human-readable), json (machine-readable), markdown (documentation)",
    )

    # Schema-specific options
    analyze_parser.add_argument(
        "--table",
        help="BigQuery/SQL table name to extract from multi-table schemas",
    )
    analyze_parser.add_argument(
        "--model",
        help="dbt model name to extract from manifest.json or schema.yml",
    )
    analyze_parser.add_argument(
        "--message",
        help="Protobuf message name to analyze from .proto files",
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

            # Also analyze policy tags for BigQuery schemas
            if schema.source_type == "bigquery" and schema.metadata.get(
                "raw_bq_schema"
            ):
                from ..advanced_analytics import analyze_policy_tags

                policy_tags = analyze_policy_tags(schema)
                results["policy_tags"] = policy_tags

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
    """Format results as plain text with color coding."""
    from .colors import BLUE, BOLD, CYAN, GREEN, RED, RESET, YELLOW

    output = []
    output.append(
        f"\n{BOLD}{CYAN}📊 Schema Analysis: {schema.source_type or 'Unknown'}{RESET}"
    )
    output.append("═" * 70)
    output.append("")

    # LEAD WITH SUGGESTIONS - most actionable information first
    if "suggestions" in results:
        suggestions = results["suggestions"]
        output.append(f"{BOLD}{YELLOW}💡 KEY RECOMMENDATIONS{RESET}")
        output.append("─" * 70)

        if not suggestions:
            output.append(f"{GREEN}✅ No issues found - schema looks good!{RESET}")
        else:
            # Group by severity
            errors = [s for s in suggestions if s["severity"] == "error"]
            warnings = [s for s in suggestions if s["severity"] == "warning"]
            infos = [s for s in suggestions if s["severity"] == "info"]

            for group, icon, color, suggestions_list in [
                ("CRITICAL ISSUES", "❌ ", RED, errors),
                ("WARNINGS", "⚠️ ", YELLOW, warnings),
                ("RECOMMENDATIONS", "ℹ️ ", BLUE, infos),
            ]:
                if suggestions_list:
                    output.append(
                        f"\n{BOLD}{color}{icon} {group} ({len(suggestions_list)}){RESET}"
                    )

                    # Group by category
                    by_category = {}
                    for suggestion in suggestions_list:
                        category = suggestion["type"]
                        by_category.setdefault(category, []).append(suggestion)

                    # Sort categories alphabetically
                    for category in sorted(by_category.keys()):
                        category_suggestions = by_category[category]
                        output.append(
                            f"\n  {BOLD}{CYAN}{category.replace('_', ' ').title()}{RESET}"
                        )

                        for suggestion in category_suggestions:
                            # Wrap long descriptions
                            desc = suggestion["description"]
                            if len(desc) > 80:
                                # Word wrap at 80 characters
                                words = desc.split()
                                lines = []
                                current_line = []
                                current_length = 0

                                for word in words:
                                    if current_length + len(word) + 1 > 80:
                                        lines.append(" ".join(current_line))
                                        current_line = [word]
                                        current_length = len(word)
                                    else:
                                        current_line.append(word)
                                        current_length += len(word) + 1

                                if current_line:
                                    lines.append(" ".join(current_line))

                                # Output wrapped lines
                                output.append(f"    • {lines[0]}")
                                for line in lines[1:]:
                                    output.append(f"      {line}")
                            else:
                                output.append(f"    • {desc}")

                            if suggestion["affected_fields"]:
                                count = len(suggestion["affected_fields"])
                                # Word wrap affected fields at 74 chars (accounting for "      → " prefix = 8 chars)
                                fields_to_show = suggestion["affected_fields"][:5]
                                fields_parts = []
                                current_line = []
                                current_length = 0

                                for field in fields_to_show:
                                    field_with_comma = f"{field}, "
                                    field_len = len(field_with_comma)

                                    if current_length + field_len > 74:
                                        # Finish current line (remove trailing comma+space)
                                        if current_line:
                                            fields_parts.append(
                                                "".join(current_line).rstrip(", ")
                                            )
                                        current_line = [field_with_comma]
                                        current_length = field_len
                                    else:
                                        current_line.append(field_with_comma)
                                        current_length += field_len

                                # Add remaining fields
                                if current_line:
                                    line_str = "".join(current_line).rstrip(", ")
                                    if count > 5:
                                        line_str += f" ... +{count - 5} more"
                                    fields_parts.append(line_str)

                                # Output wrapped lines
                                if fields_parts:
                                    output.append(
                                        f"      {CYAN}→{RESET} {fields_parts[0]}"
                                    )
                                    for part in fields_parts[1:]:
                                        output.append(f"        {part}")

                            # Add blank line between bullet items
                            output.append("")
        output.append("\n" + "─" * 70)
        output.append("")

    # SCHEMA OVERVIEW - concise summary
    if "complexity" in results:
        from .colors import BLUE, BOLD, CYAN, GREEN, RESET, YELLOW

        complexity = results["complexity"]
        output.append(f"{BOLD}{GREEN}📈 SCHEMA OVERVIEW{RESET}")
        output.append("─" * 70)

        # Single-line summary
        total = complexity["total_fields"]
        required = complexity["required_fields"]
        optional = complexity["optional_fields"]
        depth = complexity["max_nesting_depth"]

        output.append(
            f"  {CYAN}Fields:{RESET} {BOLD}{total}{RESET} total  •  {GREEN}{required}{RESET} required  •  {YELLOW}{optional}{RESET} optional"
        )
        output.append(
            f"  {CYAN}Nesting:{RESET} {BOLD}{depth}{RESET} max depth  •  {complexity['avg_nesting_depth']:.1f} avg depth"
        )
        output.append("")

        # Field composition (horizontal bar)
        scalar = complexity["scalar_fields"]
        array = complexity["array_fields"]
        obj = complexity["object_fields"]
        union = complexity["union_fields"]

        output.append(
            f"  {CYAN}Composition:{RESET} {scalar} scalar  •  {array} arrays  •  {obj} objects  •  {union} unions"
        )
        output.append("")

    # PATTERN SUMMARY - categorized and concise
    if "patterns" in results:
        from .colors import BLUE, BOLD, CYAN, RESET

        patterns = results["patterns"]
        output.append(f"{BOLD}{BLUE}🔍 DETECTED PATTERNS{RESET}")
        output.append("─" * 70)

        has_patterns = False

        # Identifiers
        id_patterns = []
        if patterns["id_fields"]:
            id_patterns.append(
                f"ID fields ({len(patterns['id_fields'])}): {', '.join(patterns['id_fields'][:5])}"
            )
            if len(patterns["id_fields"]) > 5:
                id_patterns[-1] += f" ... +{len(patterns['id_fields']) - 5} more"

        # Temporal
        time_patterns = []
        if patterns["timestamp_fields"]:
            time_patterns.append(
                f"Timestamps ({len(patterns['timestamp_fields'])}): {', '.join(patterns['timestamp_fields'][:5])}"
            )
            if len(patterns["timestamp_fields"]) > 5:
                time_patterns[
                    -1
                ] += f" ... +{len(patterns['timestamp_fields']) - 5} more"

        # Contact/Personal
        contact_patterns = []
        if patterns["email_fields"]:
            contact_patterns.append(
                f"Email fields ({len(patterns['email_fields'])}): {', '.join(patterns['email_fields'][:5])}"
            )
            if len(patterns["email_fields"]) > 5:
                contact_patterns[
                    -1
                ] += f" ... +{len(patterns['email_fields']) - 5} more"

        # Structural
        struct_patterns = []
        if patterns["array_of_objects"]:
            count = len(patterns["array_of_objects"])
            struct_patterns.append(
                f"Array-of-objects fields ({count}): {', '.join(patterns['array_of_objects'][:5])}"
            )
            if count > 5:
                struct_patterns[-1] += f" ... +{count - 5} more"
        if patterns["repeated_field_names"]:
            count = len(patterns["repeated_field_names"])
            struct_patterns.append(
                f"Repeated names ({count}): {', '.join(patterns['repeated_field_names'][:5])}"
            )
            if count > 5:
                struct_patterns[-1] += f" ... +{count - 5} more"

        # Security (Policy Tags)
        security_patterns = []
        if patterns.get("fields_with_policy_tags"):
            count = len(patterns["fields_with_policy_tags"])
            security_patterns.append(
                f"{GREEN}Policy tagged fields ({count}): {', '.join(patterns['fields_with_policy_tags'][:5])}{RESET}"
            )
            if count > 5:
                security_patterns[-1] += f" {GREEN}... +{count - 5} more{RESET}"

        for category, items in [
            ("Identifiers", id_patterns),
            ("Temporal", time_patterns),
            ("Contact/Personal", contact_patterns),
            ("Structural", struct_patterns),
            ("Security & Compliance", security_patterns),
        ]:
            if items:
                has_patterns = True
                output.append(f"\n  {CYAN}{category}:{RESET}")
                for item in items:
                    output.append(f"    • {item}")

        if not has_patterns:
            output.append("  No common patterns detected")

        output.append("")

    # POLICY TAGS ANALYSIS - show untagged PII/sensitive fields
    if "policy_tags" in results:
        from .colors import BOLD, CYAN, GREEN, RED, RESET, YELLOW

        policy_tags = results["policy_tags"]
        output.append(f"\n{BOLD}{CYAN}🔒 POLICY TAGS ANALYSIS{RESET}")
        output.append("─" * 70)

        total = policy_tags["total_fields"]
        with_tags = len(policy_tags["fields_with_tags"])
        coverage = policy_tags["coverage_percent"]

        output.append(
            f"  {CYAN}Coverage:{RESET} {BOLD}{with_tags}{RESET} of {total} fields tagged ({coverage:.1f}%)"
        )
        output.append("")

        # Show candidate PII fields (untagged)
        pii_untagged = policy_tags["pii_fields_untagged"]
        if pii_untagged:
            count = len(pii_untagged)
            output.append(
                f"  {BOLD}{YELLOW}⚠️  Candidate PII Fields Without Tags ({count}):{RESET}"
            )
            # Show as bullet points, up to 15 fields
            for field in pii_untagged[:15]:
                output.append(f"    • {field}")
            if count > 15:
                output.append(f"    {YELLOW}... +{count - 15} more{RESET}")
            output.append("")

        # Show untagged sensitive fields (credentials/secrets)
        sensitive_untagged = policy_tags["sensitive_fields_untagged"]
        if sensitive_untagged:
            count = len(sensitive_untagged)
            output.append(
                f"  {BOLD}{RED}❌ Sensitive Fields Without Tags ({count}):{RESET}"
            )
            # Show as bullet points, up to 10 fields
            for field in sensitive_untagged[:10]:
                output.append(f"    • {field}")
            if count > 10:
                output.append(f"    {RED}... +{count - 10} more{RESET}")
            output.append("")

        # Show policy tag distribution
        if policy_tags["policy_tag_distribution"]:
            output.append(f"  {CYAN}Policy Tag Categories:{RESET}")
            for category, count in sorted(
                policy_tags["policy_tag_distribution"].items()
            ):
                output.append(f"    • {category}: {count}")
            output.append("")

        # Summary message
        total_sensitive = len(pii_untagged) + len(sensitive_untagged) + with_tags
        if total_sensitive == 0:
            # No PII or sensitive fields detected at all
            output.append(
                f"  {CYAN}ℹ️  No candidate PII or sensitive fields detected in schema{RESET}"
            )
        elif not pii_untagged and not sensitive_untagged:
            # All detected PII/sensitive fields are tagged
            output.append(
                f"  {GREEN}✅ All candidate sensitive fields are properly tagged!{RESET}"
            )
        else:
            # Some fields need tags
            total_untagged = len(pii_untagged) + len(sensitive_untagged)
            output.append(
                f"  {YELLOW}💡 Consider adding policy tags to {total_untagged} candidate field(s){RESET}"
            )

        output.append("")

    if "report" in results:
        output.append(f"\n{BOLD}📋 DETAILED REPORT{RESET}")
        output.append("─" * 70)
        output.append(results["report"])

    return "\n".join(output)


def _format_as_markdown(results, schema):
    """Format results as comprehensive markdown report."""
    if "report" in results:
        return results["report"]  # The report is already in markdown format

    output = []

    # Title and metadata
    output.append("# 📊 Schema Analysis Report")
    output.append(f"\n**Schema Type:** `{schema.source_type or 'Unknown'}`")

    from datetime import datetime

    output.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append("\n---\n")

    # Table of Contents
    sections = []
    if "suggestions" in results and results["suggestions"]:
        sections.append("[Key Recommendations](#key-recommendations)")
    if "complexity" in results:
        sections.append("[Schema Overview](#schema-overview)")
    if "patterns" in results:
        sections.append("[Detected Patterns](#detected-patterns)")

    if sections:
        output.append("## 📑 Table of Contents\n")
        for section in sections:
            output.append(f"- {section}")
        output.append("\n---\n")

    # LEAD WITH SUGGESTIONS
    if "suggestions" in results:
        suggestions = results["suggestions"]
        output.append("## 💡 Key Recommendations\n")

        if not suggestions:
            output.append("✅ **No issues found** - schema looks good!\n")
    else:
        # Group by severity
        errors = [s for s in suggestions if s["severity"] == "error"]
        warnings = [s for s in suggestions if s["severity"] == "warning"]
        infos = [s for s in suggestions if s["severity"] == "info"]

        # Summary table
        if errors or warnings or infos:
            output.append("### Summary\n")
            output.append("| Severity | Count |")
            output.append("|----------|-------|")
            if errors:
                output.append(f"| 🔴 **Critical Issues** | {len(errors)} |")
            if warnings:
                output.append(f"| 🟡 **Warnings** | {len(warnings)} |")
            if infos:
                output.append(f"| 🔵 **Recommendations** | {len(infos)} |")
            output.append("")

        # Detailed recommendations - grouped by category
        for group, icon, suggestions_list in [
            ("Critical Issues", "🔴", errors),
            ("Warnings", "🟡", warnings),
            ("Recommendations", "🔵", infos),
        ]:
            if suggestions_list:
                output.append(f"### {icon} {group}\n")

                # Group by category
                by_category = {}
                for suggestion in suggestions_list:
                    category = suggestion["type"]
                    by_category.setdefault(category, []).append(suggestion)

                # Sort categories alphabetically
                for category in sorted(by_category.keys()):
                    category_suggestions = by_category[category]
                    output.append(f"#### {category.replace('_', ' ').title()}\n")

                    for suggestion in category_suggestions:
                        output.append(f"- **Issue:** {suggestion['description']}\n")

                        if suggestion["affected_fields"]:
                            count = len(suggestion["affected_fields"])
                            output.append(f"  **Affected Fields ({count}):**")
                            output.append("  ```")
                            for field in suggestion["affected_fields"][:10]:
                                output.append(f"  {field}")
                            if count > 10:
                                output.append(f"  ... +{count - 10} more")
                            output.append("  ```\n")

    output.append("---\n")

    # SCHEMA OVERVIEW
    if "complexity" in results:
        complexity = results["complexity"]
        output.append("## 📈 Schema Overview\n")

        # Key metrics table
        output.append("### Key Metrics\n")
        output.append("| Metric | Value |")
        output.append("|--------|-------|")
        output.append(f"| Total Fields | **{complexity['total_fields']}** |")
        output.append(f"| Required Fields | {complexity['required_fields']} |")
        output.append(f"| Optional Fields | {complexity['optional_fields']} |")
        output.append(f"| Max Nesting Depth | {complexity['max_nesting_depth']} |")
        output.append(f"| Avg Nesting Depth | {complexity['avg_nesting_depth']:.1f} |")
        output.append("")

        # Field composition
        output.append("### Field Composition\n")
        output.append("| Type | Count |")
        output.append("|------|-------|")
        output.append(f"| Scalar | {complexity['scalar_fields']} |")
        output.append(f"| Arrays | {complexity['array_fields']} |")
        output.append(f"| Objects | {complexity['object_fields']} |")
        output.append(f"| Unions | {complexity['union_fields']} |")
        output.append("")

        # Type distribution
        if complexity.get("type_counts"):
            output.append("### Type Distribution\n")
            output.append("| Type | Count |")
            output.append("|------|-------|")
            for type_name, count in sorted(
                complexity["type_counts"].items(), key=lambda x: x[1], reverse=True
            )[:10]:
                output.append(f"| `{type_name}` | {count} |")
            output.append("")

        output.append("---\n")

    # DETECTED PATTERNS
    if "patterns" in results:
        patterns = results["patterns"]
        output.append("## 🔍 Detected Patterns\n")

        has_patterns = False

        # Identifiers
        if patterns["id_fields"]:
            has_patterns = True
            output.append(f"### 🔑 Identifiers ({len(patterns['id_fields'])} fields)\n")
            output.append("```")
            for field in patterns["id_fields"][:15]:
                output.append(f"  {field}")
            if len(patterns["id_fields"]) > 15:
                output.append(f"  ... +{len(patterns['id_fields']) - 15} more")
            output.append("```\n")

        # Temporal
        if patterns["timestamp_fields"]:
            has_patterns = True
            output.append(
                f"### 🕐 Temporal Fields ({len(patterns['timestamp_fields'])} fields)\n"
            )
            output.append("```")
            for field in patterns["timestamp_fields"][:15]:
                output.append(f"  {field}")
            if len(patterns["timestamp_fields"]) > 15:
                output.append(f"  ... +{len(patterns['timestamp_fields']) - 15} more")
            output.append("```\n")

        # Contact/Personal
        if patterns["email_fields"]:
            has_patterns = True
            output.append(
                f"### 📧 Contact Fields ({len(patterns['email_fields'])} fields)\n"
            )
            output.append("```")
            for field in patterns["email_fields"][:15]:
                output.append(f"  {field}")
            if len(patterns["email_fields"]) > 15:
                output.append(f"  ... +{len(patterns['email_fields']) - 15} more")
            output.append("```\n")

        # Structural
        if patterns["array_of_objects"]:
            has_patterns = True
            count = len(patterns["array_of_objects"])
            output.append(f"### 📦 Array-of-Objects Fields ({count} fields)\n")
            output.append("```")
            for field in patterns["array_of_objects"][:15]:
                output.append(f"  {field}")
            if count > 15:
                output.append(f"  ... +{count - 15} more")
            output.append("```\n")

        if patterns["repeated_field_names"]:
            has_patterns = True
            count = len(patterns["repeated_field_names"])
            output.append(f"### 🔄 Repeated Field Names ({count} names)\n")
            output.append("```")
            for field in patterns["repeated_field_names"][:15]:
                output.append(f"  {field}")
            if count > 15:
                output.append(f"  ... +{count - 15} more")
            output.append("```\n")

        if not has_patterns:
            output.append("_No common patterns detected_\n")

        output.append("---\n")

    # Footer
    output.append("\n---")
    output.append("\n*Generated by schema-diff analyze*")

    return "\n".join(output)


__all__ = ["add_analyze_subcommand", "cmd_analyze"]
