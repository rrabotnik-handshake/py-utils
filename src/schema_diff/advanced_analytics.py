#!/usr/bin/env python3
"""
Advanced schema analytics using the unified Schema format.

This module provides sophisticated analysis capabilities that leverage
the rich type system of the unified Schema objects.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

from .models import FieldConstraint, Schema


def analyze_schema_complexity(schema: Schema) -> Dict[str, Any]:  # type: ignore[misc]
    """
    Analyze the complexity of a schema.

    Returns metrics like nesting depth, field count, type diversity, etc.
    """
    if not isinstance(schema, Schema):
        raise ValueError("Schema must be a unified Schema object")

    analysis = {
        "total_fields": len(schema.fields),
        "required_fields": 0,
        "optional_fields": 0,
        "max_nesting_depth": 0,
        "avg_nesting_depth": 0.0,
        "type_distribution": defaultdict(int),
        "constraint_distribution": defaultdict(int),
        "array_fields": 0,
        "object_fields": 0,
        "union_fields": 0,
        "scalar_fields": 0,
        "field_paths_by_depth": defaultdict(list),
    }

    total_depth = 0

    for field in schema.fields:
        # Count constraints
        if FieldConstraint.REQUIRED in field.constraints:
            analysis["required_fields"] += 1  # type: ignore[operator]
        else:
            analysis["optional_fields"] += 1  # type: ignore[operator]

        for constraint in field.constraints:
            analysis["constraint_distribution"][constraint.value] += 1  # type: ignore[index]

        # Analyze nesting depth
        depth = field.path.count(".") + 1
        analysis["max_nesting_depth"] = max(analysis["max_nesting_depth"], depth)  # type: ignore[call-overload]
        total_depth += depth
        analysis["field_paths_by_depth"][depth].append(field.path)  # type: ignore[index]

        # Analyze field type
        field_type_str = str(field.type)
        analysis["type_distribution"][field_type_str] += 1  # type: ignore[index]

        # Categorize field types
        if field_type_str.startswith("[") and field_type_str.endswith("]"):
            analysis["array_fields"] += 1  # type: ignore[operator]
        elif "union(" in field_type_str:
            analysis["union_fields"] += 1  # type: ignore[operator]
        elif field_type_str in ["object", "any"]:
            analysis["object_fields"] += 1  # type: ignore[operator]
        else:
            analysis["scalar_fields"] += 1  # type: ignore[operator]

    if analysis["total_fields"] > 0:  # type: ignore[operator]
        analysis["avg_nesting_depth"] = total_depth / analysis["total_fields"]  # type: ignore[operator]

    return dict(analysis)


def find_schema_patterns(schema: Schema) -> Dict[str, List[str]]:
    """
    Find common patterns in schema structure.

    Returns patterns like repeated field names, common prefixes, etc.
    """
    if not isinstance(schema, Schema):
        raise ValueError("Schema must be a unified Schema object")

    patterns: dict[str, list[str]] = {
        "repeated_field_names": [],
        "common_prefixes": [],
        "id_fields": [],
        "timestamp_fields": [],
        "email_fields": [],
        "nested_objects": [],
        "array_of_objects": [],
    }

    # Analyze field names and paths
    field_names = [field.name for field in schema.fields]
    field_paths = [field.path for field in schema.fields]

    # Find repeated field names
    name_counts: dict[str, int] = defaultdict(int)
    for name in field_names:
        name_counts[name] += 1

    patterns["repeated_field_names"] = [
        name for name, count in name_counts.items() if count > 1
    ]

    # Find common prefixes (for nested fields)
    prefixes = defaultdict(list)
    for path in field_paths:
        if "." in path:
            prefix = ".".join(path.split(".")[:-1])
            prefixes[prefix].append(path)

    patterns["common_prefixes"] = [
        prefix for prefix, paths in prefixes.items() if len(paths) > 2
    ]

    # Find semantic patterns
    for field in schema.fields:
        field_name_lower = field.name.lower()
        field_type_str = str(field.type)

        # ID fields
        if "id" in field_name_lower and field_type_str in ["int", "str"]:
            patterns["id_fields"].append(field.path)

        # Timestamp fields
        if any(
            word in field_name_lower for word in ["time", "date", "created", "updated"]
        ) or field_type_str in ["timestamp", "date", "time"]:
            patterns["timestamp_fields"].append(field.path)

        # Email fields
        if "email" in field_name_lower or "mail" in field_name_lower:
            patterns["email_fields"].append(field.path)

        # Nested objects
        if "." in field.path and field_type_str not in ["int", "str", "bool", "float"]:
            patterns["nested_objects"].append(field.path)

        # Array of objects
        if field_type_str.startswith("[") and field_type_str not in [
            "[str]",
            "[int]",
            "[float]",
            "[bool]",
        ]:
            patterns["array_of_objects"].append(field.path)

    return patterns


def suggest_schema_improvements(schema: Schema) -> List[Dict[str, str]]:
    """
    Suggest improvements for schema design.

    Returns a list of suggestions with type, description, and affected fields.
    """
    if not isinstance(schema, Schema):
        raise ValueError("Schema must be a unified Schema object")

    suggestions = []
    complexity = analyze_schema_complexity(schema)
    patterns = find_schema_patterns(schema)

    # Check for overly complex nesting
    if complexity["max_nesting_depth"] > 4:
        suggestions.append(
            {
                "type": "complexity",
                "severity": "warning",
                "description": f"Deep nesting detected (max depth: {complexity['max_nesting_depth']}). Consider flattening some structures.",
                "affected_fields": [
                    path
                    for paths in complexity["field_paths_by_depth"].values()
                    for path in paths
                    if len(path.split(".")) > 4
                ],
            }
        )

    # Check for too many optional fields
    if complexity["optional_fields"] > complexity["required_fields"] * 3:
        suggestions.append(
            {
                "type": "data_quality",
                "severity": "info",
                "description": f"Many optional fields ({complexity['optional_fields']} optional vs {complexity['required_fields']} required). Consider if some should be required.",
                "affected_fields": [
                    f.path
                    for f in schema.fields
                    if FieldConstraint.REQUIRED not in f.constraints
                ],
            }
        )

    # Check for missing ID fields
    if not patterns["id_fields"]:
        suggestions.append(
            {
                "type": "design",
                "severity": "warning",
                "description": "No ID fields detected. Consider adding unique identifiers.",
                "affected_fields": [],
            }
        )

    # Check for missing timestamp fields
    if not patterns["timestamp_fields"]:
        suggestions.append(
            {
                "type": "design",
                "severity": "info",
                "description": "No timestamp fields detected. Consider adding created_at/updated_at fields.",
                "affected_fields": [],
            }
        )

    # Check for repeated field names (potential normalization opportunity)
    if patterns["repeated_field_names"]:
        suggestions.append(
            {
                "type": "normalization",
                "severity": "info",
                "description": f"Repeated field names detected: {', '.join(patterns['repeated_field_names'])}. Consider normalization.",
                "affected_fields": patterns["repeated_field_names"],
            }
        )

    # Check for excessive use of 'any' type
    any_fields = [f.path for f in schema.fields if str(f.type) == "any"]
    if len(any_fields) > len(schema.fields) * 0.2:  # More than 20% are 'any'
        suggestions.append(
            {
                "type": "type_safety",
                "severity": "warning",
                "description": f"Many fields use 'any' type ({len(any_fields)} fields). Consider more specific types.",
                "affected_fields": any_fields,
            }
        )

    return suggestions  # type: ignore[return-value]


def compare_schema_evolution_advanced(
    old_schema: Schema, new_schema: Schema
) -> Dict[str, Any]:
    """
    Advanced analysis of schema evolution between two versions.

    Combines basic evolution analysis with complexity and pattern changes.
    """
    from .compare import analyze_schema_evolution

    # Get basic evolution analysis
    basic_evolution = analyze_schema_evolution(old_schema, new_schema)

    # Add advanced analysis
    old_complexity = analyze_schema_complexity(old_schema)
    new_complexity = analyze_schema_complexity(new_schema)

    old_patterns = find_schema_patterns(old_schema)
    new_patterns = find_schema_patterns(new_schema)

    advanced_analysis = {
        **basic_evolution,
        "complexity_changes": {
            "field_count_change": new_complexity["total_fields"]
            - old_complexity["total_fields"],
            "nesting_depth_change": new_complexity["max_nesting_depth"]
            - old_complexity["max_nesting_depth"],
            "required_fields_change": new_complexity["required_fields"]
            - old_complexity["required_fields"],
            "type_diversity_change": len(new_complexity["type_distribution"])
            - len(old_complexity["type_distribution"]),
        },
        "pattern_changes": {
            "new_id_fields": set(new_patterns["id_fields"])
            - set(old_patterns["id_fields"]),
            "removed_id_fields": set(old_patterns["id_fields"])
            - set(new_patterns["id_fields"]),
            "new_timestamp_fields": set(new_patterns["timestamp_fields"])
            - set(old_patterns["timestamp_fields"]),
            "removed_timestamp_fields": set(old_patterns["timestamp_fields"])
            - set(new_patterns["timestamp_fields"]),
        },
        "migration_complexity": "low",  # Will be calculated below
    }

    # Calculate migration complexity
    complexity_score = 0
    complexity_score += len(basic_evolution["breaking_changes"]) * 3
    complexity_score += len(basic_evolution["type_changes"]) * 2
    complexity_score += (
        abs(advanced_analysis["complexity_changes"]["nesting_depth_change"]) * 2
    )
    complexity_score += len(basic_evolution["removed_fields"])

    if complexity_score == 0:
        advanced_analysis["migration_complexity"] = "trivial"
    elif complexity_score <= 3:
        advanced_analysis["migration_complexity"] = "low"
    elif complexity_score <= 8:
        advanced_analysis["migration_complexity"] = "medium"
    elif complexity_score <= 15:
        advanced_analysis["migration_complexity"] = "high"
    else:
        advanced_analysis["migration_complexity"] = "very_high"

    return advanced_analysis


def generate_schema_report(schema: Schema) -> str:
    """
    Generate a comprehensive text report about a schema.
    """
    if not isinstance(schema, Schema):
        raise ValueError("Schema must be a unified Schema object")

    complexity = analyze_schema_complexity(schema)
    patterns = find_schema_patterns(schema)
    suggestions = suggest_schema_improvements(schema)

    report = []
    report.append("# Schema Analysis Report")
    report.append(f"**Source**: {schema.source_type or 'Unknown'}")
    report.append("")

    # Basic stats
    report.append("## Basic Statistics")
    report.append(f"- **Total Fields**: {complexity['total_fields']}")
    report.append(f"- **Required Fields**: {complexity['required_fields']}")
    report.append(f"- **Optional Fields**: {complexity['optional_fields']}")
    report.append(f"- **Max Nesting Depth**: {complexity['max_nesting_depth']}")
    report.append(f"- **Average Nesting Depth**: {complexity['avg_nesting_depth']:.2f}")
    report.append("")

    # Type distribution
    report.append("## Type Distribution")
    for type_name, count in sorted(complexity["type_distribution"].items()):
        report.append(f"- **{type_name}**: {count} fields")
    report.append("")

    # Field categorization
    report.append("## Field Categories")
    report.append(f"- **Scalar Fields**: {complexity['scalar_fields']}")
    report.append(f"- **Array Fields**: {complexity['array_fields']}")
    report.append(f"- **Object Fields**: {complexity['object_fields']}")
    report.append(f"- **Union Fields**: {complexity['union_fields']}")
    report.append("")

    # Patterns
    report.append("## Detected Patterns")
    if patterns["id_fields"]:
        report.append(f"- **ID Fields**: {', '.join(patterns['id_fields'])}")
    if patterns["timestamp_fields"]:
        report.append(
            f"- **Timestamp Fields**: {', '.join(patterns['timestamp_fields'])}"
        )
    if patterns["email_fields"]:
        report.append(f"- **Email Fields**: {', '.join(patterns['email_fields'])}")
    if patterns["repeated_field_names"]:
        report.append(
            f"- **Repeated Field Names**: {', '.join(patterns['repeated_field_names'])}"
        )
    report.append("")

    # Suggestions
    if suggestions:
        report.append("## Improvement Suggestions")
        for suggestion in suggestions:
            severity_icon = {"info": "ℹ️", "warning": "⚠️", "error": "❌"}.get(
                suggestion["severity"], "📝"
            )
            report.append(f"### {severity_icon} {suggestion['type'].title()}")
            report.append(suggestion["description"])
            if suggestion["affected_fields"]:
                report.append(
                    f"**Affected fields**: {', '.join(suggestion['affected_fields'][:5])}"
                )
                if len(suggestion["affected_fields"]) > 5:
                    report.append(
                        f"... and {len(suggestion['affected_fields']) - 5} more"
                    )
            report.append("")

    return "\n".join(report)


__all__ = [
    "analyze_schema_complexity",
    "find_schema_patterns",
    "suggest_schema_improvements",
    "compare_schema_evolution_advanced",
    "generate_schema_report",
]
