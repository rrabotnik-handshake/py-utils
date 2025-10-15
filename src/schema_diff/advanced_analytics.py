#!/usr/bin/env python3
"""Advanced schema analytics using the unified Schema format.

This module provides sophisticated analysis capabilities that leverage the rich type
system of the unified Schema objects.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List

from .models import FieldConstraint, Schema


# Helper functions for JSON-safe returns and robust pattern detection
def _to_plain(obj: Any) -> Any:
    """Recursively convert defaultdicts/sets to JSON-friendly plain types."""
    if isinstance(obj, defaultdict):
        return {k: _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, dict):
        return {k: _to_plain(v) for k, v in obj.items()}
    if isinstance(obj, set):
        return sorted(_to_plain(x) for x in obj)
    if isinstance(obj, list):
        return [_to_plain(x) for x in obj]
    return obj


_EMAIL_RE = re.compile(r"\b(e[-_ ]?mail)\b", re.IGNORECASE)


def _is_array_of_objects(field) -> bool:
    """Prefer structural type info if available; fallback to string repr."""
    t = getattr(field, "type", None)
    is_arr = getattr(t, "is_array", False)
    elem = getattr(t, "items", None) or getattr(t, "element", None)
    is_obj = getattr(elem, "is_object", False) or (
        "{" in str(elem) if elem is not None else False
    )
    if is_arr:
        return bool(is_obj)
    s = str(t)
    return (
        s.startswith("[")
        and s not in ["[str]", "[int]", "[float]", "[bool]"]
        and "{" in s
    )


def analyze_schema_complexity(schema: Schema) -> Dict[str, Any]:  # type: ignore[misc]
    """Analyze the complexity of a schema.

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

    # For BigQuery schemas, use raw schema for accurate nesting depth
    if "raw_bq_schema" in schema.metadata:
        from google.cloud.bigquery import SchemaField

        raw_bq_schema = schema.metadata["raw_bq_schema"]

        def calc_bq_nesting_depth(
            field: SchemaField, current_depth: int = 1
        ) -> tuple[int, List[tuple[str, int]]]:
            """Calculate nesting depth for BigQuery field recursively."""
            max_depth = current_depth
            depth_list = [(field.name, current_depth)]

            if field.field_type == "RECORD":
                for sub_field in field.fields:
                    sub_max, sub_list = calc_bq_nesting_depth(
                        sub_field, current_depth + 1
                    )
                    max_depth = max(max_depth, sub_max)
                    depth_list.extend(
                        [(f"{field.name}.{name}", depth) for name, depth in sub_list]
                    )

            return max_depth, depth_list

        all_depths = []
        for field in raw_bq_schema:
            max_depth, depth_list = calc_bq_nesting_depth(field)
            all_depths.extend(depth_list)
            analysis["max_nesting_depth"] = max(analysis["max_nesting_depth"], max_depth)  # type: ignore[call-overload]

        if all_depths:
            total_depth = sum(depth for _, depth in all_depths)
            analysis["avg_nesting_depth"] = total_depth / len(all_depths)  # type: ignore[operator]
            # keep the deepest 100 paths (most actionable)
            for field_name, depth in sorted(
                all_depths, key=lambda x: x[1], reverse=True
            )[:100]:
                analysis["field_paths_by_depth"][depth].append(field_name)  # type: ignore[index]
    else:
        # Fallback to unified schema field paths
        for field in schema.fields:
            depth = field.path.count(".") + 1
            analysis["max_nesting_depth"] = max(analysis["max_nesting_depth"], depth)  # type: ignore[call-overload]
            total_depth += depth
            analysis["field_paths_by_depth"][depth].append(field.path)  # type: ignore[index]

        if analysis["total_fields"] > 0:  # type: ignore[operator]
            analysis["avg_nesting_depth"] = total_depth / analysis["total_fields"]  # type: ignore[operator]

    # Analyze field constraints and types from unified schema
    for field in schema.fields:
        # Count constraints
        if FieldConstraint.REQUIRED in field.constraints:
            analysis["required_fields"] += 1  # type: ignore[operator]
        else:
            analysis["optional_fields"] += 1  # type: ignore[operator]

        for constraint in field.constraints:
            analysis["constraint_distribution"][constraint.value] += 1  # type: ignore[index]

        # Analyze field type
        field_type_str = str(field.type)
        analysis["type_distribution"][field_type_str] += 1  # type: ignore[index]

        # Categorize field types
        if field_type_str.startswith("[") and field_type_str.endswith("]"):
            # Check if it's an array of objects/structs (contains {}) or scalars
            if "{" in field_type_str:
                analysis["array_fields"] += 1  # type: ignore[operator]
                analysis["object_fields"] += 1  # type: ignore[operator]  # Also count the struct inside
            else:
                analysis["array_fields"] += 1  # type: ignore[operator]
        elif "union(" in field_type_str:
            analysis["union_fields"] += 1  # type: ignore[operator]
        elif field_type_str in ["object", "any"] or "{" in field_type_str:
            analysis["object_fields"] += 1  # type: ignore[operator]
        else:
            analysis["scalar_fields"] += 1  # type: ignore[operator]

    # JSON-friendly result
    return _to_plain(analysis)  # type: ignore[no-any-return]


def find_schema_patterns(schema: Schema) -> Dict[str, List[str]]:
    """Find common patterns in schema structure.

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
        "fields_with_policy_tags": [],
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
        if _EMAIL_RE.search(field.name):
            patterns["email_fields"].append(field.path)

        # Nested objects
        if "." in field.path and field_type_str not in ["int", "str", "bool", "float"]:
            patterns["nested_objects"].append(field.path)

        # Array of objects
        if _is_array_of_objects(field):
            patterns["array_of_objects"].append(field.path)

    # Check for BigQuery-specific patterns (policy tags, etc.)
    if schema.source_type == "bigquery" and schema.metadata.get("raw_bq_schema"):
        from google.cloud.bigquery import SchemaField

        raw_schema = schema.metadata["raw_bq_schema"]

        def check_policy_tags(field: SchemaField, path: str = "") -> None:
            """Recursively check for policy tags in BigQuery schema."""
            field_path = f"{path}.{field.name}" if path else field.name

            # Check if field has policy tags
            pt = getattr(field, "policy_tags", None)
            if pt:
                names = getattr(pt, "names", None)
                if names and len(names) > 0:
                    patterns["fields_with_policy_tags"].append(field_path)

            # Recurse into RECORD fields
            if field.field_type == "RECORD":
                for sub_field in field.fields:
                    check_policy_tags(sub_field, field_path)

        for field in raw_schema:
            check_policy_tags(field)

    # tidy: de-dupe and sort for stable output
    for k in patterns:
        patterns[k] = sorted(set(patterns[k]))
    return patterns


def analyze_policy_tags(schema: Schema) -> Dict[str, Any]:  # type: ignore[misc]
    """Analyze policy tags coverage and patterns for BigQuery schemas.

    Returns information about:
    - Fields with policy tags
    - Fields without policy tags that likely need them (PII detection)
    - Policy tag distribution by category
    - Compliance coverage metrics
    """
    if not isinstance(schema, Schema):
        raise ValueError("Schema must be a unified Schema object")

    analysis: dict[str, Any] = {
        "total_fields": len(schema.fields),
        "fields_with_tags": [],
        "fields_without_tags_pii": [],
        "policy_tag_distribution": defaultdict(int),
        "coverage_percent": 0.0,
        "pii_fields_untagged": [],
        "sensitive_fields_untagged": [],
        "tag_details": {},  # field_path -> list of tag names
    }

    # Only analyze if BigQuery schema with raw schema available
    if schema.source_type != "bigquery" or not schema.metadata.get("raw_bq_schema"):
        result: dict[str, Any] = _to_plain(analysis)  # type: ignore[assignment]
        return result

    from google.cloud.bigquery import SchemaField

    # Import canonical catalogs and helpers from bigquery_ddl
    from schema_diff.bigquery_ddl import (
        SENSITIVE_SECRETS_EXACT,
        _classify_pii_by_name,
        _policy_tag_names_on_field,
        _tokenize_name,
    )

    raw_schema = schema.metadata["raw_bq_schema"]

    def analyze_field(field: SchemaField, path: str = "") -> None:
        """Recursively analyze fields for policy tags."""
        field_path = f"{path}.{field.name}" if path else field.name

        # Check if field has policy tags (use canonical helper)
        tag_names = _policy_tag_names_on_field(field)
        has_tags = bool(tag_names)

        if has_tags:
            analysis["fields_with_tags"].append(field_path)

            # Store tag details for this field
            analysis["tag_details"][field_path] = tag_names

            # Categorize policy tags
            for tag_str in tag_names:
                # Extract category from tag path (e.g., .../pii/..., .../sensitive/...)
                if "/pii/" in tag_str.lower():
                    analysis["policy_tag_distribution"]["PII"] += 1
                elif "/sensitive/" in tag_str.lower():
                    analysis["policy_tag_distribution"]["Sensitive"] += 1
                elif "/confidential/" in tag_str.lower():
                    analysis["policy_tag_distribution"]["Confidential"] += 1
                else:
                    analysis["policy_tag_distribution"]["Other"] += 1

        # Check if field likely needs policy tags but doesn't have them
        if not has_tags:
            # Use canonical PII classifier
            pii_hits = _classify_pii_by_name(field.name)
            if pii_hits:
                # Field name suggests PII
                # Skip obvious reference IDs (ending with _id)
                name_lower = field.name.lower()
                if name_lower.endswith("_id") or name_lower == "id":
                    # Exception: standalone government IDs are actual sensitive data
                    gov_id_indicators = {
                        "national_id",
                        "tax_id",
                        "citizen_id",
                        "resident_id",
                    }
                    is_gov_id = any(
                        cat == "gov_id"
                        and any(ind in gov_id_indicators for ind in indicators)
                        for cat, indicators in pii_hits.items()
                    )
                    if is_gov_id and name_lower in gov_id_indicators:
                        analysis["pii_fields_untagged"].append(field_path)
                else:
                    # Not a reference ID, flag as PII
                    analysis["pii_fields_untagged"].append(field_path)

            # Check for sensitive indicators (credentials/secrets) using canonical catalog
            name_lower = field.name.lower()
            if not name_lower.endswith(("_id", "_key", "_ref", "id", "key")):
                name_tokens = set(_tokenize_name(field.name))
                # Exact match check
                if any(tok in SENSITIVE_SECRETS_EXACT for tok in name_tokens):
                    analysis["sensitive_fields_untagged"].append(field_path)
                # Suffix check for variants
                elif any(
                    name_lower.endswith(sfx)
                    for sfx in ("_token", "_secret", "_private_key")
                ):
                    analysis["sensitive_fields_untagged"].append(field_path)

        # Recurse into RECORD fields
        if field.field_type == "RECORD":
            for sub_field in field.fields:
                analyze_field(sub_field, field_path)

    # Analyze all fields
    for field in raw_schema:
        analyze_field(field)

    # Calculate coverage (as percentage of all fields that have tags)
    if analysis["total_fields"] > 0:
        analysis["coverage_percent"] = round(
            (len(analysis["fields_with_tags"]) / analysis["total_fields"]) * 100, 1
        )

    final_result: dict[str, Any] = _to_plain(analysis)  # type: ignore[assignment]
    return final_result


def suggest_schema_improvements(schema: Schema) -> List[Dict[str, str]]:
    """Suggest improvements for schema design.

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

    # Check policy tags coverage for BigQuery schemas
    if schema.source_type == "bigquery" and schema.metadata.get("raw_bq_schema"):
        policy_analysis = analyze_policy_tags(schema)

        # Suggest policy tags for untagged PII fields
        if policy_analysis["pii_fields_untagged"]:
            suggestions.append(
                {
                    "type": "security",
                    "severity": "warning",
                    "description": f"Candidate PII fields without policy tags detected ({len(policy_analysis['pii_fields_untagged'])} fields). Add policy tags for data governance and compliance (GDPR, HIPAA, etc.).",
                    "affected_fields": policy_analysis["pii_fields_untagged"],
                }
            )

        # Suggest policy tags for untagged sensitive fields
        if policy_analysis["sensitive_fields_untagged"]:
            suggestions.append(
                {
                    "type": "security",
                    "severity": "error",
                    "description": f"Sensitive credential fields without policy tags detected ({len(policy_analysis['sensitive_fields_untagged'])} fields). Add policy tags immediately to restrict access.",
                    "affected_fields": policy_analysis["sensitive_fields_untagged"],
                }
            )

        # Positive feedback for good policy tag coverage
        if (
            policy_analysis["coverage_percent"] >= 80
            and policy_analysis["fields_with_tags"]
        ):
            suggestions.append(
                {
                    "type": "security",
                    "severity": "info",
                    "description": f"Good policy tag coverage ({policy_analysis['coverage_percent']}%). {len(policy_analysis['fields_with_tags'])} fields are properly tagged for data governance.",
                    "affected_fields": [],
                }
            )
        elif policy_analysis["coverage_percent"] < 50 and (
            policy_analysis["pii_fields_untagged"]
            or policy_analysis["sensitive_fields_untagged"]
        ):
            suggestions.append(
                {
                    "type": "security",
                    "severity": "warning",
                    "description": f"Low policy tag coverage ({policy_analysis['coverage_percent']}%). Consider implementing a data classification strategy and tagging sensitive fields.",
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

    # Check for BigQuery-specific anti-patterns
    # This requires analyzing the raw BigQuery schema before it's flattened
    if schema.source_type == "bigquery" and schema.metadata.get("raw_bq_schema"):
        try:
            from .bigquery_ddl import detect_bigquery_antipatterns

            raw_schema = schema.metadata["raw_bq_schema"]
            antipatterns = detect_bigquery_antipatterns(raw_schema)

            if antipatterns:
                # Group by category and pattern
                pattern_groups: dict[str, list[dict[str, Any]]] = {}
                for ap in antipatterns:
                    pattern = ap["pattern"]
                    if pattern not in pattern_groups:
                        pattern_groups[pattern] = []
                    pattern_groups[pattern].append(ap)

                # STRUCT wrapper issues - consolidate both patterns into one suggestion
                struct_wrapper_issues = pattern_groups.get(
                    "unnecessary_struct_wrapper", []
                )
                element_wrapper_issues = pattern_groups.get(
                    "unnecessary_element_wrapper", []
                )

                if struct_wrapper_issues or element_wrapper_issues:
                    # The full pattern is STRUCT<list ARRAY<STRUCT<element>>>
                    # The element_wrapper is nested within struct_wrapper, so only show struct_wrapper
                    # to avoid duplication
                    if struct_wrapper_issues:
                        affected = [ap["field_name"] for ap in struct_wrapper_issues]
                        suggestions.append(
                            {
                                "type": "schema_design",
                                "severity": "warning",
                                "description": f"Unnecessary STRUCT wrappers for arrays ({len(affected)} fields). Use ARRAY<STRUCT<...>> directly instead of the nested wrapper pattern. Simplifies queries and removes unnecessary nesting levels.",
                                "affected_fields": affected,
                            }
                        )

                # Boolean as INTEGER
                if "boolean_as_integer" in pattern_groups:
                    issues = pattern_groups["boolean_as_integer"]
                    affected = [ap["field_name"] for ap in issues]
                    suggestions.append(
                        {
                            "type": "type_optimization",
                            "severity": "info",
                            "description": f"Boolean fields stored as INTEGER ({len(issues)} fields). Use BOOLEAN type for is_*/has_*/can_* fields. Saves space and improves semantics.",
                            "affected_fields": affected,
                        }
                    )

                # Deep nesting
                if "deep_nesting" in pattern_groups:
                    issues = pattern_groups["deep_nesting"]
                    affected = [ap["field_name"] for ap in issues]
                    max_depth = max(ap.get("depth", 0) for ap in issues)
                    suggestions.append(
                        {
                            "type": "complexity",
                            "severity": "warning",
                            "description": f"Deeply nested fields ({len(issues)} fields, max depth: {max_depth}). Consider flattening for better query performance and readability.",
                            "affected_fields": affected,
                        }
                    )

                # Overall deep nesting
                if "overall_deep_nesting" in pattern_groups:
                    issue = pattern_groups["overall_deep_nesting"][0]
                    suggestions.append(
                        {
                            "type": "complexity",
                            "severity": "warning",
                            "description": issue["suggestion"],
                            "affected_fields": [],
                        }
                    )

                # Missing array ordering
                if "missing_array_ordering" in pattern_groups:
                    issues = pattern_groups["missing_array_ordering"]
                    affected = [ap["field_name"] for ap in issues]
                    suggestions.append(
                        {
                            "type": "data_quality",
                            "severity": "info",
                            "description": f"REPEATED fields without ordering ({len(issues)} fields). Add order_in_profile/sequence field to ensure consistent array order across queries.",
                            "affected_fields": affected,
                        }
                    )

                # Generic field names
                if "generic_field_name" in pattern_groups:
                    issues = pattern_groups["generic_field_name"]
                    affected = [ap["field_name"] for ap in issues]
                    suggestions.append(
                        {
                            "type": "naming",
                            "severity": "info",
                            "description": f"Generic field names detected ({len(issues)} fields). Use descriptive names instead of 'data', 'value', 'info', etc.",
                            "affected_fields": affected,
                        }
                    )

                # Missing descriptions
                if "missing_description" in pattern_groups:
                    issues = pattern_groups["missing_description"]
                    affected = [ap["field_name"] for ap in issues]
                    suggestions.append(
                        {
                            "type": "documentation",
                            "severity": "info",
                            "description": f"Complex fields without descriptions ({len(issues)} fields). Add field descriptions to improve schema documentation.",
                            "affected_fields": affected,
                        }
                    )

                # Inconsistent naming
                if "inconsistent_naming" in pattern_groups:
                    issue = pattern_groups["inconsistent_naming"][0]
                    suggestions.append(
                        {
                            "type": "naming",
                            "severity": "info",
                            "description": issue["suggestion"],
                            "affected_fields": [],
                        }
                    )

                # Wide tables
                if "wide_table" in pattern_groups:
                    issue = pattern_groups["wide_table"][0]
                    suggestions.append(
                        {
                            "type": "complexity",
                            "severity": issue["severity"],
                            "description": issue["suggestion"],
                            "affected_fields": [],
                        }
                    )

                # Inconsistent timestamps
                if "inconsistent_timestamps" in pattern_groups:
                    issue = pattern_groups["inconsistent_timestamps"][0]
                    string_dates = issue.get("string_dates", [])
                    desc = f"Mix of STRING date fields and TIMESTAMP types. STRING dates: {', '.join(string_dates[:3])}{'...' if len(string_dates) > 3 else ''}. Use TIMESTAMP/DATE for better performance and type safety."
                    suggestions.append(
                        {
                            "type": "type_consistency",
                            "severity": "warning",
                            "description": desc,
                            "affected_fields": string_dates,
                        }
                    )

                # Nullable foreign keys
                if "nullable_foreign_keys" in pattern_groups:
                    issue = pattern_groups["nullable_foreign_keys"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "data_integrity",
                            "severity": "info",
                            "description": f"Foreign key fields are NULL-able ({len(affected)} fields). Make REQUIRED or add explicit null handling to prevent orphaned references.",
                            "affected_fields": affected,
                        }
                    )

                # Redundant structures
                if "redundant_structures" in pattern_groups:
                    issue = pattern_groups["redundant_structures"][0]
                    affected = issue.get("affected_fields", [])
                    field_count = issue.get("field_count", 0)
                    suggestions.append(
                        {
                            "type": "normalization",
                            "severity": "info",
                            "description": f"Duplicate structure ({field_count} fields) found in {len(affected)} places. Consider extracting to a shared type or normalizing into a separate table.",
                            "affected_fields": affected,
                        }
                    )

                # Cryptic names
                if "cryptic_names" in pattern_groups:
                    issue = pattern_groups["cryptic_names"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "naming",
                            "severity": "info",
                            "description": f"Cryptic or abbreviated field names ({len(affected)} fields). Use descriptive names (e.g., 'usr' → 'user', 'cnt' → 'count', avoid Hungarian notation).",
                            "affected_fields": affected,
                        }
                    )

                # Missing audit columns
                if "missing_audit_columns" in pattern_groups:
                    issue = pattern_groups["missing_audit_columns"][0]
                    missing = issue.get("missing_fields", [])
                    suggestions.append(
                        {
                            "type": "data_quality",
                            "severity": "info",
                            "description": f"Missing audit columns: {', '.join(missing)}. Add these timestamp fields for data lineage and debugging.",
                            "affected_fields": [],
                        }
                    )

                # Inconsistent casing
                if "inconsistent_casing" in pattern_groups:
                    issue = pattern_groups["inconsistent_casing"][0]
                    suggestions.append(
                        {
                            "type": "naming",
                            "severity": "info",
                            "description": issue["suggestion"],
                            "affected_fields": [],
                        }
                    )

                # JSON/STRING blobs
                if "json_string_blobs" in pattern_groups:
                    issue = pattern_groups["json_string_blobs"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "schema_design",
                            "severity": "warning",
                            "description": f"STRING fields containing JSON/structured data ({len(affected)} fields). Use RECORD/STRUCT for type safety, better compression, and column-level access.",
                            "affected_fields": affected,
                        }
                    )

                # Nullable ID fields
                if "nullable_id_fields" in pattern_groups:
                    issue = pattern_groups["nullable_id_fields"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "data_integrity",
                            "severity": "warning",
                            "description": f"Primary/identifier fields are NULL-able ({len(affected)} fields). IDs should be REQUIRED to ensure data integrity.",
                            "affected_fields": affected,
                        }
                    )

                # Reserved keywords
                if "reserved_keywords" in pattern_groups:
                    issue = pattern_groups["reserved_keywords"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "naming",
                            "severity": "warning",
                            "description": f"Field names use SQL reserved keywords ({len(affected)} fields). Requires backticks in queries. Rename: `select` → `selection`, `order` → `sort_order`.",
                            "affected_fields": affected,
                        }
                    )

                # Overly granular timestamps
                if "overly_granular_timestamps" in pattern_groups:
                    issue = pattern_groups["overly_granular_timestamps"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "type_optimization",
                            "severity": "info",
                            "description": f"TIMESTAMP fields with date-only semantics ({len(affected)} fields). Use DATE type for birth_date, hire_date, etc. Saves 8 bytes per row.",
                            "affected_fields": affected,
                        }
                    )

                # Expensive unnest
                if "expensive_unnest" in pattern_groups:
                    issue = pattern_groups["expensive_unnest"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "performance",
                            "severity": "info",
                            "description": f"Complex ARRAY<STRUCT> fields ({len(affected)} arrays). UNNEST on wide structs is expensive. Consider denormalizing or reducing struct width.",
                            "affected_fields": affected,
                        }
                    )

                # Negative booleans
                if "negative_booleans" in pattern_groups:
                    issue = pattern_groups["negative_booleans"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "naming",
                            "severity": "info",
                            "description": f"Negative boolean names ({len(affected)} fields). Creates double negatives. Use positive: `is_not_active` → `is_active`.",
                            "affected_fields": affected,
                        }
                    )

                # Overly long names
                if "overly_long_names" in pattern_groups:
                    issue = pattern_groups["overly_long_names"][0]
                    affected = issue.get("affected_fields", [])
                    suggestions.append(
                        {
                            "type": "naming",
                            "severity": "info",
                            "description": f"Overly long field names ({len(affected)} fields). Keep names concise (<50 chars). Use descriptions for details.",
                            "affected_fields": affected,
                        }
                    )

                # Poor field ordering
                if "poor_field_ordering" in pattern_groups:
                    issue = pattern_groups["poor_field_ordering"][0]
                    suggestions.append(
                        {
                            "type": "schema_design",
                            "severity": "info",
                            "description": issue["suggestion"],
                            "affected_fields": [],
                        }
                    )

                # ============================================================================
                # PHASE 1 & 2: Auto-handle all remaining patterns (22 new patterns)
                # ============================================================================
                # Map of patterns to their display types
                remaining_patterns = {
                    "string_type_abuse": "type_optimization",
                    "inconsistent_id_types": "type_consistency",
                    "float_for_money": "type_optimization",
                    "god_table": "schema_design",
                    "array_without_id": "data_quality",
                    "unmarked_pii": "security",
                    "plaintext_secrets": "security",
                    "unstructured_address": "data_quality",
                    "undocumented_enum": "documentation",
                    "plural_singular_confusion": "naming",
                    "type_in_name": "naming",
                    "redundant_prefix": "naming",
                    "denormalization_abuse": "normalization",
                    "eav_antipattern": "schema_design",
                    "string_for_binary": "type_optimization",
                    "missing_soft_delete": "data_quality",
                    "inconsistent_date_granularity": "type_consistency",
                    "mixed_null_representation": "data_quality",
                    "inconsistent_boolean_type": "type_consistency",
                    "nullable_partition_field": "performance",
                    "over_structuring": "schema_design",
                }

                for pattern_name, display_type in remaining_patterns.items():
                    if pattern_name in pattern_groups:
                        issues_list = pattern_groups[pattern_name]
                        for issue in issues_list:
                            suggestions.append(
                                {
                                    "type": display_type,
                                    "severity": issue["severity"],
                                    "description": issue["suggestion"],
                                    "affected_fields": issue.get("affected_fields", []),
                                }
                            )

        except Exception:
            # If detection fails, skip silently
            pass

    # --- post-process suggestions for readability ---
    for s in suggestions:
        if "affected_fields" in s and isinstance(s["affected_fields"], list):
            # de-dupe, keep stable order, and cap to 25
            seen = set()
            dedup = []
            for f in s["affected_fields"]:
                if f not in seen:
                    dedup.append(f)
                    seen.add(f)
            s["affected_fields"] = dedup[:25]
    return suggestions  # type: ignore[return-value]


def compare_schema_evolution_advanced(
    old_schema: Schema, new_schema: Schema
) -> Dict[str, Any]:
    """Advanced analysis of schema evolution between two versions.

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

    # Calculate migration complexity (robust to missing keys)
    complexity_score = 0
    complexity_score += len(basic_evolution.get("breaking_changes", [])) * 3
    complexity_score += len(basic_evolution.get("type_changes", [])) * 2
    complexity_score += (
        abs(advanced_analysis["complexity_changes"]["nesting_depth_change"]) * 2
    )
    complexity_score += len(basic_evolution.get("removed_fields", []))

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
    """Generate a comprehensive text report about a schema."""
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
    for type_name, count in sorted(
        complexity["type_distribution"].items(),
        key=lambda kv: (str(kv[0]), -int(kv[1])),
    ):
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
    "analyze_policy_tags",
]
